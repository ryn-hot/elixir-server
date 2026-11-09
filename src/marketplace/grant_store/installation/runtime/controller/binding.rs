//! Constant-size DB/filesystem restart lineage. This never proves native
//! completion: actual global-fence reconciliation and original joins still do.
use super::*;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt},
    path::Path,
};

const MARKER: &str = "owner.binding";
const LENGTH: usize = 37;

struct Marker {
    file: File,
    nonce: Uuid,
    ready: bool,
}
impl Marker {
    fn open(directory: &Path, initialize: bool) -> Result<Self> {
        let path = directory.join(MARKER);
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .mode(0o600);
        let file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if initialize && error.kind() == std::io::ErrorKind::NotFound => {
                let mut file = options.create_new(true).open(&path)?;
                let nonce = Uuid::new_v4();
                file.write_all(format!("P{nonce}").as_bytes())?;
                file.sync_all()?;
                File::open(directory)?.sync_all()?;
                file
            }
            Err(error) => {
                return Err(error).context("runtime owner marker is missing; no implicit repair");
            }
        };
        let metadata = file.metadata()?;
        valid_file(&metadata)?;
        if metadata.len() != LENGTH as u64 {
            bail!("runtime owner marker is corrupt");
        }
        let mut bytes = [0u8; LENGTH];
        file.read_exact_at(&mut bytes, 0)?;
        let ready = match bytes[0] {
            b'P' => false,
            b'R' => true,
            _ => bail!("runtime owner marker phase is corrupt"),
        };
        let nonce = std::str::from_utf8(&bytes[1..])?.parse::<Uuid>()?;
        if nonce.is_nil() || nonce.to_string().as_bytes() != &bytes[1..] {
            bail!("invalid runtime owner nonce");
        }
        let actual = std::fs::symlink_metadata(&path)?;
        if actual.dev() != metadata.dev() || actual.ino() != metadata.ino() || !actual.is_file() {
            bail!("runtime owner marker was replaced");
        }
        Ok(Self { file, nonce, ready })
    }
    fn ready(&mut self) -> Result<()> {
        self.file.write_all_at(b"R", 0)?;
        self.file.sync_all()?;
        self.ready = true;
        Ok(())
    }
}
fn valid_file(metadata: &std::fs::Metadata) -> Result<()> {
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        bail!("runtime owner file is not private and unshared");
    }
    Ok(())
}
fn identity(lane: &Path, nonce: Uuid) -> Result<Digest> {
    let paths = [
        lane.to_owned(),
        lane.join("controller.lock"),
        lane.join(OWNER_DIRECTORY),
        lane.join(OWNER_DIRECTORY).join("controller.lock"),
        lane.join(OWNER_DIRECTORY).join(MARKER),
    ];
    let mut identities = Vec::with_capacity(paths.len());
    for (index, path) in paths.iter().enumerate() {
        let metadata = std::fs::symlink_metadata(path)
            .context("runtime owner history is missing; no implicit repair")?;
        if index == 0 || index == 2 {
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.mode() & 0o077 != 0
            {
                bail!("runtime owner directory is not canonical private storage");
            }
        } else {
            valid_file(&metadata)?;
        }
        identities.push(format!("{:016x}:{:016x}", metadata.dev(), metadata.ino()));
    }
    Ok(Digest::of(canonical::to_vec(&serde_json::json!({
        "domain":"elixir-marketplace/installed-runtime-owner/v1",
        "lane":lane.to_str().context("runtime owner path is not UTF-8")?,"nonce":nonce,"identities":identities,
    }))?))
}

#[derive(Clone)]
pub(super) struct Binding {
    lane: PathBuf,
    nonce: Uuid,
    digest: Digest,
}
impl Binding {
    pub(super) async fn require_database(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()> {
        let query = "SELECT SUBSTR(nonce,1,37),SUBSTR(binding_sha256,1,72) FROM marketplace_installed_runtime_owner WHERE singleton=1";
        let query = if driver == DatabaseDriver::Postgres {
            format!("{query} FOR SHARE")
        } else {
            query.into()
        };
        let row: Option<(String, String)> =
            sqlx::query_as(&query).fetch_optional(&mut **tx).await?;
        if row != Some((self.nonce.to_string(), self.digest.to_string())) {
            bail!("runtime owner database lineage is missing or changed");
        }
        Ok(())
    }
    pub(super) fn require(&self, lease: &ControllerLease) -> Result<()> {
        self.require_matching(lease, true)
    }
    fn require_matching(&self, lease: &ControllerLease, ready: bool) -> Result<()> {
        if !lease.matches_directory(&self.lane.join(OWNER_DIRECTORY)) {
            bail!("runtime registry lock was replaced");
        }
        let marker = Marker::open(lease.directory(), false)?;
        if (ready && !marker.ready)
            || marker.nonce != self.nonce
            || identity(&self.lane, marker.nonce)? != self.digest
        {
            bail!("runtime owner lineage differs from its durable binding");
        }
        Ok(())
    }
}

pub(super) async fn open(
    database: &Database,
    configuration: &InstalledRuntime,
) -> Result<(Arc<ControllerLease>, Binding)> {
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    // Same installation writer position as staging. It cannot enroll its
    // first source between our empty-history check and lineage commit.
    let ledger:Option<(i64,i64)>=sqlx::query_as("UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1 RETURNING entries,encoded_bytes")
        .fetch_optional(&mut *tx).await?;
    let ledger = ledger.context("runtime installation ledger missing; no implicit repair")?;
    let stored:Option<(String,String)>=sqlx::query_as("SELECT SUBSTR(nonce,1,37),SUBSTR(binding_sha256,1,72) FROM marketplace_installed_runtime_owner WHERE singleton=1")
        .fetch_optional(&mut *tx).await?;
    let expected = stored
        .map(|(nonce, digest)| -> Result<(Uuid, Digest)> { Ok((nonce.parse()?, digest.parse()?)) })
        .transpose()?;
    if let Some((nonce, digest)) = expected {
        // Check BEFORE acquire's create-on-missing behavior can replace files.
        if identity(&configuration.lane, nonce)? != digest {
            bail!("runtime owner storage was replaced; reconciliation required");
        }
    } else {
        let installed:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_installations)+(SELECT COUNT(*) FROM marketplace_installation_metadata)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state WHERE purpose='installed')+(SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE purpose='installed') AS BIGINT)")
            .fetch_one(&mut *tx).await?;
        if installed != 0 || ledger != (0, 0) {
            bail!("installed history has no runtime lineage; no implicit adoption");
        }
    }
    let lane = ControllerLease::acquire_for_recovery(&configuration.lane)?;
    let lease = ControllerLease::acquire_for_recovery(&configuration.lane.join(OWNER_DIRECTORY))?;
    let mut marker = Marker::open(lease.directory(), expected.is_none())?;
    let digest = identity(&configuration.lane, marker.nonce)?;
    if let Some((nonce, bound)) = expected {
        if marker.nonce != nonce || digest != bound {
            bail!("runtime owner marker differs from the database binding");
        }
    } else if marker.ready {
        bail!("ready runtime owner lost its database binding; no implicit repair");
    }
    let binding = Binding {
        lane: configuration.lane.clone(),
        nonce: marker.nonce,
        digest,
    };
    binding.require_matching(&lease, false)?;
    // Persist BOTH new lock entries and their directory ancestry before the
    // database can commit a binding to their identities. File fsync alone
    // cannot make a newly created parent-directory entry durable.
    File::open(lease.directory())?.sync_all()?;
    File::open(lane.directory())?.sync_all()?;
    File::open(
        lane.directory()
            .parent()
            .context("runtime lane has no parent")?,
    )?
    .sync_all()?;
    if expected.is_none() {
        sqlx::query("INSERT INTO marketplace_installed_runtime_owner(singleton,nonce,binding_sha256) VALUES(1,$1,$2)")
            .bind(marker.nonce.to_string()).bind(digest.to_string()).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    drop(connection);
    // Native recovery may wait on the daemon. Retain both OS owners, but no
    // primary-DB transaction or checked-out connection across that wait.
    SourceQuarantineJob::recover_leased(configuration.runtime.as_ref(), &lane).await?;
    crate::marketplace::lease::activity::gateway::GatewayFence::recover_warm(
        configuration.runtime.as_ref(),
        &lease,
    )
    .await?;
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut confirm = connection.begin().await?;
    synchronous(&mut confirm, database.driver).await?;
    binding
        .require_database(&mut confirm, database.driver)
        .await?;
    binding.require_matching(&lease, false)?;
    // No installed begin is possible until BOTH durable records agree and
    // readiness has been fsynced. A matching pending marker can finish this
    // interrupted initialization; READY with a missing DB row never can.
    marker.ready()?;
    binding.require(&lease)?;
    confirm.commit().await?;
    drop(lane);
    Ok((Arc::new(lease), binding))
}
