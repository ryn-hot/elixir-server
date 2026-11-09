use super::super::tests::{budget, expires, fixture, populate};
use super::*;
use std::time::Duration;
use tokio::io::AsyncReadExt;

struct Fixture {
    _directory: tempfile::TempDir,
    path: PathBuf,
    lane: PathBuf,
    descriptor: Descriptor,
    package: Vec<u8>,
    proof: Vec<u8>,
}
impl Fixture {
    fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let path = root.join("archive");
        let (_, descriptor, package, proof) = fixture()?;
        let store = Store::open(&path, budget(), expires())?;
        populate(&store, &descriptor, &package, &proof)?;
        drop(store);
        Ok(Self {
            _directory: directory,
            path,
            lane: root.join("foreground"),
            descriptor,
            package,
            proof,
        })
    }
    fn expected(&self) -> SourceMatchDescriptor {
        let mut expected = SourceMatchDescriptor {
            schema_version: 1,
            workspace: self.descriptor.workspace.clone(),
            release: self.descriptor.release.clone(),
        };
        // A new native job has its own deadline, not the archive's normalized
        // maximum timeout. It must still replay exactly the same signed slots.
        expected.workspace.timeout_seconds = 120;
        expected
    }
    fn replay(&self) -> Result<Replay> {
        Replay::open(
            &self.path,
            budget(),
            &self.expected(),
            Arc::new(ControllerLease::acquire(&self.lane)?),
            2 * MIB,
            expires(),
        )
    }
    fn package_path(&self) -> PathBuf {
        self.path
            .join("releases")
            .join(self.descriptor.name())
            .join(QuarantinePurpose::Package.file_name())
    }
}

#[tokio::test]
async fn exact_fixed_slots_stream_once_and_keep_both_locks_until_the_last_reader_drops()
-> Result<()> {
    let f = Fixture::new()?;
    let mut replay = f.replay()?;
    assert!(replay.take(QuarantinePurpose::Artifact).is_err());
    let mut package = replay.take(QuarantinePurpose::Package)?;
    assert!(replay.take(QuarantinePurpose::Package).is_err());
    let mut proof = replay.take(QuarantinePurpose::Proof)?;
    drop(replay);
    let mut package_bytes = Vec::new();
    let mut proof_bytes = Vec::new();
    package.read_to_end(&mut package_bytes).await?;
    proof.read_to_end(&mut proof_bytes).await?;
    assert_eq!(package_bytes, f.package);
    assert_eq!(proof_bytes, f.proof);
    assert!(package.done && proof.done);
    assert!(ControllerLease::acquire(&f.lane).is_err());
    assert!(ControllerLease::acquire(&f.path).is_err());
    drop(package);
    assert!(ControllerLease::acquire(&f.lane).is_err());
    drop(proof);
    drop(ControllerLease::acquire(&f.lane)?);
    drop(ControllerLease::acquire(&f.path)?);
    Ok(())
}

#[tokio::test]
async fn changed_truncated_and_appended_bytes_never_get_a_successful_eof() -> Result<()> {
    for fault in ["content", "short", "long"] {
        let f = Fixture::new()?;
        let mut replay = f.replay()?;
        let mut reader = replay.take(QuarantinePurpose::Package)?;
        let path = f.package_path();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        let mut changed = f.package.clone();
        match fault {
            "content" => changed[0] ^= 1,
            "short" => {
                changed.pop();
            }
            "long" => changed.push(1),
            _ => unreachable!(),
        }
        fs::write(&path, changed)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400))?;
        let mut output = Vec::new();
        assert!(reader.read_to_end(&mut output).await.is_err(), "{fault}");
        assert!(!reader.done, "{fault}");
        assert!(
            reader.read_u8().await.is_err(),
            "failed streams stay poisoned"
        );
        reader.drain().await?;
        assert!(output.len() <= f.package.len());
    }
    Ok(())
}

#[test]
fn insufficient_allowance_rejects_before_creating_an_archive_and_other_plans_do_not_replay()
-> Result<()> {
    let f = Fixture::new()?;
    let missing = f.path.parent().unwrap().join("missing-archive");
    assert!(
        Replay::open(
            &missing,
            budget(),
            &f.expected(),
            Arc::new(ControllerLease::acquire(&f.lane)?),
            1,
            expires(),
        )
        .is_err()
    );
    assert!(!missing.exists());
    let mut expected = f.expected();
    expected.release.version = "2.0.0".into();
    expected.workspace.release_id = Digest::of(canonical::to_vec(&expected.release)?);
    expected.validate()?;
    // An otherwise valid different release cannot replay these unchanged blobs.
    assert!(
        Replay::open(
            &f.path,
            budget(),
            &expected,
            Arc::new(ControllerLease::acquire(&f.lane)?),
            2 * MIB,
            expires(),
        )
        .is_err()
    );
    drop(ControllerLease::acquire(&f.lane)?);
    Ok(())
}

#[tokio::test]
async fn zero_length_reads_do_not_start_io_and_expiry_never_grants_eof() -> Result<()> {
    let f = Fixture::new()?;
    let mut replay = f.replay()?;
    let mut reader = replay.take(QuarantinePurpose::Package)?;
    assert_eq!(reader.read(&mut []).await?, 0);
    assert!(reader.pending.is_none());
    reader.expires = Instant::now();
    assert!(reader.read_u8().await.is_err());
    assert!(!reader.done);
    assert!(reader.pending.is_none());
    Ok(())
}

#[tokio::test]
async fn cancelled_read_drain_keeps_the_same_io_and_both_os_leases_until_it_joins() -> Result<()> {
    let f = Fixture::new()?;
    let mut replay = f.replay()?;
    let mut reader = replay.take(QuarantinePurpose::Package)?;
    let mut file = reader.file.take().unwrap();
    let root = reader.root.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let (finish, finishing) = std::sync::mpsc::sync_channel(1);
    reader.pending = Some(tokio::task::spawn_blocking(move || {
        let _held = root;
        let _ = ready.send(());
        finishing.recv().unwrap();
        let mut bytes = vec![0; 2];
        let result = file.read_exact(&mut bytes).map(|()| bytes);
        (file, result)
    }));
    started.await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), reader.drain())
            .await
            .is_err()
    );
    assert!(reader.pending.is_some());
    drop(replay);
    assert!(ControllerLease::acquire(&f.path).is_err());
    assert!(ControllerLease::acquire(&f.lane).is_err());
    finish.send(())?;
    reader.drain().await?;
    assert!(reader.pending.is_none());
    let mut bytes = [0; 2];
    reader.read_exact(&mut bytes).await?;
    assert_eq!(bytes, f.package[..2]);
    assert!(
        !reader.done,
        "joining a read is not a content verification receipt"
    );
    drop(reader);
    drop(ControllerLease::acquire(&f.path)?);
    drop(ControllerLease::acquire(&f.lane)?);
    Ok(())
}

#[tokio::test]
async fn dropping_a_reader_cannot_release_the_lane_while_its_blocking_read_is_live() -> Result<()> {
    let f = Fixture::new()?;
    let mut replay = f.replay()?;
    let mut reader = replay.take(QuarantinePurpose::Package)?;
    let file = reader.file.take().unwrap();
    let root = reader.root.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let (finish, finishing) = std::sync::mpsc::sync_channel(1);
    let (done, completed) = tokio::sync::oneshot::channel();
    reader.pending = Some(tokio::task::spawn_blocking(move || {
        let _ = ready.send(());
        finishing.recv().unwrap();
        drop(root);
        let _ = done.send(());
        (file, Ok(Vec::new()))
    }));
    started.await?;
    drop(reader);
    drop(replay);
    assert!(ControllerLease::acquire(&f.path).is_err());
    assert!(ControllerLease::acquire(&f.lane).is_err());
    finish.send(())?;
    completed.await?;
    drop(ControllerLease::acquire(&f.path)?);
    drop(ControllerLease::acquire(&f.lane)?);
    Ok(())
}
