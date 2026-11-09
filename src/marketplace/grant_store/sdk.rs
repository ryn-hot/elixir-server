//! Private local SDK effects, not execution/install/activation authority. Only
//! an independently authorized controller may call the parent entry point.
//! No network, fallback scope, caller-selected instance or plaintext logging.
use super::*;
use elixir_marketplace::source_execution::{RuntimeJson, SourceSdkReply, SourceSdkRequest};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::any::AnyRow;
use std::borrow::Cow;

pub(super) mod generation_secrets;
#[cfg(unix)]
pub(super) mod local_http;
pub(super) mod namespaces;
pub(super) mod snapshot;

const MAX_REVISION: i64 = 9_007_199_254_740_991;
const ENTRY_BYTES: i64 = 96 * 1024;
const ROOT_BYTES: i64 = 8192;
const ROW_BYTES: i64 = 8192;

pub(super) enum RowLock {
    Shared,
    Exclusive,
}
pub(super) fn locked(
    query: &'static str,
    driver: DatabaseDriver,
    mode: RowLock,
) -> Cow<'static, str> {
    match driver {
        DatabaseDriver::Postgres => Cow::Owned(format!(
            "{query} FOR {}",
            match mode {
                RowLock::Shared => "SHARE",
                RowLock::Exclusive => "UPDATE",
            }
        )),
        // require_home_binding has already acquired SQLite's writer position,
        // so authority cannot change before this transaction commits.
        DatabaseDriver::Sqlite => Cow::Borrowed(query),
    }
}

async fn lock_grant(
    tx: &mut Transaction<'_, Any>,
    grant: &CommittedOwnerGrant,
    requested: &GrantReview,
    driver: DatabaseDriver,
) -> Result<()> {
    lock_grant_mode(tx, grant, requested, driver, RowLock::Shared).await
}
async fn lock_grant_mode(
    tx: &mut Transaction<'_, Any>,
    grant: &CommittedOwnerGrant,
    requested: &GrantReview,
    driver: DatabaseDriver,
    mode: RowLock,
) -> Result<()> {
    super::super::preferences::require_home_binding(tx, grant.home_id, driver).await?;
    let query = locked(
        "SELECT extension_id,home_id,revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1 AND home_id=$2 AND revision=$3 AND enabled=TRUE",
        driver,
        mode,
    );
    let row = sqlx::query(&query)
        .bind(grant.extension_id.as_str())
        .bind(grant.home_id.to_string())
        .bind(grant.revision)
        .fetch_optional(&mut **tx)
        .await?
        .context("SDK grant is no longer current")?;
    let current = StoredGrant::parse(row)?
        .active()
        .context("SDK grant is disabled")?;
    if !current.covers(requested)? || requested.scope().runtime().is_none() {
        bail!("SDK request is outside the approved runtime scope");
    }
    Ok(())
}
async fn require_foreground_extension(
    tx: &mut Transaction<'_, Any>,
    grant: &CommittedOwnerGrant,
) -> Result<()> {
    let enrolled:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM marketplace_installations WHERE extension_id=$1) OR EXISTS(SELECT 1 FROM marketplace_installation_metadata WHERE extension_id=$1) OR EXISTS(SELECT 1 FROM marketplace_sdk_namespaces WHERE extension_id=$1 AND purpose<>'foreground') OR EXISTS(SELECT 1 FROM marketplace_instance_sdk_state WHERE extension_id=$1 AND (purpose IS NULL OR purpose<>'foreground')) THEN 1 ELSE 0 END AS BIGINT)")
        .bind(grant.extension_id.as_str()).fetch_one(&mut **tx).await?;
    if enrolled != 0 {
        bail!("enrolled SDK namespaces require the installed-generation controller");
    }
    Ok(())
}
async fn require_foreground_instance(
    tx: &mut Transaction<'_, Any>,
    grant: &CommittedOwnerGrant,
    instance: Uuid,
) -> Result<()> {
    require_foreground_extension(tx, grant).await?;
    namespaces::require_identity(
        tx,
        grant.home_id,
        &grant.extension_id,
        instance,
        namespaces::Purpose::Foreground,
    )
    .await?;
    let valid:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM marketplace_instance_sdk_state WHERE instance_id=$1 AND extension_id=$2 AND home_id=$3 AND purpose='foreground') THEN 1 ELSE 0 END AS BIGINT)")
        .bind(instance.to_string()).bind(grant.extension_id.as_str()).bind(grant.home_id.to_string()).fetch_one(&mut **tx).await?;
    if valid != 1 {
        bail!("foreground SDK namespace is missing or belongs to a different scope");
    }
    Ok(())
}
pub(super) async fn lock_instance(
    tx: &mut Transaction<'_, Any>,
    grant: &CommittedOwnerGrant,
    instance: Uuid,
    driver: DatabaseDriver,
) -> Result<()> {
    // Serialize with disable/delete as well as grant revocation; identity is
    // held by the controller, not taken from the SDK operation.
    let extension_query = locked(
        "SELECT extension_id FROM extensions WHERE extension_id=$1 AND enabled=TRUE",
        driver,
        RowLock::Shared,
    );
    let extension: Option<String> = sqlx::query_scalar(&extension_query)
        .bind(grant.extension_id.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    let instance_query = locked(
        "SELECT instance_id FROM extension_instances WHERE instance_id=$1 AND extension_id=$2 AND enabled=TRUE",
        driver,
        RowLock::Exclusive,
    );
    let target: Option<String> = sqlx::query_scalar(&instance_query)
        .bind(instance.to_string())
        .bind(grant.extension_id.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    if extension.is_none() || target.is_none() || instance.is_nil() {
        bail!("SDK instance is missing, disabled or foreign");
    }
    Ok(())
}

/// Fresh disabled metadata only. INSERT conflicts never recreate missing state
/// for an existing instance. The future installer retains all admission duties.
pub(super) async fn initialize(
    database: &Database,
    grant: &CommittedOwnerGrant,
    requested: &GrantReview,
    instance: Uuid,
    name: &str,
    current: impl Fn() -> Result<()>,
) -> Result<()> {
    if instance.is_nil()
        || name.is_empty()
        || name.len() > 128
        || name.chars().any(char::is_control)
    {
        bail!("invalid fresh SDK instance");
    }
    current()?;
    let mut connection = super::super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    // Serializes namespace creation with enrollment's shared grant lock, before
    // checking absence. A concurrent staging commit cannot create its journal
    // between a stale absence check and this fresh foreground insertion.
    lock_grant_mode(
        &mut tx,
        grant,
        requested,
        database.driver,
        RowLock::Exclusive,
    )
    .await?;
    require_foreground_extension(&mut tx, grant).await?;
    sqlx::query("INSERT INTO extension_instances (instance_id,extension_id,instance_name,enabled) VALUES ($1,$2,$3,FALSE)")
        .bind(instance.to_string()).bind(grant.extension_id.as_str()).bind(name).execute(&mut *tx).await?;
    namespaces::initialize(
        &mut tx,
        database.driver,
        namespaces::Coordinates {
            home: grant.home_id,
            extension: &grant.extension_id,
            instance,
            namespace: instance,
        },
        namespaces::Purpose::Foreground,
    )
    .await?;
    current()?;
    tx.commit().await?;
    current()?;
    Ok(())
}

pub(super) async fn execute(
    database: &Database,
    secrets: &SecretsManager,
    grant: &CommittedOwnerGrant,
    requested: &GrantReview,
    instance: Uuid,
    raw: &RuntimeJson,
    current: impl Fn() -> Result<()>,
) -> Result<SourceSdkReply> {
    LocalScope {
        database,
        secrets,
        grant,
        requested,
        instance,
    }
    .execute(raw, None, current)
    .await
}

pub(super) struct InvocationAuthority<'a> {
    pub actor: &'a Actor,
    pub maximum_reply_bytes: usize,
}
pub(super) struct LocalScope<'a> {
    pub database: &'a Database,
    pub secrets: &'a SecretsManager,
    pub grant: &'a CommittedOwnerGrant,
    pub requested: &'a GrantReview,
    pub instance: Uuid,
}
pub(super) async fn authorize_invocation(
    database: &Database,
    actor: &Actor,
    grant: &CommittedOwnerGrant,
    requested: &GrantReview,
    instance: Uuid,
) -> Result<()> {
    if actor.home != grant.home_id {
        bail!("owner invocation belongs to a different home");
    }
    let started = Instant::now();
    let now = Utc::now();
    let mut connection = super::super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let valid = actor.authenticate(&mut tx, now).await?;
    lock_grant(&mut tx, grant, requested, database.driver).await?;
    lock_instance(&mut tx, grant, instance, database.driver).await?;
    require_foreground_instance(&mut tx, grant, instance).await?;
    ensure_before_expiry(valid, now, started)?;
    tx.commit().await?;
    ensure_before_expiry(valid, now, started)
}
impl LocalScope<'_> {
    pub(super) async fn execute(
        &self,
        raw: &RuntimeJson,
        invocation: Option<InvocationAuthority<'_>>,
        current: impl Fn() -> Result<()>,
    ) -> Result<SourceSdkReply> {
        let Self {
            database,
            secrets,
            grant,
            requested,
            instance,
        } = *self;
        let started = Instant::now();
        let now = Utc::now();
        if let Some(authority) = &invocation
            && (authority.actor.home != grant.home_id
                || authority.maximum_reply_bytes == 0
                || authority.maximum_reply_bytes
                    > elixir_marketplace::source_execution::MAX_JSON_BYTES)
        {
            bail!("invalid owner invocation effect authority");
        }
        current()?;
        let request = SourceSdkRequest::parse(raw)?;
        if request.is_http() {
            return Ok(SourceSdkReply::Denied);
        }
        let mut connection = super::super::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let valid_until = if let Some(authority) = &invocation {
            Some(authority.actor.authenticate(&mut tx, now).await?)
        } else {
            None
        };
        lock_grant(&mut tx, grant, requested, database.driver).await?;
        lock_instance(&mut tx, grant, instance, database.driver).await?;
        require_foreground_instance(&mut tx, grant, instance).await?;
        current()?;
        let result = perform(
            &mut tx,
            secrets,
            grant,
            requested,
            identity(grant, instance),
            &request,
        )
        .await?;
        if let SourceSdkReply::Value(json) = &result {
            request.validate_reply(json)?;
            if invocation
                .as_ref()
                .is_some_and(|authority| json.accounted_bytes() > authority.maximum_reply_bytes)
            {
                bail!("SDK result exceeds invocation reply allowance");
            }
        }
        current()?;
        if let Some(valid) = valid_until {
            ensure_before_expiry(valid, now, started)?;
        }
        tx.commit().await?;
        current()?;
        if let Some(valid) = valid_until {
            ensure_before_expiry(valid, now, started)?;
        }
        Ok(result)
    }
}

/// Called only by an independently authorized native controller. The binding
/// is a durable restriction snapshot, NOT verified bytes or execution authority.
/// No session/issuer/search TTL is borrowed from the foreground owner path.
#[cfg(unix)]
pub(super) struct InstalledScope<'a> {
    pub database: &'a Database,
    pub secrets: &'a SecretsManager,
    pub binding: &'a super::installation::standing::InstalledBinding,
}
/// Fresh restrictions only, never a reusable grant or proof of native code.
/// The transport calls this again immediately before dispatch and periodically
/// in flight. No database transaction is retained across network I/O.
#[cfg(unix)]
pub(super) async fn authorize_installed_http(
    database: &Database,
    binding: &super::installation::standing::InstalledBinding,
    current: impl Fn() -> Result<()>,
) -> Result<bool> {
    current()?;
    let mut connection = super::super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    binding
        .require_in_transaction(&mut tx, database.driver)
        .await?;
    let allowed = binding
        .review()
        .scope()
        .permissions()
        .iter()
        .any(|p| p == "network.egress");
    current()?;
    tx.rollback().await?;
    current()?;
    Ok(allowed)
}
#[cfg(unix)]
impl InstalledScope<'_> {
    pub(super) async fn execute(
        &self,
        raw: &RuntimeJson,
        maximum_reply_bytes: usize,
        current: impl Fn() -> Result<()>,
    ) -> Result<SourceSdkReply> {
        if maximum_reply_bytes == 0
            || maximum_reply_bytes > elixir_marketplace::source_execution::MAX_JSON_BYTES
        {
            bail!("invalid installed invocation reply allowance");
        }
        current()?;
        let request = SourceSdkRequest::parse(raw)?;
        if request.is_http() {
            return Ok(SourceSdkReply::Denied);
        }
        let mut connection = super::super::durability::connection(self.database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, self.database.driver).await?;
        let grant = self
            .binding
            .require_in_transaction(&mut tx, self.database.driver)
            .await?;
        current()?;
        let result = perform(
            &mut tx,
            self.secrets,
            &grant,
            self.binding.review(),
            self.binding.coordinates(),
            &request,
        )
        .await?;
        if let SourceSdkReply::Value(json) = &result {
            request.validate_reply(json)?;
            if json.accounted_bytes() > maximum_reply_bytes {
                bail!("SDK result exceeds installed invocation reply allowance");
            }
        }
        current()?;
        tx.commit().await?;
        current()?;
        Ok(result)
    }
}

/// The original candidate claim, not a pending UUID or ACTIVE binding. No
/// network effects and no secret writes. Native lifecycle identity is checked
/// independently before entering the same-transaction authority restrictions.
#[cfg(unix)]
pub(super) struct CandidateScope<'a> {
    pub database: &'a Database,
    pub secrets: &'a SecretsManager,
    pub binding: &'a super::installation::activation::candidate::CandidateBinding,
}
#[cfg(unix)]
impl CandidateScope<'_> {
    pub(super) async fn execute(
        &self,
        raw: &RuntimeJson,
        maximum_reply_bytes: usize,
        native: &crate::marketplace::quarantine::execution::NativeInvocation,
        owner: &super::installation::runtime::RuntimeOwner,
        current: impl Fn() -> Result<()>,
    ) -> Result<SourceSdkReply> {
        native.require_candidate(self.binding.source(), self.binding.execution()?)?;
        native.current().await?;
        owner.check()?;
        self.execute_checked(raw, maximum_reply_bytes, Some(owner), || {
            native.check()?;
            owner.check()?;
            current()
        })
        .await
    }
    /// Synthetic SQL/crypto tests enter only this private component boundary;
    /// production can reach it only through the real native checks above.
    async fn execute_checked(
        &self,
        raw: &RuntimeJson,
        maximum_reply_bytes: usize,
        owner: Option<&super::installation::runtime::RuntimeOwner>,
        current: impl Fn() -> Result<()>,
    ) -> Result<SourceSdkReply> {
        if maximum_reply_bytes == 0
            || maximum_reply_bytes > elixir_marketplace::source_execution::MAX_JSON_BYTES
        {
            bail!("invalid candidate invocation reply allowance");
        }
        let started = Instant::now();
        let now = Utc::now();
        current()?;
        let request = SourceSdkRequest::parse(raw)?;
        let mut connection = super::super::durability::connection(self.database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, self.database.driver).await?;
        let (grant, valid) = self
            .binding
            .require_in_transaction(&mut tx, self.database.driver, now)
            .await?;
        if let Some(owner) = owner {
            owner.require(&mut tx, self.database.driver).await?;
        }
        current()?;
        // Independent of any future ordinary-call HTTP implementation. Local
        // candidate migration/readiness must NEVER gain network authority.
        let result = if request.is_http() {
            SourceSdkReply::Denied
        } else {
            perform(
                &mut tx,
                self.secrets,
                &grant,
                self.binding.review(),
                self.binding.coordinates(),
                &request,
            )
            .await?
        };
        if let SourceSdkReply::Value(json) = &result {
            request.validate_reply(json)?;
            if json.accounted_bytes() > maximum_reply_bytes {
                bail!("SDK result exceeds candidate invocation reply allowance");
            }
        }
        current()?;
        ensure_before_expiry(valid, now, started)?;
        tx.commit().await?;
        current()?;
        ensure_before_expiry(valid, now, started)?;
        Ok(result)
    }
    #[cfg(test)]
    pub(in crate::marketplace::grant_store) async fn fixture_execute(
        &self,
        raw: &RuntimeJson,
        maximum_reply_bytes: usize,
        current: impl Fn() -> Result<()>,
    ) -> Result<SourceSdkReply> {
        self.execute_checked(raw, maximum_reply_bytes, None, current)
            .await
    }
}

fn identity(grant: &CommittedOwnerGrant, instance: Uuid) -> namespaces::Coordinates<'_> {
    namespaces::Coordinates {
        home: grant.home_id,
        extension: &grant.extension_id,
        instance,
        namespace: instance,
    }
}

/// Shared DATA operations, not execution authority. Callers have already
/// locked their authority and exact original controller-owned namespace.
async fn perform(
    tx: &mut Transaction<'_, Any>,
    secrets: &SecretsManager,
    grant: &CommittedOwnerGrant,
    requested: &GrantReview,
    scope: namespaces::Coordinates<'_>,
    request: &SourceSdkRequest,
) -> Result<SourceSdkReply> {
    if scope.home != grant.home_id
        || scope.extension != &grant.extension_id
        || requested.scope().extension_id() != &grant.extension_id
        || scope.instance.is_nil()
        || scope.namespace.is_nil()
    {
        bail!("SDK namespace differs from its held grant");
    }
    let instance = scope.namespace;
    Ok(match request {
        SourceSdkRequest::Secret { key } => {
            if !requested.scope().secret_keys().contains(key) {
                SourceSdkReply::Denied
            } else if scope.instance != scope.namespace {
                let value = generation_secrets::read(tx, secrets, scope, key).await?;
                SourceSdkReply::Value(RuntimeJson::from_value(
                    value.map_or(Value::Null, |value| Value::String(value.to_string())),
                )?)
            } else {
                let row=sqlx::query("SELECT CASE WHEN LENGTH(value_encrypted)<=98304 THEN value_encrypted ELSE NULL END AS value_encrypted FROM secrets WHERE scope='instance' AND scope_id=$1 AND key=$2")
                    .bind(instance.to_string()).bind(key).fetch_optional(&mut **tx).await?;
                let value = match row {
                    None => Value::Null,
                    Some(row) => {
                        let encrypted: Option<String> = row.try_get("value_encrypted")?;
                        let encrypted = encrypted.context("SDK instance secret exceeds bounds")?;
                        if !encrypted.is_ascii() || !SecretsManager::is_encrypted(&encrypted) {
                            bail!("SDK instance secret is not encrypted");
                        }
                        let plain = zeroize::Zeroizing::new(
                            secrets
                                .decrypt(&encrypted)
                                .context("SDK instance secret unavailable")?,
                        );
                        if plain.len() > 64 * 1024 {
                            bail!("SDK instance secret exceeds bounds");
                        }
                        Value::String(plain.to_string())
                    }
                };
                SourceSdkReply::Value(RuntimeJson::from_value(value)?)
            }
        }
        SourceSdkRequest::StateGet { key }
        | SourceSdkRequest::StateSet { key, .. }
        | SourceSdkRequest::StateDelete { key } => {
            let quota = requested
                .scope()
                .runtime()
                .context("SDK runtime scope missing")?
                .state_bytes;
            if quota == 0 {
                SourceSdkReply::Denied
            } else {
                if scope.instance != scope.namespace {
                    namespaces::require(tx, scope, namespaces::Purpose::Installed).await?;
                }
                state(tx, secrets, scope, request, key, quota).await?
            }
        }
        SourceSdkRequest::Fetch { .. }
        | SourceSdkRequest::FetchOpen { .. }
        | SourceSdkRequest::FetchRead { .. }
        | SourceSdkRequest::FetchClose { .. } => SourceSdkReply::Denied,
    })
}
struct State {
    revision: i64,
    count: i64,
    bytes: i64,
}
impl State {
    fn charge(&self) -> Result<i64> {
        if !(1..=MAX_REVISION).contains(&self.revision)
            || !(0..=128).contains(&self.count)
            || self.bytes < 0
            || self.bytes > 128 * ENTRY_BYTES
        {
            bail!("corrupt SDK state counters");
        }
        Ok(ROOT_BYTES + self.count * ROW_BYTES + self.bytes)
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    schema_version: u32,
    home_id: Uuid,
    extension_id: ExtensionId,
    instance_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner_instance_id: Option<Uuid>,
    key: String,
    revision: i64,
    value: Value,
}
impl Envelope {
    fn require_coordinates(
        &self,
        home: Uuid,
        extension: &ExtensionId,
        namespace: Uuid,
        owner: Uuid,
    ) -> Result<()> {
        let ownership = match self.schema_version {
            1 => owner == namespace && self.owner_instance_id.is_none(),
            2 => self.owner_instance_id == Some(owner),
            _ => false,
        };
        if !ownership
            || self.home_id != home
            || &self.extension_id != extension
            || self.instance_id != namespace
        {
            bail!("encrypted SDK namespace identity differs");
        }
        Ok(())
    }
}
struct Entry {
    revision: i64,
    bytes: i64,
    value: Value,
}
fn entry(
    row: AnyRow,
    manager: &SecretsManager,
    scope: namespaces::Coordinates<'_>,
    key: &str,
    head: &State,
) -> Result<Entry> {
    let revision: i64 = row.try_get("revision")?;
    let bytes: i64 = row.try_get("encoded_bytes")?;
    let cipher: Option<String> = row.try_get("value_encrypted")?;
    let cipher = cipher.context("SDK state entry exceeds bounds")?;
    if !(1..=head.revision).contains(&revision)
        || bytes <= 0
        || bytes > ENTRY_BYTES
        || cipher.len() as i64 != bytes
        || !cipher.is_ascii()
        || !SecretsManager::is_encrypted(&cipher)
    {
        bail!("corrupt SDK state entry");
    }
    let plain = zeroize::Zeroizing::new(
        manager
            .decrypt(&cipher)
            .context("SDK state decryption failed")?,
    );
    let bounded = RuntimeJson::parse(plain.as_bytes())?;
    let value: Envelope = serde_json::from_str(bounded.as_str())
        .map_err(|_| anyhow::anyhow!("invalid encrypted SDK state envelope"))?;
    value.require_coordinates(scope.home, scope.extension, scope.namespace, scope.instance)?;
    if value.key != key
        || value.revision != revision
        || RuntimeJson::serialized_len(&value.value)? > 64 * 1024
    {
        bail!("SDK state identity mismatch");
    }
    Ok(Entry {
        revision,
        bytes,
        value: value.value,
    })
}
async fn state(
    tx: &mut Transaction<'_, Any>,
    manager: &SecretsManager,
    scope: namespaces::Coordinates<'_>,
    request: &SourceSdkRequest,
    key: &str,
    quota: u64,
) -> Result<SourceSdkReply> {
    let instance = scope.namespace;
    let row=sqlx::query("SELECT extension_id,home_id,revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(instance.to_string()).fetch_optional(&mut **tx).await?.context("SDK state initialization is missing")?;
    if row.try_get::<String, _>("extension_id")? != scope.extension.as_str()
        || row.try_get::<String, _>("home_id")? != scope.home.to_string()
    {
        bail!("SDK state scope mismatch");
    }
    let mut head = State {
        revision: row.try_get("revision")?,
        count: row.try_get("key_count")?,
        bytes: row.try_get("encoded_bytes")?,
    };
    let counted=sqlx::query("SELECT COUNT(*) AS n,CAST(COALESCE(SUM(encoded_bytes),0) AS BIGINT) AS bytes,COALESCE(MAX(revision),0) AS revision FROM marketplace_instance_sdk_values WHERE instance_id=$1")
        .bind(instance.to_string()).fetch_one(&mut **tx).await?;
    if head.charge()? as u64 > quota
        || counted.try_get::<i64, _>("n")? != head.count
        || counted.try_get::<i64, _>("bytes")? != head.bytes
        || counted.try_get::<i64, _>("revision")? > head.revision
    {
        bail!("SDK state quota/counters mismatch");
    }
    let existing=sqlx::query("SELECT revision,encoded_bytes,CASE WHEN LENGTH(value_encrypted)<=98304 THEN value_encrypted ELSE NULL END AS value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key=$2")
        .bind(instance.to_string()).bind(key).fetch_optional(&mut **tx).await?.map(|row|entry(row,manager,scope,key,&head)).transpose()?;
    match request {
        SourceSdkRequest::StateGet { .. } => {
            return Ok(SourceSdkReply::Value(RuntimeJson::from_value(
                existing.map_or(Value::Null, |e| e.value),
            )?));
        }
        SourceSdkRequest::StateDelete { .. } if existing.is_none() => {
            return Ok(SourceSdkReply::Value(RuntimeJson::from_value(
                Value::Bool(false),
            )?));
        }
        SourceSdkRequest::StateSet { value, .. }
            if existing.as_ref().is_some_and(|e| &e.value == value) =>
        {
            return Ok(SourceSdkReply::Value(RuntimeJson::from_value(
                Value::Bool(true),
            )?));
        }
        _ => {}
    }
    let old_revision = head.revision;
    head.revision = head
        .revision
        .checked_add(1)
        .filter(|r| *r <= MAX_REVISION)
        .context("SDK state revision exhausted")?;
    match request {
        SourceSdkRequest::StateSet { value, .. } => {
            if existing.is_none() {
                head.count += 1;
            }
            if head.count > 128 {
                return Ok(SourceSdkReply::Denied);
            }
            let envelope = Envelope {
                schema_version: if scope.instance == scope.namespace {
                    1
                } else {
                    2
                },
                owner_instance_id: (scope.instance != scope.namespace).then_some(scope.instance),
                home_id: scope.home,
                extension_id: scope.extension.clone(),
                instance_id: instance,
                key: key.into(),
                revision: head.revision,
                value: value.clone(),
            };
            let plain = zeroize::Zeroizing::new(serde_json::to_string(&envelope)?);
            let cipher = manager.encrypt(&plain)?;
            if cipher.len() as i64 > ENTRY_BYTES {
                bail!("SDK encrypted entry exceeds bounds");
            }
            head.bytes =
                head.bytes - existing.as_ref().map_or(0, |e| e.bytes) + cipher.len() as i64;
            if head.charge()? as u64 > quota {
                return Ok(SourceSdkReply::Denied);
            }
            let changed = if let Some(old) = &existing {
                sqlx::query("UPDATE marketplace_instance_sdk_values SET revision=$1,value_encrypted=$2,encoded_bytes=$3 WHERE instance_id=$4 AND key=$5 AND revision=$6")
                    .bind(head.revision).bind(&cipher).bind(cipher.len() as i64).bind(instance.to_string()).bind(key).bind(old.revision).execute(&mut **tx).await?.rows_affected()
            } else {
                sqlx::query("INSERT INTO marketplace_instance_sdk_values (instance_id,key,revision,value_encrypted,encoded_bytes) VALUES ($1,$2,$3,$4,$5)")
                    .bind(instance.to_string()).bind(key).bind(head.revision).bind(&cipher).bind(cipher.len() as i64).execute(&mut **tx).await?.rows_affected()
            };
            if changed != 1 {
                bail!("SDK state entry changed");
            }
        }
        SourceSdkRequest::StateDelete { .. } => {
            let old = existing.context("SDK state entry missing")?;
            head.count -= 1;
            head.bytes -= old.bytes;
            if sqlx::query("DELETE FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key=$2 AND revision=$3")
                .bind(instance.to_string()).bind(key).bind(old.revision).execute(&mut **tx).await?.rows_affected()!=1 {bail!("SDK state entry changed");}
        }
        _ => bail!("invalid SDK state operation"),
    }
    if sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=$1,key_count=$2,encoded_bytes=$3,updated_at=CURRENT_TIMESTAMP WHERE instance_id=$4 AND revision=$5")
        .bind(head.revision).bind(head.count).bind(head.bytes).bind(instance.to_string()).bind(old_revision).execute(&mut **tx).await?.rows_affected()!=1 {bail!("SDK state revision changed");}
    Ok(SourceSdkReply::Value(RuntimeJson::from_value(
        Value::Bool(true),
    )?))
}

#[cfg(test)]
mod tests;
