//! Container stacks: a `source-js-v1` module may declare container modules
//! (upstream images pinned by digest), connector data and setup steps. The
//! verified manifest holds every member inline, and the module's own source
//! is activated as usual. After activation each member becomes a derived
//! local row (`stack.<id>.<name>`) that the existing orchestrator runs under
//! the stack sandbox. See docs/architecture/arr-stack-marketplace-extension.md.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use sha2::{Digest as _, Sha256};

use crate::db::models::ExtensionKind;
use crate::extensions::manifest::{
    ExtensionManifest, ManifestExecutionStep, ManifestRuntimeSecurity, ManifestStack,
};

/// Reserved prefix of derived member IDs. Legacy installs refuse it, so only
/// marketplace materialization can create such rows.
pub(crate) const STACK_MEMBER_PREFIX: &str = "stack.";
/// Built-in extensions a stack's setup steps may use without shipping them.
pub(crate) const BUILT_IN_STACK_REFERENCES: [&str; 2] =
    ["elixir.modules.qbittorrent", "elixir.modules.nzbget"];
/// Services a stack module may provide (the server's existing drivers).
const STACK_MODULE_CAPABILITIES: [&str; 7] = [
    "media.manager.tv",
    "media.manager.movies",
    "media.manager.anime",
    "media.search.tv",
    "media.search.movies",
    "media.search.anime",
    "indexer.registry",
];

/// Where a long-lived stack executor reaches one of its own members.
pub(crate) struct StackMemberEndpoint {
    pub base_url: String,
    pub api_key: Option<String>,
}

/// Resolves a stack's own members (by member name) for its long-lived
/// executor. Only members of that stack resolve.
#[async_trait::async_trait]
pub(crate) trait StackMembers: Send + Sync {
    async fn member(
        &self,
        stack_extension_id: &str,
        member: &str,
    ) -> Result<Option<StackMemberEndpoint>>;
}

/// The server's member directory: the member's provider endpoint (published
/// on this computer) and the API key Elixir read during setup.
pub(crate) struct MemberDirectory {
    pool: sqlx::AnyPool,
    secrets: crate::secrets::SecretsManager,
}
impl MemberDirectory {
    pub(crate) fn new(pool: sqlx::AnyPool, secrets: crate::secrets::SecretsManager) -> Self {
        Self { pool, secrets }
    }
}
#[async_trait::async_trait]
impl StackMembers for MemberDirectory {
    async fn member(
        &self,
        stack_extension_id: &str,
        member: &str,
    ) -> Result<Option<StackMemberEndpoint>> {
        use crate::extensions::store::ExtensionStore;
        if member_name(member).is_err() {
            return Ok(None);
        }
        let id = derived_id(stack_extension_id, member);
        let store = ExtensionStore::new(&self.pool);
        let Some(detail) = store
            .list_provider_details()
            .await?
            .into_iter()
            .find(|detail| detail.extension_id == id && detail.provider.endpoint_json.is_some())
        else {
            return Ok(None);
        };
        let provider = detail.provider;
        let endpoint: crate::orchestrator::model::ProviderEndpoint =
            serde_json::from_value(provider.endpoint_json.clone().unwrap_or_default())
                .context("stack member endpoint")?;
        let base_url = crate::http::handlers::discovery::resolve_provider_transport_base_url(
            provider.instance_id,
            &endpoint,
        )
        .await?;
        let implementation = provider.implementation.clone().unwrap_or_default();
        let mut api_key = None;
        for key in [format!("{implementation}_api_key"), "api_key".to_owned()] {
            if let Some(secret) = store
                .get_secret(
                    crate::db::models::SecretScope::Instance,
                    Some(provider.instance_id),
                    &key,
                )
                .await?
            {
                api_key = Some(self.secrets.decrypt(&secret.value_encrypted)?);
                break;
            }
        }
        Ok(Some(StackMemberEndpoint { base_url, api_key }))
    }
}

pub(crate) fn is_stack_member_id(extension_id: &str) -> bool {
    extension_id.starts_with(STACK_MEMBER_PREFIX)
}

/// The stack (its derived-ID hash) and local member name of a member ID.
pub(crate) fn split_member_id(extension_id: &str) -> Option<(&str, &str)> {
    extension_id
        .strip_prefix(STACK_MEMBER_PREFIX)?
        .split_once('.')
        .filter(|(stack, member)| !stack.is_empty() && !member.is_empty())
}

fn member_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 32
        || name.starts_with('-')
        || name.ends_with('-')
        || name.contains("--")
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        bail!("invalid stack member name '{name}'");
    }
    Ok(())
}

fn no_nested_controls(member: &ExtensionManifest) -> Result<()> {
    if member.stack.is_some()
        || member.publisher.is_some()
        || member.trust.is_some()
        || member.owner_release.is_some()
        || member.control_surface.is_some()
    {
        bail!("stack member '{}' declares a reserved field", member.id);
    }
    Ok(())
}

/// Every member reference used by setup steps, connectors and add-ons.
fn blueprint_references(blueprint: &ExtensionManifest) -> Vec<&str> {
    let mut references: Vec<&str> = blueprint.connectors.iter().map(String::as_str).collect();
    references.extend(
        blueprint
            .optional_addons
            .iter()
            .map(|addon| addon.extension_id.as_str()),
    );
    if let Some(execution) = &blueprint.execution {
        references.extend(execution.packages.iter().map(String::as_str));
        references.extend(
            execution
                .instances
                .iter()
                .map(|instance| instance.extension_id.as_str()),
        );
        references.extend(
            execution
                .ownership
                .iter()
                .map(|domain| domain.owner.as_str()),
        );
        for phase in &execution.phases {
            for step in &phase.steps {
                match step {
                    ManifestExecutionStep::EnsurePackageInstalled { extension_id } => {
                        references.push(extension_id)
                    }
                    ManifestExecutionStep::ApplyConnector { connector_id, .. } => {
                        references.push(connector_id)
                    }
                    _ => {}
                }
            }
        }
    }
    references
}

/// Typed member rules, after the crate's structural `check_stack`.
pub(crate) fn check_members(stack: &ManifestStack, release_permissions: &[String]) -> Result<()> {
    let permissions: BTreeSet<&str> = release_permissions.iter().map(String::as_str).collect();
    let mut names = BTreeSet::new();
    let members = stack
        .modules
        .iter()
        .chain(&stack.connectors)
        .chain(std::iter::once(stack.blueprint.as_ref()));
    for member in members {
        member_name(&member.id)?;
        if !names.insert(member.id.as_str()) {
            bail!("duplicate stack member '{}'", member.id);
        }
        member
            .validate()
            .with_context(|| format!("invalid stack member '{}'", member.id))?;
        no_nested_controls(member)?;
        if let Some(missing) = member
            .permissions
            .iter()
            .find(|permission| !permissions.contains(permission.as_str()))
        {
            bail!(
                "stack member '{}' requests '{missing}' outside the release",
                member.id
            );
        }
    }
    for module in &stack.modules {
        if module.kind != ExtensionKind::Module
            || !module.actions.is_empty()
            || module.execution.is_some()
        {
            bail!("stack module '{}' must be a container module", module.id);
        }
        let runtime = module
            .runtime
            .as_ref()
            .with_context(|| format!("stack module '{}' needs a runtime", module.id))?;
        let image = runtime.image.as_deref().unwrap_or_default();
        elixir_marketplace::upstream_image::check_image_reference(image)
            .with_context(|| format!("stack module '{}' image", module.id))?;
        if runtime.r#type != "container"
            || runtime.network.is_some()
            || runtime.service_name.is_some()
            || runtime.egress.is_some()
            || serde_json::to_value(&runtime.security)?
                != serde_json::to_value(ManifestRuntimeSecurity::default())?
            || runtime
                .ports
                .iter()
                .any(|port| port.host.is_some() || port.container == 0)
            || runtime.env.iter().any(|env| {
                env.from_secret.is_some()
                    || env.value.is_none()
                    || crate::runtime::model::CONTAINER_USER_ENV.contains(&env.name.as_str())
            })
        {
            bail!(
                "stack module '{}' selects a runtime control Elixir owns",
                module.id
            );
        }
        for volume in &runtime.volumes {
            elixir_marketplace::upstream_image::check_folder_role(volume)
                .with_context(|| format!("stack module '{}' folder", module.id))?;
        }
        if let Some(provide) = module
            .provides
            .iter()
            .find(|provide| !STACK_MODULE_CAPABILITIES.contains(&provide.capability.as_str()))
        {
            bail!(
                "stack module '{}' provides unsupported service '{}'",
                module.id,
                provide.capability
            );
        }
    }
    for connector in &stack.connectors {
        if connector.kind != ExtensionKind::Connector
            || connector.runtime.is_some()
            || connector
                .actions
                .iter()
                .any(|action| action.r#type != "driver_patch")
        {
            bail!(
                "stack connector '{}' may only carry driver patches",
                connector.id
            );
        }
    }
    let blueprint = stack.blueprint.as_ref();
    if blueprint.kind != ExtensionKind::Blueprint
        || blueprint.runtime.is_some()
        || blueprint.execution.is_none()
    {
        bail!(
            "stack setup '{}' must be a blueprint with execution steps",
            blueprint.id
        );
    }
    for reference in blueprint_references(blueprint) {
        if !names.contains(reference) && !BUILT_IN_STACK_REFERENCES.contains(&reference) {
            bail!("stack setup references '{reference}', which is neither a member nor built in");
        }
    }
    Ok(())
}

/// Derived local ID of a member: `stack.<8 hex of the stack ID>.<member>`.
pub(crate) fn derived_id(stack_extension_id: &str, member: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(stack_extension_id.as_bytes()));
    format!("{STACK_MEMBER_PREFIX}{}.{member}", &digest[..8])
}

/// The members as local manifests, with member names replaced by derived IDs
/// everywhere the setup steps refer to them. Built-in references are kept.
pub(crate) fn materialize(
    stack_extension_id: &str,
    version: &str,
    stack: &ManifestStack,
) -> Result<Vec<ExtensionManifest>> {
    let ids: BTreeMap<String, String> = stack
        .modules
        .iter()
        .chain(&stack.connectors)
        .chain(std::iter::once(stack.blueprint.as_ref()))
        .map(|member| {
            (
                member.id.clone(),
                derived_id(stack_extension_id, &member.id),
            )
        })
        .collect();
    let map = |value: &mut String| {
        if let Some(derived) = ids.get(value.as_str()) {
            *value = derived.clone();
        }
    };
    let mut members = Vec::new();
    for member in stack
        .modules
        .iter()
        .chain(&stack.connectors)
        .chain(std::iter::once(stack.blueprint.as_ref()))
    {
        let mut member = member.clone();
        map(&mut member.id);
        member.version = version.to_owned();
        member.connectors.iter_mut().for_each(map);
        member
            .optional_addons
            .iter_mut()
            .for_each(|addon| map(&mut addon.extension_id));
        if let Some(execution) = member.execution.as_mut() {
            execution.packages.iter_mut().for_each(map);
            execution
                .instances
                .iter_mut()
                .for_each(|instance| map(&mut instance.extension_id));
            execution
                .ownership
                .iter_mut()
                .for_each(|domain| map(&mut domain.owner));
            for phase in &mut execution.phases {
                for step in &mut phase.steps {
                    match step {
                        ManifestExecutionStep::EnsurePackageInstalled { extension_id } => {
                            map(extension_id)
                        }
                        ManifestExecutionStep::ApplyConnector { connector_id, .. } => {
                            map(connector_id)
                        }
                        _ => {}
                    }
                }
            }
        }
        member
            .validate()
            .with_context(|| format!("materialized stack member '{}'", member.id))?;
        members.push(member);
    }
    Ok(members)
}

/// The marketplace stack a derived member row belongs to, and the member's
/// name: `stack.<hash>.<member>` matched against the enabled stack extensions.
pub(crate) async fn owner_of(
    pool: &sqlx::AnyPool,
    member_extension_id: &str,
) -> Result<Option<(String, String)>> {
    use crate::extensions::store::ExtensionStore;
    let Some(member) = member_extension_id
        .strip_prefix(STACK_MEMBER_PREFIX)
        .and_then(|rest| rest.split_once('.'))
        .map(|(_, member)| member)
        .filter(|member| member_name(member).is_ok())
    else {
        return Ok(None);
    };
    for row in ExtensionStore::new(pool).list_extensions().await? {
        if row.enabled
            && row.extension_id.starts_with("elx:")
            && derived_id(&row.extension_id, member) == member_extension_id
        {
            return Ok(Some((row.extension_id, member.to_owned())));
        }
    }
    Ok(None)
}

/// The home and installed instance of an enabled marketplace extension.
pub(crate) async fn installed_instance(
    pool: &sqlx::AnyPool,
    extension_id: &str,
) -> Result<Option<(uuid::Uuid, uuid::Uuid)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT SUBSTR(g.home_id,1,37),SUBSTR(i.instance_id,1,37) FROM extension_instances i \
         JOIN extensions e ON e.extension_id=i.extension_id \
         JOIN marketplace_extension_grants g ON g.extension_id=i.extension_id \
         WHERE i.extension_id=$1 AND i.enabled=TRUE AND e.enabled=TRUE AND g.enabled=TRUE \
         ORDER BY i.instance_id LIMIT 1",
    )
    .bind(extension_id)
    .fetch_optional(pool)
    .await?;
    row.map(|(home, instance)| Ok((home.trim().parse()?, instance.trim().parse()?)))
        .transpose()
}

/// What one stack reconcile pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct StackReconcile {
    pub stacks: usize,
    pub members_written: usize,
    pub images_ready: usize,
    pub setups_offered: usize,
    pub members_disabled: usize,
    /// Members of a stack that no longer exists, removed with their apps.
    pub members_removed: usize,
}

/// One stack pass at a time: `reconcile_once` runs from the loop, install
/// handlers and the updater, and overlapping passes would race on member rows
/// and could offer the same "Finish setup" twice. Uninstalling a stack holds
/// the same lock, so a pass that listed the stack before it was removed cannot
/// write its members back afterwards.
static PASS: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Holds off stack passes (see [`PASS`]).
pub(crate) async fn exclusive_pass() -> tokio::sync::MutexGuard<'static, ()> {
    PASS.lock().await
}

/// Container work for a stack's members. The server uses its Docker runtime;
/// tests use a recording fake.
#[async_trait::async_trait]
pub(crate) trait MemberRuntime: Send + Sync {
    /// Stops and removes a member instance's containers and its app's private
    /// `/config` volume. Media and downloads folders are bind mounts and are
    /// never touched.
    async fn remove_member_runtime(&self, instance: uuid::Uuid) -> Result<()>;
}

/// What a stack reconcile pass needs from the runtime.
#[async_trait::async_trait]
pub(crate) trait StackRuntime: MemberRuntime {
    async fn ensure_image(&self, image: &str) -> Result<()>;
}

/// The containers the orchestrator gives one instance (the same names
/// `OrchestratorService::remove_instance_runtime` removes) and the instance's
/// managed config volume.
pub(crate) async fn remove_instance_containers(
    runtime: &dyn crate::runtime::RuntimeManager,
    instance: uuid::Uuid,
) -> Result<()> {
    let base = crate::orchestrator::naming::container_name(instance);
    for name in [
        base.clone(),
        format!("{base}-rollback"),
        format!("{base}-vpn"),
        format!("{base}-vpn-rollback"),
    ] {
        if let Some(handle) = runtime.get_container_handle(&name).await? {
            let _ = runtime.stop_container(&handle).await;
            runtime.remove_container(&handle).await?;
        }
    }
    runtime
        .remove_named_volume(&crate::orchestrator::executor::config_volume_name(instance))
        .await
}

#[async_trait::async_trait]
impl MemberRuntime for crate::runtime::docker::DockerRuntimeManager {
    async fn remove_member_runtime(&self, instance: uuid::Uuid) -> Result<()> {
        remove_instance_containers(self, instance).await
    }
}

#[async_trait::async_trait]
impl StackRuntime for crate::runtime::docker::DockerRuntimeManager {
    async fn ensure_image(&self, image: &str) -> Result<()> {
        self.ensure_pinned_image(image).await.map(|_| ())
    }
}

/// `stack.<8 hex>.`: every derived member ID of one stack starts with it.
pub(crate) fn member_prefix(stack_extension_id: &str) -> String {
    let id = derived_id(stack_extension_id, "x");
    id[..id.len() - 1].to_owned()
}

fn member_prefix_of(member_extension_id: &str) -> Option<String> {
    let (stack, member) = split_member_id(member_extension_id)?;
    (member_name(member).is_ok() && valid_stack_hash(stack))
        .then(|| format!("{STACK_MEMBER_PREFIX}{stack}."))
}

fn valid_stack_hash(hash: &str) -> bool {
    hash.len() == 8
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_member_prefix(prefix: &str) -> bool {
    prefix
        .strip_prefix(STACK_MEMBER_PREFIX)
        .and_then(|rest| rest.strip_suffix('.'))
        .is_some_and(valid_stack_hash)
}

/// Derived member rows a removal deleted.
#[derive(Debug, Default)]
pub(crate) struct RemovedMembers {
    pub extensions: Vec<String>,
    pub instances: Vec<uuid::Uuid>,
}

/// One instance's operational rows inside the caller's transaction: its
/// providers (and the bindings that use them), provider and instance secrets,
/// settings saved under its instance or provider ID, and the instance itself
/// (its logs and other dependants go with it through their foreign keys).
pub(crate) async fn delete_instance_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    instance: uuid::Uuid,
) -> Result<()> {
    let instance = instance.to_string();
    let providers: Vec<(String,)> =
        sqlx::query_as("SELECT provider_id FROM providers WHERE instance_id=$1")
            .bind(&instance)
            .fetch_all(&mut **tx)
            .await?;
    for (provider,) in providers {
        sqlx::query("DELETE FROM bindings WHERE consumer_provider_id=$1 OR target_provider_id=$1")
            .bind(&provider)
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM secrets WHERE scope='provider' AND scope_id=$1")
            .bind(&provider)
            .execute(&mut **tx)
            .await?;
        // UUIDs hold no LIKE wildcards: only keys ending in this exact ID.
        sqlx::query("DELETE FROM extension_settings WHERE setting_key LIKE $1")
            .bind(format!("%{provider}"))
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM providers WHERE provider_id=$1")
            .bind(&provider)
            .execute(&mut **tx)
            .await?;
    }
    sqlx::query("DELETE FROM secrets WHERE scope='instance' AND scope_id=$1")
        .bind(&instance)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM extension_settings WHERE setting_key LIKE $1")
        .bind(format!("%{instance}"))
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM extension_instances WHERE instance_id=$1")
        .bind(&instance)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Every derived member row of one stack, inside the caller's transaction:
/// the members' instances (see [`delete_instance_rows`]), the setup's desired
/// blueprint and the member rows themselves. Built-in extensions the stack
/// uses never start with `stack.` and are left alone. The caller removes the
/// returned instances' containers after it commits.
pub(crate) async fn delete_member_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    prefix: &str,
) -> Result<RemovedMembers> {
    if !valid_member_prefix(prefix) {
        bail!("invalid stack member prefix");
    }
    // A fixed-length prefix compare: `LIKE` would treat `_` as a wildcard.
    let query = format!(
        "SELECT extension_id FROM extensions WHERE SUBSTR(extension_id,1,{})=$1 ORDER BY extension_id",
        prefix.len()
    );
    let members: Vec<(String,)> = sqlx::query_as(&query)
        .bind(prefix)
        .fetch_all(&mut **tx)
        .await?;
    let mut removed = RemovedMembers::default();
    for (member,) in members {
        if !is_stack_member_id(&member) || BUILT_IN_STACK_REFERENCES.contains(&member.as_str()) {
            bail!("refusing to remove '{member}' as a stack member");
        }
        let instances: Vec<(String,)> =
            sqlx::query_as("SELECT instance_id FROM extension_instances WHERE extension_id=$1")
                .bind(&member)
                .fetch_all(&mut **tx)
                .await?;
        for (instance,) in instances {
            let instance: uuid::Uuid = instance.trim().parse()?;
            delete_instance_rows(tx, instance).await?;
            removed.instances.push(instance);
        }
        sqlx::query("DELETE FROM desired_blueprints WHERE blueprint_extension_id=$1")
            .bind(&member)
            .execute(&mut **tx)
            .await?;
        let deleted = sqlx::query("DELETE FROM extensions WHERE extension_id=$1")
            .bind(&member)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        if deleted != 1 {
            bail!("stack member '{member}' changed while it was being removed");
        }
        removed.extensions.push(member);
    }
    Ok(removed)
}

/// Removes the members' containers and config volumes after their rows are
/// gone. A failure is logged: the orchestrator removes leftover managed
/// containers at its next start. Returns how many could not be removed.
pub(crate) async fn remove_member_runtimes<R: MemberRuntime + ?Sized>(
    runtime: &R,
    instances: &[uuid::Uuid],
) -> usize {
    let mut failed = 0;
    for instance in instances {
        if let Err(error) = runtime.remove_member_runtime(*instance).await {
            failed += 1;
            tracing::warn!(%instance, "could not remove a removed stack app's container: {error:#}");
        }
    }
    failed
}

/// Removes every member of a stack that no longer exists: rows first (one
/// transaction), then containers.
pub(crate) async fn remove_members<R: MemberRuntime + ?Sized>(
    pool: &sqlx::AnyPool,
    runtime: &R,
    prefix: &str,
) -> Result<RemovedMembers> {
    if !valid_member_prefix(prefix) {
        bail!("invalid stack member prefix");
    }
    let mut tx = pool.begin().await?;
    // Take the writer position before reading (SQLite cannot upgrade a
    // reader while another connection writes).
    let lock = format!(
        "UPDATE extensions SET installed_at=installed_at WHERE SUBSTR(extension_id,1,{})=$1",
        prefix.len()
    );
    sqlx::query(&lock).bind(prefix).execute(&mut *tx).await?;
    let removed = delete_member_rows(&mut tx, prefix).await?;
    tx.commit().await?;
    remove_member_runtimes(runtime, &removed.instances).await;
    Ok(removed)
}

/// Idempotent: for every enabled marketplace extension that declares a stack,
/// write its derived member rows, make each pinned image present, and offer
/// its setup once ("Finish setup"). Members of a stack that is switched off
/// are switched off; members of a stack that no longer exists are removed with
/// their apps. Runs before each orchestrator reconcile, so it also recovers
/// after a crash between activation and this step.
pub(crate) async fn reconcile(
    pool: &sqlx::AnyPool,
    docker: &crate::runtime::docker::DockerRuntimeManager,
) -> Result<StackReconcile> {
    reconcile_with(pool, docker).await
}

pub(crate) async fn reconcile_with(
    pool: &sqlx::AnyPool,
    runtime: &dyn StackRuntime,
) -> Result<StackReconcile> {
    use crate::db::models::ExtensionTrustLevel;
    use crate::extensions::store::{ExtensionStore, NewDesiredBlueprint, NewExtension};
    let _pass = PASS.lock().await;
    let store = ExtensionStore::new(pool);
    let extensions = store.list_extensions().await?;
    let mut summary = StackReconcile::default();
    let mut wanted = BTreeSet::new();
    let desired = store.list_desired_blueprints(None).await?;
    for stack_row in extensions
        .iter()
        .filter(|row| row.enabled && row.extension_id.starts_with("elx:"))
    {
        let manifest: ExtensionManifest =
            match serde_json::from_value(stack_row.manifest_json.clone()) {
                Ok(manifest) => manifest,
                Err(_) => continue,
            };
        let Some(stack) = manifest.stack.as_ref() else {
            continue;
        };
        summary.stacks += 1;
        check_members(stack, &manifest.permissions)
            .with_context(|| format!("stack '{}'", stack_row.extension_id))?;
        for member in materialize(&stack_row.extension_id, &stack_row.version, stack)? {
            wanted.insert(member.id.clone());
            if let Some(image) = member
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.image.as_deref())
            {
                runtime
                    .ensure_image(image)
                    .await
                    .with_context(|| format!("stack image for '{}'", member.id))?;
                summary.images_ready += 1;
            }
            let manifest_json = serde_json::to_value(&member)?;
            let current = store.get_extension(&member.id).await?;
            if current.as_ref().is_none_or(|row| {
                row.version != member.version || row.manifest_json != manifest_json
            }) {
                store
                    .upsert_extension(&NewExtension {
                        extension_id: member.id.clone(),
                        name: member.name.clone(),
                        version: member.version.clone(),
                        kind: member.kind,
                        publisher_name: stack_row.publisher_name.clone(),
                        signing_key_id: stack_row.signing_key_id.clone(),
                        trust_level: ExtensionTrustLevel::Community,
                        manifest_json,
                        package_hash: stack_row.package_hash.clone(),
                        enabled: current.as_ref().is_none_or(|row| row.enabled),
                    })
                    .await?;
                summary.members_written += 1;
            }
            if member.kind == ExtensionKind::Blueprint
                && !desired
                    .iter()
                    .any(|entry| entry.blueprint_extension_id == member.id)
            {
                store
                    .create_desired_blueprint(&NewDesiredBlueprint {
                        desired_id: uuid::Uuid::new_v4(),
                        blueprint_extension_id: member.id.clone(),
                        blueprint_version: member.version.clone(),
                        params_json: None,
                    })
                    .await?;
                summary.setups_offered += 1;
            }
        }
    }
    // Every marketplace extension that still exists, switched on or not.
    let stacks: BTreeSet<String> = extensions
        .iter()
        .filter(|row| row.extension_id.starts_with("elx:"))
        .map(|row| member_prefix(&row.extension_id))
        .collect();
    let mut gone = BTreeSet::new();
    let mut gone_enabled = Vec::new();
    for orphan in extensions
        .iter()
        .filter(|row| is_stack_member_id(&row.extension_id) && !wanted.contains(&row.extension_id))
    {
        match member_prefix_of(&orphan.extension_id) {
            Some(prefix) if !stacks.contains(&prefix) => {
                gone.insert(prefix);
                if orphan.enabled {
                    gone_enabled.push(orphan.extension_id.as_str());
                }
            }
            _ if orphan.enabled => {
                store
                    .set_extension_enabled(&orphan.extension_id, false)
                    .await?;
                summary.members_disabled += 1;
            }
            _ => {}
        }
    }
    if gone.is_empty() {
        return Ok(summary);
    }
    // The orchestrator's apply lock keeps a setup run or reconcile pass from
    // starting these apps again while they are removed. If one is running,
    // they are only switched off now and the next pass removes them.
    let owner = uuid::Uuid::new_v4().to_string();
    let lock = crate::orchestrator::lock::APPLY_LOCK_NAME;
    if !store
        .acquire_lock(lock, &owner, std::time::Duration::from_secs(300))
        .await?
    {
        for member in gone_enabled {
            store.set_extension_enabled(member, false).await?;
            summary.members_disabled += 1;
        }
        return Ok(summary);
    }
    let mut result = Ok(());
    for prefix in gone {
        match remove_members(pool, runtime, &prefix).await {
            Ok(removed) => {
                tracing::info!(
                    members = removed.extensions.len(),
                    "removed the apps of a container stack that is no longer installed"
                );
                summary.members_removed += removed.extensions.len();
            }
            Err(error) => {
                result = Err(error);
                break;
            }
        }
    }
    if let Err(error) = store.release_lock(lock, &owner).await {
        tracing::warn!("could not release the orchestrator lock after a stack pass: {error:#}");
    }
    result.map(|()| summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const IMAGE: &str = "lscr.io/linuxserver/sonarr@sha256:f83bc8426c8260e1901094b43fb9796bd47b3ebd874e4c53a3711ca4df791dcc";

    fn stack() -> ManifestStack {
        serde_json::from_value(json!({
            "modules": [{"id": "sonarr", "version": "1.0.0", "kind": "module", "name": "Sonarr",
                "permissions": ["runtime.manage_containers", "network.egress"],
                "provides": [{"capability": "media.manager.tv", "slot": "default", "cardinality": "one", "implementation": "sonarr"}],
                "runtime": {"type": "container", "image": IMAGE, "ports": [{"container": 8989}],
                    "volumes": ["{downloads}:/downloads", "{media}/tv:/tv"]},
                "networking": {"service_port": {"scheme": "http", "container_port": 8989}}}],
            "connectors": [{"id": "sonarr-qbittorrent", "version": "1.0.0", "kind": "connector", "name": "Sonarr to qBittorrent",
                "permissions": ["drivers.configure.media.manager.tv"],
                "targets": [{"capability": "media.manager.tv", "slot": "default"}],
                "actions": [{"type": "driver_patch", "target": {"capability": "media.manager.tv", "slot": "default"},
                    "patch": {"op": "set_downloaders", "downloaders": []}}]}],
            "blueprint": {"id": "setup", "version": "1.0.0", "kind": "blueprint", "name": "Setup",
                "execution": {"packages": ["sonarr", "elixir.modules.qbittorrent", "sonarr-qbittorrent"],
                    "instances": [{"id": "sonarr", "extension_id": "sonarr"},
                        {"id": "qbittorrent", "extension_id": "elixir.modules.qbittorrent"}],
                    "ownership": [{"domain": "sonarr.download_clients.qbittorrent", "owner": "sonarr-qbittorrent"}],
                    "phases": [{"id": "install", "barrier": true, "steps": [
                        {"type": "ensure_package_installed", "extension_id": "sonarr"},
                        {"type": "ensure_package_installed", "extension_id": "elixir.modules.qbittorrent"},
                        {"type": "ensure_package_installed", "extension_id": "sonarr-qbittorrent"}]},
                        {"id": "wire", "barrier": true, "steps": [
                        {"type": "apply_connector", "connector_id": "sonarr-qbittorrent", "target_instance": "sonarr",
                            "target_capability": "media.manager.tv", "ownership_domains": ["sonarr.download_clients.qbittorrent"]}]}]}}
        }))
        .unwrap()
    }
    fn permissions() -> Vec<String> {
        [
            "runtime.manage_containers",
            "network.egress",
            "drivers.configure.media.manager.tv",
        ]
        .map(String::from)
        .to_vec()
    }

    #[test]
    fn a_valid_stack_materializes_with_derived_ids_and_built_ins_kept() -> Result<()> {
        let stack = stack();
        check_members(&stack, &permissions())?;
        let members = materialize("elx:publisher/arr-stack", "2.0.0", &stack)?;
        let sonarr = derived_id("elx:publisher/arr-stack", "sonarr");
        assert!(sonarr.starts_with("stack.") && sonarr.ends_with(".sonarr") && sonarr.len() == 21);
        assert_eq!(members.len(), 3);
        assert!(
            members
                .iter()
                .all(|member| is_stack_member_id(&member.id) && member.version == "2.0.0")
        );
        let setup = serde_json::to_value(&members[2])?;
        let execution = &setup["execution"];
        assert_eq!(
            execution["packages"],
            json!([
                sonarr,
                "elixir.modules.qbittorrent",
                derived_id("elx:publisher/arr-stack", "sonarr-qbittorrent")
            ])
        );
        assert_eq!(
            execution["instances"][1]["extension_id"],
            "elixir.modules.qbittorrent"
        );
        assert_eq!(
            execution["phases"][1]["steps"][0]["connector_id"],
            derived_id("elx:publisher/arr-stack", "sonarr-qbittorrent")
        );
        Ok(())
    }

    // The shipped Arr Stack manifest, when this server is checked out inside
    // the Elixir repository (skipped for a standalone server checkout).
    #[test]
    fn the_arr_stack_manifest_passes_every_stack_check() -> Result<()> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../extensions/marketplace-sources/arr-stack/manifest.yaml"
        );
        let Ok(text) = std::fs::read_to_string(path) else {
            return Ok(());
        };
        let raw: serde_json::Value = serde_yaml::from_str(&text)?;
        elixir_marketplace::upstream_image::check_stack(&raw)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let manifest: ExtensionManifest = serde_json::from_value(raw)?;
        manifest.validate()?;
        let stack = manifest.stack.as_ref().context("stack")?;
        check_members(stack, &manifest.permissions)?;
        let members = materialize("elx:publisher/arr-stack", &manifest.version, stack)?;
        assert_eq!(members.len(), 3 + 14 + 1);
        let setup = members.last().context("setup")?;
        let execution = setup.execution.as_ref().context("execution")?;
        assert_eq!(execution.packages.len(), 18);
        assert!(
            execution.packages.iter().all(
                |id| is_stack_member_id(id) || BUILT_IN_STACK_REFERENCES.contains(&id.as_str())
            )
        );
        let installed = serde_json::to_vec(&manifest)?.len();
        assert!(
            installed <= crate::marketplace::manifest::MAX_OPERATIONAL_MANIFEST_BYTES,
            "installed manifest is {installed} bytes"
        );
        Ok(())
    }

    #[test]
    fn members_cannot_take_runtime_controls_or_reach_outside_the_stack() {
        let mutations: Vec<(&str, Box<dyn Fn(&mut ManifestStack)>)> = vec![
            (
                "tag image",
                Box::new(|s| {
                    s.modules[0].runtime.as_mut().unwrap().image =
                        Some("lscr.io/linuxserver/sonarr:4.0.0".into())
                }),
            ),
            (
                "network",
                Box::new(|s| s.modules[0].runtime.as_mut().unwrap().network = Some("host".into())),
            ),
            (
                "service name",
                Box::new(|s| {
                    s.modules[0].runtime.as_mut().unwrap().service_name = Some("elx-sonarr".into())
                }),
            ),
            (
                "host port",
                Box::new(|s| s.modules[0].runtime.as_mut().unwrap().ports[0].host = Some(8989)),
            ),
            (
                "socket",
                Box::new(|s| {
                    s.modules[0]
                        .runtime
                        .as_mut()
                        .unwrap()
                        .volumes
                        .push("/var/run/docker.sock:/var/run/docker.sock".into())
                }),
            ),
            (
                "security",
                Box::new(|s| {
                    s.modules[0]
                        .runtime
                        .as_mut()
                        .unwrap()
                        .security
                        .no_new_privileges = true
                }),
            ),
            (
                "secret env",
                Box::new(|s| {
                    s.modules[0].runtime.as_mut().unwrap().env.push(
                        serde_json::from_value(json!({"name": "X", "from_secret": "instance:x"}))
                            .unwrap(),
                    )
                }),
            ),
            (
                "container user",
                Box::new(|s| {
                    s.modules[0].runtime.as_mut().unwrap().env.push(
                        serde_json::from_value(json!({"name": "PUID", "value": "0"})).unwrap(),
                    )
                }),
            ),
            (
                "capability",
                Box::new(|s| s.modules[0].provides[0].capability = "downloader.torrent".into()),
            ),
            (
                "permission",
                Box::new(|s| s.modules[0].permissions.push("live.network.private".into())),
            ),
            (
                "name",
                Box::new(|s| s.modules[0].id = "elixir.modules.sonarr".into()),
            ),
            (
                "outside reference",
                Box::new(|s| {
                    s.blueprint
                        .execution
                        .as_mut()
                        .unwrap()
                        .packages
                        .push("elixir.modules.bazarr".into())
                }),
            ),
            (
                "connector action",
                Box::new(|s| s.connectors[0].actions[0].r#type = "shell".into()),
            ),
            (
                "nested stack",
                Box::new(|s| s.blueprint.stack = Some(stack())),
            ),
        ];
        for (label, mutate) in mutations {
            let mut stack = stack();
            mutate(&mut stack);
            assert!(check_members(&stack, &permissions()).is_err(), "{label}");
        }
    }
}
