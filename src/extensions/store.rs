use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::time::Duration;
use sqlx::{any::AnyRow, AnyPool, QueryBuilder, Row, TypeInfo, Value, ValueRef};
use uuid::Uuid;

use crate::db::models::{
    Binding, BindingStatus, DesiredBlueprint, Extension, ExtensionInstance, ExtensionKind,
    ExtensionTrustLevel, OperationStep, OperationStepStatus, OrchestratorRun, OrchestratorRunStatus,
    Provider, ProviderHealthState, RuntimeLog, Secret, SecretScope, SlotCardinality,
};

#[derive(Debug, Clone)]
pub struct NewExtension {
    pub extension_id: String,
    pub name: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub publisher_name: Option<String>,
    pub signing_key_id: Option<String>,
    pub trust_level: ExtensionTrustLevel,
    pub manifest_json: serde_json::Value,
    pub package_hash: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct NewExtensionInstance {
    pub instance_id: Uuid,
    pub extension_id: String,
    pub instance_name: String,
    pub config_json: Option<serde_json::Value>,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct NewProvider {
    pub provider_id: Uuid,
    pub instance_id: Uuid,
    pub capability: String,
    pub slot_id: String,
    pub cardinality: SlotCardinality,
    pub implementation: Option<String>,
    pub endpoint_json: Option<serde_json::Value>,
    pub health_state: ProviderHealthState,
}

#[derive(Debug, Clone)]
pub struct ProviderDetails {
    pub provider: Provider,
    pub extension_id: String,
    pub trust_level: ExtensionTrustLevel,
}

#[derive(Debug, Clone)]
pub struct NewBinding {
    pub binding_id: Uuid,
    pub consumer_provider_id: Uuid,
    pub requires_capability: String,
    pub requires_slot_id: String,
    pub target_provider_id: Uuid,
    pub binding_params_json: Option<serde_json::Value>,
    pub status: BindingStatus,
}

#[derive(Debug, Clone)]
pub struct NewDesiredBlueprint {
    pub desired_id: Uuid,
    pub blueprint_extension_id: String,
    pub blueprint_version: String,
    pub params_json: Option<serde_json::Value>,
    pub decisions_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct NewSecret {
    pub secret_id: Uuid,
    pub scope: SecretScope,
    pub scope_id: Option<Uuid>,
    pub key: String,
    pub value_encrypted: String,
    pub rotatable: bool,
}

#[derive(Debug, Clone)]
pub struct NewOrchestratorRun {
    pub run_id: Uuid,
    pub source: String,
    pub status: OrchestratorRunStatus,
    pub phase: Option<String>,
    pub plan_json: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewOperationStep {
    pub step_id: Uuid,
    pub run_id: Uuid,
    pub step_index: i32,
    pub action_type: String,
    pub action_json: Option<serde_json::Value>,
    pub status: OperationStepStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewRuntimeLog {
    pub log_id: Uuid,
    pub instance_id: Uuid,
    pub log_uri: String,
}

pub struct ExtensionStore<'a> {
    pool: &'a AnyPool,
}

impl<'a> ExtensionStore<'a> {
    pub fn new(pool: &'a AnyPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_extension(&self, data: &NewExtension) -> Result<()> {
        let manifest_json = serde_json::to_string(&data.manifest_json)
            .context("serializing manifest json")?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extensions (extension_id, name, version, kind, publisher_name, signing_key_id, trust_level, manifest_json, package_hash, enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \n             ON CONFLICT(extension_id) DO UPDATE SET name = excluded.name, version = excluded.version, kind = excluded.kind, publisher_name = excluded.publisher_name, signing_key_id = excluded.signing_key_id, trust_level = excluded.trust_level, manifest_json = excluded.manifest_json, package_hash = excluded.package_hash, enabled = excluded.enabled",
        )
        .bind(&data.extension_id)
        .bind(&data.name)
        .bind(&data.version)
        .bind(data.kind.as_str())
        .bind(data.publisher_name.as_deref())
        .bind(data.signing_key_id.as_deref())
        .bind(data.trust_level.as_str())
        .bind(manifest_json)
        .bind(data.package_hash.as_deref())
        .bind(data.enabled)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_extensions(&self) -> Result<Vec<Extension>> {
        let rows = sqlx::query(
            "SELECT extension_id, name, version, kind, CAST(publisher_name AS TEXT) as publisher_name, CAST(signing_key_id AS TEXT) as signing_key_id, trust_level, manifest_json, CAST(package_hash AS TEXT) as package_hash, CAST(installed_at AS TEXT) as installed_at, CAST(enabled AS INTEGER) as enabled FROM extensions ORDER BY installed_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_extension(&row)?);
        }
        Ok(items)
    }

    pub async fn get_extension(&self, extension_id: &str) -> Result<Option<Extension>> {
        let row = sqlx::query(
            "SELECT extension_id, name, version, kind, CAST(publisher_name AS TEXT) as publisher_name, CAST(signing_key_id AS TEXT) as signing_key_id, trust_level, manifest_json, CAST(package_hash AS TEXT) as package_hash, CAST(installed_at AS TEXT) as installed_at, CAST(enabled AS INTEGER) as enabled FROM extensions WHERE extension_id = ? LIMIT 1",
        )
        .bind(extension_id)
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_extension(&row)).transpose()
    }

    pub async fn set_extension_enabled(&self, extension_id: &str, enabled: bool) -> Result<()> {
        sqlx::query::<sqlx::Any>("UPDATE extensions SET enabled = ? WHERE extension_id = ?")
            .bind(enabled)
            .bind(extension_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_extension(&self, extension_id: &str) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM extensions WHERE extension_id = ?")
            .bind(extension_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn create_instance(&self, data: &NewExtensionInstance) -> Result<()> {
        let config_json = json_to_string(data.config_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_instances (instance_id, extension_id, instance_name, config_json, enabled) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(data.instance_id.to_string())
        .bind(&data.extension_id)
        .bind(&data.instance_name)
        .bind(config_json)
        .bind(data.enabled)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_instances(&self, extension_id: Option<&str>) -> Result<Vec<ExtensionInstance>> {
        let rows = if let Some(extension_id) = extension_id {
            sqlx::query(
            "SELECT instance_id, extension_id, instance_name, CAST(config_json AS TEXT) as config_json, CAST(runtime_version AS TEXT) as runtime_version, CAST(rollback_version AS TEXT) as rollback_version, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at, CAST(enabled AS INTEGER) as enabled FROM extension_instances WHERE extension_id = ? ORDER BY created_at DESC",
            )
            .bind(extension_id)
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT instance_id, extension_id, instance_name, CAST(config_json AS TEXT) as config_json, CAST(runtime_version AS TEXT) as runtime_version, CAST(rollback_version AS TEXT) as rollback_version, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at, CAST(enabled AS INTEGER) as enabled FROM extension_instances ORDER BY created_at DESC",
            )
            .fetch_all(self.pool)
            .await?
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_extension_instance(&row)?);
        }
        Ok(items)
    }

    pub async fn get_instance(&self, instance_id: Uuid) -> Result<Option<ExtensionInstance>> {
        let row = sqlx::query(
            "SELECT instance_id, extension_id, instance_name, CAST(config_json AS TEXT) as config_json, CAST(runtime_version AS TEXT) as runtime_version, CAST(rollback_version AS TEXT) as rollback_version, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at, CAST(enabled AS INTEGER) as enabled FROM extension_instances WHERE instance_id = ? LIMIT 1",
        )
        .bind(instance_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_extension_instance(&row)).transpose()
    }

    pub async fn rename_instance(&self, instance_id: Uuid, instance_name: &str) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances SET instance_name = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?",
        )
        .bind(instance_name)
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_instance_config(
        &self,
        instance_id: Uuid,
        config_json: Option<&serde_json::Value>,
    ) -> Result<()> {
        let config_json = json_to_string(config_json)?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances SET config_json = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?",
        )
        .bind(config_json)
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_instance_enabled(&self, instance_id: Uuid, enabled: bool) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances SET enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?",
        )
        .bind(enabled)
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_instance_runtime_version(
        &self,
        instance_id: Uuid,
        runtime_version: &str,
        rollback_version: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances SET runtime_version = ?, rollback_version = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?",
        )
        .bind(runtime_version)
        .bind(rollback_version)
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_instance(&self, instance_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM extension_instances WHERE instance_id = ?")
            .bind(instance_id.to_string())
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_secrets_by_scope(
        &self,
        scope: SecretScope,
        scope_id: Option<Uuid>,
    ) -> Result<()> {
        match scope_id {
            Some(scope_id) => {
                sqlx::query::<sqlx::Any>(
                    "DELETE FROM secrets WHERE scope = ? AND scope_id = ?",
                )
                .bind(scope.as_str())
                .bind(scope_id.to_string())
                .execute(self.pool)
                .await?;
            }
            None => {
                sqlx::query::<sqlx::Any>(
                    "DELETE FROM secrets WHERE scope = ? AND scope_id IS NULL",
                )
                .bind(scope.as_str())
                .execute(self.pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn upsert_provider(&self, data: &NewProvider) -> Result<()> {
        let endpoint_json = json_to_string(data.endpoint_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO providers (provider_id, instance_id, capability, slot_id, cardinality, implementation, endpoint_json, health_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \n             ON CONFLICT(instance_id, capability, slot_id) DO UPDATE SET cardinality = excluded.cardinality, implementation = excluded.implementation, endpoint_json = excluded.endpoint_json, health_state = excluded.health_state, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.provider_id.to_string())
        .bind(data.instance_id.to_string())
        .bind(&data.capability)
        .bind(&data.slot_id)
        .bind(data.cardinality.as_str())
        .bind(data.implementation.as_deref())
        .bind(endpoint_json)
        .bind(data.health_state.as_str())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_providers(&self, instance_id: Option<Uuid>) -> Result<Vec<Provider>> {
        let rows = if let Some(instance_id) = instance_id {
            sqlx::query(
                "SELECT provider_id, instance_id, capability, slot_id, cardinality, CAST(implementation AS TEXT) as implementation, CAST(endpoint_json AS TEXT) as endpoint_json, health_state, CAST(last_healthcheck_at AS TEXT) as last_healthcheck_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM providers WHERE instance_id = ? ORDER BY created_at DESC",
            )
            .bind(instance_id.to_string())
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT provider_id, instance_id, capability, slot_id, cardinality, CAST(implementation AS TEXT) as implementation, CAST(endpoint_json AS TEXT) as endpoint_json, health_state, CAST(last_healthcheck_at AS TEXT) as last_healthcheck_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM providers ORDER BY created_at DESC",
            )
            .fetch_all(self.pool)
            .await?
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_provider(&row)?);
        }
        Ok(items)
    }

    pub async fn get_provider(&self, provider_id: Uuid) -> Result<Option<Provider>> {
        let row = sqlx::query(
            "SELECT provider_id, instance_id, capability, slot_id, cardinality, CAST(implementation AS TEXT) as implementation, CAST(endpoint_json AS TEXT) as endpoint_json, health_state, CAST(last_healthcheck_at AS TEXT) as last_healthcheck_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM providers WHERE provider_id = ? LIMIT 1",
        )
        .bind(provider_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_provider(&row)).transpose()
    }

    pub async fn list_provider_details(&self) -> Result<Vec<ProviderDetails>> {
        let rows = sqlx::query(
            "SELECT p.provider_id, p.instance_id, p.capability, p.slot_id, p.cardinality, CAST(p.implementation AS TEXT) as implementation, CAST(p.endpoint_json AS TEXT) as endpoint_json, p.health_state, CAST(p.last_healthcheck_at AS TEXT) as last_healthcheck_at, CAST(p.created_at AS TEXT) as created_at, CAST(p.updated_at AS TEXT) as updated_at, i.extension_id as extension_id, e.trust_level as trust_level FROM providers p JOIN extension_instances i ON p.instance_id = i.instance_id JOIN extensions e ON i.extension_id = e.extension_id ORDER BY p.created_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_provider_detail(&row)?);
        }
        Ok(items)
    }

    pub async fn update_provider_health(
        &self,
        provider_id: Uuid,
        health_state: ProviderHealthState,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE providers SET health_state = ?, last_healthcheck_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE provider_id = ?",
        )
        .bind(health_state.as_str())
        .bind(provider_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_provider(&self, provider_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM providers WHERE provider_id = ?")
            .bind(provider_id.to_string())
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_binding(&self, data: &NewBinding) -> Result<()> {
        let binding_params = json_to_string(data.binding_params_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO bindings (binding_id, consumer_provider_id, requires_capability, requires_slot_id, target_provider_id, binding_params_json, status) VALUES (?, ?, ?, ?, ?, ?, ?) \n             ON CONFLICT(consumer_provider_id, requires_capability, requires_slot_id, target_provider_id) DO UPDATE SET binding_params_json = excluded.binding_params_json, status = excluded.status, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.binding_id.to_string())
        .bind(data.consumer_provider_id.to_string())
        .bind(&data.requires_capability)
        .bind(&data.requires_slot_id)
        .bind(data.target_provider_id.to_string())
        .bind(binding_params)
        .bind(data.status.as_str())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_bindings(&self) -> Result<Vec<Binding>> {
        let rows = sqlx::query(
            "SELECT binding_id, consumer_provider_id, requires_capability, requires_slot_id, target_provider_id, CAST(binding_params_json AS TEXT) as binding_params_json, status, CAST(last_error AS TEXT) as last_error, CAST(last_applied_at AS TEXT) as last_applied_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM bindings ORDER BY created_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_binding(&row)?);
        }
        Ok(items)
    }

    pub async fn update_binding_status(
        &self,
        binding_id: Uuid,
        status: BindingStatus,
        last_error: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE bindings SET status = ?, last_error = ?, last_applied_at = CASE WHEN ? = 'applied' THEN CURRENT_TIMESTAMP ELSE last_applied_at END, updated_at = CURRENT_TIMESTAMP WHERE binding_id = ?",
        )
        .bind(status.as_str())
        .bind(last_error)
        .bind(status.as_str())
        .bind(binding_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_desired_blueprint(&self, data: &NewDesiredBlueprint) -> Result<()> {
        let params_json = json_to_string(data.params_json.as_ref())?;
        let decisions_json = json_to_string(data.decisions_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO desired_blueprints (desired_id, blueprint_extension_id, blueprint_version, params_json, decisions_json, applied) VALUES (?, ?, ?, ?, ?, 0)",
        )
        .bind(data.desired_id.to_string())
        .bind(&data.blueprint_extension_id)
        .bind(&data.blueprint_version)
        .bind(params_json)
        .bind(decisions_json)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_desired_blueprints(
        &self,
        applied: Option<bool>,
    ) -> Result<Vec<DesiredBlueprint>> {
        let rows = if let Some(applied) = applied {
            sqlx::query(
                "SELECT desired_id, blueprint_extension_id, blueprint_version, CAST(params_json AS TEXT) as params_json, CAST(decisions_json AS TEXT) as decisions_json, CAST(applied AS INTEGER) as applied, CAST(created_at AS TEXT) as created_at, CAST(applied_at AS TEXT) as applied_at FROM desired_blueprints WHERE applied = ? ORDER BY created_at DESC",
            )
            .bind(applied)
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT desired_id, blueprint_extension_id, blueprint_version, CAST(params_json AS TEXT) as params_json, CAST(decisions_json AS TEXT) as decisions_json, CAST(applied AS INTEGER) as applied, CAST(created_at AS TEXT) as created_at, CAST(applied_at AS TEXT) as applied_at FROM desired_blueprints ORDER BY created_at DESC",
            )
            .fetch_all(self.pool)
            .await?
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_desired_blueprint(&row)?);
        }
        Ok(items)
    }

    pub async fn mark_desired_applied(&self, desired_id: Uuid, applied: bool) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE desired_blueprints SET applied = ?, applied_at = CURRENT_TIMESTAMP WHERE desired_id = ?",
        )
        .bind(applied)
        .bind(desired_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_desired_decisions(
        &self,
        desired_id: Uuid,
        decisions_json: Option<serde_json::Value>,
    ) -> Result<()> {
        let decisions_json = json_to_string(decisions_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "UPDATE desired_blueprints SET decisions_json = ? WHERE desired_id = ?",
        )
        .bind(decisions_json)
        .bind(desired_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_desired_blueprints(
        &self,
        applied: Option<bool>,
    ) -> Result<u64> {
        let result = if let Some(applied) = applied {
            sqlx::query::<sqlx::Any>(
                "DELETE FROM desired_blueprints WHERE applied = ?",
            )
            .bind(applied)
            .execute(self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Any>("DELETE FROM desired_blueprints")
                .execute(self.pool)
                .await?
        };
        Ok(result.rows_affected())
    }

    pub async fn upsert_secret(&self, data: &NewSecret) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO secrets (secret_id, scope, scope_id, key, value_encrypted, rotatable) VALUES (?, ?, ?, ?, ?, ?) \n             ON CONFLICT(scope, scope_id, key) DO UPDATE SET value_encrypted = excluded.value_encrypted, rotatable = excluded.rotatable",
        )
        .bind(data.secret_id.to_string())
        .bind(data.scope.as_str())
        .bind(data.scope_id.map(|id| id.to_string()))
        .bind(&data.key)
        .bind(&data.value_encrypted)
        .bind(data.rotatable)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_secret(
        &self,
        scope: SecretScope,
        scope_id: Option<Uuid>,
        key: &str,
    ) -> Result<Option<Secret>> {
        let row = if let Some(scope_id) = scope_id {
            sqlx::query(
                "SELECT secret_id, scope, CAST(scope_id AS TEXT) as scope_id, key, value_encrypted, CAST(created_at AS TEXT) as created_at, CAST(rotatable AS INTEGER) as rotatable FROM secrets WHERE scope = ? AND scope_id = ? AND key = ? LIMIT 1",
            )
            .bind(scope.as_str())
            .bind(scope_id.to_string())
            .bind(key)
            .fetch_optional(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT secret_id, scope, CAST(scope_id AS TEXT) as scope_id, key, value_encrypted, CAST(created_at AS TEXT) as created_at, CAST(rotatable AS INTEGER) as rotatable FROM secrets WHERE scope = ? AND scope_id IS NULL AND key = ? LIMIT 1",
            )
            .bind(scope.as_str())
            .bind(key)
            .fetch_optional(self.pool)
            .await?
        };
        row.map(|row| map_secret(&row)).transpose()
    }

    pub async fn get_secret_by_id(&self, secret_id: Uuid) -> Result<Option<Secret>> {
        let row = sqlx::query(
            "SELECT secret_id, scope, CAST(scope_id AS TEXT) as scope_id, key, value_encrypted, CAST(created_at AS TEXT) as created_at, CAST(rotatable AS INTEGER) as rotatable FROM secrets WHERE secret_id = ? LIMIT 1",
        )
        .bind(secret_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_secret(&row)).transpose()
    }

    pub async fn list_secrets(
        &self,
        scope: Option<SecretScope>,
        scope_id: Option<Uuid>,
        key: Option<&str>,
    ) -> Result<Vec<Secret>> {
        let mut builder = QueryBuilder::<sqlx::Any>::new(
            "SELECT secret_id, scope, CAST(scope_id AS TEXT) as scope_id, key, value_encrypted, CAST(created_at AS TEXT) as created_at, CAST(rotatable AS INTEGER) as rotatable FROM secrets",
        );
        let mut has_where = false;
        if let Some(scope) = scope {
            builder.push(if has_where { " AND " } else { " WHERE " });
            builder.push("scope = ");
            builder.push_bind(scope.as_str());
            has_where = true;
        }
        if let Some(scope_id) = scope_id {
            builder.push(if has_where { " AND " } else { " WHERE " });
            builder.push("scope_id = ");
            builder.push_bind(scope_id.to_string());
            has_where = true;
        }
        if let Some(key) = key {
            builder.push(if has_where { " AND " } else { " WHERE " });
            builder.push("key = ");
            builder.push_bind(key);
        }

        let rows = builder.build().fetch_all(self.pool).await?;
        let mut secrets = Vec::with_capacity(rows.len());
        for row in rows {
            secrets.push(map_secret(&row)?);
        }
        Ok(secrets)
    }

    pub async fn delete_secret(&self, secret_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM secrets WHERE secret_id = ?")
            .bind(secret_id.to_string())
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_secret(
        &self,
        secret_id: Uuid,
        value_encrypted: &str,
        rotatable: bool,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE secrets SET value_encrypted = ?, rotatable = ? WHERE secret_id = ?",
        )
        .bind(value_encrypted)
        .bind(rotatable)
        .bind(secret_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_run(&self, data: &NewOrchestratorRun) -> Result<()> {
        let plan_json = json_to_string(data.plan_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO orchestrator_runs (run_id, source, status, phase, plan_json, error) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(data.run_id.to_string())
        .bind(&data.source)
        .bind(data.status.as_str())
        .bind(data.phase.as_deref())
        .bind(plan_json)
        .bind(data.error.as_deref())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn acquire_lock(
        &self,
        lock_name: &str,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let insert = sqlx::query::<sqlx::Any>(
            "INSERT INTO orchestrator_locks (lock_name, owner_id) VALUES (?, ?)",
        )
        .bind(lock_name)
        .bind(owner_id)
        .execute(self.pool)
        .await;

        if insert.is_ok() {
            return Ok(true);
        }

        let err = insert.err().expect("insert error");
        if !is_unique_violation(&err) {
            return Err(err.into());
        }

        let ttl_seconds = ttl.as_secs().max(1);
        let stale_before =
            Utc::now() - chrono::Duration::seconds(ttl_seconds as i64);
        let stale_str = stale_before.format("%Y-%m-%d %H:%M:%S").to_string();
        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_locks SET owner_id = ?, locked_at = CURRENT_TIMESTAMP WHERE lock_name = ? AND locked_at < ?",
        )
        .bind(owner_id)
        .bind(lock_name)
        .bind(stale_str)
        .execute(self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn release_lock(&self, lock_name: &str, owner_id: &str) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "DELETE FROM orchestrator_locks WHERE lock_name = ? AND owner_id = ?",
        )
        .bind(lock_name)
        .bind(owner_id)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_run_status(
        &self,
        run_id: Uuid,
        status: OrchestratorRunStatus,
        phase: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_runs SET status = ?, phase = ?, error = ?, started_at = CASE WHEN ? = 'running' AND started_at IS NULL THEN CURRENT_TIMESTAMP ELSE started_at END, finished_at = CASE WHEN ? IN ('failed', 'completed', 'canceled') THEN CURRENT_TIMESTAMP ELSE finished_at END WHERE run_id = ?",
        )
        .bind(status.as_str())
        .bind(phase)
        .bind(error)
        .bind(status.as_str())
        .bind(status.as_str())
        .bind(run_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_run_plan(
        &self,
        run_id: Uuid,
        plan_json: serde_json::Value,
    ) -> Result<()> {
        let plan_json = json_to_string(Some(&plan_json))?;
        sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_runs SET plan_json = ? WHERE run_id = ?",
        )
        .bind(plan_json)
        .bind(run_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn cancel_pending_runs_by_source(
        &self,
        source: &str,
        error: Option<&str>,
    ) -> Result<u64> {
        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_runs SET status = 'canceled', phase = 'canceled', error = ?, finished_at = CURRENT_TIMESTAMP WHERE source = ? AND status = 'pending'",
        )
        .bind(error)
        .bind(source)
        .execute(self.pool)
        .await?;
        Ok(updated.rows_affected())
    }

    pub async fn list_runs(&self, limit: Option<i64>) -> Result<Vec<OrchestratorRun>> {
        let rows = if let Some(limit) = limit {
            sqlx::query(
                "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs ORDER BY created_at DESC LIMIT ?",
            )
            .bind(limit)
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs ORDER BY created_at DESC",
            )
            .fetch_all(self.pool)
            .await?
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_run(&row)?);
        }
        Ok(items)
    }

    pub async fn delete_run_history(&self) -> Result<u64> {
        let deleted = sqlx::query::<sqlx::Any>(
            "DELETE FROM orchestrator_runs WHERE status IN ('failed', 'completed', 'canceled')",
        )
        .execute(self.pool)
        .await?;
        Ok(deleted.rows_affected())
    }

    pub async fn get_latest_run_by_phase(
        &self,
        phase: &str,
    ) -> Result<Option<OrchestratorRun>> {
        let row = sqlx::query(
            "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs WHERE phase = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(phase)
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_run(&row)).transpose()
    }

    pub async fn get_latest_run_by_source(
        &self,
        source: &str,
        status: Option<OrchestratorRunStatus>,
    ) -> Result<Option<OrchestratorRun>> {
        let row = if let Some(status) = status {
            sqlx::query(
                "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs WHERE source = ? AND status = ? ORDER BY created_at DESC LIMIT 1",
            )
            .bind(source)
            .bind(status.as_str())
            .fetch_optional(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs WHERE source = ? ORDER BY created_at DESC LIMIT 1",
            )
            .bind(source)
            .fetch_optional(self.pool)
            .await?
        };
        row.map(|row| map_run(&row)).transpose()
    }

    pub async fn get_run(&self, run_id: Uuid) -> Result<Option<OrchestratorRun>> {
        let row = sqlx::query(
            "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs WHERE run_id = ? LIMIT 1",
        )
        .bind(run_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_run(&row)).transpose()
    }

    pub async fn create_step(&self, data: &NewOperationStep) -> Result<()> {
        let action_json = json_to_string(data.action_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO operation_steps (step_id, run_id, step_index, action_type, action_json, status, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(data.step_id.to_string())
        .bind(data.run_id.to_string())
        .bind(data.step_index)
        .bind(&data.action_type)
        .bind(action_json)
        .bind(data.status.as_str())
        .bind(data.error.as_deref())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_step_status(
        &self,
        step_id: Uuid,
        status: OperationStepStatus,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE operation_steps SET status = ?, error = ?, started_at = CASE WHEN ? = 'running' AND started_at IS NULL THEN CURRENT_TIMESTAMP ELSE started_at END, finished_at = CASE WHEN ? IN ('failed', 'completed', 'skipped') THEN CURRENT_TIMESTAMP ELSE finished_at END WHERE step_id = ?",
        )
        .bind(status.as_str())
        .bind(error)
        .bind(status.as_str())
        .bind(status.as_str())
        .bind(step_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_steps(&self, run_id: Uuid) -> Result<Vec<OperationStep>> {
        let rows = sqlx::query(
            "SELECT step_id, run_id, step_index, action_type, CAST(action_json AS TEXT) as action_json, status, CAST(error AS TEXT) as error, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at, CAST(created_at AS TEXT) as created_at FROM operation_steps WHERE run_id = ? ORDER BY step_index",
        )
        .bind(run_id.to_string())
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_step(&row)?);
        }
        Ok(items)
    }

    pub async fn create_runtime_log(&self, data: &NewRuntimeLog) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO runtime_logs (log_id, instance_id, log_uri) VALUES (?, ?, ?)",
        )
        .bind(data.log_id.to_string())
        .bind(data.instance_id.to_string())
        .bind(&data.log_uri)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_runtime_logs(&self, instance_id: Uuid) -> Result<Vec<RuntimeLog>> {
        let rows = sqlx::query(
            "SELECT log_id, instance_id, log_uri, CAST(created_at AS TEXT) as created_at FROM runtime_logs WHERE instance_id = ? ORDER BY created_at DESC",
        )
        .bind(instance_id.to_string())
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_runtime_log(&row)?);
        }
        Ok(items)
    }

    pub async fn get_extension_setting(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query(
            "SELECT CAST(value_json AS TEXT) as value_json FROM extension_settings WHERE setting_key = ? LIMIT 1",
        )
        .bind(key)
        .fetch_optional(self.pool)
        .await?;
        match row {
            Some(row) => parse_json_opt(
                row_get_opt_string(&row, "value_json")?,
                "extension_settings.value_json",
            ),
            None => Ok(None),
        }
    }

    pub async fn upsert_extension_setting(
        &self,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<()> {
        let value_json = json_to_string(Some(value))?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_settings (setting_key, value_json) VALUES (?, ?) ON CONFLICT(setting_key) DO UPDATE SET value_json = excluded.value_json, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(key)
        .bind(value_json)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_auto_wire_enabled(&self) -> Result<bool> {
        let value = self.get_extension_setting("auto_wire_enabled").await?;
        Ok(value.and_then(|value| value.as_bool()).unwrap_or(true))
    }

    pub async fn set_auto_wire_enabled(&self, enabled: bool) -> Result<()> {
        self.upsert_extension_setting("auto_wire_enabled", &serde_json::json!(enabled))
            .await
    }
}

fn json_to_string(value: Option<&serde_json::Value>) -> Result<Option<String>> {
    match value {
        Some(value) => Ok(Some(
            serde_json::to_string(value).context("serializing json")?,
        )),
        None => Ok(None),
    }
}

fn map_extension(row: &AnyRow) -> Result<Extension> {
    let extension_id: String = row.try_get("extension_id")?;
    let name: String = row.try_get("name")?;
    let version: String = row.try_get("version")?;
    let kind_raw: String = row.try_get("kind")?;
    let trust_raw: String = row.try_get("trust_level")?;
    let manifest_raw: String = row.try_get("manifest_json")?;
    let installed_at_raw: String = row.try_get("installed_at")?;

    Ok(Extension {
        extension_id,
        name,
        version,
        kind: parse_enum(&kind_raw, "extensions.kind")?,
        publisher_name: row_get_opt_string(row, "publisher_name")?,
        signing_key_id: row_get_opt_string(row, "signing_key_id")?,
        trust_level: parse_enum(&trust_raw, "extensions.trust_level")?,
        manifest_json: parse_json(&manifest_raw, "extensions.manifest_json")?,
        package_hash: row_get_opt_string(row, "package_hash")?,
        installed_at: parse_datetime(&installed_at_raw, "extensions.installed_at")?,
        enabled: row_get_bool(row, "enabled")?,
    })
}

fn map_extension_instance(row: &AnyRow) -> Result<ExtensionInstance> {
    let instance_id_raw: String = row.try_get("instance_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(ExtensionInstance {
        instance_id: parse_uuid(&instance_id_raw, "extension_instances.instance_id")?,
        extension_id: row.try_get("extension_id")?,
        instance_name: row.try_get("instance_name")?,
        config_json: parse_json_opt(
            row_get_opt_string(row, "config_json")?,
            "extension_instances.config_json",
        )?,
        runtime_version: row_get_opt_string(row, "runtime_version")?,
        rollback_version: row_get_opt_string(row, "rollback_version")?,
        created_at: parse_datetime(&created_at_raw, "extension_instances.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "extension_instances.updated_at")?,
        enabled: row_get_bool(row, "enabled")?,
    })
}

fn map_provider(row: &AnyRow) -> Result<Provider> {
    let provider_id_raw: String = row.try_get("provider_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let cardinality_raw: String = row.try_get("cardinality")?;
    let health_raw: String = row.try_get("health_state")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(Provider {
        provider_id: parse_uuid(&provider_id_raw, "providers.provider_id")?,
        instance_id: parse_uuid(&instance_id_raw, "providers.instance_id")?,
        capability: row.try_get("capability")?,
        slot_id: row.try_get("slot_id")?,
        cardinality: parse_enum(&cardinality_raw, "providers.cardinality")?,
        implementation: row_get_opt_string(row, "implementation")?,
        endpoint_json: parse_json_opt(
            row_get_opt_string(row, "endpoint_json")?,
            "providers.endpoint_json",
        )?,
        health_state: parse_enum(&health_raw, "providers.health_state")?,
        last_healthcheck_at: parse_datetime_opt(
            row_get_opt_string(row, "last_healthcheck_at")?,
            "providers.last_healthcheck_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "providers.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "providers.updated_at")?,
    })
}

fn map_provider_detail(row: &AnyRow) -> Result<ProviderDetails> {
    let provider = map_provider(row)?;
    let extension_id: String = row.try_get("extension_id")?;
    let trust_raw: String = row.try_get("trust_level")?;
    Ok(ProviderDetails {
        provider,
        extension_id,
        trust_level: parse_enum(&trust_raw, "extensions.trust_level")?,
    })
}

fn map_binding(row: &AnyRow) -> Result<Binding> {
    let binding_id_raw: String = row.try_get("binding_id")?;
    let consumer_id_raw: String = row.try_get("consumer_provider_id")?;
    let target_id_raw: String = row.try_get("target_provider_id")?;
    let status_raw: String = row.try_get("status")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(Binding {
        binding_id: parse_uuid(&binding_id_raw, "bindings.binding_id")?,
        consumer_provider_id: parse_uuid(&consumer_id_raw, "bindings.consumer_provider_id")?,
        requires_capability: row.try_get("requires_capability")?,
        requires_slot_id: row.try_get("requires_slot_id")?,
        target_provider_id: parse_uuid(&target_id_raw, "bindings.target_provider_id")?,
        binding_params_json: parse_json_opt(
            row_get_opt_string(row, "binding_params_json")?,
            "bindings.binding_params_json",
        )?,
        status: parse_enum(&status_raw, "bindings.status")?,
        last_error: row_get_opt_string(row, "last_error")?,
        last_applied_at: parse_datetime_opt(
            row_get_opt_string(row, "last_applied_at")?,
            "bindings.last_applied_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "bindings.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "bindings.updated_at")?,
    })
}

fn map_desired_blueprint(row: &AnyRow) -> Result<DesiredBlueprint> {
    let desired_id_raw: String = row.try_get("desired_id")?;
    let created_at_raw: String = row.try_get("created_at")?;

    Ok(DesiredBlueprint {
        desired_id: parse_uuid(&desired_id_raw, "desired_blueprints.desired_id")?,
        blueprint_extension_id: row.try_get("blueprint_extension_id")?,
        blueprint_version: row.try_get("blueprint_version")?,
        params_json: parse_json_opt(
            row_get_opt_string(row, "params_json")?,
            "desired_blueprints.params_json",
        )?,
        decisions_json: parse_json_opt(
            row_get_opt_string(row, "decisions_json")?,
            "desired_blueprints.decisions_json",
        )?,
        applied: row_get_bool(row, "applied")?,
        created_at: parse_datetime(&created_at_raw, "desired_blueprints.created_at")?,
        applied_at: parse_datetime_opt(
            row_get_opt_string(row, "applied_at")?,
            "desired_blueprints.applied_at",
        )?,
    })
}

fn map_secret(row: &AnyRow) -> Result<Secret> {
    let secret_id_raw: String = row.try_get("secret_id")?;
    let scope_raw: String = row.try_get("scope")?;
    let created_at_raw: String = row.try_get("created_at")?;

    Ok(Secret {
        secret_id: parse_uuid(&secret_id_raw, "secrets.secret_id")?,
        scope: parse_enum(&scope_raw, "secrets.scope")?,
        scope_id: parse_uuid_opt(row_get_opt_string(row, "scope_id")?, "secrets.scope_id")?,
        key: row.try_get("key")?,
        value_encrypted: row.try_get("value_encrypted")?,
        created_at: parse_datetime(&created_at_raw, "secrets.created_at")?,
        rotatable: row_get_bool(row, "rotatable")?,
    })
}

fn map_run(row: &AnyRow) -> Result<OrchestratorRun> {
    let run_id_raw: String = row.try_get("run_id")?;
    let source_raw: String = row.try_get("source")?;
    let status_raw: String = row.try_get("status")?;
    let created_at_raw: String = row.try_get("created_at")?;

    Ok(OrchestratorRun {
        run_id: parse_uuid(&run_id_raw, "orchestrator_runs.run_id")?,
        source: source_raw,
        status: parse_enum(&status_raw, "orchestrator_runs.status")?,
        phase: row_get_opt_string(row, "phase")?,
        plan_json: parse_json_opt(
            row_get_opt_string(row, "plan_json")?,
            "orchestrator_runs.plan_json",
        )?,
        error: row_get_opt_string(row, "error")?,
        created_at: parse_datetime(&created_at_raw, "orchestrator_runs.created_at")?,
        started_at: parse_datetime_opt(
            row_get_opt_string(row, "started_at")?,
            "orchestrator_runs.started_at",
        )?,
        finished_at: parse_datetime_opt(
            row_get_opt_string(row, "finished_at")?,
            "orchestrator_runs.finished_at",
        )?,
    })
}

fn map_step(row: &AnyRow) -> Result<OperationStep> {
    let step_id_raw: String = row.try_get("step_id")?;
    let run_id_raw: String = row.try_get("run_id")?;
    let status_raw: String = row.try_get("status")?;
    let created_at_raw: String = row.try_get("created_at")?;

    let step_index: i64 = row.try_get("step_index")?;
    let step_index = i32::try_from(step_index)
        .context("operation_steps.step_index overflow")?;

    Ok(OperationStep {
        step_id: parse_uuid(&step_id_raw, "operation_steps.step_id")?,
        run_id: parse_uuid(&run_id_raw, "operation_steps.run_id")?,
        step_index,
        action_type: row.try_get("action_type")?,
        action_json: parse_json_opt(
            row_get_opt_string(row, "action_json")?,
            "operation_steps.action_json",
        )?,
        status: parse_enum(&status_raw, "operation_steps.status")?,
        error: row_get_opt_string(row, "error")?,
        started_at: parse_datetime_opt(
            row_get_opt_string(row, "started_at")?,
            "operation_steps.started_at",
        )?,
        finished_at: parse_datetime_opt(
            row_get_opt_string(row, "finished_at")?,
            "operation_steps.finished_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "operation_steps.created_at")?,
    })
}

fn map_runtime_log(row: &AnyRow) -> Result<RuntimeLog> {
    let log_id_raw: String = row.try_get("log_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let created_at_raw: String = row.try_get("created_at")?;

    Ok(RuntimeLog {
        log_id: parse_uuid(&log_id_raw, "runtime_logs.log_id")?,
        instance_id: parse_uuid(&instance_id_raw, "runtime_logs.instance_id")?,
        log_uri: row.try_get("log_uri")?,
        created_at: parse_datetime(&created_at_raw, "runtime_logs.created_at")?,
    })
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    let details = err.to_string();
    details.contains("UNIQUE") || details.contains("unique")
}

fn parse_enum<T>(value: &str, field: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse::<T>().map_err(|err| {
        anyhow::anyhow!("invalid {field} '{value}': {err}")
    })
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(value).with_context(|| format!("invalid {field} '{value}'"))
}

fn parse_uuid_opt(value: Option<String>, field: &str) -> Result<Option<Uuid>> {
    match value {
        Some(value) => Ok(Some(parse_uuid(&value, field)?)),
        None => Ok(None),
    }
}

fn parse_datetime(value: &str, field: &str) -> Result<DateTime<Utc>> {
    let value = value.trim();
    let parsed = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .with_context(|| format!("invalid {field} '{value}'"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc))
}

fn parse_datetime_opt(value: Option<String>, field: &str) -> Result<Option<DateTime<Utc>>> {
    match value {
        Some(value) => Ok(Some(parse_datetime(&value, field)?)),
        None => Ok(None),
    }
}

fn parse_json(value: &str, field: &str) -> Result<serde_json::Value> {
    serde_json::from_str(value).with_context(|| format!("invalid {field} json"))
}

fn parse_json_opt(
    value: Option<String>,
    field: &str,
) -> Result<Option<serde_json::Value>> {
    match value {
        Some(value) => Ok(Some(parse_json(&value, field)?)),
        None => Ok(None),
    }
}

fn row_get_opt_string(row: &AnyRow, field: &str) -> Result<Option<String>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value))
}

fn row_get_bool(row: &AnyRow, field: &str) -> Result<bool> {
    if let Ok(value) = row.try_get::<bool, _>(field) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<i64, _>(field) {
        return Ok(value != 0);
    }
    if let Ok(value) = row.try_get::<i32, _>(field) {
        return Ok(value != 0);
    }
    let value: String = row
        .try_get(field)
        .with_context(|| format!("missing {field}"))?;
    Ok(matches!(value.as_str(), "1" | "true" | "TRUE"))
}
