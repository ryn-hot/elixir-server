use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sqlx::{AnyPool, Row, TypeInfo, Value, ValueRef, any::AnyRow};
use uuid::Uuid;

use crate::acquisition::release_resolution::{
    models::{
        AcquisitionAniDbFileCache, AcquisitionAniDbFileXref, AcquisitionAnimeIdentityMismatch,
        AcquisitionAnimeMatchAttempt, AniDbFileLookupStatus, AnimeEpisodeType, AnimeMatchOutcome,
        AnimeMismatchState, NewAcquisitionAniDbFileCache, NewAcquisitionAniDbFileXref,
        NewAcquisitionAnimeIdentityMismatch, NewAcquisitionAnimeMatchAttempt, ReleaseConfidence,
    },
    store::{
        create_anime_identity_mismatch, create_anime_match_attempt, get_anidb_file_cache,
        upsert_anidb_file_cache, upsert_anidb_file_xref,
    },
};

pub const ANIDB_FILE_PROVIDER_VERSION: &str = "rr3h-anidb-file-cache-provider-v0";
pub const DEFAULT_ANIDB_NEGATIVE_CACHE_TTL_DAYS: i64 = 7;
pub const DEFAULT_ANIDB_BAN_COOLDOWN_MINUTES: i64 = 60;
pub const DEFAULT_ANIDB_DISABLED_CACHE_MINUTES: i64 = 60;
pub const ANIDB_FILE_RECONCILIATION_PROVIDER: &str = "AniDB";
pub const ANIDB_RATE_LIMITER_VERSION: &str = "rr3j-anidb-rate-limiter-v0";
pub const DEFAULT_ANIDB_BASE_DELAY_SECONDS: i64 = 2;
pub const DEFAULT_ANIDB_SUSTAINED_DELAY_SECONDS: i64 = 6;
pub const DEFAULT_ANIDB_SLOW_ACTIVATION_SECONDS: i64 = 10;
pub const DEFAULT_ANIDB_IDLE_RESET_SECONDS: i64 = 120;
pub const DEFAULT_ANIDB_PADDING_MILLIS: i64 = 50;
pub const DEFAULT_ANIDB_UDP_BAN_COOLDOWN_MINUTES: i64 = 90;
pub const DEFAULT_ANIDB_HTTP_BAN_COOLDOWN_HOURS: i64 = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AniDbFileLookupRequest {
    pub ed2k: String,
    pub size_bytes: i64,
    pub force_refresh: bool,
}

impl AniDbFileLookupRequest {
    pub fn normalized(ed2k: impl Into<String>, size_bytes: i64) -> Result<Self> {
        let ed2k = normalize_ed2k(&ed2k.into())?;
        ensure!(size_bytes > 0, "AniDB FILE lookup size must be positive");
        Ok(Self {
            ed2k,
            size_bytes,
            force_refresh: false,
        })
    }

    pub fn lookup_key(&self) -> String {
        build_lookup_key(&self.ed2k, self.size_bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AniDbFileIdentity {
    pub anidb_file_id: i64,
    pub anidb_anime_id: i64,
    pub anidb_episode_ids: Vec<i64>,
    pub anidb_group_id: Option<i64>,
    pub anidb_group_name: Option<String>,
    pub anidb_group_short_name: Option<String>,
    pub anidb_version: Option<i64>,
    pub anidb_source: Option<String>,
    pub anidb_quality: Option<String>,
    pub anidb_audio_languages: Vec<String>,
    pub anidb_subtitle_languages: Vec<String>,
    pub anidb_state_flags: Vec<String>,
    pub anidb_original_filename: Option<String>,
    pub released_at: Option<DateTime<Utc>>,
    pub raw_response: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AniDbFileProviderResponse {
    Hit(AniDbFileIdentity),
    NoSuchFile {
        raw_response: Option<String>,
    },
    Banned {
        retry_after: Option<DateTime<Utc>>,
        raw_response: Option<String>,
    },
    TransportFailed {
        message: String,
        raw_response: Option<String>,
    },
    Disabled {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub struct AniDbFileLookupOutcome {
    pub lookup_key: String,
    pub cache: AcquisitionAniDbFileCache,
    pub cache_hit: bool,
    pub provider_attempted: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AniDbChannel {
    Udp,
    Http,
}

impl AniDbChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Udp => "udp",
            Self::Http => "http",
        }
    }

    pub fn default_ban_cooldown(self, config: &AniDbRateLimiterConfig) -> ChronoDuration {
        match self {
            Self::Udp => config.udp_ban_cooldown,
            Self::Http => config.http_ban_cooldown,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AniDbRateMode {
    Short,
    Sustained,
}

impl AniDbRateMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Short => "short",
            Self::Sustained => "sustained",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AniDbRateLimiterConfig {
    pub base_delay: ChronoDuration,
    pub sustained_delay: ChronoDuration,
    pub slow_activation_window: ChronoDuration,
    pub idle_reset_window: ChronoDuration,
    pub padding: ChronoDuration,
    pub udp_ban_cooldown: ChronoDuration,
    pub http_ban_cooldown: ChronoDuration,
}

impl Default for AniDbRateLimiterConfig {
    fn default() -> Self {
        Self {
            base_delay: ChronoDuration::seconds(DEFAULT_ANIDB_BASE_DELAY_SECONDS),
            sustained_delay: ChronoDuration::seconds(DEFAULT_ANIDB_SUSTAINED_DELAY_SECONDS),
            slow_activation_window: ChronoDuration::seconds(DEFAULT_ANIDB_SLOW_ACTIVATION_SECONDS),
            idle_reset_window: ChronoDuration::seconds(DEFAULT_ANIDB_IDLE_RESET_SECONDS),
            padding: ChronoDuration::milliseconds(DEFAULT_ANIDB_PADDING_MILLIS),
            udp_ban_cooldown: ChronoDuration::minutes(DEFAULT_ANIDB_UDP_BAN_COOLDOWN_MINUTES),
            http_ban_cooldown: ChronoDuration::hours(DEFAULT_ANIDB_HTTP_BAN_COOLDOWN_HOURS),
        }
    }
}

impl AniDbRateLimiterConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.base_delay >= ChronoDuration::seconds(2),
            "AniDB base delay must be at least 2 seconds"
        );
        ensure!(
            self.sustained_delay >= ChronoDuration::seconds(4),
            "AniDB sustained delay must be at least 4 seconds"
        );
        ensure!(
            self.sustained_delay >= self.base_delay,
            "AniDB sustained delay cannot be shorter than base delay"
        );
        ensure!(
            self.slow_activation_window >= self.base_delay * 5,
            "AniDB slow activation window must be at least base delay * 5"
        );
        ensure!(
            self.idle_reset_window >= self.base_delay * 60,
            "AniDB idle reset window must be at least base delay * 60"
        );
        ensure!(
            self.padding >= ChronoDuration::zero(),
            "AniDB limiter padding cannot be negative"
        );
        ensure!(
            self.udp_ban_cooldown >= ChronoDuration::minutes(90),
            "AniDB UDP ban cooldown must be at least 90 minutes"
        );
        ensure!(
            self.http_ban_cooldown >= ChronoDuration::hours(12),
            "AniDB HTTP ban cooldown must be at least 12 hours"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AniDbChannelState {
    pub channel: AniDbChannel,
    pub banned_until: Option<DateTime<Utc>>,
    pub ban_reason: Option<String>,
    pub backoff_until: Option<DateTime<Utc>>,
    pub last_failure_reason: Option<String>,
    pub consecutive_failures: i64,
    pub active_since: Option<DateTime<Utc>>,
    pub last_request_at: Option<DateTime<Utc>>,
    pub request_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AniDbChannelGateDecision {
    Allowed {
        channel: AniDbChannel,
        mode: AniDbRateMode,
        next_request_after: DateTime<Utc>,
    },
    RateLimited {
        channel: AniDbChannel,
        retry_after: DateTime<Utc>,
    },
    Banned {
        channel: AniDbChannel,
        retry_after: DateTime<Utc>,
        reason: Option<String>,
    },
    BackingOff {
        channel: AniDbChannel,
        retry_after: DateTime<Utc>,
        reason: Option<String>,
    },
    Disabled {
        channel: AniDbChannel,
        reason: String,
    },
}

impl AniDbChannelGateDecision {
    pub fn allowed(&self) -> bool {
        matches!(self, Self::Allowed { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AniDbEpisodeXrefSource {
    pub anidb_file_id: Option<i64>,
    pub anidb_anime_id: Option<i64>,
    pub anidb_episode_id: i64,
    pub episode_type: AnimeEpisodeType,
    pub percentage_start: Option<i64>,
    pub percentage_end: Option<i64>,
    pub episode_order: Option<i64>,
    pub provider: String,
    pub confidence: ReleaseConfidence,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AniDbPlannedTargetIdentity {
    pub target_id: Option<Uuid>,
    pub target_key: Option<String>,
    pub title: Option<String>,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub anidb_anime_id: Option<i64>,
    pub anidb_episode_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct AniDbXrefValidationInput {
    pub lookup_key: String,
    pub release_id: Option<Uuid>,
    pub release_file_id: Option<Uuid>,
    pub planned_targets: Vec<AniDbPlannedTargetIdentity>,
    pub sources: Vec<AniDbEpisodeXrefSource>,
}

#[derive(Debug, Clone, Default)]
pub struct AniDbXrefValidationOutcome {
    pub xrefs: Vec<NewAcquisitionAniDbFileXref>,
    pub rejected: Vec<String>,
    pub review_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AniDbFileReconciliationInput {
    pub lookup_key: String,
    pub release_id: Option<Uuid>,
    pub release_file_id: Option<Uuid>,
    pub candidate_fingerprint: Option<String>,
    pub planned_targets: Vec<AniDbPlannedTargetIdentity>,
}

#[derive(Debug, Clone)]
pub struct AniDbFileReconciliationOutcome {
    pub lookup_key: String,
    pub xrefs: Vec<AcquisitionAniDbFileXref>,
    pub match_attempt: AcquisitionAnimeMatchAttempt,
    pub mismatches: Vec<AcquisitionAnimeIdentityMismatch>,
    pub outcome: AnimeMatchOutcome,
    pub review_reasons: Vec<String>,
}

#[async_trait]
pub trait AniDbFileProvider: Send + Sync {
    async fn lookup_file(
        &self,
        request: &AniDbFileLookupRequest,
    ) -> Result<AniDbFileProviderResponse>;
}

#[derive(Debug, Clone)]
pub struct AniDbFileCacheLookupService<P>
where
    P: AniDbFileProvider,
{
    pool: AnyPool,
    provider: Arc<P>,
    in_flight: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl<P> AniDbFileCacheLookupService<P>
where
    P: AniDbFileProvider + 'static,
{
    pub fn new(pool: AnyPool, provider: Arc<P>) -> Self {
        Self {
            pool,
            provider,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn lookup(&self, request: AniDbFileLookupRequest) -> Result<AniDbFileLookupOutcome> {
        let request = normalize_request(request)?;
        let lookup_key = request.lookup_key();

        if !request.force_refresh {
            if let Some(cache) = self.fresh_cache(&lookup_key).await? {
                return Ok(AniDbFileLookupOutcome {
                    lookup_key,
                    cache,
                    cache_hit: true,
                    provider_attempted: false,
                });
            }
        }

        let key_lock = self.lock_for_key(&lookup_key)?;
        let _guard = key_lock.lock().await;

        if !request.force_refresh {
            if let Some(cache) = self.fresh_cache(&lookup_key).await? {
                return Ok(AniDbFileLookupOutcome {
                    lookup_key,
                    cache,
                    cache_hit: true,
                    provider_attempted: false,
                });
            }
        }

        let now = Utc::now();
        let response = match self.provider.lookup_file(&request).await {
            Ok(response) => response,
            Err(error) => AniDbFileProviderResponse::TransportFailed {
                message: error.to_string(),
                raw_response: None,
            },
        };
        let cache = upsert_anidb_file_cache(
            &self.pool,
            cache_from_provider_response(&request, response, now)?,
        )
        .await?;

        Ok(AniDbFileLookupOutcome {
            lookup_key,
            cache,
            cache_hit: false,
            provider_attempted: true,
        })
    }

    async fn fresh_cache(&self, lookup_key: &str) -> Result<Option<AcquisitionAniDbFileCache>> {
        let Some(cache) = get_anidb_file_cache(&self.pool, lookup_key).await? else {
            return Ok(None);
        };

        Ok(is_fresh_cache_entry(&cache, Utc::now()).then_some(cache))
    }

    fn lock_for_key(&self, lookup_key: &str) -> Result<Arc<tokio::sync::Mutex<()>>> {
        let mut locks = self
            .in_flight
            .lock()
            .map_err(|_| anyhow!("AniDB FILE coalescing lock was poisoned"))?;
        Ok(locks
            .entry(lookup_key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AniDbDirectProviderConfig {
    pub enabled: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_name: Option<String>,
    pub client_version: Option<i64>,
    pub udp_limiter_ready: bool,
    pub negative_cache_ready: bool,
    pub duplicate_coalescing_ready: bool,
    pub ban_cooldown_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AniDbDirectProviderGate {
    pub ready: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DirectAniDbFileProvider {
    config: AniDbDirectProviderConfig,
}

impl DirectAniDbFileProvider {
    pub fn new(config: AniDbDirectProviderConfig) -> Self {
        Self { config }
    }

    pub fn gate_status(&self) -> AniDbDirectProviderGate {
        direct_provider_gate_status(&self.config)
    }
}

#[async_trait]
impl AniDbFileProvider for DirectAniDbFileProvider {
    async fn lookup_file(
        &self,
        _request: &AniDbFileLookupRequest,
    ) -> Result<AniDbFileProviderResponse> {
        let gate = self.gate_status();
        if !gate.ready {
            return Ok(AniDbFileProviderResponse::Disabled {
                reason: gate.reasons.join(", "),
            });
        }

        Ok(AniDbFileProviderResponse::Disabled {
            reason: "direct_anidb_udp_network_client_not_implemented".to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct AniDbGatedFileProvider<P>
where
    P: AniDbFileProvider,
{
    pool: AnyPool,
    provider: Arc<P>,
    channel: AniDbChannel,
    direct_config: AniDbDirectProviderConfig,
    limiter_config: AniDbRateLimiterConfig,
}

impl<P> AniDbGatedFileProvider<P>
where
    P: AniDbFileProvider,
{
    pub fn new(
        pool: AnyPool,
        provider: Arc<P>,
        channel: AniDbChannel,
        direct_config: AniDbDirectProviderConfig,
        limiter_config: AniDbRateLimiterConfig,
    ) -> Self {
        Self {
            pool,
            provider,
            channel,
            direct_config,
            limiter_config,
        }
    }
}

#[async_trait]
impl<P> AniDbFileProvider for AniDbGatedFileProvider<P>
where
    P: AniDbFileProvider + 'static,
{
    async fn lookup_file(
        &self,
        request: &AniDbFileLookupRequest,
    ) -> Result<AniDbFileProviderResponse> {
        let direct_gate = direct_provider_gate_status(&self.direct_config);
        if !direct_gate.ready {
            return Ok(AniDbFileProviderResponse::Disabled {
                reason: direct_gate.reasons.join(", "),
            });
        }

        match reserve_anidb_channel_request(
            &self.pool,
            self.channel,
            &self.limiter_config,
            Utc::now(),
        )
        .await?
        {
            AniDbChannelGateDecision::Allowed { .. } => {
                let response = self.provider.lookup_file(request).await?;
                match &response {
                    AniDbFileProviderResponse::Banned {
                        retry_after,
                        raw_response,
                    } => {
                        pause_anidb_channel_for_ban_until(
                            &self.pool,
                            self.channel,
                            retry_after.unwrap_or_else(|| {
                                Utc::now() + self.channel.default_ban_cooldown(&self.limiter_config)
                            }),
                            raw_response
                                .as_deref()
                                .unwrap_or("provider returned banned"),
                        )
                        .await?;
                    }
                    AniDbFileProviderResponse::TransportFailed { message, .. } => {
                        record_anidb_channel_transport_failure(
                            &self.pool,
                            self.channel,
                            message,
                            Utc::now(),
                        )
                        .await?;
                    }
                    _ => {}
                }
                Ok(response)
            }
            AniDbChannelGateDecision::RateLimited { retry_after, .. } => {
                Ok(AniDbFileProviderResponse::Disabled {
                    reason: format!(
                        "anidb_{}_rate_limited_until:{}",
                        self.channel.as_str(),
                        retry_after.to_rfc3339()
                    ),
                })
            }
            AniDbChannelGateDecision::Banned {
                retry_after,
                reason,
                ..
            } => Ok(AniDbFileProviderResponse::Disabled {
                reason: format!(
                    "anidb_{}_banned_until:{}:{}",
                    self.channel.as_str(),
                    retry_after.to_rfc3339(),
                    reason.unwrap_or_else(|| "banned".to_string())
                ),
            }),
            AniDbChannelGateDecision::BackingOff {
                retry_after,
                reason,
                ..
            } => Ok(AniDbFileProviderResponse::Disabled {
                reason: format!(
                    "anidb_{}_backoff_until:{}:{}",
                    self.channel.as_str(),
                    retry_after.to_rfc3339(),
                    reason.unwrap_or_else(|| "backoff".to_string())
                ),
            }),
            AniDbChannelGateDecision::Disabled { reason, .. } => {
                Ok(AniDbFileProviderResponse::Disabled { reason })
            }
        }
    }
}

pub fn direct_provider_gate_status(config: &AniDbDirectProviderConfig) -> AniDbDirectProviderGate {
    let mut reasons = Vec::new();
    if !config.enabled {
        reasons.push("direct_anidb_disabled".to_string());
    }
    if blank(config.username.as_deref()) {
        reasons.push("missing_username".to_string());
    }
    if blank(config.password.as_deref()) {
        reasons.push("missing_password".to_string());
    }
    if blank(config.client_name.as_deref()) {
        reasons.push("missing_client_name".to_string());
    }
    if config.client_version.unwrap_or_default() <= 0 {
        reasons.push("missing_client_version".to_string());
    }
    if !config.udp_limiter_ready {
        reasons.push("udp_limiter_not_ready".to_string());
    }
    if !config.negative_cache_ready {
        reasons.push("negative_cache_not_ready".to_string());
    }
    if !config.duplicate_coalescing_ready {
        reasons.push("duplicate_coalescing_not_ready".to_string());
    }
    if !config.ban_cooldown_ready {
        reasons.push("ban_cooldown_not_ready".to_string());
    }

    AniDbDirectProviderGate {
        ready: reasons.is_empty(),
        reasons,
    }
}

pub async fn get_anidb_channel_state(
    pool: &AnyPool,
    channel: AniDbChannel,
) -> Result<AniDbChannelState> {
    ensure_anidb_channel_state(pool, channel).await
}

pub async fn reserve_anidb_channel_request(
    pool: &AnyPool,
    channel: AniDbChannel,
    config: &AniDbRateLimiterConfig,
    now: DateTime<Utc>,
) -> Result<AniDbChannelGateDecision> {
    if let Err(err) = config.validate() {
        return Ok(AniDbChannelGateDecision::Disabled {
            channel,
            reason: format!("unsafe_anidb_limiter_config:{err}"),
        });
    }

    let state = ensure_anidb_channel_state(pool, channel).await?;
    if let Some(banned_until) = state.banned_until
        && banned_until > now
    {
        return Ok(AniDbChannelGateDecision::Banned {
            channel,
            retry_after: banned_until,
            reason: state.ban_reason,
        });
    }
    if let Some(backoff_until) = state.backoff_until
        && backoff_until > now
    {
        return Ok(AniDbChannelGateDecision::BackingOff {
            channel,
            retry_after: backoff_until,
            reason: state.last_failure_reason,
        });
    }

    let active_since = effective_active_since(&state, config, now);
    let mode = rate_mode(active_since, config, now);
    if let Some(last_request_at) = state.last_request_at {
        let retry_after = last_request_at + request_delay(mode, config);
        if retry_after > now {
            return Ok(AniDbChannelGateDecision::RateLimited {
                channel,
                retry_after,
            });
        }
    }

    let next_request_after = now + request_delay(mode, config);
    update_anidb_channel_request(pool, channel, active_since, now, state.request_count + 1).await?;
    Ok(AniDbChannelGateDecision::Allowed {
        channel,
        mode,
        next_request_after,
    })
}

pub async fn anidb_channel_gate_status(
    pool: &AnyPool,
    channel: AniDbChannel,
    config: &AniDbRateLimiterConfig,
    now: DateTime<Utc>,
) -> Result<AniDbChannelGateDecision> {
    if let Err(err) = config.validate() {
        return Ok(AniDbChannelGateDecision::Disabled {
            channel,
            reason: format!("unsafe_anidb_limiter_config:{err}"),
        });
    }

    let state = ensure_anidb_channel_state(pool, channel).await?;
    if let Some(banned_until) = state.banned_until
        && banned_until > now
    {
        return Ok(AniDbChannelGateDecision::Banned {
            channel,
            retry_after: banned_until,
            reason: state.ban_reason,
        });
    }
    if let Some(backoff_until) = state.backoff_until
        && backoff_until > now
    {
        return Ok(AniDbChannelGateDecision::BackingOff {
            channel,
            retry_after: backoff_until,
            reason: state.last_failure_reason,
        });
    }
    let active_since = effective_active_since(&state, config, now);
    let mode = rate_mode(active_since, config, now);
    if let Some(last_request_at) = state.last_request_at {
        let retry_after = last_request_at + request_delay(mode, config);
        if retry_after > now {
            return Ok(AniDbChannelGateDecision::RateLimited {
                channel,
                retry_after,
            });
        }
    }

    Ok(AniDbChannelGateDecision::Allowed {
        channel,
        mode,
        next_request_after: now + request_delay(mode, config),
    })
}

pub async fn pause_anidb_channel_for_ban(
    pool: &AnyPool,
    channel: AniDbChannel,
    config: &AniDbRateLimiterConfig,
    reason: impl AsRef<str>,
    now: DateTime<Utc>,
) -> Result<AniDbChannelState> {
    config.validate()?;
    pause_anidb_channel_for_ban_until(
        pool,
        channel,
        now + channel.default_ban_cooldown(config),
        reason,
    )
    .await
}

pub async fn pause_anidb_channel_for_ban_until(
    pool: &AnyPool,
    channel: AniDbChannel,
    banned_until: DateTime<Utc>,
    reason: impl AsRef<str>,
) -> Result<AniDbChannelState> {
    ensure_anidb_channel_state(pool, channel).await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_anidb_channel_state
         SET banned_until = $1,
             ban_reason = $2,
             backoff_until = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE channel = $3",
    )
    .bind(db_datetime_string(banned_until))
    .bind(reason.as_ref().trim())
    .bind(channel.as_str())
    .execute(pool)
    .await
    .context("pausing AniDB channel for ban")?;
    ensure_anidb_channel_state(pool, channel).await
}

pub async fn record_anidb_channel_transport_failure(
    pool: &AnyPool,
    channel: AniDbChannel,
    reason: impl AsRef<str>,
    now: DateTime<Utc>,
) -> Result<AniDbChannelState> {
    let state = ensure_anidb_channel_state(pool, channel).await?;
    let failures = (state.consecutive_failures + 1).max(1);
    let backoff_until = now + transport_failure_backoff(failures);
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_anidb_channel_state
         SET backoff_until = $1,
             last_failure_reason = $2,
             consecutive_failures = $3,
             updated_at = CURRENT_TIMESTAMP
         WHERE channel = $4",
    )
    .bind(db_datetime_string(backoff_until))
    .bind(reason.as_ref().trim())
    .bind(failures)
    .bind(channel.as_str())
    .execute(pool)
    .await
    .context("recording AniDB channel transport failure")?;
    ensure_anidb_channel_state(pool, channel).await
}

pub async fn clear_anidb_channel_pause(
    pool: &AnyPool,
    channel: AniDbChannel,
) -> Result<AniDbChannelState> {
    ensure_anidb_channel_state(pool, channel).await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_anidb_channel_state
         SET banned_until = NULL,
             ban_reason = NULL,
             backoff_until = NULL,
             last_failure_reason = NULL,
             consecutive_failures = 0,
             updated_at = CURRENT_TIMESTAMP
         WHERE channel = $1",
    )
    .bind(channel.as_str())
    .execute(pool)
    .await
    .context("clearing AniDB channel pause")?;
    ensure_anidb_channel_state(pool, channel).await
}

pub fn validate_anidb_file_xrefs(
    input: AniDbXrefValidationInput,
) -> Result<AniDbXrefValidationOutcome> {
    let lookup_key = input.lookup_key.trim().to_string();
    ensure!(!lookup_key.is_empty(), "AniDB xref lookup_key is required");

    let target_by_episode = input
        .planned_targets
        .iter()
        .filter_map(|target| {
            target
                .anidb_episode_id
                .map(|episode_id| (episode_id, target))
        })
        .collect::<HashMap<_, _>>();
    let mut rejected = Vec::new();
    let mut review_reasons = Vec::new();
    let mut dedupe = BTreeSet::new();
    let mut candidates = Vec::<NewAcquisitionAniDbFileXref>::new();

    for (source_index, source) in input.sources.into_iter().enumerate() {
        let source_label = format!("source[{source_index}]");
        if source.anidb_episode_id <= 0 {
            rejected.push(format!(
                "{source_label}:non_positive_episode_id:{}",
                source.anidb_episode_id
            ));
            continue;
        }
        let Some(anidb_anime_id) = source.anidb_anime_id.filter(|value| *value > 0) else {
            rejected.push(format!(
                "{source_label}:missing_or_non_positive_anime_id:{}",
                source.anidb_episode_id
            ));
            continue;
        };
        let Some((percentage_start, percentage_end)) =
            normalize_percentage_range(source.percentage_start, source.percentage_end)
        else {
            rejected.push(format!(
                "{source_label}:zero_length_percentage_range:{}",
                source.anidb_episode_id
            ));
            continue;
        };

        let provider = if source.provider.trim().is_empty() {
            ANIDB_FILE_RECONCILIATION_PROVIDER.to_string()
        } else {
            source.provider.trim().to_string()
        };
        let episode_order = source
            .episode_order
            .unwrap_or_else(|| i64::try_from(source_index).unwrap_or(i64::MAX));
        let dedupe_key = (
            anidb_anime_id,
            source.anidb_episode_id,
            percentage_start,
            percentage_end,
        );
        if !dedupe.insert(dedupe_key) {
            review_reasons.push(format!(
                "duplicate_xref:{}:{}:{}-{}",
                anidb_anime_id, source.anidb_episode_id, percentage_start, percentage_end
            ));
            continue;
        }
        let created_from_target_id = target_by_episode
            .get(&source.anidb_episode_id)
            .and_then(|target| target.target_id);
        candidates.push(NewAcquisitionAniDbFileXref {
            xref_id: None,
            lookup_key: lookup_key.clone(),
            release_file_id: input.release_file_id,
            anidb_file_id: source.anidb_file_id.filter(|value| *value > 0),
            anidb_anime_id,
            anidb_episode_id: source.anidb_episode_id,
            episode_type: source.episode_type,
            percentage_start,
            percentage_end,
            episode_order,
            provider,
            confidence: source.confidence,
            is_manual_override: false,
            created_from_release_id: input.release_id,
            created_from_target_id,
        });
    }

    let partial_episode_keys = candidates
        .iter()
        .filter(|xref| xref.percentage_start != 0 || xref.percentage_end != 100)
        .map(|xref| (xref.anidb_anime_id, xref.anidb_episode_id))
        .collect::<BTreeSet<_>>();
    if !partial_episode_keys.is_empty() {
        let before = candidates.len();
        candidates.retain(|xref| {
            !partial_episode_keys.contains(&(xref.anidb_anime_id, xref.anidb_episode_id))
                || xref.percentage_start != 0
                || xref.percentage_end != 100
        });
        if candidates.len() != before {
            review_reasons.push("full_file_xrefs_replaced_by_partial_ranges".to_string());
        }
    }

    candidates.sort_by_key(|xref| {
        (
            xref.episode_order,
            xref.anidb_anime_id,
            xref.anidb_episode_id,
            xref.percentage_start,
            xref.percentage_end,
        )
    });

    Ok(AniDbXrefValidationOutcome {
        xrefs: candidates,
        rejected,
        review_reasons,
    })
}

pub async fn reconcile_anidb_file_identity(
    pool: &AnyPool,
    input: AniDbFileReconciliationInput,
) -> Result<AniDbFileReconciliationOutcome> {
    let lookup_key = input.lookup_key.trim().to_string();
    ensure!(
        !lookup_key.is_empty(),
        "AniDB reconciliation lookup_key is required"
    );

    let cache = get_anidb_file_cache(pool, &lookup_key)
        .await?
        .ok_or_else(|| anyhow!("AniDB FILE cache entry '{lookup_key}' was not found"))?;
    if cache.lookup_status != AniDbFileLookupStatus::Hit {
        let reason = format!("anidb_file_cache_status:{}", cache.lookup_status.as_str());
        let attempt = create_anime_match_attempt(
            pool,
            NewAcquisitionAnimeMatchAttempt {
                match_attempt_id: None,
                release_id: input.release_id,
                release_file_id: input.release_file_id,
                attempted_providers: json!(["local_anidb_file_cache"]),
                selected_provider: None,
                ed2k: Some(cache.ed2k.clone()),
                size_bytes: Some(cache.size_bytes),
                candidate_fingerprint: input.candidate_fingerprint.clone(),
                planned_targets: json!(input.planned_targets),
                verified_targets: json!([]),
                outcome: AnimeMatchOutcome::Deferred,
                rejection_reason: Some(reason.clone()),
            },
        )
        .await?;
        return Ok(AniDbFileReconciliationOutcome {
            lookup_key,
            xrefs: Vec::new(),
            match_attempt: attempt,
            mismatches: Vec::new(),
            outcome: AnimeMatchOutcome::Deferred,
            review_reasons: vec![reason],
        });
    }

    let sources = xref_sources_from_cache(&cache);
    let validation = validate_anidb_file_xrefs(AniDbXrefValidationInput {
        lookup_key: lookup_key.clone(),
        release_id: input.release_id,
        release_file_id: input.release_file_id,
        planned_targets: input.planned_targets.clone(),
        sources,
    })?;

    let mut review_reasons = validation.review_reasons.clone();
    if !validation.rejected.is_empty() {
        review_reasons.push("anidb_xref_validation_rejections".to_string());
    }

    let mut xrefs = Vec::with_capacity(validation.xrefs.len());
    for xref in validation.xrefs {
        xrefs.push(upsert_anidb_file_xref(pool, xref).await?);
    }

    if xrefs.is_empty() {
        let reason = if validation.rejected.is_empty() {
            "no_anidb_xrefs".to_string()
        } else {
            format!("no_valid_anidb_xrefs:{}", validation.rejected.join(","))
        };
        let attempt = create_anime_match_attempt(
            pool,
            NewAcquisitionAnimeMatchAttempt {
                match_attempt_id: None,
                release_id: input.release_id,
                release_file_id: input.release_file_id,
                attempted_providers: json!(["local_anidb_file_cache"]),
                selected_provider: Some(ANIDB_FILE_RECONCILIATION_PROVIDER.to_string()),
                ed2k: Some(cache.ed2k.clone()),
                size_bytes: Some(cache.size_bytes),
                candidate_fingerprint: input.candidate_fingerprint.clone(),
                planned_targets: json!(input.planned_targets),
                verified_targets: json!([]),
                outcome: AnimeMatchOutcome::NoMatch,
                rejection_reason: Some(reason.clone()),
            },
        )
        .await?;
        return Ok(AniDbFileReconciliationOutcome {
            lookup_key,
            xrefs,
            match_attempt: attempt,
            mismatches: Vec::new(),
            outcome: AnimeMatchOutcome::NoMatch,
            review_reasons: vec![reason],
        });
    }

    let planned_episode_ids = input
        .planned_targets
        .iter()
        .filter_map(|target| target.anidb_episode_id)
        .collect::<BTreeSet<_>>();
    let mut mismatches = Vec::new();

    if planned_episode_ids.is_empty() {
        review_reasons.push("missing_planned_anidb_episode_id".to_string());
    } else {
        for planned in &input.planned_targets {
            let Some(expected_episode_id) = planned.anidb_episode_id else {
                review_reasons.push("planned_target_without_anidb_episode_id".to_string());
                continue;
            };
            let episode_match = xrefs
                .iter()
                .find(|xref| xref.anidb_episode_id == expected_episode_id);
            let anime_id_matches = episode_match.is_some_and(|xref| {
                planned
                    .anidb_anime_id
                    .map(|anime_id| anime_id == xref.anidb_anime_id)
                    .unwrap_or(true)
            });
            if episode_match.is_none() || !anime_id_matches {
                mismatches.push(
                    create_anime_identity_mismatch(
                        pool,
                        NewAcquisitionAnimeIdentityMismatch {
                            mismatch_id: None,
                            release_id: input.release_id,
                            release_file_id: input.release_file_id,
                            target_id: planned.target_id,
                            planned_target: json!(planned),
                            verified_identity: verified_identity_json(&lookup_key, &xrefs),
                            provider: ANIDB_FILE_RECONCILIATION_PROVIDER.to_string(),
                            confidence: ReleaseConfidence::High,
                            state: AnimeMismatchState::Open,
                            reason: Some("hash_identity_disagrees_with_plan".to_string()),
                        },
                    )
                    .await?,
                );
            }
        }

        if mismatches.is_empty() {
            for extra in xrefs
                .iter()
                .filter(|xref| !planned_episode_ids.contains(&xref.anidb_episode_id))
            {
                mismatches.push(
                    create_anime_identity_mismatch(
                        pool,
                        NewAcquisitionAnimeIdentityMismatch {
                            mismatch_id: None,
                            release_id: input.release_id,
                            release_file_id: input.release_file_id,
                            target_id: None,
                            planned_target: json!({
                                "plannedAnidbEpisodeIds": planned_episode_ids,
                                "plannedTargets": input.planned_targets,
                            }),
                            verified_identity: xref_identity_json(&lookup_key, extra),
                            provider: ANIDB_FILE_RECONCILIATION_PROVIDER.to_string(),
                            confidence: ReleaseConfidence::High,
                            state: AnimeMismatchState::Open,
                            reason: Some("verified_episode_not_in_planned_targets".to_string()),
                        },
                    )
                    .await?,
                );
            }
        }
    }

    let outcome = if !mismatches.is_empty() {
        AnimeMatchOutcome::Mismatch
    } else if planned_episode_ids.is_empty() {
        AnimeMatchOutcome::Deferred
    } else {
        AnimeMatchOutcome::Verified
    };
    let rejection_reason = match outcome {
        AnimeMatchOutcome::Mismatch => Some("hash_identity_disagrees_with_plan".to_string()),
        AnimeMatchOutcome::Deferred => Some("missing_planned_anidb_episode_id".to_string()),
        _ => None,
    };
    let attempt = create_anime_match_attempt(
        pool,
        NewAcquisitionAnimeMatchAttempt {
            match_attempt_id: None,
            release_id: input.release_id,
            release_file_id: input.release_file_id,
            attempted_providers: json!(["local_anidb_file_cache"]),
            selected_provider: Some(ANIDB_FILE_RECONCILIATION_PROVIDER.to_string()),
            ed2k: Some(cache.ed2k),
            size_bytes: Some(cache.size_bytes),
            candidate_fingerprint: input.candidate_fingerprint,
            planned_targets: json!(input.planned_targets),
            verified_targets: verified_identity_json(&lookup_key, &xrefs),
            outcome,
            rejection_reason,
        },
    )
    .await?;

    Ok(AniDbFileReconciliationOutcome {
        lookup_key,
        xrefs,
        match_attempt: attempt,
        mismatches,
        outcome,
        review_reasons,
    })
}

fn xref_sources_from_cache(cache: &AcquisitionAniDbFileCache) -> Vec<AniDbEpisodeXrefSource> {
    let episode_ids = cache
        .anidb_episode_ids
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| json_i64(&value))
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    if episode_ids.is_empty() {
        return Vec::new();
    }

    let total = i64::try_from(episode_ids.len()).unwrap_or(1).max(1);
    episode_ids
        .into_iter()
        .enumerate()
        .map(|(index, episode_id)| {
            let order = i64::try_from(index).unwrap_or(i64::MAX);
            let (percentage_start, percentage_end, confidence) = if total == 1 {
                (Some(0), Some(100), ReleaseConfidence::High)
            } else {
                (
                    Some((order * 100) / total),
                    Some(((order + 1) * 100) / total),
                    ReleaseConfidence::Medium,
                )
            };
            AniDbEpisodeXrefSource {
                anidb_file_id: cache.anidb_file_id,
                anidb_anime_id: cache.anidb_anime_id,
                anidb_episode_id: episode_id,
                episode_type: AnimeEpisodeType::Normal,
                percentage_start,
                percentage_end,
                episode_order: Some(order),
                provider: ANIDB_FILE_RECONCILIATION_PROVIDER.to_string(),
                confidence,
            }
        })
        .collect()
}

async fn ensure_anidb_channel_state(
    pool: &AnyPool,
    channel: AniDbChannel,
) -> Result<AniDbChannelState> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_anidb_channel_state (channel)
         VALUES ($1)
         ON CONFLICT(channel) DO NOTHING",
    )
    .bind(channel.as_str())
    .execute(pool)
    .await
    .context("creating AniDB channel state")?;

    let row = sqlx::query(
        "SELECT channel,
                CAST(banned_until AS TEXT) AS banned_until,
                ban_reason,
                CAST(backoff_until AS TEXT) AS backoff_until,
                last_failure_reason,
                consecutive_failures,
                CAST(active_since AS TEXT) AS active_since,
                CAST(last_request_at AS TEXT) AS last_request_at,
                request_count
         FROM acquisition_anidb_channel_state
         WHERE channel = $1
         LIMIT 1",
    )
    .bind(channel.as_str())
    .fetch_optional(pool)
    .await?;

    row.map(|row| map_anidb_channel_state(&row))
        .transpose()?
        .ok_or_else(|| anyhow!("AniDB channel state was not readable"))
}

fn map_anidb_channel_state(row: &AnyRow) -> Result<AniDbChannelState> {
    let channel_raw: String = row.try_get("channel")?;
    Ok(AniDbChannelState {
        channel: parse_anidb_channel(&channel_raw)?,
        banned_until: row_datetime_opt(row, "banned_until")?,
        ban_reason: row_get_opt_string(row, "ban_reason")?,
        backoff_until: row_datetime_opt(row, "backoff_until")?,
        last_failure_reason: row_get_opt_string(row, "last_failure_reason")?,
        consecutive_failures: row.try_get("consecutive_failures")?,
        active_since: row_datetime_opt(row, "active_since")?,
        last_request_at: row_datetime_opt(row, "last_request_at")?,
        request_count: row.try_get("request_count")?,
    })
}

async fn update_anidb_channel_request(
    pool: &AnyPool,
    channel: AniDbChannel,
    active_since: DateTime<Utc>,
    request_at: DateTime<Utc>,
    request_count: i64,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_anidb_channel_state
         SET active_since = $1,
             last_request_at = $2,
             request_count = $3,
             backoff_until = NULL,
             last_failure_reason = NULL,
             consecutive_failures = 0,
             updated_at = CURRENT_TIMESTAMP
         WHERE channel = $4",
    )
    .bind(db_datetime_string(active_since))
    .bind(db_datetime_string(request_at))
    .bind(request_count)
    .bind(channel.as_str())
    .execute(pool)
    .await
    .context("updating AniDB channel request state")?;
    Ok(())
}

fn effective_active_since(
    state: &AniDbChannelState,
    config: &AniDbRateLimiterConfig,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    let idle_reset = state
        .last_request_at
        .map(|last_request_at| now - last_request_at >= config.idle_reset_window)
        .unwrap_or(true);
    if idle_reset {
        now
    } else {
        state.active_since.unwrap_or(now)
    }
}

fn rate_mode(
    active_since: DateTime<Utc>,
    config: &AniDbRateLimiterConfig,
    now: DateTime<Utc>,
) -> AniDbRateMode {
    if now - active_since >= config.slow_activation_window {
        AniDbRateMode::Sustained
    } else {
        AniDbRateMode::Short
    }
}

fn request_delay(mode: AniDbRateMode, config: &AniDbRateLimiterConfig) -> ChronoDuration {
    match mode {
        AniDbRateMode::Short => config.base_delay + config.padding,
        AniDbRateMode::Sustained => config.sustained_delay + config.padding,
    }
}

fn transport_failure_backoff(consecutive_failures: i64) -> ChronoDuration {
    match consecutive_failures {
        0 | 1 => ChronoDuration::seconds(30),
        2 => ChronoDuration::minutes(2),
        3 => ChronoDuration::minutes(5),
        4 => ChronoDuration::minutes(10),
        5 => ChronoDuration::minutes(30),
        _ => ChronoDuration::hours(2),
    }
}

fn parse_anidb_channel(value: &str) -> Result<AniDbChannel> {
    match value.trim().to_ascii_lowercase().as_str() {
        "udp" => Ok(AniDbChannel::Udp),
        "http" => Ok(AniDbChannel::Http),
        other => Err(anyhow!("unknown AniDB channel '{other}'")),
    }
}

fn row_datetime_opt(row: &AnyRow, column: &str) -> Result<Option<DateTime<Utc>>> {
    row_get_opt_string(row, column)?
        .filter(|value| !value.trim().is_empty())
        .map(|value| parse_datetime(&value))
        .transpose()
}

fn row_get_opt_string(row: &AnyRow, field: &str) -> Result<Option<String>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value))
}

fn db_datetime_string(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

fn normalize_percentage_range(
    percentage_start: Option<i64>,
    percentage_end: Option<i64>,
) -> Option<(i64, i64)> {
    let mut start = percentage_start.unwrap_or(0);
    let mut end = percentage_end.unwrap_or(100);
    if start > end {
        std::mem::swap(&mut start, &mut end);
    }
    start = start.clamp(0, 100);
    end = end.clamp(0, 100);
    (start < end).then_some((start, end))
}

fn verified_identity_json(lookup_key: &str, xrefs: &[AcquisitionAniDbFileXref]) -> JsonValue {
    json!({
        "lookupKey": lookup_key,
        "xrefs": xrefs.iter().map(|xref| xref_identity_json(lookup_key, xref)).collect::<Vec<_>>(),
    })
}

fn xref_identity_json(lookup_key: &str, xref: &AcquisitionAniDbFileXref) -> JsonValue {
    json!({
        "lookupKey": lookup_key,
        "xrefId": xref.xref_id,
        "releaseFileId": xref.release_file_id,
        "anidbFileId": xref.anidb_file_id,
        "anidbAnimeId": xref.anidb_anime_id,
        "anidbEpisodeId": xref.anidb_episode_id,
        "episodeType": xref.episode_type.as_str(),
        "percentageStart": xref.percentage_start,
        "percentageEnd": xref.percentage_end,
        "episodeOrder": xref.episode_order,
        "provider": xref.provider,
        "confidence": xref.confidence.as_str(),
        "createdFromTargetId": xref.created_from_target_id,
    })
}

pub fn parse_anidb_file_response(raw: &str) -> Result<AniDbFileProviderResponse> {
    let trimmed = raw.trim();
    ensure!(!trimmed.is_empty(), "AniDB FILE response is empty");

    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("320") || upper.contains("NO SUCH FILE") || upper.contains("NO_SUCH_FILE")
    {
        return Ok(AniDbFileProviderResponse::NoSuchFile {
            raw_response: Some(trimmed.to_string()),
        });
    }
    if upper.starts_with("555") || upper.contains("BANNED") {
        return Ok(AniDbFileProviderResponse::Banned {
            retry_after: None,
            raw_response: Some(trimmed.to_string()),
        });
    }
    if upper.starts_with("598") || upper.starts_with("600") || upper.contains("NOT LOGGED IN") {
        return Ok(AniDbFileProviderResponse::Disabled {
            reason: trimmed.to_string(),
        });
    }
    if !(upper.starts_with("220") || trimmed.contains("fid=") || trimmed.contains("file_id=")) {
        return Ok(AniDbFileProviderResponse::TransportFailed {
            message: format!("unexpected AniDB FILE response code: {trimmed}"),
            raw_response: Some(trimmed.to_string()),
        });
    }

    Ok(AniDbFileProviderResponse::Hit(parse_file_identity(
        trimmed,
    )?))
}

fn parse_file_identity(raw: &str) -> Result<AniDbFileIdentity> {
    let fields = parse_response_fields(raw);
    let file_id = parse_required_i64(&fields, &["fid", "file_id", "fileid"])?;
    let anime_id = parse_required_i64(&fields, &["aid", "anime_id", "animeid"])?;
    let episode_ids = parse_i64_list(
        fields
            .get("eids")
            .or_else(|| fields.get("episodes"))
            .or_else(|| fields.get("episode_ids"))
            .or_else(|| fields.get("eid"))
            .map(String::as_str)
            .unwrap_or_default(),
    );
    ensure!(
        !episode_ids.is_empty(),
        "AniDB FILE response did not contain episode ids"
    );

    Ok(AniDbFileIdentity {
        anidb_file_id: file_id,
        anidb_anime_id: anime_id,
        anidb_episode_ids: episode_ids,
        anidb_group_id: parse_optional_i64(&fields, &["gid", "group_id", "groupid"]),
        anidb_group_name: first_nonblank(&fields, &["group", "group_name", "groupname"]),
        anidb_group_short_name: first_nonblank(
            &fields,
            &["group_short", "group_short_name", "short_group"],
        ),
        anidb_version: parse_optional_i64(&fields, &["version", "v"]),
        anidb_source: first_nonblank(&fields, &["source"]),
        anidb_quality: first_nonblank(&fields, &["quality"]),
        anidb_audio_languages: parse_string_list(
            fields
                .get("audio")
                .or_else(|| fields.get("audio_languages"))
                .map(String::as_str)
                .unwrap_or_default(),
        ),
        anidb_subtitle_languages: parse_string_list(
            fields
                .get("subs")
                .or_else(|| fields.get("subtitles"))
                .or_else(|| fields.get("subtitle_languages"))
                .map(String::as_str)
                .unwrap_or_default(),
        ),
        anidb_state_flags: parse_string_list(
            fields
                .get("state")
                .or_else(|| fields.get("state_flags"))
                .map(String::as_str)
                .unwrap_or_default(),
        ),
        anidb_original_filename: first_nonblank(&fields, &["filename", "original_filename"]),
        released_at: first_nonblank(&fields, &["released", "released_at", "date"])
            .and_then(|value| parse_datetime(&value).ok()),
        raw_response: Some(raw.to_string()),
    })
}

pub fn build_lookup_key(ed2k: &str, size_bytes: i64) -> String {
    format!("{}:{size_bytes}", ed2k.trim().to_ascii_lowercase())
}

pub fn normalize_ed2k(value: &str) -> Result<String> {
    let normalized = value.trim().to_ascii_lowercase();
    ensure!(
        normalized.len() == 32 && normalized.chars().all(|ch| ch.is_ascii_hexdigit()),
        "AniDB FILE lookup requires a 32-character ED2K hex digest"
    );
    Ok(normalized)
}

fn normalize_request(mut request: AniDbFileLookupRequest) -> Result<AniDbFileLookupRequest> {
    request.ed2k = normalize_ed2k(&request.ed2k)?;
    ensure!(
        request.size_bytes > 0,
        "AniDB FILE lookup size must be positive"
    );
    Ok(request)
}

fn is_fresh_cache_entry(cache: &AcquisitionAniDbFileCache, now: DateTime<Utc>) -> bool {
    match cache.lookup_status {
        AniDbFileLookupStatus::Hit => true,
        AniDbFileLookupStatus::NoSuchFile
        | AniDbFileLookupStatus::Banned
        | AniDbFileLookupStatus::Disabled => cache
            .negative_cached_until
            .map(|expires_at| expires_at > now)
            .unwrap_or(false),
        AniDbFileLookupStatus::TransportFailed | AniDbFileLookupStatus::Pending => false,
    }
}

fn cache_from_provider_response(
    request: &AniDbFileLookupRequest,
    response: AniDbFileProviderResponse,
    now: DateTime<Utc>,
) -> Result<NewAcquisitionAniDbFileCache> {
    let lookup_key = request.lookup_key();
    let base = |lookup_status| NewAcquisitionAniDbFileCache {
        lookup_key: lookup_key.clone(),
        ed2k: request.ed2k.clone(),
        size_bytes: request.size_bytes,
        lookup_status,
        anidb_file_id: None,
        anidb_anime_id: None,
        anidb_episode_ids: json!([]),
        anidb_group_id: None,
        anidb_group_name: None,
        anidb_group_short_name: None,
        anidb_version: None,
        anidb_source: None,
        anidb_quality: None,
        anidb_audio_languages: json!([]),
        anidb_subtitle_languages: json!([]),
        anidb_state_flags: json!([]),
        anidb_original_filename: None,
        released_at: None,
        raw_response: None,
        positive_cached_at: None,
        negative_cached_until: None,
        last_lookup_attempt_at: Some(now),
    };

    match response {
        AniDbFileProviderResponse::Hit(identity) => {
            ensure!(
                identity.anidb_file_id > 0 && identity.anidb_anime_id > 0,
                "AniDB FILE hit requires positive file and anime ids"
            );
            ensure!(
                !identity.anidb_episode_ids.is_empty(),
                "AniDB FILE hit requires at least one episode id"
            );
            Ok(NewAcquisitionAniDbFileCache {
                lookup_status: AniDbFileLookupStatus::Hit,
                anidb_file_id: Some(identity.anidb_file_id),
                anidb_anime_id: Some(identity.anidb_anime_id),
                anidb_episode_ids: json!(identity.anidb_episode_ids),
                anidb_group_id: identity.anidb_group_id,
                anidb_group_name: identity.anidb_group_name,
                anidb_group_short_name: identity.anidb_group_short_name,
                anidb_version: identity.anidb_version,
                anidb_source: identity.anidb_source,
                anidb_quality: identity.anidb_quality,
                anidb_audio_languages: json!(identity.anidb_audio_languages),
                anidb_subtitle_languages: json!(identity.anidb_subtitle_languages),
                anidb_state_flags: json!(identity.anidb_state_flags),
                anidb_original_filename: identity.anidb_original_filename,
                released_at: identity.released_at,
                raw_response: identity.raw_response,
                positive_cached_at: Some(now),
                ..base(AniDbFileLookupStatus::Hit)
            })
        }
        AniDbFileProviderResponse::NoSuchFile { raw_response } => {
            Ok(NewAcquisitionAniDbFileCache {
                lookup_status: AniDbFileLookupStatus::NoSuchFile,
                raw_response,
                negative_cached_until: Some(
                    now + ChronoDuration::days(DEFAULT_ANIDB_NEGATIVE_CACHE_TTL_DAYS),
                ),
                ..base(AniDbFileLookupStatus::NoSuchFile)
            })
        }
        AniDbFileProviderResponse::Banned {
            retry_after,
            raw_response,
        } => Ok(NewAcquisitionAniDbFileCache {
            lookup_status: AniDbFileLookupStatus::Banned,
            raw_response,
            negative_cached_until: Some(
                retry_after
                    .unwrap_or(now + ChronoDuration::minutes(DEFAULT_ANIDB_BAN_COOLDOWN_MINUTES)),
            ),
            ..base(AniDbFileLookupStatus::Banned)
        }),
        AniDbFileProviderResponse::TransportFailed {
            message,
            raw_response,
        } => Ok(NewAcquisitionAniDbFileCache {
            lookup_status: AniDbFileLookupStatus::TransportFailed,
            raw_response: raw_response.or(Some(message)),
            ..base(AniDbFileLookupStatus::TransportFailed)
        }),
        AniDbFileProviderResponse::Disabled { reason } => Ok(NewAcquisitionAniDbFileCache {
            lookup_status: AniDbFileLookupStatus::Disabled,
            raw_response: Some(reason),
            negative_cached_until: Some(
                now + ChronoDuration::minutes(DEFAULT_ANIDB_DISABLED_CACHE_MINUTES),
            ),
            ..base(AniDbFileLookupStatus::Disabled)
        }),
    }
}

fn parse_response_fields(raw: &str) -> HashMap<String, String> {
    let payload = raw
        .trim()
        .strip_prefix("220 FILE")
        .unwrap_or(raw.trim())
        .trim_start_matches('|')
        .trim();

    payload
        .split('|')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((
                key.trim().to_ascii_lowercase(),
                value.trim().trim_matches('"').to_string(),
            ))
        })
        .collect()
}

fn parse_required_i64(fields: &HashMap<String, String>, keys: &[&str]) -> Result<i64> {
    parse_optional_i64(fields, keys)
        .ok_or_else(|| anyhow!("AniDB FILE response is missing {}", keys.join("/")))
}

fn parse_optional_i64(fields: &HashMap<String, String>, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| fields.get(*key))
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
}

fn first_nonblank(fields: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| fields.get(*key))
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_i64_list(value: &str) -> Vec<i64> {
    split_list(value)
        .into_iter()
        .filter_map(|item| item.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .collect()
}

fn json_i64(value: &JsonValue) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
}

fn parse_string_list(value: &str) -> Vec<String> {
    split_list(value)
        .into_iter()
        .map(|item| item.to_ascii_lowercase())
        .collect()
}

fn split_list(value: &str) -> Vec<String> {
    value
        .split([',', ';', '+'])
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_datetime(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(datetime) = DateTime::parse_from_rfc3339(value) {
        return Ok(datetime.with_timezone(&Utc));
    }
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("parsing AniDB FILE release date '{value}'"))?;
    Ok(date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("invalid AniDB FILE release date '{value}'"))?
        .and_utc())
}

fn blank(value: Option<&str>) -> bool {
    value.map(str::trim).unwrap_or_default().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::DatabaseConfig, db::Database};
    use anyhow::bail;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct RecordingProvider {
        response: AniDbFileProviderResponse,
        calls: AtomicUsize,
    }

    impl RecordingProvider {
        fn new(response: AniDbFileProviderResponse) -> Self {
            Self {
                response,
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl AniDbFileProvider for RecordingProvider {
        async fn lookup_file(
            &self,
            _request: &AniDbFileLookupRequest,
        ) -> Result<AniDbFileProviderResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.response.clone())
        }
    }

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    fn sample_identity() -> AniDbFileIdentity {
        AniDbFileIdentity {
            anidb_file_id: 100,
            anidb_anime_id: 200,
            anidb_episode_ids: vec![300],
            anidb_group_id: Some(400),
            anidb_group_name: Some("Group Name".to_string()),
            anidb_group_short_name: Some("GRP".to_string()),
            anidb_version: Some(2),
            anidb_source: Some("Web".to_string()),
            anidb_quality: Some("1080p".to_string()),
            anidb_audio_languages: vec!["japanese".to_string()],
            anidb_subtitle_languages: vec!["english".to_string()],
            anidb_state_flags: vec!["crc_match".to_string()],
            anidb_original_filename: Some("Anime - 01.mkv".to_string()),
            released_at: parse_datetime("2026-05-14").ok(),
            raw_response: Some("220 FILE|fid=100|aid=200|eids=300".to_string()),
        }
    }

    fn sample_request() -> AniDbFileLookupRequest {
        AniDbFileLookupRequest::normalized("0123456789ABCDEF0123456789ABCDEF", 1234)
            .expect("valid request")
    }

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 14, 12, 0, 0)
            .single()
            .expect("valid test datetime")
    }

    fn ready_direct_config() -> AniDbDirectProviderConfig {
        AniDbDirectProviderConfig {
            enabled: true,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            client_name: Some("elixir".to_string()),
            client_version: Some(1),
            udp_limiter_ready: true,
            negative_cache_ready: true,
            duplicate_coalescing_ready: true,
            ban_cooldown_ready: true,
        }
    }

    async fn store_hit_cache(
        database: &Database,
        lookup_key: &str,
        anidb_anime_id: i64,
        anidb_episode_ids: Vec<i64>,
    ) -> Result<AcquisitionAniDbFileCache> {
        let (ed2k, size_bytes) = lookup_key
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid lookup key"))?;
        upsert_anidb_file_cache(
            &database.pool,
            NewAcquisitionAniDbFileCache {
                lookup_key: lookup_key.to_string(),
                ed2k: ed2k.to_string(),
                size_bytes: size_bytes.parse()?,
                lookup_status: AniDbFileLookupStatus::Hit,
                anidb_file_id: Some(100),
                anidb_anime_id: Some(anidb_anime_id),
                anidb_episode_ids: json!(anidb_episode_ids),
                anidb_group_id: Some(400),
                anidb_group_name: Some("Group Name".to_string()),
                anidb_group_short_name: Some("GRP".to_string()),
                anidb_version: Some(1),
                anidb_source: Some("Web".to_string()),
                anidb_quality: Some("1080p".to_string()),
                anidb_audio_languages: json!(["japanese"]),
                anidb_subtitle_languages: json!(["english"]),
                anidb_state_flags: json!(["crc_match"]),
                anidb_original_filename: Some("Anime - 01.mkv".to_string()),
                released_at: None,
                raw_response: Some("220 FILE".to_string()),
                positive_cached_at: Some(Utc::now()),
                negative_cached_until: None,
                last_lookup_attempt_at: Some(Utc::now()),
            },
        )
        .await
    }

    fn planned_target(anidb_episode_id: Option<i64>) -> AniDbPlannedTargetIdentity {
        AniDbPlannedTargetIdentity {
            target_id: None,
            target_key: Some("anime:abs:1".to_string()),
            title: Some("Anime".to_string()),
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            anidb_anime_id: Some(200),
            anidb_episode_id,
        }
    }

    #[test]
    fn xref_validation_normalizes_ranges_dedupes_and_prefers_partials() -> Result<()> {
        let target_id = Uuid::new_v4();
        let outcome = validate_anidb_file_xrefs(AniDbXrefValidationInput {
            lookup_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1234".to_string(),
            release_id: None,
            release_file_id: None,
            planned_targets: vec![AniDbPlannedTargetIdentity {
                target_id: Some(target_id),
                anidb_anime_id: Some(200),
                anidb_episode_id: Some(30),
                ..Default::default()
            }],
            sources: vec![
                AniDbEpisodeXrefSource {
                    anidb_file_id: Some(100),
                    anidb_anime_id: Some(200),
                    anidb_episode_id: 30,
                    episode_type: AnimeEpisodeType::Normal,
                    percentage_start: Some(0),
                    percentage_end: Some(100),
                    episode_order: Some(0),
                    provider: "AniDB".to_string(),
                    confidence: ReleaseConfidence::High,
                },
                AniDbEpisodeXrefSource {
                    anidb_file_id: Some(100),
                    anidb_anime_id: Some(200),
                    anidb_episode_id: 30,
                    episode_type: AnimeEpisodeType::Normal,
                    percentage_start: Some(75),
                    percentage_end: Some(25),
                    episode_order: Some(1),
                    provider: "AniDB".to_string(),
                    confidence: ReleaseConfidence::High,
                },
                AniDbEpisodeXrefSource {
                    anidb_file_id: Some(100),
                    anidb_anime_id: Some(200),
                    anidb_episode_id: 30,
                    episode_type: AnimeEpisodeType::Normal,
                    percentage_start: Some(25),
                    percentage_end: Some(75),
                    episode_order: Some(2),
                    provider: "AniDB".to_string(),
                    confidence: ReleaseConfidence::High,
                },
                AniDbEpisodeXrefSource {
                    anidb_file_id: Some(100),
                    anidb_anime_id: Some(200),
                    anidb_episode_id: 0,
                    episode_type: AnimeEpisodeType::Normal,
                    percentage_start: Some(0),
                    percentage_end: Some(100),
                    episode_order: Some(3),
                    provider: "AniDB".to_string(),
                    confidence: ReleaseConfidence::High,
                },
                AniDbEpisodeXrefSource {
                    anidb_file_id: Some(100),
                    anidb_anime_id: Some(200),
                    anidb_episode_id: 31,
                    episode_type: AnimeEpisodeType::Normal,
                    percentage_start: Some(50),
                    percentage_end: Some(50),
                    episode_order: Some(4),
                    provider: "AniDB".to_string(),
                    confidence: ReleaseConfidence::High,
                },
            ],
        })?;

        assert_eq!(outcome.xrefs.len(), 1);
        assert_eq!(outcome.xrefs[0].anidb_episode_id, 30);
        assert_eq!(outcome.xrefs[0].percentage_start, 25);
        assert_eq!(outcome.xrefs[0].percentage_end, 75);
        assert_eq!(outcome.xrefs[0].created_from_target_id, Some(target_id));
        assert!(
            outcome
                .review_reasons
                .contains(&"full_file_xrefs_replaced_by_partial_ranges".to_string())
        );
        assert!(
            outcome
                .rejected
                .iter()
                .any(|reason| reason.contains("non_positive_episode_id"))
        );
        assert!(
            outcome
                .rejected
                .iter()
                .any(|reason| reason.contains("zero_length_percentage_range"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_persists_verified_attempt_and_xrefs() -> Result<()> {
        let database = setup_db().await?;
        let lookup_key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1234";
        store_hit_cache(&database, lookup_key, 200, vec![30]).await?;

        let outcome = reconcile_anidb_file_identity(
            &database.pool,
            AniDbFileReconciliationInput {
                lookup_key: lookup_key.to_string(),
                release_id: None,
                release_file_id: None,
                candidate_fingerprint: Some("candidate:1".to_string()),
                planned_targets: vec![planned_target(Some(30))],
            },
        )
        .await?;

        assert_eq!(outcome.outcome, AnimeMatchOutcome::Verified);
        assert_eq!(outcome.match_attempt.outcome, AnimeMatchOutcome::Verified);
        assert_eq!(outcome.xrefs.len(), 1);
        assert_eq!(outcome.xrefs[0].anidb_episode_id, 30);
        assert!(outcome.mismatches.is_empty());
        assert_eq!(
            outcome.match_attempt.verified_targets["xrefs"][0]["anidbEpisodeId"],
            30
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_records_mismatch_for_wrong_planned_episode() -> Result<()> {
        let database = setup_db().await?;
        let lookup_key = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1234";
        store_hit_cache(&database, lookup_key, 200, vec![999]).await?;

        let outcome = reconcile_anidb_file_identity(
            &database.pool,
            AniDbFileReconciliationInput {
                lookup_key: lookup_key.to_string(),
                release_id: None,
                release_file_id: None,
                candidate_fingerprint: Some("candidate:2".to_string()),
                planned_targets: vec![planned_target(Some(30))],
            },
        )
        .await?;

        assert_eq!(outcome.outcome, AnimeMatchOutcome::Mismatch);
        assert_eq!(outcome.match_attempt.outcome, AnimeMatchOutcome::Mismatch);
        assert_eq!(outcome.mismatches.len(), 1);
        assert_eq!(outcome.mismatches[0].state, AnimeMismatchState::Open);
        assert_eq!(
            outcome.mismatches[0].reason.as_deref(),
            Some("hash_identity_disagrees_with_plan")
        );
        assert_eq!(
            outcome.mismatches[0].verified_identity["xrefs"][0]["anidbEpisodeId"],
            999
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_defers_when_planned_target_lacks_anidb_episode_id() -> Result<()> {
        let database = setup_db().await?;
        let lookup_key = "cccccccccccccccccccccccccccccccc:1234";
        store_hit_cache(&database, lookup_key, 200, vec![30]).await?;

        let outcome = reconcile_anidb_file_identity(
            &database.pool,
            AniDbFileReconciliationInput {
                lookup_key: lookup_key.to_string(),
                release_id: None,
                release_file_id: None,
                candidate_fingerprint: Some("candidate:3".to_string()),
                planned_targets: vec![planned_target(None)],
            },
        )
        .await?;

        assert_eq!(outcome.outcome, AnimeMatchOutcome::Deferred);
        assert_eq!(outcome.xrefs.len(), 1);
        assert!(outcome.mismatches.is_empty());
        assert!(
            outcome
                .review_reasons
                .contains(&"missing_planned_anidb_episode_id".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn rate_limiter_uses_short_then_sustained_spacing() -> Result<()> {
        let database = setup_db().await?;
        let config = AniDbRateLimiterConfig::default();
        let now = fixed_now();

        let first =
            reserve_anidb_channel_request(&database.pool, AniDbChannel::Udp, &config, now).await?;
        assert_eq!(
            first,
            AniDbChannelGateDecision::Allowed {
                channel: AniDbChannel::Udp,
                mode: AniDbRateMode::Short,
                next_request_after: now
                    + ChronoDuration::seconds(DEFAULT_ANIDB_BASE_DELAY_SECONDS)
                    + ChronoDuration::milliseconds(DEFAULT_ANIDB_PADDING_MILLIS),
            }
        );

        let early = reserve_anidb_channel_request(
            &database.pool,
            AniDbChannel::Udp,
            &config,
            now + ChronoDuration::seconds(1),
        )
        .await?;
        assert!(matches!(
            early,
            AniDbChannelGateDecision::RateLimited {
                channel: AniDbChannel::Udp,
                ..
            }
        ));

        let sustained_at = now + ChronoDuration::seconds(DEFAULT_ANIDB_SLOW_ACTIVATION_SECONDS + 2);
        let sustained =
            reserve_anidb_channel_request(&database.pool, AniDbChannel::Udp, &config, sustained_at)
                .await?;
        assert_eq!(
            sustained,
            AniDbChannelGateDecision::Allowed {
                channel: AniDbChannel::Udp,
                mode: AniDbRateMode::Sustained,
                next_request_after: sustained_at
                    + ChronoDuration::seconds(DEFAULT_ANIDB_SUSTAINED_DELAY_SECONDS)
                    + ChronoDuration::milliseconds(DEFAULT_ANIDB_PADDING_MILLIS),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn idle_reset_returns_limiter_to_short_spacing() -> Result<()> {
        let database = setup_db().await?;
        let config = AniDbRateLimiterConfig::default();
        let now = fixed_now();

        reserve_anidb_channel_request(&database.pool, AniDbChannel::Udp, &config, now).await?;
        let after_idle = now + ChronoDuration::seconds(DEFAULT_ANIDB_IDLE_RESET_SECONDS + 1);
        let decision =
            reserve_anidb_channel_request(&database.pool, AniDbChannel::Udp, &config, after_idle)
                .await?;

        assert!(matches!(
            decision,
            AniDbChannelGateDecision::Allowed {
                mode: AniDbRateMode::Short,
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unsafe_limiter_config_disables_gated_provider() -> Result<()> {
        let database = setup_db().await?;
        let provider = Arc::new(RecordingProvider::new(AniDbFileProviderResponse::Hit(
            sample_identity(),
        )));
        let unsafe_config = AniDbRateLimiterConfig {
            base_delay: ChronoDuration::seconds(1),
            ..Default::default()
        };
        let gated = AniDbGatedFileProvider::new(
            database.pool.clone(),
            provider.clone(),
            AniDbChannel::Udp,
            ready_direct_config(),
            unsafe_config,
        );

        let response = gated.lookup_file(&sample_request()).await?;

        assert!(matches!(
            response,
            AniDbFileProviderResponse::Disabled { ref reason }
                if reason.contains("unsafe_anidb_limiter_config")
        ));
        assert_eq!(provider.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn ban_pause_blocks_udp_without_touching_http() -> Result<()> {
        let database = setup_db().await?;
        let config = AniDbRateLimiterConfig::default();
        let now = Utc::now();
        pause_anidb_channel_for_ban(&database.pool, AniDbChannel::Udp, &config, "test ban", now)
            .await?;
        let provider = Arc::new(RecordingProvider::new(AniDbFileProviderResponse::Hit(
            sample_identity(),
        )));
        let gated = AniDbGatedFileProvider::new(
            database.pool.clone(),
            provider.clone(),
            AniDbChannel::Udp,
            ready_direct_config(),
            config.clone(),
        );

        let udp_response = gated.lookup_file(&sample_request()).await?;
        assert!(matches!(
            udp_response,
            AniDbFileProviderResponse::Disabled { ref reason }
                if reason.contains("anidb_udp_banned_until")
        ));
        assert_eq!(provider.calls(), 0);

        let http_decision =
            reserve_anidb_channel_request(&database.pool, AniDbChannel::Http, &config, now).await?;
        assert!(matches!(
            http_decision,
            AniDbChannelGateDecision::Allowed {
                channel: AniDbChannel::Http,
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn udp_and_http_cooldowns_are_independent() -> Result<()> {
        let database = setup_db().await?;
        let config = AniDbRateLimiterConfig::default();
        let now = fixed_now();

        let udp =
            pause_anidb_channel_for_ban(&database.pool, AniDbChannel::Udp, &config, "udp ban", now)
                .await?;
        let http = pause_anidb_channel_for_ban(
            &database.pool,
            AniDbChannel::Http,
            &config,
            "http ban",
            now,
        )
        .await?;

        assert_eq!(
            udp.banned_until,
            Some(now + ChronoDuration::minutes(DEFAULT_ANIDB_UDP_BAN_COOLDOWN_MINUTES))
        );
        assert_eq!(
            http.banned_until,
            Some(now + ChronoDuration::hours(DEFAULT_ANIDB_HTTP_BAN_COOLDOWN_HOURS))
        );
        Ok(())
    }

    #[tokio::test]
    async fn provider_ban_response_pauses_channel_and_blocks_next_call() -> Result<()> {
        let database = setup_db().await?;
        let config = AniDbRateLimiterConfig::default();
        let retry_after = Utc::now() + ChronoDuration::minutes(90);
        let provider = Arc::new(RecordingProvider::new(AniDbFileProviderResponse::Banned {
            retry_after: Some(retry_after),
            raw_response: Some("555 BANNED".to_string()),
        }));
        let gated = AniDbGatedFileProvider::new(
            database.pool.clone(),
            provider.clone(),
            AniDbChannel::Udp,
            ready_direct_config(),
            config.clone(),
        );

        let first = gated.lookup_file(&sample_request()).await?;
        let state = get_anidb_channel_state(&database.pool, AniDbChannel::Udp).await?;
        let second = gated.lookup_file(&sample_request()).await?;

        assert!(matches!(first, AniDbFileProviderResponse::Banned { .. }));
        assert_eq!(state.ban_reason.as_deref(), Some("555 BANNED"));
        assert!(state.banned_until.is_some_and(|value| value >= retry_after));
        assert!(matches!(
            second,
            AniDbFileProviderResponse::Disabled { ref reason }
                if reason.contains("anidb_udp_banned_until")
        ));
        assert_eq!(provider.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn transport_failure_sets_backoff_and_blocks_next_call() -> Result<()> {
        let database = setup_db().await?;
        let config = AniDbRateLimiterConfig::default();
        let provider = Arc::new(RecordingProvider::new(
            AniDbFileProviderResponse::TransportFailed {
                message: "udp timeout".to_string(),
                raw_response: None,
            },
        ));
        let gated = AniDbGatedFileProvider::new(
            database.pool.clone(),
            provider.clone(),
            AniDbChannel::Udp,
            ready_direct_config(),
            config.clone(),
        );

        let first = gated.lookup_file(&sample_request()).await?;
        let status =
            anidb_channel_gate_status(&database.pool, AniDbChannel::Udp, &config, Utc::now())
                .await?;
        let second = gated.lookup_file(&sample_request()).await?;

        assert!(matches!(
            first,
            AniDbFileProviderResponse::TransportFailed { .. }
        ));
        assert!(matches!(
            status,
            AniDbChannelGateDecision::BackingOff {
                channel: AniDbChannel::Udp,
                ..
            }
        ));
        assert!(matches!(
            second,
            AniDbFileProviderResponse::Disabled { ref reason }
                if reason.contains("anidb_udp_backoff_until")
        ));
        assert_eq!(provider.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn negative_cache_suppresses_gated_provider_calls() -> Result<()> {
        let database = setup_db().await?;
        let request = sample_request();
        upsert_anidb_file_cache(
            &database.pool,
            NewAcquisitionAniDbFileCache {
                lookup_key: request.lookup_key(),
                ed2k: request.ed2k.clone(),
                size_bytes: request.size_bytes,
                lookup_status: AniDbFileLookupStatus::NoSuchFile,
                anidb_file_id: None,
                anidb_anime_id: None,
                anidb_episode_ids: json!([]),
                anidb_group_id: None,
                anidb_group_name: None,
                anidb_group_short_name: None,
                anidb_version: None,
                anidb_source: None,
                anidb_quality: None,
                anidb_audio_languages: json!([]),
                anidb_subtitle_languages: json!([]),
                anidb_state_flags: json!([]),
                anidb_original_filename: None,
                released_at: None,
                raw_response: Some("320 NO SUCH FILE".to_string()),
                positive_cached_at: None,
                negative_cached_until: Some(Utc::now() + ChronoDuration::days(7)),
                last_lookup_attempt_at: Some(Utc::now()),
            },
        )
        .await?;
        let provider = Arc::new(RecordingProvider::new(AniDbFileProviderResponse::Hit(
            sample_identity(),
        )));
        let gated = Arc::new(AniDbGatedFileProvider::new(
            database.pool.clone(),
            provider.clone(),
            AniDbChannel::Udp,
            ready_direct_config(),
            AniDbRateLimiterConfig::default(),
        ));
        let service = AniDbFileCacheLookupService::new(database.pool.clone(), gated);

        let outcome = service.lookup(request).await?;

        assert!(outcome.cache_hit);
        assert!(!outcome.provider_attempted);
        assert_eq!(provider.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn hit_response_is_cached_and_reused_without_provider() -> Result<()> {
        let database = setup_db().await?;
        let provider = Arc::new(RecordingProvider::new(AniDbFileProviderResponse::Hit(
            sample_identity(),
        )));
        let service = AniDbFileCacheLookupService::new(database.pool.clone(), provider.clone());

        let first = service.lookup(sample_request()).await?;
        let second = service.lookup(sample_request()).await?;

        assert!(!first.cache_hit);
        assert!(first.provider_attempted);
        assert_eq!(first.cache.lookup_status, AniDbFileLookupStatus::Hit);
        assert_eq!(first.cache.anidb_file_id, Some(100));
        assert_eq!(first.cache.anidb_episode_ids, json!([300]));
        assert!(second.cache_hit);
        assert!(!second.provider_attempted);
        assert_eq!(provider.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn no_such_file_uses_negative_cache() -> Result<()> {
        let database = setup_db().await?;
        let provider = Arc::new(RecordingProvider::new(
            AniDbFileProviderResponse::NoSuchFile {
                raw_response: Some("320 NO SUCH FILE".to_string()),
            },
        ));
        let service = AniDbFileCacheLookupService::new(database.pool.clone(), provider.clone());

        let first = service.lookup(sample_request()).await?;
        let second = service.lookup(sample_request()).await?;

        assert_eq!(first.cache.lookup_status, AniDbFileLookupStatus::NoSuchFile);
        assert!(first.cache.negative_cached_until.is_some());
        assert!(second.cache_hit);
        assert_eq!(provider.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn banned_response_sets_cooldown_cache() -> Result<()> {
        let database = setup_db().await?;
        let retry_after = Utc::now() + ChronoDuration::minutes(30);
        let provider = Arc::new(RecordingProvider::new(AniDbFileProviderResponse::Banned {
            retry_after: Some(retry_after),
            raw_response: Some("555 BANNED".to_string()),
        }));
        let service = AniDbFileCacheLookupService::new(database.pool.clone(), provider.clone());

        let first = service.lookup(sample_request()).await?;
        let second = service.lookup(sample_request()).await?;

        assert_eq!(first.cache.lookup_status, AniDbFileLookupStatus::Banned);
        assert!(first.cache.negative_cached_until.is_some());
        assert!(
            first.cache.negative_cached_until.unwrap() >= retry_after - ChronoDuration::seconds(1)
        );
        assert!(second.cache_hit);
        assert_eq!(provider.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn transport_failure_is_stored_without_positive_cache() -> Result<()> {
        let database = setup_db().await?;
        let provider = Arc::new(RecordingProvider::new(
            AniDbFileProviderResponse::TransportFailed {
                message: "udp timeout".to_string(),
                raw_response: None,
            },
        ));
        let service = AniDbFileCacheLookupService::new(database.pool.clone(), provider.clone());

        let outcome = service.lookup(sample_request()).await?;

        assert_eq!(
            outcome.cache.lookup_status,
            AniDbFileLookupStatus::TransportFailed
        );
        assert!(outcome.cache.positive_cached_at.is_none());
        assert!(outcome.cache.negative_cached_until.is_none());
        assert_eq!(outcome.cache.raw_response.as_deref(), Some("udp timeout"));
        assert_eq!(provider.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_concurrent_lookup_coalesces_provider_call() -> Result<()> {
        let database = setup_db().await?;
        let provider = Arc::new(RecordingProvider::new(AniDbFileProviderResponse::Hit(
            sample_identity(),
        )));
        let service = Arc::new(AniDbFileCacheLookupService::new(
            database.pool.clone(),
            provider.clone(),
        ));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let service = service.clone();
            tasks.push(tokio::spawn(async move {
                service.lookup(sample_request()).await
            }));
        }
        for task in tasks {
            let outcome = task.await??;
            assert_eq!(outcome.cache.lookup_status, AniDbFileLookupStatus::Hit);
        }

        assert_eq!(provider.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn direct_provider_disabled_when_gates_are_incomplete() -> Result<()> {
        let database = setup_db().await?;
        let provider = Arc::new(DirectAniDbFileProvider::new(AniDbDirectProviderConfig {
            enabled: true,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            client_name: Some("elixir".to_string()),
            client_version: Some(1),
            udp_limiter_ready: false,
            negative_cache_ready: true,
            duplicate_coalescing_ready: true,
            ban_cooldown_ready: true,
        }));
        let service = AniDbFileCacheLookupService::new(database.pool.clone(), provider.clone());

        let gate = provider.gate_status();
        assert!(!gate.ready);
        assert!(gate.reasons.contains(&"udp_limiter_not_ready".to_string()));

        let outcome = service.lookup(sample_request()).await?;
        assert_eq!(outcome.cache.lookup_status, AniDbFileLookupStatus::Disabled);
        assert!(
            outcome
                .cache
                .raw_response
                .as_deref()
                .unwrap_or_default()
                .contains("udp_limiter_not_ready")
        );
        Ok(())
    }

    #[tokio::test]
    async fn direct_provider_is_disabled_until_network_client_is_explicitly_implemented()
    -> Result<()> {
        let provider = DirectAniDbFileProvider::new(ready_direct_config());

        let response = provider.lookup_file(&sample_request()).await?;

        assert!(matches!(
            response,
            AniDbFileProviderResponse::Disabled { ref reason }
                if reason == "direct_anidb_udp_network_client_not_implemented"
        ));
        Ok(())
    }

    #[test]
    fn parses_udp_file_payload_into_identity() -> Result<()> {
        let raw = "220 FILE|fid=100|aid=200|eids=300,301|gid=400|group=Group Name|group_short=GRP|version=2|source=Web|quality=1080p|audio=Japanese,English|subs=English|state=crc_match+uncensored|filename=Anime - 01.mkv|released=2026-05-14";
        let AniDbFileProviderResponse::Hit(identity) = parse_anidb_file_response(raw)? else {
            bail!("expected FILE hit");
        };

        assert_eq!(identity.anidb_file_id, 100);
        assert_eq!(identity.anidb_anime_id, 200);
        assert_eq!(identity.anidb_episode_ids, vec![300, 301]);
        assert_eq!(identity.anidb_group_id, Some(400));
        assert_eq!(identity.anidb_group_short_name.as_deref(), Some("GRP"));
        assert_eq!(identity.anidb_audio_languages, vec!["japanese", "english"]);
        assert_eq!(identity.anidb_state_flags, vec!["crc_match", "uncensored"]);
        assert_eq!(
            identity.anidb_original_filename.as_deref(),
            Some("Anime - 01.mkv")
        );
        assert!(identity.released_at.is_some());
        Ok(())
    }

    #[test]
    fn parses_no_such_file_banned_and_transport_statuses() -> Result<()> {
        assert!(matches!(
            parse_anidb_file_response("320 NO SUCH FILE")?,
            AniDbFileProviderResponse::NoSuchFile { .. }
        ));
        assert!(matches!(
            parse_anidb_file_response("555 BANNED")?,
            AniDbFileProviderResponse::Banned { .. }
        ));
        assert!(matches!(
            parse_anidb_file_response("500 INTERNAL SERVER ERROR")?,
            AniDbFileProviderResponse::TransportFailed { .. }
        ));
        Ok(())
    }
}
