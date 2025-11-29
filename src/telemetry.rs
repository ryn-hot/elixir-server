use anyhow::{Context, Result};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::TelemetryConfig;

pub fn init_tracing(config: &TelemetryConfig) -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.log_directives))?;

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true).compact();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .try_init()
        .context("failed to install tracing subscriber")?;

    Ok(())
}
