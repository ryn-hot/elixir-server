//! Live relay boundary.

mod coalesce;
pub mod hls;
mod service;

pub(crate) use service::LiveRemuxSource;
pub use service::{
    LiveRelayBuildError, LiveRelayError, LiveRelayPayload, LiveRelayPayloadBody, LiveRelayService,
};
