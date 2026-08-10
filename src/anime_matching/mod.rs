//! Narrow, versioned boundary for optional local-model anime matching.
//!
//! The deterministic resolvers remain authoritative fast paths and exact
//! fallbacks. This module only assists difficult candidate groups and has no
//! public HTTP or user-configuration surface.

mod bundle;
mod certification;
mod hardware;
mod inference;
mod local_model;
mod model_smoke;
mod profile_probe;
mod service;
mod types;
mod update_channel;

pub use bundle::*;
pub use certification::*;
pub use hardware::*;
pub use inference::*;
pub use local_model::*;
pub use model_smoke::*;
pub use profile_probe::*;
pub use service::*;
pub use types::*;
pub use update_channel::*;
