//! Server-owned marketplace integration. No network or contribution is enabled
//! merely by compiling this module; the production controller is unfinished.
pub mod accounting;
#[cfg(unix)]
mod application;
#[cfg(unix)]
pub mod application_egress;
#[cfg(unix)]
pub mod archive;
mod blob_download;
pub mod client;
pub mod connectivity;
pub mod direct_import;
mod durability;
pub mod grant_store;
pub mod grants;
pub(crate) mod host_observation;
#[cfg(unix)]
pub mod installed_startup;
mod lease;
mod lifecycle;
#[cfg(unix)]
mod local_http;
pub mod manifest;
#[cfg(all(test, unix))]
pub(in crate::marketplace) mod network_lab;
pub mod packet;
mod packet_client;
pub mod preferences;
pub(crate) mod provider_setup;
mod preparation;
#[cfg(unix)]
pub mod provider;
#[cfg(unix)]
mod provider_client;
mod provider_lookup;
#[cfg(unix)]
mod provider_state;
pub mod quarantine;
pub mod runtime;
pub mod search;
pub mod security_state;
mod selected_source;
pub mod service;
#[cfg(unix)]
mod sharing;
pub mod startup;
pub mod storage;
#[cfg(unix)]
mod updates;
pub mod worker;
pub(crate) mod withdrawal;
