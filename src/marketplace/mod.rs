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
mod observation_time;
pub mod packet;
mod packet_client;
pub mod preferences;
mod preparation;
#[cfg(unix)]
pub mod provider;
#[cfg(unix)]
mod provider_client;
mod provider_lookup;
pub(crate) mod provider_setup;
#[cfg(unix)]
mod provider_state;
pub mod quarantine;
pub mod runtime;
pub mod search;
#[cfg(test)]
mod search_timing;
pub mod security_state;
mod selected_source;
pub mod service;
#[cfg(unix)]
mod sharing;
pub mod startup;
pub mod storage;
#[cfg(unix)]
mod updates;
pub(crate) mod withdrawal;
pub mod worker;
