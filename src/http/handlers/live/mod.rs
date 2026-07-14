//! HTTP handler boundaries for the standalone Live domain.

pub mod admin;
pub mod artwork;
pub mod catalog;
pub mod delivery;
pub mod diagnostics;
pub mod sessions;

#[cfg(test)]
mod admin_tests;
