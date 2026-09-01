pub mod api;
pub mod config;
pub mod db;
pub mod domain;
pub mod engine;
pub mod notifications;
pub mod progress;
pub mod schedule;
pub mod worker;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
