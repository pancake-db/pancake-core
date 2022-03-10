#[doc = include_str!("../README.md")]

pub use client::Client;
pub use types::SegmentKey;
pub use utils::new_correlation_id;

pub mod errors;
pub mod row_helpers;
pub mod partition_helpers;

mod client;
mod types;
mod utils;
