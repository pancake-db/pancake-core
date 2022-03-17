//! Most common usage:
//!
//! ```
//! use pancake_db_client::{Client, make_partition, make_row};
//! use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};
//! # use pancake_db_client::errors::ClientError;
//!
//! # async { // we don't actually run this in the test, only compile
//! let mut client = Client::connect("http://localhost:3842").await?;
//! let req = WriteToPartitionRequest {
//!   table_name: "my_table".to_string(),
//!   partition: make_partition! {
//!     "string_partition_col" => "my_partition_value".to_string(),
//!   },
//!   rows: vec![
//!     make_row! {
//!       "bool_col" => false,
//!       "f32_col" => 1.1_f32,
//!     }
//!   ],
//! };
//! let resp: WriteToPartitionResponse = client.write_to_partition(req).await?;
//! # Ok::<(), ClientError>(())
//! # };
//! ```
//!
//! See [`Client`] for more details.
#[doc = include_str!("../README.md")]

pub use types::SegmentKey;
pub use utils::new_correlation_id;

pub mod errors;
pub mod row_helpers;
pub mod partition_helpers;

pub use client::Client;

mod types;
mod utils;
mod client;
