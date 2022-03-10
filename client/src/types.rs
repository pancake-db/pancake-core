use std::collections::HashMap;

use pancake_db_idl::dml::PartitionFieldValue;

/// A fully-specified segment.
///
/// Consists of a table name, partition, and segment ID.
/// Used in certain high-level client functionality.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SegmentKey {
  pub table_name: String,
  pub partition: HashMap<String, PartitionFieldValue>,
  pub segment_id: String,
}
