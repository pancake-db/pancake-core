use std::collections::HashMap;
use pancake_db_idl::dml::PartitionFieldValue;

#[derive(Clone, Debug)]
pub struct SegmentKey {
  pub table_name: String,
  pub partition: HashMap<String, PartitionFieldValue>,
  pub segment_id: String,
}
