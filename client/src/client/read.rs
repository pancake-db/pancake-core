use std::collections::HashMap;

use pancake_db_idl::dml::{FieldValue, PartitionFieldValue, ReadSegmentColumnRequest, Row};
use pancake_db_idl::schema::ColumnMeta;

use pancake_db_core::compression;
use pancake_db_core::encoding;

use crate::errors::{ClientError, ClientResult};

use super::Client;

impl Client {
  pub async fn decode_segment_column(
    &self,
    table_name: &str,
    partition: &HashMap<String, PartitionFieldValue>,
    segment_id: &str,
    column_name: &str,
    column: &ColumnMeta,
  ) -> ClientResult<Vec<FieldValue>> {
    let mut initial_request = true;
    let mut continuation_token = "".to_string();
    let mut compressed_bytes = Vec::new();
    let mut uncompressed_bytes = Vec::new();
    let mut codec = "".to_string();
    let mut implicit_nulls_count = 0;
    while initial_request || !continuation_token.is_empty() {
      let req = ReadSegmentColumnRequest {
        table_name: table_name.to_string(),
        partition: partition.clone(),
        segment_id: segment_id.to_string(),
        column_name: column_name.to_string(),
        continuation_token,
        ..Default::default()
      };
      let resp = self.api_read_segment_column(&req).await?;
      if resp.codec.is_empty() {
        uncompressed_bytes.extend(&resp.data);
      } else {
        compressed_bytes.extend(&resp.data);
        codec = resp.codec.clone();
      }
      continuation_token = resp.continuation_token;
      implicit_nulls_count = resp.implicit_nulls_count;
      initial_request = false;
    }

    let mut res = Vec::new();

    let dtype = column.dtype.enum_value_or_default();
    if !compressed_bytes.is_empty() {
      if implicit_nulls_count > 0 {
        return Err(ClientError::other(
          "contradictory read responses containing both compacted and implicit data received".to_string()
        ));
      }

      let decompressor = compression::new_codec(
        dtype,
        &codec,
      )?;
      res.extend(decompressor.decompress(
        compressed_bytes,
        column.nested_list_depth as u8,
      )?);
    }

    for _ in 0..implicit_nulls_count {
      res.push(FieldValue::new());
    }

    if !uncompressed_bytes.is_empty() {
      let decoder = encoding::new_field_value_decoder(
        dtype,
        column.nested_list_depth as u8,
      );
      res.extend(decoder.decode(&uncompressed_bytes)?);
    }

    Ok(res)
  }

  pub async fn decode_segment(
    &self,
    table_name: &str,
    partition: &HashMap<String, PartitionFieldValue>,
    segment_id: &str,
    columns: &HashMap<String, ColumnMeta>,
  ) -> ClientResult<Vec<Row>> {
    if columns.is_empty() {
      return Err(ClientError::other(
        "unable to decode segment with no columns specified".to_string()
      ))
    }

    let mut n = usize::MAX;
    let mut rows = Vec::new();
    for (column_name, column_meta) in columns {
      let fvalues = self.decode_segment_column(
        table_name,
        partition,
        segment_id,
        column_name,
        column_meta,
      ).await?;
      n = n.min(fvalues.len());
      for _ in rows.len()..n {
        rows.push(Row::new());
      }
      for i in 0..n {
        rows[i].fields.insert(column_name.clone(), fvalues[i].clone());
      }
    }

    Ok(rows)
  }
}
