use std::collections::HashMap;

use pancake_db_idl::dml::{FieldValue, ReadSegmentColumnRequest, ReadSegmentDeletionsRequest, Row};
use pancake_db_idl::schema::ColumnMeta;

use pancake_db_core::compression;
use pancake_db_core::deletion;
use pancake_db_core::encoding;

use crate::errors::{ClientError, ClientResult};

use super::Client;
use super::types::SegmentKey;

impl Client {
  pub async fn decode_is_deleted(
    &self,
    segment_key: &SegmentKey,
    correlation_id: &str,
  ) -> ClientResult<Vec<bool>> {
    let SegmentKey {
      table_name,
      partition,
      segment_id,
    } = segment_key;

    let req = ReadSegmentDeletionsRequest {
      table_name: table_name.to_string(),
      partition: partition.clone(),
      segment_id: segment_id.to_string(),
      correlation_id: correlation_id.to_string(),
      ..Default::default()
    };

    let resp = self.api_read_segment_deletions(&req).await?;
    let bools = if resp.data.is_empty() {
      Vec::new()
    } else {
      deletion::decompress_deletions(resp.data)?
    };
    Ok(bools)
  }

  pub async fn decode_segment_column(
    &self,
    segment_key: &SegmentKey,
    column_name: &str,
    column: &ColumnMeta,
    is_deleted: &[bool],
    correlation_id: &str,
  ) -> ClientResult<Vec<FieldValue>> {
    let SegmentKey {
      table_name,
      partition,
      segment_id,
    } = segment_key;
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
        correlation_id: correlation_id.to_string(),
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
    let mut row_idx = 0;
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
      let fvs = decompressor.decompress(
        compressed_bytes,
        column.nested_list_depth as u8,
      )?;
      for fv in fvs {
        if row_idx >= is_deleted.len() || !is_deleted[row_idx] {
          res.push(fv);
        }
        row_idx += 1
      }
    }

    for _ in 0..implicit_nulls_count {
      if row_idx >= is_deleted.len() || !is_deleted[row_idx] {
        res.push(FieldValue::new());
      }
      row_idx += 1;
    }

    if !uncompressed_bytes.is_empty() {
      let decoder = encoding::new_field_value_decoder(
        dtype,
        column.nested_list_depth as u8,
      );
      for fv in decoder.decode(&uncompressed_bytes)? {
        if row_idx >= is_deleted.len() || !is_deleted[row_idx] {
          res.push(fv);
        }
        row_idx += 1
      }
    }

    Ok(res)
  }

  pub async fn decode_segment(
    &self,
    segment_key: &SegmentKey,
    columns: &HashMap<String, ColumnMeta>,
  ) -> ClientResult<Vec<Row>> {
    if columns.is_empty() {
      return Err(ClientError::other(
        "unable to decode segment with no columns specified".to_string()
      ))
    }

    let correlation_id = super::new_correlation_id();

    let is_deleted = self.decode_is_deleted(segment_key, &correlation_id).await?;

    let mut n = usize::MAX;
    let mut rows = Vec::new();
    for (column_name, column_meta) in columns {
      let fvalues = self.decode_segment_column(
        segment_key,
        column_name,
        column_meta,
        &is_deleted,
        &correlation_id,
      ).await?;
      n = n.min(fvalues.len());
      for _ in rows.len()..n {
        rows.push(Row::new());
      }
      for i in 0..n {
        rows[i].fields.insert(column_name.clone(), fvalues[i].clone());
      }
    }

    Ok(rows[0..n].to_vec())
  }
}
