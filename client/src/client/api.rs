use hyper::Method;
use pancake_db_idl::ddl::{AlterTableRequest, AlterTableResponse, CreateTableRequest, CreateTableResponse, GetSchemaRequest, GetSchemaResponse};
use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};
use pancake_db_idl::dml::{DeleteFromSegmentRequest, DeleteFromSegmentResponse, ReadSegmentDeletionsRequest, ReadSegmentDeletionsResponse, WriteToPartitionRequest, WriteToPartitionResponse};
use pancake_db_idl::dml::{ListSegmentsRequest, ListSegmentsResponse};
use pancake_db_idl::dml::{ReadSegmentColumnRequest, ReadSegmentColumnResponse};
use protobuf::Message;

use crate::errors::{ClientError, ClientResult};

use super::Client;

fn parse_pb_with_bytes<Resp: Message>(bytes: Vec<u8>) -> ClientResult<(Resp, Vec<u8>)> {
  let delim_bytes = "}\n".as_bytes();
  let mut i = 0;
  loop {
    let end_idx = i + delim_bytes.len();
    if end_idx > bytes.len() {
      return Err(ClientError::other(
        "could not parse read segment column response".to_string()
      ));
    }
    if &bytes[i..end_idx] == delim_bytes {
      break;
    }
    i += 1;
  }
  let content_str = String::from_utf8(bytes[0..i + 1].to_vec())?;
  let mut res = Resp::new();
  protobuf::json::merge_from_str(&mut res, &content_str)?;
  Ok((res, bytes[i + delim_bytes.len()..].to_vec()))
}

impl Client {
  pub async fn api_create_table(&self, req: &CreateTableRequest) -> ClientResult<CreateTableResponse> {
    self.simple_json_request::<CreateTableRequest, CreateTableResponse>(
      "create_table",
      Method::POST,
      req,
    ).await
  }

  pub async fn api_alter_table(&self, req: &AlterTableRequest) -> ClientResult<AlterTableResponse> {
    self.simple_json_request::<AlterTableRequest, AlterTableResponse>(
      "alter_table",
      Method::POST,
      req,
    ).await
  }

  pub async fn api_drop_table(&self, req: &DropTableRequest) -> ClientResult<DropTableResponse> {
    self.simple_json_request::<DropTableRequest, DropTableResponse>(
      "drop_table",
      Method::POST,
      req,
    ).await
  }

  pub async fn api_get_schema(&self, req: &GetSchemaRequest) -> ClientResult<GetSchemaResponse> {
    self.simple_json_request::<GetSchemaRequest, GetSchemaResponse>(
      "get_schema",
      Method::GET,
      req,
    ).await
  }

  pub async fn api_list_segments(&self, req: &ListSegmentsRequest) -> ClientResult<ListSegmentsResponse> {
    self.simple_json_request::<ListSegmentsRequest, ListSegmentsResponse>(
      "list_segments",
      Method::GET,
      req,
    ).await
  }

  pub async fn api_write_to_partition(&self, req: &WriteToPartitionRequest) -> ClientResult<WriteToPartitionResponse> {
    self.simple_json_request::<WriteToPartitionRequest, WriteToPartitionResponse>(
      "write_to_partition",
      Method::POST,
      req,
    ).await
  }

  pub async fn api_delete_from_segment(&self, req: &DeleteFromSegmentRequest) -> ClientResult<DeleteFromSegmentResponse> {
    self.simple_json_request::<DeleteFromSegmentRequest, DeleteFromSegmentResponse>(
      "delete_from_segment",
      Method::POST,
      req,
    ).await
  }

  pub async fn api_read_segment_deletions(&self, req: &ReadSegmentDeletionsRequest) -> ClientResult<ReadSegmentDeletionsResponse> {
    let content = self.simple_json_request_bytes(
      "read_segment_deletions",
      Method::GET,
      req,
    ).await?;

    let (mut resp, data) = parse_pb_with_bytes::<ReadSegmentDeletionsResponse>(content)?;
    resp.data = data;
    Ok(resp)
  }

  pub async fn api_read_segment_column(&self, req: &ReadSegmentColumnRequest) -> ClientResult<ReadSegmentColumnResponse> {
    let content = self.simple_json_request_bytes(
      "read_segment_column",
      Method::GET,
      req,
    ).await?;

    let (mut resp, data) = parse_pb_with_bytes::<ReadSegmentColumnResponse>(content)?;
    resp.data = data;
    Ok(resp)
  }
}
