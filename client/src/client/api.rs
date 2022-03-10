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
  /// Create a table.
  ///
  /// Can be done assertively, declaratively, or returning an error if the
  /// table already exists.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use std::collections::HashMap;
  /// use pancake_db_idl::ddl::CreateTableRequest;
  /// use pancake_db_idl::ddl::create_table_request::SchemaMode;
  /// use pancake_db_idl::schema::{ColumnMeta, PartitionMeta, Schema};
  /// use protobuf::{MessageField, ProtobufEnumOrUnknown};
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let mut columns = HashMap::new();
  /// columns.insert("new_col".to_string(), ColumnMeta::default()); // defaults to string dtype
  /// let mut partitioning = HashMap::new();
  /// partitioning.insert("partition_col".to_string(), PartitionMeta::default()); // defaults to string dtype
  /// let schema = Schema {
  ///   columns,
  ///   partitioning,
  ///  ..Default::default()
  /// };
  /// let req = CreateTableRequest {
  ///   table_name: "table".to_string(),
  ///   schema: MessageField::some(schema),
  ///   mode: ProtobufEnumOrUnknown::new(SchemaMode::FAIL_IF_EXISTS),
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_create_table(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
  pub async fn api_create_table(&self, req: &CreateTableRequest) -> ClientResult<CreateTableResponse> {
    self.simple_json_request::<CreateTableRequest, CreateTableResponse>(
      "create_table",
      Method::POST,
      req,
    ).await
  }

  /// Alter a table, e.g. by adding columns.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use std::collections::HashMap;
  /// use pancake_db_idl::ddl::AlterTableRequest;
  /// use pancake_db_idl::schema::ColumnMeta;
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let mut new_columns = HashMap::new();
  /// new_columns.insert("new_col".to_string(), ColumnMeta::default()); // typically with more specific dtype
  /// let req = AlterTableRequest {
  ///   table_name: "table".to_string(),
  ///   new_columns,
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_alter_table(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
  pub async fn api_alter_table(&self, req: &AlterTableRequest) -> ClientResult<AlterTableResponse> {
    self.simple_json_request::<AlterTableRequest, AlterTableResponse>(
      "alter_table",
      Method::POST,
      req,
    ).await
  }

  /// Drop a table, deleting all its data.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use pancake_db_idl::ddl::DropTableRequest;
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let req = DropTableRequest {
  ///   table_name: "table".to_string(),
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_drop_table(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
  pub async fn api_drop_table(&self, req: &DropTableRequest) -> ClientResult<DropTableResponse> {
    self.simple_json_request::<DropTableRequest, DropTableResponse>(
      "drop_table",
      Method::POST,
      req,
    ).await
  }

  /// Get the schema for a table.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use pancake_db_idl::ddl::GetSchemaRequest;
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let req = GetSchemaRequest {
  ///   table_name: "table".to_string(),
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_get_schema(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
  pub async fn api_get_schema(&self, req: &GetSchemaRequest) -> ClientResult<GetSchemaResponse> {
    self.simple_json_request::<GetSchemaRequest, GetSchemaResponse>(
      "get_schema",
      Method::GET,
      req,
    ).await
  }

  /// List the segments for a table.
  ///
  /// Supports partition filtering and optional metadata retrieval.
  /// Typically used before calling
  /// [`read_segment_deletions`][Client::api_read_segment_deletions] and
  /// [`read_segment_column`][Client::api_read_segment_column] to complete a bulk read.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use pancake_db_idl::dml::ListSegmentsRequest;
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let req = ListSegmentsRequest {
  ///   table_name: "table".to_string(),
  ///   include_metadata: true,
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_list_segments(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
  pub async fn api_list_segments(&self, req: &ListSegmentsRequest) -> ClientResult<ListSegmentsResponse> {
    self.simple_json_request::<ListSegmentsRequest, ListSegmentsResponse>(
      "list_segments",
      Method::GET,
      req,
    ).await
  }

  ///  This is typically the client method you'll be using most frequently.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use pancake_db_idl::dml::WriteToPartitionRequest;
  /// use pancake_db_client::{make_row, make_partition};
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let req = WriteToPartitionRequest {
  ///   table_name: "table".to_string(),
  ///   partition: make_partition! {
  ///     "my_partition_key_0" => "val0".to_string(),
  ///     "my_partition_key_1" => 5,
  ///   },
  ///   rows: vec![],
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_write_to_partition(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
  pub async fn api_write_to_partition(&self, req: &WriteToPartitionRequest) -> ClientResult<WriteToPartitionResponse> {
    self.simple_json_request::<WriteToPartitionRequest, WriteToPartitionResponse>(
      "write_to_partition",
      Method::POST,
      req,
    ).await
  }

  /// Delete rows from a segment by row ID.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use pancake_db_idl::dml::DeleteFromSegmentRequest;
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let req = DeleteFromSegmentRequest {
  ///   table_name: "table".to_string(),
  ///   segment_id: "some_id".to_string(),
  ///   row_ids: vec![1, 2, 3],
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_delete_from_segment(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
  pub async fn api_delete_from_segment(&self, req: &DeleteFromSegmentRequest) -> ClientResult<DeleteFromSegmentResponse> {
    self.simple_json_request::<DeleteFromSegmentRequest, DeleteFromSegmentResponse>(
      "delete_from_segment",
      Method::POST,
      req,
    ).await
  }

  /// Reads compressed data for which rows from a segment have been deleted.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use pancake_db_idl::dml::ReadSegmentDeletionsRequest;
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let req = ReadSegmentDeletionsRequest {
  ///   table_name: "table".to_string(),
  ///   segment_id: "some_id".to_string(),
  ///   correlation_id: pancake_db_client::new_correlation_id(),
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_read_segment_deletions(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
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

  /// Reads compressed data for a segment column.
  ///
  /// ```
  /// # use pancake_db_client::Client;
  /// # use std::net::{IpAddr, Ipv4Addr};
  /// # use pancake_db_client::errors::ClientError;
  /// use pancake_db_idl::dml::ReadSegmentColumnRequest;
  ///
  /// # let client = Client::from_ip_port(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3841);
  /// let req = ReadSegmentColumnRequest {
  ///   table_name: "table".to_string(),
  ///   segment_id: "some_id".to_string(),
  ///   correlation_id: pancake_db_client::new_correlation_id(),
  ///   column_name: "my_col".to_string(),
  ///   ..Default::default()
  /// };
  /// # async { // we don't actually run this in the test, only compile
  /// let response = client.api_read_segment_column(&req).await?;
  /// # Ok::<(), ClientError>(())
  /// # };
  /// ```
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
