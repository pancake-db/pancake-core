use pancake_db_idl::ddl::*;
use pancake_db_idl::dml::*;
use pancake_db_idl::service::pancake_db_client::PancakeDbClient;
use tonic::codegen::StdError;
use tonic::transport::Channel;

use crate::errors::ClientResult;

#[cfg(feature = "read")]
mod read;

/// The best way to communicate with a PancakeDB server from Rust.
///
/// Supports the entire PancakeDB API.
/// Additionally, since PancakeDB reads return raw byte data in a compressed
/// format, `Client` supports some higher-level functionality for reading
/// whole segments into a meaningful representation.
///
/// ```
/// use pancake_db_client::Client;
/// # use pancake_db_client::errors::ClientError;
///
/// # async { // we don't actually run this in the test, only compile
/// let client = Client::connect("http://localhost:3842").await?;
/// # Ok::<(), ClientError>(())
/// # };
/// ```
#[derive(Clone, Debug)]
pub struct Client {
  /// The generated Tonic GRPC client.
  ///
  /// All client calls ultimately go through this.
  /// You can manually make low-level calls like `read_segment_columns` through
  /// this GRPC client.
  pub grpc: PancakeDbClient<Channel>,
}

impl Client {
  /// Creates a new client connected to the given endpoint.
  ///
  /// See [`tonic::transport::Endpoint`] for what qualifies as an endpoint.
  /// One option is a string of format `"http://$HOST:$PORT"`
  pub async fn connect<D>(dst: D) -> ClientResult<Self> where
    D: std::convert::TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
  {
    let grpc = PancakeDbClient::connect(dst).await?;
    Ok(Client { grpc })
  }

  /// Alters a table, e.g. by adding columns.
  pub async fn alter_table(&mut self, req: AlterTableRequest) -> ClientResult<AlterTableResponse> {
    let resp = self.grpc.alter_table(req).await?.into_inner();
    Ok(resp)
  }

  /// Creates or asserts or declaratively updates a table.
  pub async fn create_table(&mut self, req: CreateTableRequest) -> ClientResult<CreateTableResponse> {
    let resp = self.grpc.create_table(req).await?.into_inner();
    Ok(resp)
  }

  /// Drops a table, deleting all its data.
  pub async fn drop_table(&mut self, req: DropTableRequest) -> ClientResult<DropTableResponse> {
    let resp = self.grpc.drop_table(req).await?.into_inner();
    Ok(resp)
  }

  /// Returns the table's schema.
  pub async fn get_schema(&mut self, req: GetSchemaRequest) -> ClientResult<GetSchemaResponse> {
    let resp = self.grpc.get_schema(req).await?.into_inner();
    Ok(resp)
  }

  /// Deletes specific rows from the segment.
  pub async fn delete_from_segment(&mut self, req: DeleteFromSegmentRequest) -> ClientResult<DeleteFromSegmentResponse> {
    let resp = self.grpc.delete_from_segment(req).await?.into_inner();
    Ok(resp)
  }

  /// Lists of all tables.
  pub async fn list_tables(&mut self, req: ListTablesRequest) -> ClientResult<ListTablesResponse> {
    let resp = self.grpc.list_tables(req).await?.into_inner();
    Ok(resp)
  }

  /// Lists all segments in the table, optionally subject to a partition
  /// filter.
  pub async fn list_segments(&mut self, req: ListSegmentsRequest) -> ClientResult<ListSegmentsResponse> {
    let resp = self.grpc.list_segments(req).await?.into_inner();
    Ok(resp)
  }

  /// Reads the binary data for the rows deleted.
  ///
  /// Uncommonly used; you should typically use
  /// [`Client::decode_segment`] instead.
  pub async fn read_segment_deletions(&mut self, req: ReadSegmentDeletionsRequest) -> ClientResult<ReadSegmentDeletionsResponse> {
    let resp = self.grpc.read_segment_deletions(req).await?.into_inner();
    Ok(resp)
  }

  /// Writes rows to a partition of a table.
  ///
  /// The request can be easily constructed with macros:
  /// ```
  /// use std::time::SystemTime;
  /// use pancake_db_idl::dml::WriteToPartitionRequest;
  /// use pancake_db_client::{make_row, make_partition};
  ///
  /// let req = WriteToPartitionRequest {
  ///   table_name: "my_table".to_string(),
  ///   partition: make_partition! {
  ///     "string_partition_col" => "my_partition_value".to_string(),
  ///     "int_partition_col" => 77,
  ///   },
  ///   rows: vec![
  ///     make_row! {
  ///       "timestamp_col" => SystemTime::now(),
  ///       "bool_col" => false,
  ///     }
  ///   ],
  /// };
  /// ```
  pub async fn write_to_partition(&mut self, req: WriteToPartitionRequest) -> ClientResult<WriteToPartitionResponse> {
    let resp = self.grpc.write_to_partition(req).await?.into_inner();
    Ok(resp)
  }
}
