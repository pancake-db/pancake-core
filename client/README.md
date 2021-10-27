# Example

Create a client instance via
```
use pancake_db_client::Client;
let client = Client::from_ip_port(
  IpAddr::V4(Ipv4Addr::new(...)),
  your_port,
);
```

For a detailed example, see [the client runthrough](./examples/client-runthrough/src/main.rs).

# Essential API

Each of these calls simply sends a request to the server and
translates the response into a `ClientResult<>` of the corresponding
response struct.
For details about the API calls and what all their fields mean,
see the [API docs]().

### Create Table
```
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
// invoke this client method:
pub async fn api_create_table(&self, req: &CreateTableRequest) -> ClientResult<CreateTableResponse>
```

### Drop Table
```
use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};
// invoke this client method:
pub async fn api_drop_table(&self, req: &DropTableRequest) -> ClientResult<DropTableResponse> {
```

### List Segments
```
use pancake_db_idl::dml::{ListSegmentsRequest, ListSegmentsResponse};
// invoke this client method:
pub async fn api_list_segments(&self, req: &ListSegmentsRequest) -> ClientResult<ListSegmentsResponse>
```

### Write to Partition
```
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};
// invoke this client method:
pub async fn api_write_to_partition(&self, req: &WriteToPartitionRequest) -> ClientResult<WriteToPartitionResponse>
```

### Read Segment Column
```
use pancake_db_idl::dml::{ReadSegmentColumnRequest, ReadSegmentColumnResponse};
// invoke this client method:
pub async fn api_read_segment_column(&self, req: &ReadSegmentColumnRequest) -> ClientResult<ReadSegmentColumnResponse>
```

# Higher-level Functionality

The raw API for `read_segment_column` returns serialized bytes that aren't
immediately helpful.
To make sense of that data, the client supports:

### Decode Segment Column

This reads the segment column, following continuation tokens.
It decodes to a vector of `FieldValue`s, which contain deserialized data.

```
use pancake_db_idl::ddl::ColumnMeta;
use pancake_db_idl::dml::{FieldValue, PartitionField};
// invoke this client method:
pub async fn decode_segment_column(
  &self,
  table_name: &str,
  partition: &[PartitionField],
  segment_id: &str,
  column: &ColumnMeta,
) -> ClientResult<Vec<FieldValue>>
```

### Decode Segment

This reads multiple columns for the same segment, following continuation
tokens.
It decodes them together into a vector of `Row`s, which contain
deserialized data.

```
use pancake_db_idl::ddl::ColumnMeta;
use pancake_db_idl::dml::{Row, PartitionField};
// invoke this client method:
pub async fn decode_segment(
  &self,
  table_name: &str,
  partition: &[PartitionField],
  segment_id: &str,
  columns: &[ColumnMeta],
) -> ClientResult<Vec<Row>>
```