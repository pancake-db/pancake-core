use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

use futures::StreamExt;
use pancake_db_idl::dml::partition_field_value::Value as PartitionValue;
use pancake_db_idl::ddl::{CreateTableRequest, DropTableRequest, GetSchemaRequest};
use pancake_db_idl::dml::{DeleteFromSegmentRequest, FieldValue, ListSegmentsRequest, PartitionFieldValue, RepeatedFieldValue, Row, WriteToPartitionRequest, Segment};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta, Schema};
use protobuf::{MessageField, ProtobufEnumOrUnknown};
use rand::Rng;
use tokio;

use pancake_db_client::{Client, SegmentKey};
use pancake_db_client::errors::ClientResult;

const TABLE_NAME: &str = "client_runthrough_table";
const N_PARTITIONS: i64 = 3;

#[tokio::main]
async fn main() -> ClientResult<()> {
  // Instantiate a client
  let client = Client::from_ip_port(
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
    1337,
  );

  // Create a table
  let i_meta = ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  };
  let s_meta = ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
    nested_list_depth: 1,
    ..Default::default()
  };
  let mut columns = HashMap::new();
  columns.insert("i".to_string(), i_meta);
  columns.insert("s".to_string(), s_meta);
  let mut partitioning = HashMap::new();
  partitioning.insert("pk".to_string(), PartitionMeta {
    dtype: ProtobufEnumOrUnknown::new(PartitionDataType::INT64),
    ..Default::default()
  });
  let create_table_req = CreateTableRequest {
    table_name: TABLE_NAME.to_string(),
    schema: MessageField::some(Schema {
      partitioning,
      columns: columns.clone(),
      ..Default::default()
    }),
    ..Default::default()
  };
  let create_resp = client.api_create_table(&create_table_req).await?;
  println!("Created table: {:?}", create_resp);

  let get_schema_req = GetSchemaRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  };
  let get_schema_resp = client.api_get_schema(&get_schema_req).await?;
  println!("Got schema: {:?}", get_schema_resp);

  // Write rows
  // you can put up to 256 rows into one request for efficiency,
  // but here we use just 2 for demonstration purposes
  let mut rows = Vec::new();
  let list_of_strings = Value::list_val(RepeatedFieldValue {
    vals: vec![
      FieldValue {
        value: Some(Value::string_val("item 0".to_string())),
        ..Default::default()
      },
      FieldValue {
        value: Some(Value::string_val("item 1".to_string())),
        ..Default::default()
      },
    ],
    ..Default::default()
  });
  let mut basic_fields = HashMap::new();
  basic_fields.insert(
    "i".to_string(),
    FieldValue {
      value: Some(Value::int64_val(33)),
      ..Default::default()
    }
  );
  basic_fields.insert(
    "s".to_string(),
    FieldValue {
      value: Some(list_of_strings),
      ..Default::default()
    }
  );
  rows.push(
    Row {
      fields: basic_fields,
      ..Default::default()
    },
  );
  rows.push(Row::new()); // a row full of nulls
  let write_to_partition_req = WriteToPartitionRequest {
    table_name: TABLE_NAME.to_string(),
    rows,
    ..Default::default()
  };

  // limit the number of concurrent write futures
  // server configuration might limit this and refuse connections after a point
  let max_concurrency = 16;
  futures::stream::repeat(0).take(1000) // write our 2 rows 1000 times (2000 rows)
    .for_each_concurrent(
      max_concurrency,
      |_| async {
        let mut req = write_to_partition_req.clone();
        let mut rng = rand::thread_rng();
        req.partition.insert("pk".to_string(), PartitionFieldValue {
          value: Some(PartitionValue::int64_val(rng.gen_range(0..N_PARTITIONS))),
          ..Default::default()
        });
        client.api_write_to_partition(&req).await.expect("write failed");
      }
    )
    .await;

  // List segments
  let list_segments_req = ListSegmentsRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  };
  let list_resp = client.api_list_segments(&list_segments_req).await?;
  println!("Listed segments: {:?}", list_resp);

  // Delete from segment
  let segment = list_resp.segments[0].clone();
  let Segment { segment_id, partition, .. } = segment;
  let delete_req = DeleteFromSegmentRequest {
    table_name: TABLE_NAME.to_string(),
    segment_id: segment_id.clone(),
    partition,
    row_ids: vec![0, 4, 10],
    ..Default::default()
  };
  let delete_resp = client.api_delete_from_segment(&delete_req).await?;
  println!("Deleted rows from segment {}: {:?}", segment_id, delete_resp);
  let delete_resp = client.api_delete_from_segment(&delete_req).await?;
  println!("Idempotently deleted same rows again: {:?}", delete_resp);

  let mut read_columns = columns;
  read_columns.insert("_row_id".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  });

  // Read segments
  let mut total = 0;
  for segment in &list_resp.segments {
    let segment_key = SegmentKey {
      table_name: TABLE_NAME.to_string(),
      segment_id: segment.segment_id.clone(),
      partition: segment.partition.clone(),
      ..Default::default()
    };
    let rows = client.decode_segment(
      &segment_key,
      &read_columns,
    ).await?;
    let count = rows.len();
    total += count;
    println!("read segment {} with {} rows (total {})", segment.segment_id, count, total);
    for i in 0..10 {
      println!("\t{}th row: {:?}", i, rows[i].clone());
    }
  }

  // Drop table
  let drop_resp = client.api_drop_table(&DropTableRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  }).await?;
  println!("Dropped table: {:?}", drop_resp);

  Ok(())
}
