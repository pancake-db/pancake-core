use std::collections::HashMap;

use futures::StreamExt;
use pancake_db_client::{Client, make_partition, make_row, SegmentKey};
use pancake_db_client::errors::{ClientResult, ClientErrorKind};
use pancake_db_idl::ddl::{CreateTableRequest, DropTableRequest, GetSchemaRequest};
use pancake_db_idl::dml::{DeleteFromSegmentRequest, ListSegmentsRequest, Segment, WriteToPartitionRequest};
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta, Schema};
use rand::{Rng, thread_rng};
use tokio;
use tonic::Code;

const TABLE_NAME: &str = "client_runthrough_table";
const N_PARTITIONS: i64 = 3;

#[tokio::main]
async fn main() -> ClientResult<()> {
  // Instantiate a client
  let mut client = Client::connect("http://localhost:3842").await?;

  // Drop table (if it exists)
  let drop_table_res = client.drop_table(DropTableRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  }).await;
  match drop_table_res {
    Ok(resp) => {
      println!("Dropped existing table: {:?}", resp);
      Ok(())
    },
    Err(err) => {
      match err.kind {
        ClientErrorKind::Grpc { code: Code::NotFound } => Ok(()),
        _ => Err(err),
      }
    }
  }?;

  // Create a table
  let i_meta = ColumnMeta {
    dtype: DataType::Int64 as i32,
    ..Default::default()
  };
  let s_meta = ColumnMeta {
    dtype: DataType::String as i32,
    nested_list_depth: 1,
    ..Default::default()
  };
  let mut columns = HashMap::new();
  columns.insert("i".to_string(), i_meta);
  columns.insert("s".to_string(), s_meta);
  let mut partitioning = HashMap::new();
  partitioning.insert("pk".to_string(), PartitionMeta {
    dtype: PartitionDataType::Int64 as i32,
    ..Default::default()
  });
  let create_table_req = CreateTableRequest {
    table_name: TABLE_NAME.to_string(),
    schema: Some(Schema {
      partitioning,
      columns: columns.clone(),
      ..Default::default()
    }),
    ..Default::default()
  };
  let create_resp = client.create_table(create_table_req).await?;
  println!("Created table: {:?}", create_resp);

  let get_schema_req = GetSchemaRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  };
  let get_schema_resp = client.get_schema(get_schema_req).await?;
  println!("Got schema: {:?}", get_schema_resp);

  // Write rows
  // you can put up to 256 rows into one request for efficiency,
  // but here we use just 2 for demonstration purposes
  // limit the number of concurrent write futures
  // server configuration might limit this and refuse connections after a point
  let max_concurrency = 16;
  futures::stream::repeat(0).take(1000) // write 50 rows 1000 times (50000 rows)
    .for_each_concurrent(
      max_concurrency,
      |_| async {
        let mut rows = Vec::with_capacity(255);
        let mut rng = thread_rng();
        for _ in 0..49 {
          rows.push(make_row! {
            "i" => i64::MAX / rng.gen_range(1..i64::MAX),
            "s" => vec!["item 0".to_string(), "item 1".to_string()],
          })
        }
        rows.push(make_row! {});
        let req = WriteToPartitionRequest {
          table_name: TABLE_NAME.to_string(),
          rows,
          partition: make_partition! {
            "pk" => rng.gen_range(0..N_PARTITIONS)
          },
          ..Default::default()
        };
        client.clone().write_to_partition(req).await.expect("write failed");
      }
    )
    .await;

  // List segments
  let list_segments_req = ListSegmentsRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  };
  let list_resp = client.list_segments(list_segments_req).await?;
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
  let delete_resp = client.delete_from_segment(delete_req.clone()).await?;
  println!("Deleted rows from segment {}: {:?}", segment_id, delete_resp);
  let delete_resp = client.delete_from_segment(delete_req).await?;
  println!("Idempotently deleted same rows again: {:?}", delete_resp);

  let mut read_columns = columns;
  read_columns.insert("_row_id".to_string(), ColumnMeta {
    dtype: DataType::Int64 as i32,
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

  Ok(())
}
