use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};

use futures::StreamExt;
use hyper::StatusCode;
use pancake_db_idl::ddl::{AlterTableRequest, CreateTableRequest, DropTableRequest, GetSchemaRequest};
use pancake_db_idl::ddl::create_table_request::SchemaMode;
use pancake_db_idl::dml::{FieldValue, ListSegmentsRequest, Row, WriteToPartitionRequest, DeleteFromSegmentRequest};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::schema::{ColumnMeta, Schema};
use protobuf::{MessageField, ProtobufEnumOrUnknown};
use rand::Rng;
use structopt::StructOpt;
use tokio;
use tokio::time::Duration;

use pancake_db_client::{Client, SegmentKey};
use pancake_db_client::errors::{ClientError, ClientErrorKind, ClientResult};

const TABLE_NAME: &str = "fuzz_test_table";
const BATCH_SIZE: usize = 250;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "Fuzz Test")]
pub struct Opt {
  #[structopt(long, default_value = "2")]
  pub small_n_rows: usize,

  #[structopt(long, default_value = "10000")]
  pub big_n_rows: usize,

  #[structopt(long, default_value = "10")]
  pub max_deletions_per_req: usize,

  #[structopt(long, default_value = "12")]
  pub compaction_wait_time: u64,

  // the number of times we amend the schema and add more rows
  #[structopt(long, default_value = "10")]
  pub num_evolutions: usize,
}

impl Opt {
  pub fn validate(&self) {
    if self.big_n_rows % BATCH_SIZE != 0 {
      panic!("please make big-n-rows a multiple of {}", BATCH_SIZE)
    }
  }
}

#[tokio::main]
async fn main() -> ClientResult<()> {
  // Instantiate a client
  let client = Client::from_ip_port(
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
    3841,
  );

  // drop table if it already exists, to clean state
  let drop_table_res = client.api_drop_table(&DropTableRequest {
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
        ClientErrorKind::Http(StatusCode::NOT_FOUND) => Ok(()),
        _ => Err(err),
      }
    }
  }?;

  // create the table
  let mut initial_columns = HashMap::new();
  initial_columns.insert(
    "col_0".to_string(),
    ColumnMeta {
      dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
      ..Default::default()
    }
  );
  let mut schema = Schema {
    columns: initial_columns,
    ..Default::default()
  };
  let create_table_req = CreateTableRequest {
    table_name: TABLE_NAME.to_string(),
    schema: MessageField::some(schema.clone()),
    ..Default::default()
  };
  let create_resp = client.api_create_table(&create_table_req).await?;
  println!("Created table for the first time: {:?}", create_resp);

  let opt: Opt = Opt::from_args();

  // do random schema evolutions, writing rows, and deleting rows,
  // each time checking that the data makes sense
  let mut row_counts = vec![0, 0]; // expected row counts after each schema evolution
  let mut n_deletions_ub = 0;
  for iter in 1..opt.num_evolutions + 1 {
    iterate(iter, &mut schema, &opt, &client, &mut row_counts, &mut n_deletions_ub).await?;
  }

  Ok(())
}

async fn iterate(i: usize, schema: &mut Schema, opt: &Opt, client: &Client, row_counts: &mut Vec<usize>, n_deletions_ub: &mut usize) -> ClientResult<()> {
  evolve_schema(i, schema, client).await?;
  let write_rows_future = write_rows(i, opt, client, row_counts);
  if i > 1 {
    let delete_future = delete(opt, client, n_deletions_ub);
    let (write_rows_res, delete_res) = tokio::join!(
      write_rows_future,
      delete_future,
    );
    write_rows_res?;
    delete_res?;
  } else {
    write_rows_future.await?;
  }
  assert_reads(i, client, row_counts, *n_deletions_ub).await?;
  Ok(())
}

async fn evolve_schema(i: usize, schema: &mut Schema, client: &Client) -> ClientResult<()> {
  let new_column_name = format!("col_{}", i);
  let new_column = ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  };
  schema.columns.insert(new_column_name.clone(), new_column.clone());

  // randomly either alter table or declaratively update schema
  let mut rng = rand::thread_rng();
  if rng.gen_bool(0.5) {
    let mut new_columns = HashMap::new();
    new_columns.insert(new_column_name, new_column);
    let alter_req = AlterTableRequest {
      table_name: TABLE_NAME.to_string(),
      new_columns,
      ..Default::default()
    };
    println!("altering table: {:?}", alter_req);
    client.api_alter_table(&alter_req).await?;
  } else {
    let create_req = CreateTableRequest {
      table_name: TABLE_NAME.to_string(),
      schema: MessageField::some(schema.clone()),
      mode: ProtobufEnumOrUnknown::new(SchemaMode::ADD_NEW_COLUMNS),
      ..Default::default()
    };
    println!("declaratively creating table: {:?}", create_req);
    client.api_create_table(&create_req).await?;
  }

  // check that schema is as expected
  let get_schema_req = GetSchemaRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  };
  let resp = client.api_get_schema(&get_schema_req).await.expect("getting schema failed");
  let resp_schema = resp.schema.unwrap();
  if &resp_schema != schema {
    return Err(ClientError::other(format!(
      "schema mismatch; expected {:?} but DB responded {:?}",
      schema,
      resp_schema
    )));
  }

  Ok(())
}

async fn write_rows(i: usize, opt: &Opt, client: &Client, row_counts: &mut Vec<usize>) -> ClientResult<()> {
  let mut rng = rand::thread_rng();
  let last_row_count = *row_counts.last().unwrap();
  let small_write = rng.gen_bool(0.5);
  let (n_batches, n_rows_per_batch) = if small_write {
    (1, opt.small_n_rows)
  } else {
    (opt.big_n_rows / BATCH_SIZE, BATCH_SIZE)
  };

  let mut rows = Vec::with_capacity(n_rows_per_batch);
  for _ in 0..n_rows_per_batch {
    let mut row = Row::new();
    for col_idx in 0..i + 1 {
      if rng.gen_bool(0.5) {
        row.fields.insert(
          format!("col_{}", col_idx),
          FieldValue {
            value: Some(Value::int64_val(rng.gen())),
            ..Default::default()
          }
        );
      }
    }
    rows.push(row);
  }
  let write_to_partition_req = WriteToPartitionRequest {
    table_name: TABLE_NAME.to_string(),
    rows,
    ..Default::default()
  };

  if small_write {
    println!("writing {} rows: {:?}", opt.small_n_rows, write_to_partition_req.rows);
  } else {
    println!("writing {} rows in {} concurrent batches", opt.big_n_rows, n_batches);
  }

  // limit the number of concurrent write futures
  // server configuration might limit this and refuse connections after a point
  let max_concurrency = 16;
  futures::stream::repeat(0).take(n_batches) // write our 2 rows 1000 times (2000 rows)
    .for_each_concurrent(
      max_concurrency,
      |_| async {
        client.api_write_to_partition(&write_to_partition_req).await.expect("write failed");
      }
    )
    .await;
  println!("done with writes");

  row_counts.push(last_row_count + n_batches * n_rows_per_batch);

  if !small_write && rng.gen_bool(0.5) {
    println!("waiting {} seconds for compaction to settle...", opt.compaction_wait_time);
    tokio::time::sleep(Duration::from_secs(opt.compaction_wait_time)).await;
  }
  Ok(())
}

async fn delete(opt: &Opt, client: &Client, n_deletions_ub: &mut usize) -> ClientResult<()> {
  println!("listing segments for deletion");
  let list_segments_response = client.api_list_segments(&ListSegmentsRequest {
    table_name: TABLE_NAME.to_string(),
    include_metadata: true,
    ..Default::default()
  }).await?;
  let mut rng = rand::thread_rng();
  let segments = &list_segments_response.segments;
  let segment = &segments[rng.gen_range(0..segments.len())];
  let mut to_delete = Vec::new();
  for _ in 0..opt.max_deletions_per_req {
    let metadata = segment.metadata.clone().unwrap();
    let row_id_range = metadata.row_count; // could go higher
    to_delete.push(rng.gen_range(0..row_id_range) as u32);
  }
  let distinct_to_delete: HashSet<_> = to_delete.iter().cloned().collect();
  println!("deleting {} distinct row ids", distinct_to_delete.len());
  let req = DeleteFromSegmentRequest {
    table_name: TABLE_NAME.to_string(),
    segment_id: segment.segment_id.to_string(),
    row_ids: to_delete,
    ..Default::default()
  };
  *n_deletions_ub += opt.max_deletions_per_req;
  client.api_delete_from_segment(&req).await?;
  Ok(())
}

async fn assert_reads(i: usize, client: &Client, row_counts: &[usize], n_deletions_ub: usize) -> ClientResult<()> {
  // List segments
  let list_req = ListSegmentsRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  };
  println!("listing segments: {:?}", list_req);
  let list_resp = client.api_list_segments(&list_req).await?;

  // Read segments
  let current_row_count = *row_counts.last().unwrap();
  let mut col_row_counts = Vec::new();
  let mut col_null_counts = Vec::new();
  for _ in 0..i + 1 {
    col_row_counts.push(0);
    col_null_counts.push(0);
  }
  for segment in &list_resp.segments {
    println!("checking all columns for segment {}", segment.segment_id);
    let segment_key = SegmentKey {
      table_name: TABLE_NAME.to_string(),
      partition: HashMap::new(),
      segment_id: segment.segment_id.clone(),
    };
    let correlation_id = pancake_db_client::new_correlation_id();
    let is_deleted = client.decode_is_deleted(&segment_key, &correlation_id).await?;
    for col_idx in 0..i + 1 {
      let col_meta = ColumnMeta {
        dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
        ..Default::default()
      };
      let col_name = format!("col_{}", col_idx);
      let fvs = client.decode_segment_column(
        &segment_key,
        &col_name,
        &col_meta,
        &is_deleted,
        &correlation_id,
      ).await?;

      col_row_counts[col_idx] += fvs.len();
      for fv in &fvs {
        if fv.value.is_none() {
          col_null_counts[col_idx] += 1;
        }
      }
    }
  }

  for col_idx in 0..i + 1 {
    let l = col_row_counts[col_idx];
    if l > current_row_count || l + n_deletions_ub < current_row_count {
      return Err(ClientError::other(format!(
        "expected {} to {} rows for col_{} at evolution {} but found {}",
        current_row_count as i64 - n_deletions_ub as i64,
        current_row_count,
        col_idx,
        i,
        l,
      )));
    }

    let n_nulls = col_null_counts[col_idx];
    let n_nulls_lb = row_counts[col_idx].max(n_deletions_ub) - n_deletions_ub;
    if n_nulls < n_nulls_lb {
      return Err(ClientError::other(format!(
        "expected the first {} rows for col_{} to be null, but only {} rows were null",
        n_nulls_lb,
        col_idx,
        n_nulls,
      )));
    }
  }

  Ok(())
}
