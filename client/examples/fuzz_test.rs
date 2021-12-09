use std::net::{IpAddr, Ipv4Addr};

use futures::StreamExt;
use pancake_db_idl::ddl::{CreateTableRequest, DropTableRequest, GetSchemaRequest, AlterTableRequest};
use pancake_db_idl::dml::{Field, FieldValue, ListSegmentsRequest, PartitionField, Row, WriteToPartitionRequest};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::schema::{ColumnMeta, Schema};
use protobuf::{MessageField, ProtobufEnumOrUnknown};
use structopt::StructOpt;
use tokio;
use hyper::StatusCode;

use pancake_db_client::Client;
use pancake_db_client::errors::{ClientResult, ClientErrorKind, ClientError};
use rand::Rng;
use pancake_db_idl::ddl::create_table_request::SchemaMode;
use tokio::time::Duration;

const TABLE_NAME: &str = "fuzz_test_table";
const BATCH_SIZE: usize = 250;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "Fuzz Test")]
pub struct Opt {
  #[structopt(long, default_value = "2")]
  pub small_n_rows: usize,

  #[structopt(long, default_value = "10000")]
  pub big_n_rows: usize,

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
    1337,
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
  let i_meta = ColumnMeta {
    name: "col_0".to_string(),
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  };
  let mut schema = Schema {
    columns: vec![i_meta],
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

  // do random schema evolutions and writing rows,
  // each time checking that the data makes sense
  let mut row_counts = vec![0, 0]; // expected row counts after each schema evolution
  for iter in 1..opt.num_evolutions + 1 {
    evolve(iter, &mut schema, &opt, &client, &mut row_counts).await?;
  }

  Ok(())
}

async fn evolve(i: usize, schema: &mut Schema, opt: &Opt, client: &Client, row_counts: &mut Vec<usize>) -> ClientResult<()> {
  evolve_schema(i, schema, client).await?;
  write_rows(i, opt, client, row_counts).await?;
  assert_reads(i, client, row_counts).await?;
  Ok(())
}

async fn evolve_schema(i: usize, schema: &mut Schema, client: &Client) -> ClientResult<()> {
  let new_column = ColumnMeta {
    name: format!("col_{}", i),
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  };
  schema.columns.push(new_column.clone());

  // randomly either alter table or declaratively update schema
  let mut rng = rand::thread_rng();
  if rng.gen_bool(0.5) {
    let alter_req = AlterTableRequest {
      table_name: TABLE_NAME.to_string(),
      new_columns: vec![new_column],
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
        row.fields.push(Field {
          name: format!("col_{}", col_idx),
          value: MessageField::some(FieldValue {
            value: Some(Value::int64_val(rng.gen())),
            ..Default::default()
          }),
          ..Default::default()
        })
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

  row_counts.push(last_row_count + n_batches * n_rows_per_batch);

  if !small_write && rng.gen_bool(0.5) {
    println!("waiting {} seconds for data to settle...", opt.compaction_wait_time);
    tokio::time::sleep(Duration::from_secs(opt.compaction_wait_time)).await;
  }
  Ok(())
}

async fn assert_reads(i: usize, client: &Client, row_counts: &[usize]) -> ClientResult<()> {
  // List segments
  let list_req = ListSegmentsRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  };
  println!("listing segments: {:?}", list_req);
  let list_resp = client.api_list_segments(&list_req).await?;

  // Read segments
  let current_row_count = *row_counts.last().unwrap();
  let partition = Vec::<PartitionField>::new();
  for segment in &list_resp.segments {
    println!("checking all columns for segment {}", segment.segment_id);
    for col_idx in 0..i + 1 {
      let col_meta = ColumnMeta {
        name: format!("col_{}", col_idx),
        dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
        ..Default::default()
      };
      let fvs = client.decode_segment_column(
        TABLE_NAME,
        &partition,
        &segment.segment_id,
        &col_meta,
      ).await?;

      if fvs.len() != current_row_count {
        return Err(ClientError::other(format!(
          "expected {} rows for col_{} at evolution {} but found {}",
          current_row_count,
          col_idx,
          i,
          fvs.len()
        )));
      }

      for row_idx in 0..row_counts[col_idx] {
        if let Some(x) = &fvs[row_idx].value {
          return Err(ClientError::other(format!(
            "expected the first {} rows for col_{} to be null, but row {} was {:?}",
            row_counts[col_idx],
            col_idx,
            row_idx,
            x,
          )));
        }
      }
    }
  }

  Ok(())
}
