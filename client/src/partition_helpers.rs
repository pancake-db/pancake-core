use pancake_db_idl::dml::partition_field_value::Value;
use protobuf::well_known_types::Timestamp;

/// Re-export for the purpose of [`make_partition`].
pub use pancake_db_idl::dml::PartitionFieldValue;

/// Trait used by [`make_partition`] to convert native types to Pancake IDL types.
pub trait PartitionFieldValueConverter {
  fn to_value(self) -> Value;
}

impl PartitionFieldValueConverter for i64 {
  fn to_value(self) -> Value {
    Value::int64_val(self)
  }
}

impl PartitionFieldValueConverter for bool {
  fn to_value(self) -> Value {
    Value::bool_val(self)
  }
}

impl PartitionFieldValueConverter for String {
  fn to_value(self) -> Value {
    Value::string_val(self)
  }
}

impl PartitionFieldValueConverter for Timestamp {
  fn to_value(self) -> Value {
    Value::timestamp_val(self)
  }
}

/// Helper macro to support [`make_partition`].
#[macro_export]
macro_rules! make_partition_insert {
  {$partition: expr;} => {};
  {$partition: expr; $key:expr => $val:expr $(,$keys:expr => $vals:expr)* $(,)?} => {
    let fv = $crate::partition_helpers::PartitionFieldValue {
      value: Some($crate::partition_helpers::PartitionFieldValueConverter::to_value($val)),
      ..Default::default()
    };
    $partition.insert($key.to_string(), fv);
    $crate::make_partition_insert! { $partition; $($keys => $vals),* }
  };
}

/// Outputs a partition, given native Rust key => value pairings.
///
/// Since instantiating protobuf-generated types is very verbose,
/// this macro exists to make partitions
/// (`HashMap<String, PartitionFieldValue>`) with ease:
///
/// ```
/// use pancake_db_client::make_partition;
/// use protobuf::well_known_types::Timestamp;
///
/// let my_partition = make_partition! {
///   "t" => Timestamp::now(),
///   "action" => "click".to_string(),
///   "is_final" => true,
///   "int_bucket" => 7,
/// };
/// ```
///
/// Keys can be any type supporting `.to_string()`.
/// Values can be `i64`s, `bool`s, `String`s, or `Timestamp`s.
#[macro_export]
macro_rules! make_partition {
  {} => {
    std::collections::HashMap::<String, $crate::partition_helpers::PartitionFieldValue>::new()
  };
  {$($keys:expr => $vals:expr),+ $(,)?} => {
    {
      let mut row = std::collections::HashMap::<String, $crate::partition_helpers::PartitionFieldValue>::new();
      $crate::make_partition_insert! { row; $($keys => $vals),+ }
      row
    }
  };
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;
  use pancake_db_idl::dml::partition_field_value::Value;
  use pancake_db_idl::dml::PartitionFieldValue;

  use protobuf::well_known_types::Timestamp;

  use crate::make_partition;

  #[test]
  fn test_partition_macro() {
    let proto_t = Timestamp::now();
    let p0 = make_partition! {};
    let p1 = make_partition! { "i64" => 5_i64 };
    let p2 = make_partition! {
      "i64" => 5_i64,
      "bool" => true,
      "timestamp" => proto_t.clone(),
      "string" => "asdf".to_string(),
    };

    assert!(p0.is_empty());

    assert_eq!(p1.len(), 1);

    assert_eq!(p2.len(), 4);
    fn assert_val_eq(partition: &HashMap<String, PartitionFieldValue>, key: &str, value: Value) {
      assert_eq!(partition[key].clone(), PartitionFieldValue {
        value: Some(value),
        ..Default::default()
      });
    }
    assert_val_eq(&p2, "i64", Value::int64_val(5));
    assert_val_eq(&p2, "bool", Value::bool_val(true));
    assert_val_eq(&p2, "timestamp", Value::timestamp_val(proto_t.clone()));
    assert_val_eq(&p2, "string", Value::string_val("asdf".to_string()));
  }
}

#[cfg(test)]
mod tests_no_imports {
  use crate::make_partition;

  #[test]
  fn test_partition_macro() {
    println!("{:?}", make_partition! {});
    println!("{:?}", make_partition! { "a" => 5_i64 });
  }
}
