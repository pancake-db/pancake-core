use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dml::RepeatedFieldValue;
use protobuf::well_known_types::Timestamp;

/// Re-export for the purpose of [`make_row`].
pub use pancake_db_idl::dml::{FieldValue, Row};

/// Trait used by [`make_row`] to convert native types to Pancake IDL types.
pub trait FieldValueConverter {
  fn to_value(self) -> Option<Value>;
}

impl FieldValueConverter for f32 {
  fn to_value(self) -> Option<Value> {
    Some(Value::float32_val(self))
  }
}

impl FieldValueConverter for f64 {
  fn to_value(self) -> Option<Value> {
    Some(Value::float64_val(self))
  }
}

impl FieldValueConverter for i64 {
  fn to_value(self) -> Option<Value> {
    Some(Value::int64_val(self))
  }
}

impl FieldValueConverter for Timestamp {
  fn to_value(self) -> Option<Value> {
    Some(Value::timestamp_val(self))
  }
}

impl FieldValueConverter for bool {
  fn to_value(self) -> Option<Value> {
    Some(Value::bool_val(self))
  }
}

impl FieldValueConverter for String {
  fn to_value(self) -> Option<Value> {
    Some(Value::string_val(self))
  }
}

impl FieldValueConverter for Vec<u8> {
  fn to_value(self) -> Option<Value> {
    Some(Value::bytes_val(self))
  }
}

impl<T: FieldValueConverter> FieldValueConverter for Option<T> {
  fn to_value(self) -> Option<Value> {
    self.and_then(|inner| inner.to_value())
  }
}

impl<T: FieldValueConverter> FieldValueConverter for Vec<T> {
  fn to_value(self) -> Option<Value> {
    let mut vals = Vec::with_capacity(self.len());
    for inner in self {
      vals.push(FieldValue {
        value: inner.to_value(),
        ..Default::default()
      })
    }
    Some(Value::list_val(RepeatedFieldValue {
      vals,
      ..Default::default()
    }))
  }
}

/// Helper macro to support [`make_row`].
#[macro_export]
macro_rules! make_row_insert {
  {$row: expr;} => {};
  {$row: expr; $key:expr => $val:expr $(,$keys:expr => $vals:expr)* $(,)?} => {
    let fv = $crate::row_helpers::FieldValue {
      value: $crate::row_helpers::FieldValueConverter::to_value($val),
      ..Default::default()
    };
    $row.insert($key.to_string(), fv);
    $crate::make_row_insert! { $row; $($keys => $vals),* }
  };
}

/// Outputs a row, given native Rust key => value pairings.
///
/// Since instantiating protobuf-generated types is very verbose,
/// this macro exists to make rows with ease:
///
/// ```
/// use pancake_db_client::make_row;
/// use protobuf::well_known_types::Timestamp;
///
/// let my_row = make_row! {
///   "string_col" => "some string".to_string(),
///   "timestamp_col" => Timestamp::now(),
///   "int_col" => Some(77),
///   "bool_col" => Option::<bool>::None,
///   "bytes_col" => vec![97_u8, 98_u8, 99_u8],
///   "bool_list_col" => vec![true, false],
/// };
/// ```
///
/// Keys can be any type supporting `.to_string()`.
/// Values can be any
/// Rust type that corresponds to a Pancake type, or `Option`s or nested `Vec`s
/// thereof.
#[macro_export]
macro_rules! make_row {
  {} => {
    $crate::row_helpers::Row::new()
  };
  {$($keys:expr => $vals:expr),+ $(,)?} => {
    {
      let mut fields = std::collections::HashMap::<String, $crate::row_helpers::FieldValue>::new();
      $crate::make_row_insert! { fields; $($keys => $vals),+ }
      $crate::row_helpers::Row {
        fields,
        ..Default::default()
      }
    }
  };
}

#[cfg(test)]
mod tests {
  use pancake_db_idl::dml::field_value::Value;
  use pancake_db_idl::dml::{FieldValue, Row};
  use protobuf::well_known_types::Timestamp;

  use crate::make_row;

  #[test]
  fn test_row_macro() {
    let proto_t = Timestamp::now();
    let row0 = make_row! {};
    let row1 = make_row! { "f32" => 3.3_f32 };
    let row2 = make_row! {
      "f32" => 3.3_f32,
      "i64" => 4_i64,
      "bool" => false,
      "timestamp" => proto_t.clone(),
      "present" => Some("asdf".to_string()),
      "absent" => Option::<String>::None,
      "bytes" => vec![0_u8, 1_u8],
      "list" => vec![1_i64, 2_i64],
    };

    assert!(row0.fields.is_empty());

    assert_eq!(row1.fields.len(), 1);

    assert_eq!(row2.fields.len(), 8);
    fn assert_val_eq(row: &Row, key: &str, value: Option<Value>) {
      assert_eq!(row.fields[key].clone(), FieldValue {
        value,
        ..Default::default()
      });
    }
    assert_val_eq(&row2, "f32", Some(Value::float32_val(3.3)));
    assert_val_eq(&row2, "i64", Some(Value::int64_val(4)));
    assert_val_eq(&row2, "bool", Some(Value::bool_val(false)));
    assert_val_eq(&row2, "timestamp", Some(Value::timestamp_val(proto_t.clone())));
    assert_val_eq(&row2, "present", Some(Value::string_val("asdf".to_string())));
    assert_val_eq(&row2, "absent", None);
    assert_val_eq(&row2, "bytes", Some(Value::bytes_val(vec![0, 1])));
    assert!(matches!(&row2.fields["list"].value, Some(Value::list_val(_))));
  }
}

#[cfg(test)]
mod tests_no_imports {
  use crate::make_row;

  #[test]
  fn test_row_macro() {
    println!("{:?}", make_row! {});
    println!("{:?}", make_row! { "a" => 3.3_f64 });
  }
}
