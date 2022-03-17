use pancake_db_idl::dtype::DataType;
use q_compress::data_types::TimestampMicros;

use crate::errors::{CoreError, CoreResult};
use crate::primitives::Primitive;

use super::{Q_COMPRESS, ZSTD};
use super::ValueCodec;

pub fn new_codec(
  dtype: DataType,
  codec: &str,
) -> CoreResult<Box<dyn ValueCodec>> {
  let maybe_res: Option<Box<dyn ValueCodec>> = match dtype {
    DataType::String => String::new_value_codec(codec),
    DataType::Int64 => i64::new_value_codec(codec),
    DataType::Bytes => Vec::<u8>::new_value_codec(codec),
    DataType::Bool => bool::new_value_codec(codec),
    DataType::Float32 => f32::new_value_codec(codec),
    DataType::Float64 => f64::new_value_codec(codec),
    DataType::TimestampMicros => TimestampMicros::new_value_codec(codec),
  };

  match maybe_res {
    Some(res) => Ok(res),
    None => Err(CoreError::invalid(&format!(
      "compression codec {} unavailable for data type {:?}",
      codec,
      dtype,
    )))
  }
}

pub fn choose_codec(dtype: DataType) -> String {
  match dtype {
    DataType::Int64 => Q_COMPRESS.to_string(),
    DataType::String => ZSTD.to_string(),
    DataType::Bytes => ZSTD.to_string(),
    DataType::Float32 => Q_COMPRESS.to_string(),
    DataType::Float64 => Q_COMPRESS.to_string(),
    DataType::Bool => Q_COMPRESS.to_string(),
    DataType::TimestampMicros => Q_COMPRESS.to_string(),
  }
}

