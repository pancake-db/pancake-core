use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use protobuf::well_known_types::Timestamp;
use q_compress::TimestampMicros;
use q_compress::types::NumberLike;

use crate::compression::Codec;
use crate::compression::q_codec::TimestampMicrosQCodec;
use crate::compression::Q_COMPRESS;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};

impl Atom for TimestampMicros {
  const BYTE_SIZE: usize = 12;

  fn to_bytes(&self) -> Vec<u8> {
    TimestampMicros::bytes_from(*self)
  }

  fn try_from_bytes(bytes: &[u8]) -> CoreResult<Self> {
    Ok(TimestampMicros::from_bytes_safe(bytes)?)
  }
}

impl Primitive for TimestampMicros {
  const DTYPE: DataType = DataType::TIMESTAMP_MICROS;
  const IS_ATOMIC: bool = true;

  type A = Self;

  fn try_from_value(v: &Value) -> CoreResult<TimestampMicros> {
    match v {
      Value::timestamp_val(res) => Ok(TimestampMicros::from_secs_and_nanos(res.seconds, res.nanos as u32)),
      _ => Err(CoreError::invalid("cannot read timestamp from value")),
    }
  }

  fn to_value(&self) -> Value {
    let mut t = Timestamp::new();
    let (secs, nanos) = self.to_secs_and_nanos();
    t.seconds = secs;
    t.nanos = nanos as i32;
    Value::timestamp_val(t)
  }

  fn to_atoms(&self) -> Vec<Self> {
    vec![*self]
  }

  fn try_from_atoms(atoms: &[Self]) -> CoreResult<Self> {
    Ok(atoms[0])
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>> {
    if codec == Q_COMPRESS {
      Some(Box::new(TimestampMicrosQCodec {}))
    } else {
      None
    }
  }
}

