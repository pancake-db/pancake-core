use std::time::SystemTime;
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use q_compress::data_types::{NumberLike, TimestampMicros};

use prost_types::Timestamp;

use crate::compression::Codec;
use crate::compression::q_codec::TimestampMicrosQCodec;
use crate::compression::Q_COMPRESS;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};

impl Atom for TimestampMicros {
  const BYTE_SIZE: usize = 12;

  fn to_bytes(&self) -> Vec<u8> {
    NumberLike::to_bytes(*self)
  }

  fn try_from_bytes(bytes: &[u8]) -> CoreResult<Self> {
    Ok(TimestampMicros::from_bytes(bytes.to_vec())?)
  }
}

impl Primitive for TimestampMicros {
  type A = Self;
  const DTYPE: DataType = DataType::TimestampMicros;

  const IS_ATOMIC: bool = true;

  fn to_value(&self) -> Value {
    let mut t = Timestamp::from(SystemTime::now());
    let (secs, nanos) = self.to_secs_and_nanos();
    t.seconds = secs;
    t.nanos = nanos as i32;
    Value::TimestampVal(t)
  }

  fn try_from_value(v: &Value) -> CoreResult<TimestampMicros> {
    match v {
      Value::TimestampVal(res) => Ok(TimestampMicros::from_secs_and_nanos(res.seconds, res.nanos as u32)),
      _ => Err(CoreError::invalid("cannot read timestamp from value")),
    }
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

