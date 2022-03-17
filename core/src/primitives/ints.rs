use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::compression::Codec;
use crate::compression::q_codec::I64QCodec;
use crate::compression::Q_COMPRESS;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};
use crate::utils;

impl Atom for i64 {
  const BYTE_SIZE: usize = 8;

  fn to_bytes(&self) -> Vec<u8> {
    self.to_be_bytes().to_vec()
  }

  fn try_from_bytes(bytes: &[u8]) -> CoreResult<Self> {
    let byte_array = utils::try_byte_array::<8>(bytes)?;
    Ok(i64::from_be_bytes(byte_array))
  }
}

impl Primitive for i64 {
  type A = Self;
  const DTYPE: DataType = DataType::Int64;

  const IS_ATOMIC: bool = true;

  fn to_value(&self) -> Value {
    Value::Int64Val(*self)
  }

  fn try_from_value(v: &Value) -> CoreResult<i64> {
    match v {
      Value::Int64Val(res) => Ok(*res),
      _ => Err(CoreError::invalid("cannot read i64 from value")),
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
      Some(Box::new(I64QCodec {}))
    } else {
      None
    }
  }
}

