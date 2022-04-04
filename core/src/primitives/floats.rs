use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::compression::Codec;
use crate::compression::q_codec::{F64QCodec, F32QCodec};
use crate::compression::Q_COMPRESS;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};
use crate::utils;

impl Atom for f32 {
  const BYTE_SIZE: usize = 4;

  fn to_bytes(&self) -> Vec<u8> {
    self.to_be_bytes().to_vec()
  }

  fn try_from_bytes(bytes: &[u8]) -> CoreResult<Self> where Self: Sized {
    let byte_array = utils::try_byte_array::<4>(bytes)?;
    Ok(f32::from_be_bytes(byte_array))
  }
}

impl Atom for f64 {
  const BYTE_SIZE: usize = 8;

  fn to_bytes(&self) -> Vec<u8> {
    self.to_be_bytes().to_vec()
  }

  fn try_from_bytes(bytes: &[u8]) -> CoreResult<Self> {
    let byte_array = utils::try_byte_array::<8>(bytes)?;
    Ok(f64::from_be_bytes(byte_array))
  }
}

impl Primitive for f32 {
  type A = Self;
  const DTYPE: DataType = DataType::Float32;

  const IS_ATOMIC: bool = true;

  fn to_value(&self) -> Value {
    Value::Float32Val(*self)
  }

  fn try_from_value(v: &Value) -> CoreResult<f32> {
    match v {
      Value::Float32Val(res) => Ok(*res),
      _ => Err(CoreError::invalid("cannot read f32 from value")),
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
      Some(Box::new(F32QCodec {}))
    } else {
      None
    }
  }
}

impl Primitive for f64 {
  type A = Self;
  const DTYPE: DataType = DataType::Float64;

  const IS_ATOMIC: bool = true;

  fn to_value(&self) -> Value {
    Value::Float64Val(*self)
  }

  fn try_from_value(v: &Value) -> CoreResult<f64> {
    match v {
      Value::Float64Val(res) => Ok(*res),
      _ => Err(CoreError::invalid("cannot read f64 from value")),
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
      Some(Box::new(F64QCodec {}))
    } else {
      None
    }
  }
}

