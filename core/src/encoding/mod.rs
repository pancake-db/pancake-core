use pancake_db_idl::dml::FieldValue;
use pancake_db_idl::dtype::DataType;
use q_compress::data_types::TimestampMicros;

pub use decoder::ByteIdx;
pub use decoder::Decodable;
pub use decoder::Decoder;
pub use decoder::DecoderImpl;
pub use encoder::Encoder;
pub use encoder::EncoderImpl;

use crate::primitives::Primitive;

mod byte_reader;
mod decoder;
mod encoder;

const ESCAPE_BYTE: u8 = 255;
const COUNT_BYTE: u8 = 254;
const NULL_BYTE: u8 = 253;

fn encoder_for<P: Primitive>(nested_list_depth: u8) -> Box<dyn Encoder> {
  Box::new(EncoderImpl::<P>::new(nested_list_depth))
}

fn field_value_decoder_for<P: Primitive>(nested_list_depth: u8) -> Box<dyn Decoder<FieldValue>> {
  Box::new(DecoderImpl::<P, FieldValue>::new(nested_list_depth))
}

fn byte_idx_decoder_for<P: Primitive>(nested_list_depth: u8) -> Box<dyn Decoder<ByteIdx>> {
  Box::new(DecoderImpl::<P, ByteIdx>::new(nested_list_depth))
}

pub fn new_encoder(dtype: DataType, nested_list_depth: u8) -> Box<dyn Encoder> {
  match dtype {
    DataType::Int64 => encoder_for::<i64>(nested_list_depth),
    DataType::String => encoder_for::<String>(nested_list_depth),
    DataType::Float32 => encoder_for::<f32>(nested_list_depth),
    DataType::Float64 => encoder_for::<f64>(nested_list_depth),
    DataType::Bytes => encoder_for::<Vec<u8>>(nested_list_depth),
    DataType::Bool => encoder_for::<bool>(nested_list_depth),
    DataType::TimestampMicros => encoder_for::<TimestampMicros>(nested_list_depth),
  }
}

pub fn new_field_value_decoder(dtype: DataType, nested_list_depth: u8) -> Box<dyn Decoder<FieldValue>> {
  match dtype {
    DataType::Int64 => field_value_decoder_for::<i64>(nested_list_depth),
    DataType::String => field_value_decoder_for::<String>(nested_list_depth),
    DataType::Float32 => field_value_decoder_for::<f32>(nested_list_depth),
    DataType::Float64 => field_value_decoder_for::<f64>(nested_list_depth),
    DataType::Bytes => field_value_decoder_for::<Vec<u8>>(nested_list_depth),
    DataType::Bool => field_value_decoder_for::<bool>(nested_list_depth),
    DataType::TimestampMicros => field_value_decoder_for::<TimestampMicros>(nested_list_depth),
  }
}

pub fn new_byte_idx_decoder(dtype: DataType, nested_list_depth: u8) -> Box<dyn Decoder<ByteIdx>> {
  match dtype {
    DataType::Int64 => byte_idx_decoder_for::<i64>(nested_list_depth),
    DataType::String => byte_idx_decoder_for::<String>(nested_list_depth),
    DataType::Float32 => byte_idx_decoder_for::<f32>(nested_list_depth),
    DataType::Float64 => byte_idx_decoder_for::<f64>(nested_list_depth),
    DataType::Bytes => byte_idx_decoder_for::<Vec<u8>>(nested_list_depth),
    DataType::Bool => byte_idx_decoder_for::<bool>(nested_list_depth),
    DataType::TimestampMicros => byte_idx_decoder_for::<TimestampMicros>(nested_list_depth),
  }
}

#[cfg(test)]
mod tests {
  use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
  use pancake_db_idl::dml::field_value::Value;

  use crate::errors::CoreResult;
  use crate::primitives::Primitive;

  use super::*;
  use crate::rep_levels::RepLevelsAndAtoms;

  fn build_list_val(l: Vec<Value>) -> Value {
    Value::ListVal(RepeatedFieldValue {
      vals: l.iter().map(|x| FieldValue {
        value: Some(x.clone()),
      }).collect(),
    })
  }

  fn encode<P: Primitive>(fvs: &[FieldValue], escape_depth: u8) -> CoreResult<Vec<u8>> {
    let encoder = EncoderImpl::<P>::new(escape_depth);
    encoder.encode(fvs)
  }

  fn decode<P: Primitive>(encoded: &[u8], escape_depth: u8) -> CoreResult<Vec<FieldValue>> {
    let decoder = DecoderImpl::<P, FieldValue>::new(escape_depth);
    decoder.decode(encoded)
  }

  #[test]
  fn test_bytess() -> CoreResult<()> {
    let bytess = vec![
      Some(vec![0_u8, 255, 255, 254, 253]), // some bytes that need escaping
      None,
      Some(vec![]),
      Some(vec![77].repeat(2081))
    ];

    let values = bytess.iter()
      .map(|maybe_bytes| FieldValue {
        value: maybe_bytes.as_ref().map(|bytes| Value::BytesVal(bytes.to_vec())),
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode::<Vec<u8>>(&values, 0)?;
    let decoded = decode::<Vec<u8>>(&encoded, 0)?;
    let recovered = decoded.iter()
      .map(|fv| fv.value.as_ref().map(|v| match v {
        Value::BytesVal(b) => b.clone(),
        _ => panic!(),
      }))
      .collect::<Vec<Option<Vec<u8>>>>();

    assert_eq!(recovered, bytess);
    Ok(())
  }

  #[test]
  fn test_ints() -> CoreResult<()> {
    let ints: Vec<Option<i64>> = vec![
      Some(i64::MIN),
      Some(i64::MAX),
      None,
      Some(0),
      Some(-1),
    ];

    let values = ints.iter()
      .map(|maybe_x| FieldValue {
        value: maybe_x.map(|x| Value::Int64Val(x)),
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode::<i64>(&values, 0)?;
    let decoded = decode::<i64>(&encoded, 0)?;
    let recovered = decoded.iter()
      .map(|fv| fv.value.as_ref().map(|v| match v {
        Value::Int64Val(x) => *x,
        _ => panic!(),
      }))
      .collect::<Vec<Option<i64>>>();

    assert_eq!(recovered, ints);
    Ok(())
  }

  #[test]
  fn test_nested_strings() -> CoreResult<()> {
    let strings = vec![
      Some(vec![
        vec!["azAZ09﹝ﾂﾂﾂ﹞ꗽꗼ".to_string(), "abc".to_string()],
        vec!["/\\''!@#$%^&*()".to_string()],
      ]),
      None,
      Some(vec![
        vec!["".to_string()],
        vec!["z".repeat(2)],
        vec!["null".to_string()]
      ]),
      Some(vec![vec![]]),
      Some(vec![])
    ];

    let values = strings.iter()
      .map(|maybe_x| FieldValue {
        value: maybe_x.as_ref().map(|x0| build_list_val(
          x0.iter().map(|x1| build_list_val(
            x1.iter().map(|x2| Value::StringVal(x2.to_string())).collect()
          )).collect()
        )),
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode::<String>(&values, 2)?;
    let decoded = decode::<String>(&encoded, 2)?;
    let recovered = decoded.iter()
      .map(|fv| fv.value.as_ref().map(|v| match v {
        Value::ListVal(RepeatedFieldValue { vals }) => vals.iter()
          .map(|fv| match fv.value.as_ref().unwrap() {
            Value::ListVal(RepeatedFieldValue { vals }) => vals.iter()
              .map(|fv| match fv.value.as_ref().unwrap() {
                Value::StringVal(s) => s.to_string(),
                _ => panic!(),
              })
              .collect(),
            _ => panic!()
          })
          .collect(),
        _ => panic!()
      }))
      .collect::<Vec<Option<Vec<Vec<String>>>>>();

    assert_eq!(recovered, strings);
    Ok(())
  }

  #[test]
  fn test_decode_rep_levels() -> CoreResult<()> {
    let strings = vec![
      Some(vec![
        "abc".to_string(),
        "de".to_string(),
      ]),
      None,
      Some(vec![
        "f".to_string(),
      ]),
      Some(vec!["".to_string()]),
      Some(vec![])
    ];

    let values = strings.iter()
      .map(|maybe_x| FieldValue {
        value: maybe_x.as_ref().map(|x0| build_list_val(
          x0.iter().map(|x1| Value::StringVal(x1.to_string())).collect()
        )),
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode::<String>(&values, 1)?;
    let decoder = DecoderImpl::<String, RepLevelsAndAtoms<u8>>::new(1);
    let decoded = decoder.decode(&encoded)?;
    let mut combined = RepLevelsAndAtoms::default();
    for x in &decoded {
      combined.extend(x);
    }
    assert_eq!(
      combined.levels,
      vec![
        3, 3, 3, 2,
        3, 3, 2, 1,
        0,
        3, 2, 1,
        2, 1,
        1,
      ]
    );
    assert_eq!(
      combined.atoms,
      vec![97_u8, 98, 99, 100, 101, 102] // a through f
    );
    Ok(())
  }
}
