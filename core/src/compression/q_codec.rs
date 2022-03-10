use q_compress::Compressor as RawQCompressor;
use q_compress::Decompressor as RawQDecompressor;
use q_compress::CompressorConfig;
use q_compress::data_types::{NumberLike, TimestampMicros};

use crate::compression::Codec;
use crate::errors::CoreResult;
use crate::primitives::Primitive;

const Q_COMPRESSION_LEVEL: usize = 7;

pub trait QCodec {
  type T: Primitive + NumberLike;
}

macro_rules! qcompressor {
  ($struct_name:ident, $primitive_type:ty) => {
    #[derive(Clone, Debug)]
    pub struct $struct_name {}

    impl Codec for $struct_name {
      type P = $primitive_type;

      fn compress_atoms(&self, primitives: &[$primitive_type]) -> CoreResult<Vec<u8>> {
        let compressor = RawQCompressor::<$primitive_type>::from_config(CompressorConfig {
          compression_level: Q_COMPRESSION_LEVEL,
          ..Default::default()
        });
        Ok(compressor.simple_compress(primitives))
      }

      fn decompress_atoms(&self, bytes: &[u8]) -> CoreResult<Vec<$primitive_type>> {
        let decompressor = RawQDecompressor::<$primitive_type>::default();
        Ok(decompressor.simple_decompress(bytes)?)
      }
    }
  }
}

qcompressor!(I64QCodec, i64);
qcompressor!(BoolQCodec, bool);
qcompressor!(F32QCodec, f32);
qcompressor!(F64QCodec, f64);
qcompressor!(TimestampMicrosQCodec, TimestampMicros);
