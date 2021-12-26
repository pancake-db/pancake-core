use crate::errors::CoreResult;
use q_compress::{Compressor, Decompressor};

pub fn compress_deletions(is_deleted: Vec<bool>) -> CoreResult<Vec<u8>> {
  let compressor = Compressor::<bool>::default();
  Ok(compressor.simple_compress(&is_deleted)?)
}

pub fn decompress_deletions(bytes: Vec<u8>) -> CoreResult<Vec<bool>> {
  let decompressor = Decompressor::<bool>::default();
  Ok(decompressor.simple_decompress(bytes)?)
}
