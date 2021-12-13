use crate::errors::CoreResult;
use q_compress::{Decompressor, BitReader, Compressor};

pub fn compress_deletions(is_deleted: Vec<bool>) -> CoreResult<Vec<u8>> {
  let compressor = Compressor::train(is_deleted.clone(), 1)?;
  Ok(compressor.compress(&is_deleted)?)
}

pub fn decompress_deletions(bytes: Vec<u8>) -> CoreResult<Vec<bool>> {
  let mut reader = BitReader::from(bytes);
  let decompressor = Decompressor::<bool>::from_reader(&mut reader)?;
  Ok(decompressor.decompress(&mut reader))
}
