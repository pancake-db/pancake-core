use pancake_db_idl::dml::FieldValue;
use q_compress::{BitReader, BitWords, Decompressor};

use crate::errors::CoreResult;
use crate::primitives::Primitive;
use crate::rep_levels;
use crate::rep_levels::{RepLevelsAndAtoms, RepLevelsAndBytes};
use crate::rep_levels::AtomNester;

pub trait Codec: Send + Sync {
  type P: Primitive;

  fn compress_atoms(&self, atoms: &[<<Self as Codec>::P as Primitive>::A]) -> CoreResult<Vec<u8>>;
  fn decompress_atoms(&self, bytes: &[u8]) -> CoreResult<Vec<<<Self as Codec>::P as Primitive>::A>>;
}

pub trait ValueCodec: Send + Sync {
  fn compress(&self, values: &[FieldValue], nested_list_depth: u8) -> CoreResult<Vec<u8>>;

  fn decompress_rep_levels(&self, bytes: &[u8]) -> CoreResult<RepLevelsAndBytes>;
  fn decompress(&self, bytes: &[u8], nested_list_depth: u8) -> CoreResult<Vec<FieldValue>>;
}

impl<P: Primitive> ValueCodec for Box<dyn Codec<P=P>> {
  fn compress(&self, field_values: &[FieldValue], nested_list_depth: u8) -> CoreResult<Vec<u8>> {
    let RepLevelsAndAtoms { levels, atoms } = rep_levels::extract_levels_and_atoms::<P>(
      field_values,
      nested_list_depth,
    )?;
    let mut res = rep_levels::compress_rep_levels(levels)?;
    res.extend(self.compress_atoms(&atoms)?);
    Ok(res)
  }

  fn decompress_rep_levels(&self, bytes: &[u8]) -> CoreResult<RepLevelsAndBytes> {
    let decompressor = Decompressor::<u32>::default();
    let words = BitWords::from(bytes);
    let mut reader = BitReader::from(&words);
    let flags = decompressor.header(&mut reader)?;
    let mut rep_levels = Vec::new();
    while let Some(chunk) = decompressor.chunk(&mut reader, &flags)? {
      rep_levels.extend(
        chunk.nums
          .iter()
          .map(|&l| l as u8)
      );
    }

    let byte_idx = reader.aligned_byte_idx()?;
    Ok(RepLevelsAndBytes {
      remaining_bytes: reader.read_aligned_bytes(bytes.len() - byte_idx)?.to_vec(),
      levels: rep_levels,
    })
  }

  fn decompress(&self, bytes: &[u8], nested_list_depth: u8) -> CoreResult<Vec<FieldValue>> {
    let RepLevelsAndBytes { remaining_bytes, levels } = self.decompress_rep_levels(bytes)?;
    let atoms: Vec<P::A> = self.decompress_atoms(&remaining_bytes)?;
    let mut nester = AtomNester::<P>::from_levels_and_values(
      levels,
      atoms,
      nested_list_depth,
    );
    nester.nested_field_values()
  }
}
