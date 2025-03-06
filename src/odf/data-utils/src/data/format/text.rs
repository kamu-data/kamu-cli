// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arrow::array::OffsetSizeTrait;
use arrow::datatypes::ArrowPrimitiveType;
use serde::ser::Serializer;

use super::traits::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BooleanEncoder<'a>(pub &'a arrow::array::BooleanArray);
impl Encoder for BooleanEncoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        if self.0.value(idx) {
            write!(buf, "true")?;
        } else {
            write!(buf, "false")?;
        }
        Ok(())
    }
}

pub struct IntegerEncoder<'a, T: ArrowPrimitiveType>(pub &'a arrow::array::PrimitiveArray<T>);
impl<T: ArrowPrimitiveType> Encoder for IntegerEncoder<'_, T>
where
    <T as ArrowPrimitiveType>::Native: std::fmt::Display,
{
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "{}", self.0.value(idx))?;
        Ok(())
    }
}

pub struct Float16Encoder<'a>(pub &'a arrow::array::Float16Array);
impl Encoder for Float16Encoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let mut serializer = serde_json::Serializer::new(buf);
        serializer
            .serialize_f32(self.0.value(idx).to_f32())
            .unwrap();
        Ok(())
    }
}

pub struct Float32Encoder<'a>(pub &'a arrow::array::Float32Array);
impl Encoder for Float32Encoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let mut serializer = serde_json::Serializer::new(buf);
        serializer.serialize_f32(self.0.value(idx)).unwrap();
        Ok(())
    }
}

pub struct Float64Encoder<'a>(pub &'a arrow::array::Float64Array);
impl Encoder for Float64Encoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let mut serializer = serde_json::Serializer::new(buf);
        serializer.serialize_f64(self.0.value(idx)).unwrap();
        Ok(())
    }
}

pub struct Decimal128Encoder<'a>(pub &'a arrow::array::Decimal128Array);
impl Encoder for Decimal128Encoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        // TODO: PERF: Avoid allocation
        write!(buf, "{}", self.0.value_as_string(idx))?;
        Ok(())
    }
}

pub struct Decimal256Encoder<'a>(pub &'a arrow::array::Decimal256Array);
impl Encoder for Decimal256Encoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        // TODO: PERF: Avoid allocation
        write!(buf, "{}", self.0.value_as_string(idx))?;
        Ok(())
    }
}

pub struct StringEncoder<'a, OffsetSize: OffsetSizeTrait>(
    pub &'a arrow::array::GenericStringArray<OffsetSize>,
);
impl<OffsetSize: OffsetSizeTrait> Encoder for StringEncoder<'_, OffsetSize> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "{}", self.0.value(idx))?;
        Ok(())
    }
}

pub struct StringViewEncoder<'a>(pub &'a arrow::array::StringViewArray);
impl Encoder for StringViewEncoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "{}", self.0.value(idx))?;
        Ok(())
    }
}

pub struct BinaryHexEncoder<'a, OffsetSize: OffsetSizeTrait>(
    pub &'a arrow::array::GenericBinaryArray<OffsetSize>,
);
impl<OffsetSize: OffsetSizeTrait> Encoder for BinaryHexEncoder<'_, OffsetSize> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let hex_str = hex::encode(self.0.value(idx));
        write!(buf, "{hex_str}")?;
        Ok(())
    }
}

pub struct BinaryViewHexEncoder<'a>(pub &'a arrow::array::BinaryViewArray);
impl Encoder for BinaryViewHexEncoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let hex_str = hex::encode(self.0.value(idx));
        write!(buf, "{hex_str}")?;
        Ok(())
    }
}

pub struct BinaryFixedHexEncoder<'a>(pub &'a arrow::array::FixedSizeBinaryArray);
impl Encoder for BinaryFixedHexEncoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        let hex_str = hex::encode(self.0.value(idx));
        write!(buf, "{hex_str}")?;
        Ok(())
    }
}

/// This encoder uses default arrow representation as determined by
/// [`arrow::util::display::ArrayFormatter`]. When using this encoder you have
/// to be absolutely sure that result does not include symbols that have special
/// meaning within a JSON string.
pub struct ArrowEncoder<'a>(pub arrow::util::display::ArrayFormatter<'a>);
impl Encoder for ArrowEncoder<'_> {
    fn encode(&mut self, idx: usize, buf: &mut dyn std::io::Write) -> Result<(), WriterError> {
        write!(buf, "{}", self.0.value(idx))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
