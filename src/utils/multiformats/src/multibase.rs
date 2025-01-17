// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::stack_string::StackString;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Multibase {
    /// Hexadecimal (lowercase)
    Base16 = b'f',
    /// RFC4648 no padding
    Base64Url = b'u',
    /// Base58 Bitcoin
    Base58Btc = b'z',
}

impl Multibase {
    pub fn decode(s: &str, buf: &mut [u8]) -> Result<usize, MultibaseError> {
        let (prefix, encoded) = s.as_bytes().split_at(1);
        match prefix[0] {
            b'f' | b'F' => {
                let data_len = encoded.len() / 2;
                if data_len > buf.len() {
                    Err(MultibaseError::BufferTooSmall)
                } else {
                    match hex::decode_to_slice(encoded, &mut buf[..data_len]) {
                        Ok(()) => Ok(encoded.len() / 2),
                        Err(_) => Err(MultibaseError::Malformed),
                    }
                }
            }
            b'u' => {
                use base64::Engine as _;

                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode_slice(encoded, buf)
                    .map_err(|e| match e {
                        base64::DecodeSliceError::DecodeError(_) => MultibaseError::Malformed,
                        base64::DecodeSliceError::OutputSliceTooSmall => {
                            MultibaseError::BufferTooSmall
                        }
                    })
            }
            b'z' => match bs58::decode(encoded)
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .onto(buf)
            {
                Ok(len) => Ok(len),
                Err(bs58::decode::Error::BufferTooSmall) => Err(MultibaseError::BufferTooSmall),
                Err(_) => Err(MultibaseError::Malformed),
            },
            _ => Err(MultibaseError::UnsupportedEncoding(prefix[0])),
        }
    }

    pub fn encode<const S: usize>(bytes: &[u8], encoding: Multibase) -> StackString<S> {
        Self::format(bytes, encoding).to_str()
    }

    pub fn format<const S: usize>(bytes: &[u8], encoding: Multibase) -> MultibaseFmt<'_, S> {
        MultibaseFmt::new(bytes, encoding)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MultibaseFmt<'a, const S: usize> {
    bytes: &'a [u8],
    encoding: Multibase,
}

impl<'a, const S: usize> MultibaseFmt<'a, S> {
    pub fn new(bytes: &'a [u8], encoding: Multibase) -> Self {
        Self { bytes, encoding }
    }

    pub fn encoding(self, encoding: Multibase) -> Self {
        Self { encoding, ..self }
    }

    pub fn to_str(&self) -> StackString<S> {
        let mut buf = [0_u8; S];

        let len = match self.encoding {
            Multibase::Base16 => {
                buf[0] = b'f';
                let str_len = self.bytes.len() * 2;
                hex::encode_to_slice(self.bytes, &mut buf[1..=str_len]).unwrap();
                1 + str_len
            }
            Multibase::Base64Url => {
                use base64::Engine as _;

                buf[0] = b'u';
                let str_len = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode_slice(self.bytes, &mut buf[1..])
                    .unwrap();
                1 + str_len
            }
            Multibase::Base58Btc => {
                buf[0] = b'z';
                let len = bs58::encode(self.bytes)
                    .with_alphabet(bs58::Alphabet::BITCOIN)
                    .onto(&mut buf[1..])
                    .unwrap();

                1 + len
            }
        };

        StackString::new(buf, len)
    }
}

impl<const S: usize> std::fmt::Display for MultibaseFmt<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MultibaseError {
    #[error("Unsupported multibase encoding code '{0}'")]
    UnsupportedEncoding(u8),
    #[error("Buffer too small")]
    BufferTooSmall,
    #[error("Malformed")]
    Malformed,
}
