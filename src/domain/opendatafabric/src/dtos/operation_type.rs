// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OperationType {
    Append = 0,
    Retract = 1,
    CorrectFrom = 2,
    CorrectTo = 3,
}

impl OperationType {
    pub const SHORT_CODE_APPEND: &'static str = "+A";
    pub const SHORT_CODE_RETRACT: &'static str = "-R";
    pub const SHORT_CODE_CORRECTFROM: &'static str = "-C";
    pub const SHORT_CODE_CORRECTTO: &'static str = "+C";

    pub fn short_code(&self) -> &'static str {
        match self {
            OperationType::Append => Self::SHORT_CODE_APPEND,
            OperationType::Retract => Self::SHORT_CODE_RETRACT,
            OperationType::CorrectFrom => Self::SHORT_CODE_CORRECTFROM,
            OperationType::CorrectTo => Self::SHORT_CODE_CORRECTTO,
        }
    }
}

impl TryFrom<u8> for OperationType {
    type Error = InvalidOperationType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OperationType::Append),
            1 => Ok(OperationType::Retract),
            2 => Ok(OperationType::CorrectFrom),
            3 => Ok(OperationType::CorrectTo),
            _ => Err(InvalidOperationType(value)),
        }
    }
}

impl From<OperationType> for u8 {
    fn from(val: OperationType) -> Self {
        val as u8
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid operation type value {0}")]
pub struct InvalidOperationType(pub u8);
