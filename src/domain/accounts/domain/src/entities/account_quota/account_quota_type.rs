// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QuotaType(String);

impl QuotaType {
    pub const STORAGE_SPACE: &'static str = "dev.kamu.quota.storage.space";

    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn storage_space() -> Self {
        Self::new(Self::STORAGE_SPACE)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for QuotaType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for QuotaType {
    fn from(value: &str) -> Self {
        QuotaType::new(value)
    }
}

impl From<String> for QuotaType {
    fn from(value: String) -> Self {
        QuotaType::new(value)
    }
}

impl AsRef<str> for QuotaType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum QuotaUnit {
    Bytes,
}

impl Display for QuotaUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaUnit::Bytes => write!(f, "Bytes"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
