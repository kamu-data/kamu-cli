// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use alloy_core::dyn_abi::TypedData as AlloyEip712TypedData;
use alloy_core::primitives::B256;
use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype::nutype(derive(Debug, AsRef), cfg_attr(feature = "serde", derive(Deserialize)))]
pub struct Eip712TypedData(AlloyEip712TypedData);

impl Eip712TypedData {
    pub fn from_json(json: serde_json::Value) -> Result<Self, InternalError> {
        let typed_data = serde_json::from_value(json).int_err()?;
        Ok(Eip712TypedData::new(typed_data))
    }

    pub fn hash_struct(&self) -> Result<B256, InternalError> {
        self.as_ref().hash_struct().int_err()
    }

    pub fn domain_separator(&self) -> B256 {
        self.as_ref().domain.separator()
    }

    pub fn eip712_signing_hash(&self) -> Result<B256, InternalError> {
        self.as_ref().eip712_signing_hash().int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
