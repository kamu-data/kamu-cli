// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde::{Deserialize, Serialize};

use super::*;
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type AccountHandleProxy = crate::serde::yaml::auth::AccountHandle;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AccountHandleProxy", into = "AccountHandleProxy")]
pub struct AccountHandle {
    /// ID of the account resource.
    pub id: crate::resource::ResourceID,
    /// DID of the account.
    pub did: AccountID,
    /// Name of the account.
    pub name: AccountName,
}

impl AccountHandle {
    pub fn new(id: crate::resource::ResourceID, did: AccountID, name: AccountName) -> Self {
        Self { id, did, name }
    }

    pub fn new_test(name: &str) -> AccountHandle {
        AccountHandle {
            id: crate::resource::ResourceID::new(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                name.as_bytes(),
            )),
            did: AccountID::new_seeded_ed25519(name.as_bytes()),
            name: AccountName::new_unchecked(name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for AccountHandle {
    type OffsetT = WIPOffset<fb::AccountHandle<'fb>>;

    fn serialize(&self, _fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        todo!()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AccountHandle<'fb>> for AccountHandle {
    fn deserialize(_proxy: fb::AccountHandle<'fb>) -> Self {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
