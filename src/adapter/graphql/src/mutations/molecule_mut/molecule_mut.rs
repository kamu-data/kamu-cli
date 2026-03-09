// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::molecule_mut::v2;
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeMut;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeMut {
    /// 2-nd Molecule API version (mutation).
    async fn v2(&self) -> v2::MoleculeMutV2 {
        v2::MoleculeMutV2
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
