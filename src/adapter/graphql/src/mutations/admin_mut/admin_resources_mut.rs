// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::{ResourceApplyResult, ResourceDeleteResult};
use crate::prelude::*;
use crate::queries::{ResourceManifestFormat, ResourceSelectorInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AdminResourcesMut {
    account_id: odf::AccountID,
}

impl AdminResourcesMut {
    pub fn new(account_id: AccountID<'_>) -> Self {
        Self {
            account_id: account_id.into(),
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl AdminResourcesMut {
    #[tracing::instrument(level = "info", name = AdminResourcesMut_apply_manifest, skip_all)]
    async fn apply_manifest(
        &self,
        ctx: &Context<'_>,
        manifest: String,
        format: ResourceManifestFormat,
        dry_run: Option<bool>,
    ) -> Result<ResourceApplyResult> {
        let _ = (&self.account_id, ctx, manifest, format, dry_run);
        todo!("AdminResourcesMut.apply_manifest is not implemented yet");
    }

    #[tracing::instrument(level = "info", name = AdminResourcesMut_delete, skip_all)]
    async fn delete(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<ResourceDeleteResult> {
        let _ = (&self.account_id, ctx, selector);
        todo!("AdminResourcesMut.delete is not implemented yet");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
