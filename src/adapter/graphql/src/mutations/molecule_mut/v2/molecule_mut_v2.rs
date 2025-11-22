// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::DatasetAction;

use crate::mutations::molecule_mut::v1;
use crate::mutations::molecule_mut::v2::MoleculeProjectMutV2;
use crate::prelude::*;
use crate::queries::molecule::v1 as qv1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct MoleculeMutV2 {
    v1: v1::MoleculeMutV1,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeMutV2 {
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV2_create_project, skip_all, fields(?ipnft_symbol, ?ipnft_uid))]
    async fn create_project(
        &self,
        ctx: &Context<'_>,
        ipnft_symbol: String,
        ipnft_uid: String,
        ipnft_address: String,
        ipnft_token_id: U256,
    ) -> Result<v1::CreateProjectResult> {
        self.v1
            .create_project(ctx, ipnft_symbol, ipnft_uid, ipnft_address, ipnft_token_id)
            .await
    }

    /// Retracts a project from the `projects` dataset.
    /// History of this project existing will be preserved,
    /// its symbol will remain reserved, data will remain intact,
    /// but the project will no longer appear in the listing.
    #[tracing::instrument(level = "info", name = MoleculeMutV2_disable_project, skip_all, fields(?ipnft_uid))]
    async fn disable_project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<MoleculeDisableProjectResultV2> {
        let _ = ipnft_uid;
        todo!()
    }

    #[tracing::instrument(level = "info", name = MoleculeMutV2_enable_project, skip_all, fields(?ipnft_uid))]
    async fn enable_project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<MoleculeEnableProjectResultV2> {
        let _ = ipnft_uid;
        todo!()
    }

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMutV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectMutV2>> {
        use datafusion::logical_expr::{col, lit};

        let Some(df) = qv1::MoleculeV1::get_projects_snapshot(ctx, DatasetAction::Read, false)
            .await?
            .1
        else {
            return Ok(None);
        };

        let df = df.filter(col("ipnft_uid").eq(lit(ipnft_uid))).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);
        let entry = MoleculeProjectMutV2::from_json(records.into_iter().next().unwrap())?;

        Ok(Some(entry))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDisableProjectResultV2 {
    pub dummy: String,
}

#[derive(SimpleObject)]
pub struct MoleculeEnableProjectResultV2 {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
