// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::GetSummaryOpts;
use opendatafabric as odf;

use crate::prelude::*;
use crate::utils;

///////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_scheduling_permission(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
) -> Result<()> {
    use kamu_core::auth;
    let dataset_action_authorizer = from_catalog::<dyn auth::DatasetActionAuthorizer>(ctx).unwrap();

    dataset_action_authorizer
        .check_action_allowed(dataset_handle, auth::DatasetAction::Write)
        .await
        .map_err(|_| {
            GqlError::Gql(
                Error::new("Dataset access error")
                    .extend_with(|_, eev| eev.set("alias", dataset_handle.alias.to_string())),
            )
        })?;

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_expected_dataset_kind(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
    dataset_flow_type: DatasetFlowType,
) -> Result<Option<FlowIncompatibleDatasetKind>> {
    let dataset_flow_type: kamu_flow_system::DatasetFlowType = dataset_flow_type.into();
    match dataset_flow_type.dataset_kind_restriction() {
        Some(expected_kind) => {
            let dataset = utils::get_dataset(ctx, dataset_handle).await?;

            let dataset_kind = dataset
                .get_summary(GetSummaryOpts::default())
                .await
                .int_err()?
                .kind;

            if dataset_kind != expected_kind {
                Ok(Some(FlowIncompatibleDatasetKind {
                    expected_dataset_kind: expected_kind.into(),
                    actual_dataset_kind: dataset_kind.into(),
                }))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct FlowIncompatibleDatasetKind {
    pub expected_dataset_kind: DatasetKind,
    pub actual_dataset_kind: DatasetKind,
}

#[ComplexObject]
impl FlowIncompatibleDatasetKind {
    pub(crate) async fn message(&self) -> String {
        format!(
            "Expected a {} dataset, but a {} dataset was provided",
            self.expected_dataset_kind, self.actual_dataset_kind,
        )
    }
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                DatasetKind::Root => "Root",
                DatasetKind::Derivative => "Derivative",
            }
        )
    }
}

///////////////////////////////////////////////////////////////////////////////
