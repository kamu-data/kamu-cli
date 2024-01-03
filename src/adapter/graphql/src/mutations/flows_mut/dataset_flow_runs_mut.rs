// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_core::CurrentAccountSubject;
use kamu_flow_system::{FlowKeyDataset, FlowService, RequestFlowError};
use opendatafabric as odf;

use super::{
    ensure_expected_dataset_kind,
    ensure_scheduling_permission,
    FlowIncompatibleDatasetKind,
};
use crate::prelude::*;
use crate::LoggedInGuard;

///////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowRunsMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetFlowRunsMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn trigger_flow(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<TriggerFlowResult> {
        if let Some(e) =
            ensure_expected_dataset_kind(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(TriggerFlowResult::IncompatibleDatasetKind(e));
        }

        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

        let flow_service = from_catalog::<dyn FlowService>(ctx).unwrap();
        let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();
        let logged_account = match current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(la) => la,
            CurrentAccountSubject::Anonymous(_) => {
                unreachable!("LoggedInGuard would not allow anonymous accounts here")
            }
        };

        let res = flow_service
            .trigger_manual_flow(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                odf::AccountID::from(odf::FAKE_ACCOUNT_ID),
                logged_account.account_name.clone(),
            )
            .await
            .map_err(|e| match e {
                RequestFlowError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: res.into(),
        }))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum TriggerFlowResult {
    Success(TriggerFlowSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct TriggerFlowSuccess {
    pub flow: Flow,
}

#[ComplexObject]
impl TriggerFlowSuccess {
    pub async fn message(&self) -> String {
        format!("Success")
    }
}

///////////////////////////////////////////////////////////////////////////////
