// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::transaction;
use dill::{component, interface};
use kamu_accounts::Principal;
use kamu_core::{SyncError, SyncListener, SyncOptions, SyncResult, SyncService, SyncToJob};
use kamu_datasets::{DatasetOperation, DatasetOperationGuard, DatasetOperationRequest};

use crate::SyncRequestBuilder;
use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SyncToJob)]
pub struct SyncToJobImpl {
    catalog: dill::Catalog,
    sync_service: Arc<dyn SyncService>,
    principal: Arc<Principal>,
}

impl SyncToJobImpl {
    async fn resolve_and_transfer(
        &self,
        src: &odf::DatasetHandle,
        dst: odf::DatasetRefRemote,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        // 1: Authorize and resolve the source and the destination
        let request =
            transaction!(|dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                          sync_request_builder: Arc<SyncRequestBuilder>| {
                dataset_operation_guard
                    .check_operation(
                        &self.principal,
                        &DatasetOperationRequest::new(src.clone(), DatasetOperation::SyncTo),
                    )
                    .await?;

                let request = sync_request_builder
                    .build_sync_request(src.as_local_ref(), dst, options.create_if_not_exists)
                    .await?;
                request.src.detach_from_transaction();
                request
            });

        // 2: Transfer, with no transaction open; the destination commits
        self.sync_service
            .sync_to(request, options, maybe_listener)
            .await
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl SyncToJob for SyncToJobImpl {
    #[tracing::instrument(level = "info", name = SyncToJobImpl_run, skip_all, fields(%src, %dst, outcome))]
    async fn run(
        &self,
        src: &odf::DatasetHandle,
        dst: odf::DatasetRefRemote,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let result = self
            .resolve_and_transfer(src, dst, options, maybe_listener)
            .await;
        match &result {
            Ok(SyncResult::Updated { .. }) => SpanOutcome::Updated,
            Ok(SyncResult::UpToDate) => SpanOutcome::UpToDate,
            Err(e) => SpanOutcome::from(e),
        }
        .record();
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
