// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::DatabaseTransactionRunner;
use dill::Catalog;
use internal_error::ResultIntoInternal;
use kamu_datasets::DatasetRegistry;
use tracing::Instrument;
use url::Url;

use super::error_close::ErrorReplyClose;
use super::errors::*;
use super::messages::*;
use super::phases::*;
use super::protocol_dataset_helper::*;
use crate::BearerHeader;
use crate::smart_protocol::*;
use crate::ws_common::ReadMessageError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AxumServerPullProtocolInstance {
    socket: axum::extract::ws::WebSocket,
    catalog: Catalog,
    dataset_handle: odf::DatasetHandle,
    dataset_url: Url,
    maybe_bearer_header: Option<BearerHeader>,
}

impl AxumServerPullProtocolInstance {
    pub fn new(
        socket: axum::extract::ws::WebSocket,
        catalog: Catalog,
        dataset_handle: odf::DatasetHandle,
        dataset_url: Url,
        maybe_bearer_header: Option<BearerHeader>,
    ) -> Self {
        Self {
            socket,
            catalog,
            dataset_handle,
            dataset_url,
            maybe_bearer_header,
        }
    }

    pub async fn serve(mut self) {
        match self.pull_main_flow().await {
            Ok(_) => {
                tracing::debug!("Pull process success");
            }
            Err(ref e @ PullServerError::Internal(ref int_err)) => {
                axum_write_error_close(
                    &mut self.socket,
                    ErrorReplyClose::internal(int_err.phase.clone()),
                )
                .await;

                tracing::error!(
                  error = ?e,
                  error_msg = %e,
                  "Pull process aborted with internal error",
                );
            }
            Err(PullServerError::ReadFailed(err)) => tracing::warn!(
              error = ?err,
              error_msg = %err,
              "Pull process aborted: reading from the client failed",
            ),
            Err(PullServerError::WriteFailed(err)) => tracing::warn!(
              error = ?err,
              error_msg = %err,
              "Pull process aborted: writing to the client failed",
            ),
        }

        axum_await_client_close(&mut self.socket).await;
    }

    async fn pull_main_flow(&mut self) -> Result<(), PullServerError> {
        let planned_pull = self.handle_pull_request_initiation().await?;

        let received_pull_metadata_request =
            self.try_handle_pull_metadata_request(planned_pull).await?;

        if received_pull_metadata_request {
            loop {
                let should_continue = self.try_handle_pull_objects_request().await?;
                if !should_continue {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_pull_request_initiation(&mut self) -> Result<PlannedPull, PullServerError> {
        let pull_request = axum_read_payload::<DatasetPullRequest>(&mut self.socket)
            .await
            .map_err(|e| {
                PullServerError::ReadFailed(PullReadError::new(e, PullPhase::InitialRequest))
            })?;

        tracing::debug!(
            "Pull client sent a pull request: beginAfter={:?} stopAt={:?} \
             force_update_if_diverged={}",
            pull_request
                .begin_after
                .as_ref()
                .map(ToString::to_string)
                .ok_or("None"),
            pull_request
                .stop_at
                .as_ref()
                .map(ToString::to_string)
                .ok_or("None"),
            pull_request.force_update_if_diverged,
        );

        let dataset_handle = self.dataset_handle.clone();
        let stop_at = pull_request.stop_at.clone();
        let begin_faster = pull_request.begin_after.clone();

        let transfer_plan_result = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(|dataset_registry: Arc<dyn DatasetRegistry>| async move {
                let resolved_dataset = dataset_registry
                    .get_dataset_by_handle(&dataset_handle)
                    .await;

                let metadata_chain = resolved_dataset.as_metadata_chain();
                let stop_at = match stop_at {
                    Some(stop_at) => stop_at,
                    None => metadata_chain
                        .resolve_ref(&odf::BlockRef::Head)
                        .await
                        .int_err()?,
                };

                let transfer_plan = prepare_dataset_transfer_plan(
                    metadata_chain,
                    &stop_at,
                    begin_faster.as_ref(),
                    pull_request.force_update_if_diverged,
                )
                .await?;

                Ok::<_, PrepareDatasetTransferEstimateError>(PlannedTransfer {
                    stop_at,
                    transfer_plan,
                })
            })
            .instrument(tracing::debug_span!(
                "AxumServerPullProtocolInstance::handle_pull_request_initiation"
            ))
            .await;

        let planned_stop_at = transfer_plan_result
            .as_ref()
            .ok()
            .map(|planned| planned.stop_at.clone());

        axum_write_payload::<DatasetPullResponse>(
            &mut self.socket,
            match transfer_plan_result {
                Ok(PlannedTransfer { transfer_plan, .. }) => {
                    tracing::debug!("Sending size estimate: {:?}", transfer_plan);
                    Ok(DatasetPullSuccessResponse { transfer_plan })
                }
                Err(PrepareDatasetTransferEstimateError::InvalidInterval(e)) => {
                    tracing::debug!("Sending invalid interval error: {:?}", e);
                    Err(DatasetPullRequestError::InvalidInterval(
                        DatasetPullInvalidIntervalError {
                            head: e.head,
                            tail: e.tail,
                        },
                    ))
                }
                Err(PrepareDatasetTransferEstimateError::Internal(e)) => {
                    tracing::debug!("Sending internal error: {:?}", e);
                    Err(DatasetPullRequestError::Internal(TransferInternalError {
                        phase: TransferPhase::Pull(PullPhase::InitialRequest),
                        error_message: e.to_string(),
                    }))
                }
            },
        )
        .await
        .map_err(|e| {
            PullServerError::WriteFailed(PullWriteError::new(e, PullPhase::InitialRequest))
        })?;

        Ok(PlannedPull {
            request: pull_request,
            stop_at: planned_stop_at,
        })
    }

    async fn try_handle_pull_metadata_request(
        &mut self,
        planned_pull: PlannedPull,
    ) -> Result<bool, PullServerError> {
        let maybe_pull_metadata_request =
            axum_read_payload::<DatasetPullMetadataRequest>(&mut self.socket).await;

        match maybe_pull_metadata_request {
            Ok(_) => {
                tracing::debug!("Pull client sent a pull metadata request");

                let dataset_handle = self.dataset_handle.clone();
                let PlannedPull { request, stop_at } = planned_pull;
                // The batch ends at the head the plan was made from, so a head that
                // moved since sends the blocks the plan counted
                let stop_at = stop_at.ok_or_else(|| {
                    "Metadata was requested after a failed plan"
                        .protocol_int_err(PullPhase::MetadataRequest)
                })?;

                let metadata_batch = DatabaseTransactionRunner::new(self.catalog.clone())
                    .transactional_with(|dataset_registry: Arc<dyn DatasetRegistry>| async move {
                        let resolved_dataset = dataset_registry
                            .get_dataset_by_handle(&dataset_handle)
                            .await;

                        prepare_dataset_metadata_batch(
                            resolved_dataset.as_metadata_chain(),
                            &stop_at,
                            request.begin_after.as_ref(),
                            request.force_update_if_diverged,
                        )
                        .await
                    })
                    .instrument(tracing::debug_span!(
                        "AxumServerPullProtocolInstance::try_handle_pull_metadata_request",
                    ))
                    .await
                    .protocol_int_err(PullPhase::MetadataRequest)?;

                tracing::debug!(
                    num_blocks = % metadata_batch.num_blocks,
                    payload_size = % metadata_batch.payload.len(),
                    "Metadata batch of blocks formed",
                );

                axum_write_payload::<DatasetMetadataPullResponse>(
                    &mut self.socket,
                    DatasetMetadataPullResponse {
                        blocks: metadata_batch,
                    },
                )
                .await
                .map_err(|e| {
                    PullServerError::WriteFailed(PullWriteError::new(e, PullPhase::MetadataRequest))
                })?;

                Ok(true)
            }
            Err(ReadMessageError::Closed) => Ok(false),
            Err(e) => Err(PullServerError::ReadFailed(PullReadError::new(
                e,
                PullPhase::MetadataRequest,
            ))),
        }
    }

    async fn try_handle_pull_objects_request(&mut self) -> Result<bool, PullServerError> {
        let maybe_pull_objects_transfer_request =
            axum_read_payload::<DatasetPullObjectsTransferRequest>(&mut self.socket).await;

        match maybe_pull_objects_transfer_request {
            Ok(request) => {
                tracing::debug!(
                    objects_count = %request.object_files.len(),
                    "Pull client sent a pull objects request"
                );

                let object_transfer_strategies =
                    prepare_object_transfer_strategies::<PullObjectTransferStrategy>(
                        &self.catalog,
                        self.dataset_handle.clone(),
                        request.object_files,
                        self.dataset_url.clone(),
                        self.maybe_bearer_header.clone(),
                    )
                    .instrument(tracing::debug_span!(
                        "AxumServerPullProtocolInstance::try_handle_pull_objects_request",
                    ))
                    .await
                    .protocol_int_err(PullPhase::ObjectsRequest)?;

                tracing::debug!("Object transfer strategies defined");

                axum_write_payload::<DatasetPullObjectsTransferResponse>(
                    &mut self.socket,
                    DatasetPullObjectsTransferResponse {
                        object_transfer_strategies,
                    },
                )
                .await
                .map_err(|e| {
                    PullServerError::WriteFailed(PullWriteError::new(e, PullPhase::ObjectsRequest))
                })?;

                Ok(true)
            }
            Err(ReadMessageError::Closed) => Ok(false),
            Err(e) => Err(PullServerError::ReadFailed(PullReadError::new(
                e,
                PullPhase::ObjectsRequest,
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A pull request with the head its transfer plan was made from, or `None`
/// when planning failed
struct PlannedPull {
    request: DatasetPullRequest,
    stop_at: Option<odf::Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PlannedTransfer {
    stop_at: odf::Multihash,
    transfer_plan: TransferPlan,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
