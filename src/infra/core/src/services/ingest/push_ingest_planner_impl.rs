// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::*;
use random_strings::{get_random_string, AllowedSymbols};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PushIngestPlannerImpl {
    data_format_registry: Arc<dyn DataFormatRegistry>,
    time_source: Arc<dyn SystemTimeSource>,
    run_info_dir: Arc<RunInfoDir>,
}

#[dill::component(pub)]
#[dill::interface(dyn PushIngestPlanner)]
impl PushIngestPlannerImpl {
    pub fn new(
        data_format_registry: Arc<dyn DataFormatRegistry>,
        time_source: Arc<dyn SystemTimeSource>,
        run_info_dir: Arc<RunInfoDir>,
    ) -> Self {
        Self {
            data_format_registry,
            time_source,
            run_info_dir,
        }
    }

    async fn prepare_metadata_state(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
    ) -> Result<DataWriterMetadataState, PushIngestPlanningError> {
        let metadata_state =
            DataWriterMetadataState::build(target, &odf::BlockRef::Head, source_name)
                .await
                .map_err(|e| match e {
                    ScanMetadataError::SourceNotFound(err) => {
                        PushIngestPlanningError::SourceNotFound(err.into())
                    }
                    ScanMetadataError::Internal(err) => PushIngestPlanningError::Internal(err),
                })?;
        Ok(metadata_state)
    }

    async fn auto_create_push_source(
        &self,
        target: ResolvedDataset,
        source_name: &str,
        opts: &PushIngestOpts,
    ) -> Result<odf::metadata::AddPushSource, PushIngestPlanningError> {
        let read = match &opts.media_type {
            Some(media_type) => {
                match self
                    .data_format_registry
                    .get_best_effort_config(None, media_type)
                {
                    Ok(read_step) => Ok(read_step),
                    Err(e) => Err(PushIngestPlanningError::UnsupportedMediaType(e)),
                }
            }
            None => Err(PushIngestPlanningError::SourceNotFound(
                PushSourceNotFoundError::new(Some(source_name)),
            )),
        }?;

        let add_push_source_event = odf::metadata::AddPushSource {
            source_name: String::from("auto"),
            read,
            preprocess: None,
            merge: odf::metadata::MergeStrategy::Append(odf::metadata::MergeStrategyAppend {}),
        };

        let commit_result = target
            .commit_event(
                odf::MetadataEvent::AddPushSource(add_push_source_event.clone()),
                odf::dataset::CommitOpts {
                    system_time: opts.source_event_time,
                    ..Default::default()
                },
            )
            .await;
        match commit_result {
            Ok(_) => Ok(add_push_source_event),
            Err(e) => Err(PushIngestPlanningError::CommitError(e)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PushIngestPlanner for PushIngestPlannerImpl {
    /// Uses or auto-creates push source definition in metadata to plan
    /// ingestion
    #[tracing::instrument(level = "debug", skip_all, fields(target=%target.get_handle(), ?source_name, ?opts))]
    async fn plan_ingest(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
        opts: PushIngestOpts,
    ) -> Result<PushIngestPlan, PushIngestPlanningError> {
        let mut metadata_state = self
            .prepare_metadata_state(target.clone(), source_name)
            .await?;

        let push_source = match (&metadata_state.source_event, opts.auto_create_push_source) {
            // No push source, and it's allowed to create
            (None, true) => {
                tracing::debug!("Auto-creating new push source");
                let add_push_source_event = self
                    .auto_create_push_source(target.clone(), "auto", &opts)
                    .await?;

                // Update data writer, as we've modified the dataset
                metadata_state = self.prepare_metadata_state(target, source_name).await?;
                Ok(add_push_source_event)
            }

            // Got existing push source
            (Some(odf::MetadataEvent::AddPushSource(e)), _) => Ok(e.clone()),

            // No push source and not allowed to create
            _ => Err(PushIngestPlanningError::SourceNotFound(
                PushSourceNotFoundError::new(source_name),
            )),
        }?;

        let operation_id = get_random_string(None, 10, &AllowedSymbols::Alphanumeric);
        let operation_dir = self.run_info_dir.join(format!("ingest-{operation_id}"));

        Ok(PushIngestPlan {
            args: PushIngestArgs {
                operation_id,
                operation_dir,
                system_time: self.time_source.now(),
                opts,
                push_source,
            },
            metadata_state: Box::new(metadata_state),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
