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
use kamu_datasets::ResolvedDataset;
use random_strings::get_random_name;
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
        expected_head: Option<odf::Multihash>,
    ) -> Result<DataWriterMetadataState, PushIngestPlanningError> {
        let metadata_state = DataWriterMetadataState::build(
            target,
            &odf::BlockRef::Head,
            source_name,
            expected_head,
        )
        .await
        .map_err(|e| match e {
            ScanMetadataError::SourceNotFound(err) => {
                PushIngestPlanningError::SourceNotFound(err.into())
            }
            ScanMetadataError::HeadNotFound(err) => PushIngestPlanningError::HeadNotFound(err),
            ScanMetadataError::Internal(err) => PushIngestPlanningError::Internal(err),
        })?;
        Ok(metadata_state)
    }

    fn make_auto_push_source(
        &self,
        opts: &PushIngestOpts,
    ) -> Result<odf::metadata::AddPushSource, PushIngestPlanningError> {
        let read = match &opts.media_type {
            Some(media_type) => self
                .data_format_registry
                .get_best_effort_config(None, media_type)
                .map_err(PushIngestPlanningError::UnsupportedMediaType),
            None => Err(PushIngestPlanningError::SourceNotFound(
                PushSourceNotFoundError::new(Some("auto")),
            )),
        }?;

        Ok(odf::metadata::AddPushSource {
            source_name: String::from("auto"),
            read,
            preprocess: None,
            merge: odf::metadata::MergeStrategy::Append(odf::metadata::MergeStrategyAppend {}),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl PushIngestPlanner for PushIngestPlannerImpl {
    #[tracing::instrument(
        name = PushIngestPlannerImpl_plan_ingest
        level = "debug",
        skip_all,
        fields(target=%target.get_handle(), ?source_name, ?opts)
    )]
    async fn plan_ingest(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
        opts: PushIngestOpts,
    ) -> Result<PushIngestPlan, PushIngestPlanningError> {
        let metadata_state = self
            .prepare_metadata_state(target, source_name, opts.expected_head.clone())
            .await?;

        let Some(odf::MetadataEvent::AddPushSource(push_source)) = &metadata_state.source_event
        else {
            return Err(PushIngestPlanningError::SourceNotFound(
                PushSourceNotFoundError::new(source_name),
            ));
        };
        let push_source = push_source.clone();

        let operation_id = get_random_name(None, 10);
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

    #[tracing::instrument(
        name = PushIngestPlannerImpl_plan_push_source_creation
        level = "debug",
        skip_all,
        fields(target=%target.get_handle(), ?source_name, ?opts)
    )]
    async fn plan_push_source_creation(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
        opts: &PushIngestOpts,
    ) -> Result<Option<odf::metadata::AddPushSource>, PushIngestPlanningError> {
        let metadata_state = self
            .prepare_metadata_state(target, source_name, opts.expected_head.clone())
            .await?;

        if metadata_state.source_event.is_some() {
            return Ok(None);
        }

        tracing::debug!("Dataset has no source, planning to add a push source");
        self.make_auto_push_source(opts).map(Some)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
