// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::{
    AppendError,
    AppendValidationError,
    BlockRef,
    CommitError,
    DataWriter,
    DataWriterMetadataState,
    GetAliasesError,
    GetSummaryOpts,
    GetWatermarkError,
    MetadataChainExt,
    RemoteAliasKind,
    RemoteAliasesRegistry,
    ResolvedDataset,
    SearchAddDataVisitor,
    SetWatermarkError,
    SetWatermarkResult,
    WatermarkService,
    WriteWatermarkError,
    WriteWatermarkOpts,
};
use kamu_ingest_datafusion::DataWriterDataFusion;
use opendatafabric::DatasetKind;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WatermarkServiceImpl {
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

#[component(pub)]
#[interface(dyn WatermarkService)]
impl WatermarkServiceImpl {
    pub fn new(
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        system_time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            remote_alias_reg,
            system_time_source,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WatermarkService for WatermarkServiceImpl {
    /// Attempt reading watermark that is currently associated with a dataset
    #[tracing::instrument(level = "info", skip_all)]
    async fn try_get_current_watermark(
        &self,
        resolved_dataset: ResolvedDataset,
    ) -> Result<Option<DateTime<Utc>>, GetWatermarkError> {
        let head = resolved_dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .int_err()?;

        let mut add_data_visitor = SearchAddDataVisitor::new();

        resolved_dataset
            .as_metadata_chain()
            .accept_by_hash(&mut [&mut add_data_visitor], &head)
            .await
            .int_err()?;

        let current_watermark = add_data_visitor.into_event().and_then(|e| e.new_watermark);

        Ok(current_watermark)
    }

    /// Manually advances the watermark of a root dataset
    #[tracing::instrument(level = "info", skip_all, fields(%new_watermark))]
    async fn set_watermark(
        &self,
        target: ResolvedDataset,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError> {
        let aliases = match self
            .remote_alias_reg
            .get_remote_aliases(target.get_handle())
            .await
        {
            Ok(v) => Ok(v),
            Err(GetAliasesError::Internal(e)) => Err(SetWatermarkError::Internal(e)),
        }?;

        if !aliases.is_empty(RemoteAliasKind::Pull) {
            return Err(SetWatermarkError::IsRemote);
        }

        let summary = target
            .get_summary(GetSummaryOpts::default())
            .await
            .int_err()?;

        if summary.kind != DatasetKind::Root {
            return Err(SetWatermarkError::IsDerivative);
        }

        let metadata_state = DataWriterMetadataState::build(target.clone(), &BlockRef::Head, None)
            .await
            .int_err()?;

        let mut writer = DataWriterDataFusion::builder(
            target.clone(),
            datafusion::prelude::SessionContext::new(),
        )
        .with_metadata_state(metadata_state)
        .build();

        match writer
            .write_watermark(
                new_watermark,
                WriteWatermarkOpts {
                    system_time: self.system_time_source.now(),
                    new_source_state: None,
                },
            )
            .await
        {
            Ok(res) => Ok(SetWatermarkResult::Updated {
                old_head: Some(res.old_head),
                new_head: res.new_head,
            }),
            Err(
                WriteWatermarkError::EmptyCommit(_)
                | WriteWatermarkError::CommitError(CommitError::MetadataAppendError(
                    AppendError::InvalidBlock(AppendValidationError::WatermarkIsNotMonotonic),
                )),
            ) => Ok(SetWatermarkResult::UpToDate),
            Err(e) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
