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
    CommitError,
    DataWriter,
    Dataset,
    GetAliasesError,
    GetSummaryOpts,
    RemoteAliasKind,
    RemoteAliasesRegistry,
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
    /// Manually advances the watermark of a root dataset
    async fn set_watermark(
        &self,
        dataset: Arc<dyn Dataset>,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError> {
        let aliases = match self
            .remote_alias_reg
            .get_remote_aliases(dataset.clone())
            .await
        {
            Ok(v) => Ok(v),
            Err(GetAliasesError::Internal(e)) => Err(SetWatermarkError::Internal(e)),
        }?;

        if !aliases.is_empty(RemoteAliasKind::Pull) {
            return Err(SetWatermarkError::IsRemote);
        }

        let summary = dataset
            .get_summary(GetSummaryOpts::default())
            .await
            .int_err()?;

        if summary.kind != DatasetKind::Root {
            return Err(SetWatermarkError::IsDerivative);
        }

        let mut writer =
            DataWriterDataFusion::builder(dataset, datafusion::prelude::SessionContext::new())
                .with_metadata_state_scanned(None)
                .await
                .int_err()?
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
