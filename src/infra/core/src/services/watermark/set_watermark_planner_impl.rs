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
use internal_error::ResultIntoInternal;
use kamu_core::*;
use kamu_datasets::ResolvedDataset;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SetWatermarkPlannerImpl {
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

#[component(pub)]
#[interface(dyn SetWatermarkPlanner)]
impl SetWatermarkPlannerImpl {
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
impl SetWatermarkPlanner for SetWatermarkPlannerImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(target=%target.get_handle(), new_watermark))]
    async fn plan_set_watermark(
        &self,
        target: ResolvedDataset,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkPlan, SetWatermarkPlanningError> {
        let aliases = match self
            .remote_alias_reg
            .get_remote_aliases(target.get_handle())
            .await
        {
            Ok(v) => Ok(v),
            Err(GetAliasesError::Internal(e)) => Err(SetWatermarkPlanningError::Internal(e)),
        }?;

        if !aliases.is_empty(RemoteAliasKind::Pull) {
            return Err(SetWatermarkPlanningError::IsRemote);
        }

        if target.get_kind() != odf::DatasetKind::Root {
            return Err(SetWatermarkPlanningError::IsDerivative);
        }

        let metadata_state =
            DataWriterMetadataState::build(target.clone(), &odf::BlockRef::Head, None, None)
                .await
                .int_err()?;

        Ok(SetWatermarkPlan {
            system_time: self.system_time_source.now(),
            new_watermark,
            metadata_state: Box::new(metadata_state),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
