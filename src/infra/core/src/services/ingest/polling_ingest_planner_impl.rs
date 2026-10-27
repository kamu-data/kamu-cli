// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{DatasetEnvVarResolver, ResolvedDataset, SecretsEncryptionConfig};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn PollingIngestPlanner)]
pub struct PollingIngestPlannerImpl {
    catalog: dill::Catalog,
}

impl PollingIngestPlannerImpl {
    async fn plan_at(
        target: ResolvedDataset,
        head: Option<odf::Multihash>,
    ) -> Result<PollingIngestPlan, PollingIngestPlanError> {
        let metadata_state =
            DataWriterMetadataState::build(target.clone(), &odf::BlockRef::Head, None, head)
                .await?;

        Ok(PollingIngestPlan::new(
            target.get_handle(),
            Box::new(metadata_state),
        )?)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PollingIngestPlanner for PollingIngestPlannerImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(target=%target.get_handle()))]
    async fn plan_ingest(
        &self,
        target: ResolvedDataset,
    ) -> Result<PollingIngestPlan, PollingIngestPlanError> {
        Self::plan_at(target, None).await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(target=%target.get_handle()))]
    async fn rebuild_ingest(
        &self,
        target: ResolvedDataset,
        stored: &PollingIngestPlanPayload,
    ) -> Result<PollingIngestPlan, PollingIngestRebuildError> {
        let dataset_id = target.get_id().clone();
        let mismatch = |reason: String| PollingIngestPlanMismatchError {
            dataset_id: dataset_id.clone(),
            reason,
        };

        if stored.dataset_id != dataset_id {
            return Err(mismatch(format!("the plan is for dataset {}", stored.dataset_id)).into());
        }

        let actual_head = target
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?;
        if actual_head != stored.expected_head {
            return Err(PollingIngestHeadMovedError {
                dataset_id,
                expected_head: stored.expected_head.clone(),
                actual_head,
            }
            .into());
        }

        let plan = match Self::plan_at(target, Some(actual_head)).await {
            Ok(plan) => plan,
            Err(e @ PollingIngestPlanError::SourceNotDefined(_)) => {
                return Err(mismatch(e.to_string()).into());
            }
            Err(PollingIngestPlanError::ScanMetadata(e)) => return Err(e.int_err().into()),
            Err(PollingIngestPlanError::Internal(e)) => return Err(e.into()),
        };

        let rebuilt = PollingIngestPlanPayload::from_plan(
            dataset_id.clone(),
            &plan,
            &PollingIngestOptions {
                fetch_uncacheable: stored.fetch_uncacheable,
                ..Default::default()
            },
        )?;
        if rebuilt != *stored {
            return Err(mismatch(format!("rebuilt as {rebuilt:?}")).into());
        }

        Ok(plan)
    }

    /// Dataset environment variables are enabled only when secrets encryption
    /// is configured and enabled. Both dependencies are looked up leniently:
    /// a catalog that does not wire them has the feature disabled.
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn resolve_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<IngestEnvVars, InternalError> {
        let enabled = self
            .catalog
            .get_one::<SecretsEncryptionConfig>()
            .is_ok_and(|config| config.is_enabled());
        if !enabled {
            return Ok(IngestEnvVars::default());
        }

        let Ok(resolver) = self.catalog.get_one::<dyn DatasetEnvVarResolver>() else {
            return Ok(IngestEnvVars::default());
        };

        let env_vars = resolver.resolve_effective_env_vars(dataset_id).await?;
        Ok(IngestEnvVars::new(env_vars))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
