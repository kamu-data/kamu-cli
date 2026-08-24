// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestDocuments,
    ApplyManifestPlanningDecision,
    ApplyManifestRejection,
    ApplyManifestResult,
    ApplyResourceOutcome,
    Resource,
    ResourceWarning,
};
use kamu_resources_facade::{ApplyManifestError, ResourceFacade};

use crate::resources::DiscoveredResourceManifest;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait ResourceManifestExecutionService: Send + Sync {
    async fn execute(
        &self,
        resource_facade: &dyn ResourceFacade,
        manifest: &DiscoveredResourceManifest,
        dry_run: bool,
    ) -> Result<ExecuteResourceManifestOutcome, ExecuteResourceManifestError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Fallible because canonicalizing the apply documents can fail; a failure is an
// internal error rather than an apply outcome to display.
impl TryFrom<ApplyManifestPlanningDecision> for ExecuteResourceManifestOutcome {
    type Error = InternalError;

    fn try_from(decision: ApplyManifestPlanningDecision) -> Result<Self, Self::Error> {
        Ok(match decision {
            ApplyManifestPlanningDecision::Planned(plan) => {
                let documents = plan.documents()?;

                Self::Accepted(ExecutedResourceManifestResult {
                    outcome: plan.outcome,
                    resource: plan.resource,
                    warnings: plan.warnings,
                    documents,
                })
            }
            ApplyManifestPlanningDecision::Rejected(rejection) => Self::Rejected(rejection),
        })
    }
}

impl TryFrom<ApplyManifestApplicationDecision> for ExecuteResourceManifestOutcome {
    type Error = InternalError;

    fn try_from(decision: ApplyManifestApplicationDecision) -> Result<Self, Self::Error> {
        Ok(match decision {
            ApplyManifestApplicationDecision::Applied(result) => {
                // Unlike before, a live apply carries the same canonical
                // documents a dry run does, so both render the same diff.
                let documents = result.documents()?;
                let ApplyManifestResult {
                    resource,
                    outcome,
                    warnings,
                    ..
                } = result;

                Self::Accepted(ExecutedResourceManifestResult {
                    outcome,
                    resource,
                    warnings,
                    documents,
                })
            }
            ApplyManifestApplicationDecision::Rejected(rejection) => Self::Rejected(rejection),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum ExecuteResourceManifestOutcome {
    Accepted(ExecutedResourceManifestResult),
    Rejected(ApplyManifestRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ExecutedResourceManifestResult {
    pub outcome: ApplyResourceOutcome,
    pub resource: Resource,
    pub warnings: Vec<ResourceWarning>,
    pub documents: ApplyManifestDocuments,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ExecuteResourceManifestError {
    #[error(transparent)]
    Apply(#[from] ApplyManifestError),

    #[error("Failed to read manifest")]
    ReadManifest(#[source] std::io::Error),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
