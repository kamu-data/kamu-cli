// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use domain::events::DatasetEventDependenciesUpdated;
use domain::DatasetRepository;
use event_bus::EventBus;
use kamu_core::{self as domain};
use opendatafabric as odf;

use crate::prelude::*;
use crate::utils::{check_dataset_write_access, CheckDatasetAccessError};
use crate::LoggedInGuard;

////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainMut {
    dataset_handle: odf::DatasetHandle,
}

////////////////////////////////////////////////////////////////////////////////////////

#[Object]
impl MetadataChainMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(skip)]
    async fn get_dataset(&self, ctx: &Context<'_>) -> Result<std::sync::Arc<dyn domain::Dataset>> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = dataset_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;
        Ok(dataset)
    }

    /// Commits new event to the metadata chain
    #[tracing::instrument(level = "info", skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn commit_event(
        &self,
        ctx: &Context<'_>,
        event: String,
        event_format: MetadataManifestFormat,
    ) -> Result<CommitResult> {
        match check_dataset_write_access(ctx, &self.dataset_handle).await {
            Ok(_) => {}
            Err(e) => match e {
                CheckDatasetAccessError::Access(_) => {
                    return Err(GqlError::Gql(
                        Error::new("Dataset access error").extend_with(|_, eev| {
                            eev.set("alias", self.dataset_handle.alias.to_string())
                        }),
                    ))
                }
                CheckDatasetAccessError::Internal(e) => return Err(e.into()),
            },
        };

        let event = match event_format {
            MetadataManifestFormat::Yaml => {
                let de = odf::serde::yaml::YamlMetadataEventDeserializer;
                match de.read_manifest(event.as_bytes()) {
                    Ok(event) => event,
                    Err(e @ odf::serde::Error::SerdeError { .. }) => {
                        return Ok(CommitResult::Malformed(MetadataManifestMalformed {
                            message: e.to_string(),
                        }))
                    }
                    Err(odf::serde::Error::UnsupportedVersion(e)) => {
                        return Ok(CommitResult::UnsupportedVersion(e.into()))
                    }
                    Err(e @ odf::serde::Error::IoError { .. }) => return Err(e.int_err().into()),
                }
            }
        };

        use domain::DatasetRepositoryExt;
        let dataset_repo = from_catalog::<dyn DatasetRepository>(ctx).unwrap();

        let mut new_upstream_ids: Vec<opendatafabric::DatasetID> = vec![];

        if let opendatafabric::MetadataEvent::SetTransform(transform) = &event {
            for new_input in transform.inputs.iter() {
                if let Some(id) = &new_input.id {
                    new_upstream_ids.push(id.clone());
                } else if let Some(dataset_ref_any) = &new_input.dataset_ref {
                    let maybe_hdl = dataset_repo
                        .try_resolve_dataset_ref_any(dataset_ref_any)
                        .await
                        .int_err()?;
                    if let Some(hdl) = maybe_hdl {
                        new_upstream_ids.push(hdl.id);
                    }
                } else {
                    let local_ref = opendatafabric::DatasetAlias::new(None, new_input.name.clone())
                        .as_local_ref();
                    let hdl = dataset_repo
                        .resolve_dataset_ref(&local_ref)
                        .await
                        .int_err()?;
                    new_upstream_ids.push(hdl.id);
                }
            }
        }

        let dataset = self.get_dataset(ctx).await?;

        let result = match dataset
            .commit_event(event, domain::CommitOpts::default())
            .await
        {
            Ok(result) => CommitResult::Success(CommitResultSuccess {
                old_head: result.old_head.map(Into::into),
                new_head: result.new_head.into(),
            }),
            Err(domain::CommitError::ObjectNotFound(e)) => {
                CommitResult::AppendError(CommitResultAppendError {
                    message: format!("Event is referencing a non-existent object {}", e.hash),
                })
            }
            Err(domain::CommitError::MetadataAppendError(e)) => {
                CommitResult::AppendError(CommitResultAppendError {
                    message: e.to_string(),
                })
            }
            Err(e @ domain::CommitError::Internal(_)) => return Err(e.int_err().into()),
        };

        // TODO: encapsulate this inside dataset/chain
        if !new_upstream_ids.is_empty() {
            let event_bus = from_catalog::<EventBus>(ctx).unwrap();
            event_bus
                .dispatch_event(DatasetEventDependenciesUpdated {
                    dataset_id: self.dataset_handle.id.clone(),
                    new_upstream_ids,
                })
                .await
                .int_err()?;
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CommitResult {
    Success(CommitResultSuccess),
    NoChanges(NoChanges),
    Malformed(MetadataManifestMalformed),
    UnsupportedVersion(MetadataManifestUnsupportedVersion),
    AppendError(CommitResultAppendError),
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct CommitResultSuccess {
    pub old_head: Option<Multihash>,
    pub new_head: Multihash,
}

#[ComplexObject]
impl CommitResultSuccess {
    pub async fn message(&self) -> String {
        format!("Success")
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct NoChanges;

#[Object]
impl NoChanges {
    pub async fn message(&self) -> String {
        format!("No changes")
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct CommitResultAppendError {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////
