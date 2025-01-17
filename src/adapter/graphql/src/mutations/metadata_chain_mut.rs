// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::CommitDatasetEventUseCase;

use crate::prelude::*;
use crate::utils::make_dataset_access_error;
use crate::LoggedInGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainMut {
    dataset_handle: odf::DatasetHandle,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[Object]
impl MetadataChainMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
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
        let event = match event_format {
            MetadataManifestFormat::Yaml => {
                let de = odf::metadata::serde::yaml::YamlMetadataEventDeserializer;
                match de.read_manifest(event.as_bytes()) {
                    Ok(event) => event,
                    Err(e @ odf::metadata::serde::Error::SerdeError { .. }) => {
                        return Ok(CommitResult::Malformed(MetadataManifestMalformed {
                            message: e.to_string(),
                        }))
                    }
                    Err(odf::metadata::serde::Error::UnsupportedVersion(e)) => {
                        return Ok(CommitResult::UnsupportedVersion(e.into()))
                    }
                    Err(e @ odf::metadata::serde::Error::IoError { .. }) => {
                        return Err(e.int_err().into())
                    }
                }
            }
        };

        let commit_dataset_event = from_catalog_n!(ctx, dyn CommitDatasetEventUseCase);

        let result = match commit_dataset_event
            .execute(
                &self.dataset_handle,
                event,
                odf::dataset::CommitOpts::default(),
            )
            .await
        {
            Ok(result) => CommitResult::Success(CommitResultSuccess {
                old_head: result.old_head.map(Into::into),
                new_head: result.new_head.into(),
            }),
            Err(odf::dataset::CommitError::ObjectNotFound(e)) => {
                CommitResult::AppendError(CommitResultAppendError {
                    message: format!("Event is referencing a non-existent object {}", e.hash),
                })
            }
            Err(odf::dataset::CommitError::MetadataAppendError(e)) => {
                CommitResult::AppendError(CommitResultAppendError {
                    message: e.to_string(),
                })
            }
            Err(odf::dataset::CommitError::Access(_)) => {
                return Err(make_dataset_access_error(&self.dataset_handle))
            }
            Err(e @ odf::dataset::CommitError::Internal(_)) => return Err(e.int_err().into()),
        };

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CommitResult {
    Success(CommitResultSuccess),
    NoChanges(NoChanges),
    Malformed(MetadataManifestMalformed),
    UnsupportedVersion(MetadataManifestUnsupportedVersion),
    AppendError(CommitResultAppendError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct CommitResultSuccess {
    pub old_head: Option<Multihash>,
    pub new_head: Multihash,
}

#[ComplexObject]
impl CommitResultSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct NoChanges;

#[Object]
impl NoChanges {
    pub async fn message(&self) -> String {
        "No changes".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct CommitResultAppendError {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
