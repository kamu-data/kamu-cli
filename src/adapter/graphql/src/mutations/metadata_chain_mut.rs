// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::CommitDatasetEventUseCase;

use crate::prelude::*;
use crate::queries::DatasetRequestState;
use crate::utils::make_dataset_access_error;
use crate::LoggedInGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> MetadataChainMut<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Commits new event to the metadata chain
    #[tracing::instrument(level = "info", name = MetadataChainMut_commit_event, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn commit_event(
        &self,
        ctx: &Context<'_>,
        event: String,
        event_format: MetadataManifestFormat,
    ) -> Result<CommitResult> {
        // NOTE: Access verification is handled by the use-case

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
        let dataset_handle = self.dataset_request_state.dataset_handle();

        let result = match commit_dataset_event.execute(dataset_handle, event).await {
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
                return Err(make_dataset_access_error(dataset_handle))
            }
            Err(e @ odf::dataset::CommitError::Internal(_)) => return Err(e.int_err().into()),
        };

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CommitResult<'a> {
    Success(CommitResultSuccess<'a>),
    NoChanges(NoChanges),
    Malformed(MetadataManifestMalformed),
    UnsupportedVersion(MetadataManifestUnsupportedVersion),
    AppendError(CommitResultAppendError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CommitResultSuccess<'a> {
    pub old_head: Option<Multihash<'a>>,
    pub new_head: Multihash<'a>,
}

#[ComplexObject]
impl CommitResultSuccess<'_> {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct NoChanges;

#[Object]
impl NoChanges {
    pub async fn message(&self) -> String {
        "No changes".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct CommitResultAppendError {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
