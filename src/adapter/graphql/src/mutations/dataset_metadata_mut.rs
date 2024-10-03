// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{
    self as domain,
    CommitDatasetEventUseCase,
    MetadataChainExt,
    SearchSetAttachmentsVisitor,
};
use opendatafabric as odf;

use super::{CommitResultAppendError, CommitResultSuccess, NoChanges};
use crate::mutations::MetadataChainMut;
use crate::prelude::*;
use crate::utils::make_dataset_access_error;
use crate::LoggedInGuard;

pub struct DatasetMetadataMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetMetadataMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(skip)]
    fn get_dataset(&self, ctx: &Context<'_>) -> domain::ResolvedDataset {
        // TODO: cut off this dependency - extract a higher level use case
        let dataset_registry = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        dataset_registry.get_dataset_by_handle(&self.dataset_handle)
    }

    /// Access to the mutable metadata chain of the dataset
    async fn chain(&self) -> MetadataChainMut {
        MetadataChainMut::new(self.dataset_handle.clone())
    }

    /// Updates or clears the dataset readme
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn update_readme(
        &self,
        ctx: &Context<'_>,
        content: Option<String>,
    ) -> Result<UpdateReadmeResult> {
        let resolved_dataset = self.get_dataset(ctx);

        let old_attachments = resolved_dataset
            .as_metadata_chain()
            .accept_one(SearchSetAttachmentsVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(|e| {
                let odf::Attachments::Embedded(at) = e.attachments;

                at
            });

        // TODO: Move this logic into a service once we have a better idea how we will
        // manage readmes and other attachments
        let new_attachments = match (content, old_attachments) {
            (None, None) => None,
            (None, Some(old)) if old.items.is_empty() => None,
            (None, Some(old)) => Some(odf::AttachmentsEmbedded {
                items: old
                    .items
                    .into_iter()
                    .filter(|a| !a.path.to_lowercase().starts_with("readme."))
                    .collect(),
            }),
            (Some(content), None) => Some(odf::AttachmentsEmbedded {
                items: vec![odf::AttachmentEmbedded {
                    path: "README.md".to_string(),
                    content,
                }],
            }),
            (Some(content), Some(old)) => {
                let mut new = odf::AttachmentsEmbedded {
                    items: old
                        .items
                        .iter()
                        .filter(|a| !a.path.to_lowercase().starts_with("readme."))
                        .cloned()
                        .collect(),
                };
                new.items.push(odf::AttachmentEmbedded {
                    path: "README.md".to_string(),
                    content,
                });
                if new != old {
                    Some(new)
                } else {
                    None
                }
            }
        };

        let Some(new_attachments) = new_attachments else {
            return Ok(UpdateReadmeResult::NoChanges(NoChanges));
        };

        let event = odf::SetAttachments {
            attachments: new_attachments.into(),
        };

        let commit_event = from_catalog::<dyn CommitDatasetEventUseCase>(ctx).unwrap();

        let result = match commit_event
            .execute(
                &self.dataset_handle,
                event.into(),
                domain::CommitOpts::default(),
            )
            .await
        {
            Ok(result) => UpdateReadmeResult::Success(CommitResultSuccess {
                old_head: result.old_head.map(Into::into),
                new_head: result.new_head.into(),
            }),
            Err(domain::CommitError::ObjectNotFound(e)) => {
                UpdateReadmeResult::AppendError(CommitResultAppendError {
                    message: format!("Event is referencing a non-existent object {}", e.hash),
                })
            }
            Err(domain::CommitError::MetadataAppendError(e)) => {
                UpdateReadmeResult::AppendError(CommitResultAppendError {
                    message: e.to_string(),
                })
            }
            Err(domain::CommitError::Access(_)) => {
                return Err(make_dataset_access_error(&self.dataset_handle))
            }
            Err(e @ domain::CommitError::Internal(_)) => return Err(e.int_err().into()),
        };

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum UpdateReadmeResult {
    Success(CommitResultSuccess),
    NoChanges(NoChanges),
    AppendError(CommitResultAppendError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
