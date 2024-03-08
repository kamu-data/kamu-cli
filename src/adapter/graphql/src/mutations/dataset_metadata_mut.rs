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
    MetadataChainExt,
    SearchSetAttachmentsVisitor,
    SearchSingleTypedBlockVisitor,
};
use odf::VariantOf;
use opendatafabric as odf;

use super::{CommitResultAppendError, CommitResultSuccess, NoChanges};
use crate::mutations::MetadataChainMut;
use crate::prelude::*;
use crate::utils::check_dataset_write_access;
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
    async fn get_dataset(&self, ctx: &Context<'_>) -> Result<std::sync::Arc<dyn domain::Dataset>> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = dataset_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;
        Ok(dataset)
    }

    #[graphql(skip)]
    async fn get_last_block_of_type<T: VariantOf<odf::MetadataEvent>, const F: u32>(
        &self,
        ctx: &Context<'_>,
        mut visitor: SearchSingleTypedBlockVisitor<T, InternalError, F>,
    ) -> Result<Option<odf::MetadataBlockTyped<T>>>
    where
        T: VariantOf<odf::MetadataEvent> + Send + Sync,
    {
        let dataset = self.get_dataset(ctx).await?;

        dataset
            .as_metadata_chain()
            .accept(&mut [&mut visitor])
            .await
            .int_err()?;

        Ok(visitor.into_block())
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
        check_dataset_write_access(ctx, &self.dataset_handle).await?;

        let dataset = self.get_dataset(ctx).await?;

        let old_attachments = self
            .get_last_block_of_type(ctx, <SearchSetAttachmentsVisitor>::default())
            .await?
            .map(|b| {
                let odf::Attachments::Embedded(at) = b.event.attachments;
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

        let result = match dataset
            .commit_event(event.into(), domain::CommitOpts::default())
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
            Err(e @ domain::CommitError::Internal(_)) => return Err(e.int_err().into()),
        };

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum UpdateReadmeResult {
    Success(CommitResultSuccess),
    NoChanges(NoChanges),
    AppendError(CommitResultAppendError),
}

////////////////////////////////////////////////////////////////////////////////////////
