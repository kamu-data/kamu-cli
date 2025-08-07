// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use kamu_datasets::{
    DatasetDependency,
    GetDatasetDownstreamDependenciesUseCase,
    GetDatasetUpstreamDependenciesUseCase,
};
use odf::dataset::MetadataChainExt as _;

use crate::prelude::*;
use crate::queries::*;
use crate::scalars::DatasetPushStatuses;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetMetadata<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

impl<'a> DatasetMetadata<'a> {
    #[tracing::instrument(level = "info", skip_all)]
    async fn get_current_polling_source(
        &'a self,
        ctx: &Context<'_>,
    ) -> Result<
        Option<(
            odf::Multihash,
            odf::metadata::MetadataBlockTyped<odf::metadata::SetPollingSource>,
        )>,
    > {
        let metadata_query_service = from_catalog_n!(ctx, dyn kamu_core::MetadataQueryService);

        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        let result = metadata_query_service
            .get_active_polling_source(resolved_dataset)
            .await
            .int_err()?;

        Ok(result)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_current_push_sources(
        &'a self,
        ctx: &Context<'_>,
    ) -> Result<
        Vec<(
            odf::Multihash,
            odf::metadata::MetadataBlockTyped<odf::metadata::AddPushSource>,
        )>,
    > {
        let metadata_query_service = from_catalog_n!(ctx, dyn kamu_core::MetadataQueryService);

        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        let result = metadata_query_service
            .get_active_push_sources(resolved_dataset)
            .await
            .int_err()?;

        Ok(result)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_current_transform(
        &'a self,
        ctx: &Context<'_>,
    ) -> Result<
        Option<(
            odf::Multihash,
            odf::metadata::MetadataBlockTyped<odf::metadata::SetTransform>,
        )>,
    > {
        let metadata_query_service = from_catalog_n!(ctx, dyn kamu_core::MetadataQueryService);

        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        let result = metadata_query_service
            .get_active_transform(resolved_dataset)
            .await?;

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetMetadata<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Access to the temporal metadata chain of the dataset
    async fn chain(&self) -> MetadataChain {
        MetadataChain::new(self.dataset_request_state)
    }

    /// Last recorded watermark
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_watermark, skip_all)]
    async fn current_watermark(&self, ctx: &Context<'_>) -> Result<Option<DateTime<Utc>>> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .last_data_block()
            .await
            .int_err()?
            .into_block()
            .and_then(|b| b.event.new_watermark))
    }

    /// Latest data schema
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_schema, skip_all)]
    async fn current_schema(
        &self,
        ctx: &Context<'_>,
        format: Option<DataSchemaFormat>,
    ) -> Result<Option<DataSchema>> {
        let query_svc = from_catalog_n!(ctx, dyn kamu_core::QueryService);

        // TODO: Default to Arrow eventually
        let format = format.unwrap_or(DataSchemaFormat::Parquet);

        if let Some(schema) = query_svc
            .get_schema(&self.dataset_request_state.dataset_handle().as_local_ref())
            .await
            .int_err()?
        {
            Ok(Option::from(DataSchema::from_arrow_schema(
                schema.as_ref(),
                format,
            )))
        } else {
            Ok(None)
        }
    }

    /// Current upstream dependencies of a dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_upstream_dependencies, skip_all)]
    async fn current_upstream_dependencies(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<DependencyDatasetResult>> {
        let get_dataset_upstream_dependencies_use_case =
            from_catalog_n!(ctx, dyn GetDatasetUpstreamDependenciesUseCase);

        let upstream_dependencies = get_dataset_upstream_dependencies_use_case
            .execute(&self.dataset_request_state.dataset_handle().id)
            .await
            .int_err()?
            .into_iter()
            .map(|dependency| match dependency {
                DatasetDependency::Resolved(r) => {
                    let account = Account::new(r.owner_id.into(), r.owner_name.into());
                    let dataset = Dataset::new_access_checked(account, r.dataset_handle);

                    DependencyDatasetResult::accessible(dataset)
                }
                DatasetDependency::Unresolved(id) => DependencyDatasetResult::not_accessible(id),
            })
            .collect::<Vec<_>>();

        Ok(upstream_dependencies)
    }

    // TODO: Convert to connection (page_based_connection!)
    /// Current downstream dependencies of a dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_downstream_dependencies, skip_all)]
    async fn current_downstream_dependencies(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<DependencyDatasetResult>> {
        let get_dataset_downstream_dependencies_use_case =
            from_catalog_n!(ctx, dyn GetDatasetDownstreamDependenciesUseCase);

        let downstream_dependencies = get_dataset_downstream_dependencies_use_case
            .execute(&self.dataset_request_state.dataset_handle().id)
            .await
            .int_err()?
            .into_iter()
            .map(|dependency| match dependency {
                DatasetDependency::Resolved(r) => {
                    let account = Account::new(r.owner_id.into(), r.owner_name.into());
                    let dataset = Dataset::new_access_checked(account, r.dataset_handle);

                    DependencyDatasetResult::accessible(dataset)
                }
                DatasetDependency::Unresolved(id) => DependencyDatasetResult::not_accessible(id),
            })
            .collect::<Vec<_>>();

        Ok(downstream_dependencies)
    }

    /// Current polling source used by the root dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_polling_source, skip_all)]
    async fn current_polling_source(&self, ctx: &Context<'_>) -> Result<Option<SetPollingSource>> {
        let source_maybe = self.get_current_polling_source(ctx).await?;

        Ok(source_maybe.map(|(_hash, block)| block.event.into()))
    }

    /// Current push sources used by the root dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_push_sources, skip_all)]
    async fn current_push_sources(&self, ctx: &Context<'_>) -> Result<Vec<AddPushSource>> {
        let mut push_sources: Vec<AddPushSource> = self
            .get_current_push_sources(ctx)
            .await?
            .into_iter()
            .map(|(_hash, block)| block.event.into())
            .collect();

        push_sources.sort_by(|a, b| a.source_name.cmp(&b.source_name));

        Ok(push_sources)
    }

    /// Sync statuses of push remotes
    #[tracing::instrument(level = "info", name = DatasetMetadata_push_sync_statuses, skip_all)]
    async fn push_sync_statuses(&self, ctx: &Context<'_>) -> Result<DatasetPushStatuses> {
        let service = from_catalog_n!(ctx, dyn kamu_core::RemoteStatusService);

        let statuses = service
            .check_remotes_status(self.dataset_request_state.dataset_handle())
            .await?;

        Ok(statuses.into())
    }

    /// Current transformation used by the derivative dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_transform, skip_all)]
    async fn current_transform(&self, ctx: &Context<'_>) -> Result<Option<SetTransform>> {
        let source_maybe = self.get_current_transform(ctx).await?;

        if let Some((_hash, block)) = source_maybe {
            Ok(Some(
                SetTransform::with_extended_aliases(ctx, block.event)
                    .await?
                    .into(),
            ))
        } else {
            Ok(None)
        }
    }

    /// Current descriptive information about the dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_info, skip_all)]
    async fn current_info(&self, ctx: &Context<'_>) -> Result<SetInfo> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetInfoVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map_or(
                SetInfo {
                    description: None,
                    keywords: None,
                },
                Into::into,
            ))
    }

    /// Current readme file as discovered from attachments associated with the
    /// dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_readme, skip_all)]
    async fn current_readme(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetAttachmentsVisitor::new())
            .await
            .int_err()?
            .into_event()
            .and_then(|e| {
                let odf::metadata::Attachments::Embedded(at) = e.attachments;

                at.items
                    .into_iter()
                    .filter(|i| i.path == "README.md")
                    .map(|i| i.content)
                    .next()
            }))
    }

    /// Current license associated with the dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_license, skip_all)]
    async fn current_license(&self, ctx: &Context<'_>) -> Result<Option<SetLicense>> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetLicenseVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(Into::into))
    }

    /// Current vocabulary associated with the dataset
    #[tracing::instrument(level = "info", name = DatasetMetadata_current_vocab, skip_all)]
    async fn current_vocab(&self, ctx: &Context<'_>) -> Result<Option<SetVocab>> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetVocabVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(Into::into))
    }

    #[tracing::instrument(level = "info", name = DatasetMetadata_metadata_projection, skip_all)]
    async fn metadata_projection(
        &self,
        ctx: &Context<'_>,
        event_types: Vec<MetadataEventType>,
        encoding: Option<MetadataManifestFormat>,
        head: Option<Multihash<'_>>,
    ) -> Result<Vec<MetadataBlockExtended>> {
        let account =
            Account::from_dataset_alias(ctx, &self.dataset_request_state.dataset_handle().alias)
                .await?
                .expect("Account must exist");

        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        let mut visitors_list: Vec<MetadataVisitor> =
            event_types.iter().map(|event| (*event).into()).collect();

        let mut visitors: Vec<
            &mut dyn odf::dataset::MetadataChainVisitor<Error = odf::dataset::Infallible>,
        > = visitors_list
            .iter_mut()
            .map(MetadataVisitor::as_visitor)
            .collect();

        let head_hash = match head {
            Some(h) => h.into(),
            None => resolved_dataset
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
                .int_err()?,
        };

        resolved_dataset
            .as_metadata_chain()
            .accept_by_interval(visitors.as_mut_slice(), Some(&head_hash), None)
            .await
            .int_err()?;

        let mut result = vec![];

        for visitor in visitors_list {
            let blocks = visitor.into_boxed_extractor().extract_blocks();
            for (hash, block) in blocks {
                let extended_block =
                    MetadataBlockExtended::new(ctx, hash, block, account.clone(), encoding).await?;
                result.push(extended_block);
            }
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
enum DependencyDatasetResult<'a> {
    Accessible(DependencyDatasetResultAccessible),
    NotAccessible(DependencyDatasetResultNotAccessible<'a>),
}

impl DependencyDatasetResult<'_> {
    pub fn accessible(dataset: Dataset) -> Self {
        Self::Accessible(DependencyDatasetResultAccessible { dataset })
    }

    pub fn not_accessible(dataset_id: odf::DatasetID) -> Self {
        Self::NotAccessible(DependencyDatasetResultNotAccessible {
            id: dataset_id.into(),
        })
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DependencyDatasetResultAccessible {
    pub dataset: Dataset,
}

#[ComplexObject]
impl DependencyDatasetResultAccessible {
    async fn message(&self) -> String {
        "Found".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DependencyDatasetResultNotAccessible<'a> {
    pub id: DatasetID<'a>,
}

#[ComplexObject]
impl DependencyDatasetResultNotAccessible<'_> {
    async fn message(&self) -> String {
        "Not Accessible".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
