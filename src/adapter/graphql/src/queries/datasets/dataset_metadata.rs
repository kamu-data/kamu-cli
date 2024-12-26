// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::prelude::*;
use kamu_accounts::AccountService;
use kamu_core::auth::{ClassifyByAllowanceIdsResponse, DatasetAction};
use kamu_core::{
    self as domain,
    MetadataChainExt,
    SearchSetAttachmentsVisitor,
    SearchSetInfoVisitor,
    SearchSetLicenseVisitor,
    SearchSetVocabVisitor,
};
use kamu_datasets::DatasetEntriesResolution;
use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::*;
use crate::scalars::DatasetPushStatuses;
use crate::utils::get_dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetMetadata {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetMetadata {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    /// Access to the temporal metadata chain of the dataset
    async fn chain(&self) -> MetadataChain {
        MetadataChain::new(self.dataset_handle.clone())
    }

    /// Last recorded watermark
    async fn current_watermark(&self, ctx: &Context<'_>) -> Result<Option<DateTime<Utc>>> {
        let resolved_dataset = get_dataset(ctx, &self.dataset_handle)?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .last_data_block()
            .await
            .int_err()?
            .into_block()
            .and_then(|b| b.event.new_watermark))
    }

    /// Latest data schema
    async fn current_schema(
        &self,
        ctx: &Context<'_>,
        format: Option<DataSchemaFormat>,
    ) -> Result<Option<DataSchema>> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // TODO: Default to Arrow eventually
        let format = format.unwrap_or(DataSchemaFormat::Parquet);

        if let Some(schema) = query_svc
            .get_schema(&self.dataset_handle.as_local_ref())
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

    // TODO: Private Datasets: tests
    /// Current upstream dependencies of a dataset
    async fn current_upstream_dependencies(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<DependencyDatasetResult>> {
        let (
            dependency_graph_service,
            dataset_action_authorizer,
            dataset_entry_repository,
            account_service,
        ) = from_catalog_n!(
            ctx,
            dyn domain::DependencyGraphService,
            dyn kamu_core::auth::DatasetActionAuthorizer,
            dyn kamu_datasets::DatasetEntryRepository,
            dyn AccountService
        );

        use tokio_stream::StreamExt;

        // TODO: PERF: chunk the stream
        let upstream_dependency_ids = dependency_graph_service
            .get_upstream_dependencies(&self.dataset_handle.id)
            .await
            .int_err()?
            .collect::<Vec<_>>()
            .await;

        let mut upstream_dependencies = Vec::with_capacity(upstream_dependency_ids.len());

        let ClassifyByAllowanceIdsResponse {
            authorized_ids,
            unauthorized_ids_with_errors,
        } = dataset_action_authorizer
            .classify_dataset_ids_by_allowance(upstream_dependency_ids, DatasetAction::Read)
            .await?;

        upstream_dependencies.extend(unauthorized_ids_with_errors.into_iter().map(
            |(unauthorized_dataset_id, _)| {
                DependencyDatasetResult::not_accessible(unauthorized_dataset_id)
            },
        ));

        let DatasetEntriesResolution {
            resolved_entries,
            unresolved_entries,
        } = dataset_entry_repository
            .get_multiple_dataset_entries(&authorized_ids)
            .await
            .int_err()?;

        upstream_dependencies.extend(
            unresolved_entries
                .into_iter()
                .map(DependencyDatasetResult::not_accessible),
        );

        let owner_ids = resolved_entries
            .iter()
            .fold(HashSet::new(), |mut acc, entry| {
                acc.insert(entry.owner_id.clone());
                acc
            });
        let account_map = account_service
            .get_account_map(owner_ids.into_iter().collect())
            .await
            .int_err()?;

        for dataset_entry in resolved_entries {
            let maybe_account = account_map.get(&dataset_entry.owner_id);
            if let Some(account) = maybe_account {
                let dataset_handle = odf::DatasetHandle {
                    id: dataset_entry.id,
                    alias: odf::DatasetAlias::new(
                        Some(account.account_name.clone()),
                        dataset_entry.name,
                    ),
                };
                let dataset = Dataset::new(Account::from_account(account.clone()), dataset_handle);

                upstream_dependencies.push(DependencyDatasetResult::accessible(dataset));
            } else {
                tracing::warn!(
                    "Upstream owner's account not found for dataset: {:?}",
                    &dataset_entry
                );
                upstream_dependencies
                    .push(DependencyDatasetResult::not_accessible(dataset_entry.id));
            }
        }

        Ok(upstream_dependencies)
    }

    // TODO: Convert to collection
    // TODO: Private Datasets: tests
    /// Current downstream dependencies of a dataset
    async fn current_downstream_dependencies(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<DependencyDatasetResult>> {
        let (
            dependency_graph_service,
            dataset_action_authorizer,
            dataset_entry_repository,
            account_service,
        ) = from_catalog_n!(
            ctx,
            dyn domain::DependencyGraphService,
            dyn kamu_core::auth::DatasetActionAuthorizer,
            dyn kamu_datasets::DatasetEntryRepository,
            dyn AccountService
        );

        use tokio_stream::StreamExt;

        // TODO: PERF: chunk the stream
        let downstream_dependency_ids = dependency_graph_service
            .get_downstream_dependencies(&self.dataset_handle.id)
            .await
            .int_err()?
            .collect::<Vec<_>>()
            .await;

        let mut downstream_dependencies = Vec::with_capacity(downstream_dependency_ids.len());

        // Cut off datasets that we don't have access to
        let authorized_ids = dataset_action_authorizer
            .classify_dataset_ids_by_allowance(downstream_dependency_ids, DatasetAction::Read)
            .await?
            .authorized_ids;

        let DatasetEntriesResolution {
            resolved_entries,
            unresolved_entries,
        } = dataset_entry_repository
            .get_multiple_dataset_entries(&authorized_ids)
            .await
            .int_err()?;

        downstream_dependencies.extend(
            unresolved_entries
                .into_iter()
                .map(DependencyDatasetResult::not_accessible),
        );

        let owner_ids = resolved_entries
            .iter()
            .fold(HashSet::new(), |mut acc, entry| {
                acc.insert(entry.owner_id.clone());
                acc
            });
        let account_map = account_service
            .get_account_map(owner_ids.into_iter().collect())
            .await
            .int_err()?;

        for dataset_entry in resolved_entries {
            let maybe_account = account_map.get(&dataset_entry.owner_id);
            if let Some(account) = maybe_account {
                let dataset_handle = odf::DatasetHandle {
                    id: dataset_entry.id,
                    alias: odf::DatasetAlias::new(
                        Some(account.account_name.clone()),
                        dataset_entry.name,
                    ),
                };
                let dataset = Dataset::new(Account::from_account(account.clone()), dataset_handle);

                downstream_dependencies.push(DependencyDatasetResult::accessible(dataset));
            } else {
                tracing::warn!(
                    "Downstream owner's account not found for dataset: {:?}",
                    &dataset_entry
                );
                downstream_dependencies
                    .push(DependencyDatasetResult::not_accessible(dataset_entry.id));
            }
        }

        Ok(downstream_dependencies)
    }

    /// Current polling source used by the root dataset
    async fn current_polling_source(&self, ctx: &Context<'_>) -> Result<Option<SetPollingSource>> {
        let (dataset_registry, metadata_query_service) = from_catalog_n!(
            ctx,
            dyn domain::DatasetRegistry,
            dyn domain::MetadataQueryService
        );

        let target = dataset_registry.get_dataset_by_handle(&self.dataset_handle);
        let source = metadata_query_service
            .get_active_polling_source(target)
            .await
            .int_err()?;

        Ok(source.map(|(_hash, block)| block.event.into()))
    }

    /// Current push sources used by the root dataset
    async fn current_push_sources(&self, ctx: &Context<'_>) -> Result<Vec<AddPushSource>> {
        let (metadata_query_service, dataset_registry) = from_catalog_n!(
            ctx,
            dyn domain::MetadataQueryService,
            dyn domain::DatasetRegistry
        );

        let target = dataset_registry.get_dataset_by_handle(&self.dataset_handle);
        let mut push_sources: Vec<AddPushSource> = metadata_query_service
            .get_active_push_sources(target)
            .await
            .int_err()?
            .into_iter()
            .map(|(_hash, block)| block.event.into())
            .collect();

        push_sources.sort_by(|a, b| a.source_name.cmp(&b.source_name));

        Ok(push_sources)
    }

    /// Sync statuses of push remotes
    async fn push_sync_statuses(&self, ctx: &Context<'_>) -> Result<DatasetPushStatuses> {
        let service = from_catalog_n!(ctx, dyn domain::RemoteStatusService);
        let statuses = service.check_remotes_status(&self.dataset_handle).await?;

        Ok(statuses.into())
    }

    /// Current transformation used by the derivative dataset
    async fn current_transform(&self, ctx: &Context<'_>) -> Result<Option<SetTransform>> {
        let (metadata_query_service, dataset_registry) = from_catalog_n!(
            ctx,
            dyn kamu_core::MetadataQueryService,
            dyn domain::DatasetRegistry
        );

        let target = dataset_registry.get_dataset_by_handle(&self.dataset_handle);
        let source = metadata_query_service.get_active_transform(target).await?;

        Ok(source.map(|(_hash, block)| block.event.into()))
    }

    /// Current descriptive information about the dataset
    async fn current_info(&self, ctx: &Context<'_>) -> Result<SetInfo> {
        let resolved_dataset = get_dataset(ctx, &self.dataset_handle)?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(SearchSetInfoVisitor::new())
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
    async fn current_readme(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let resolved_dataset = get_dataset(ctx, &self.dataset_handle)?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(SearchSetAttachmentsVisitor::new())
            .await
            .int_err()?
            .into_event()
            .and_then(|e| {
                let odf::Attachments::Embedded(at) = e.attachments;

                at.items
                    .into_iter()
                    .filter(|i| i.path == "README.md")
                    .map(|i| i.content)
                    .next()
            }))
    }

    /// Current license associated with the dataset
    async fn current_license(&self, ctx: &Context<'_>) -> Result<Option<SetLicense>> {
        let resolved_dataset = get_dataset(ctx, &self.dataset_handle)?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(SearchSetLicenseVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(Into::into))
    }

    /// Current vocabulary associated with the dataset
    async fn current_vocab(&self, ctx: &Context<'_>) -> Result<Option<SetVocab>> {
        let resolved_dataset = get_dataset(ctx, &self.dataset_handle)?;

        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(SearchSetVocabVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(Into::into))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
enum DependencyDatasetResult {
    Accessible(DependencyDatasetResultAccessible),
    NotAccessible(DependencyDatasetResultNotAccessible),
}

impl DependencyDatasetResult {
    pub fn accessible(dataset: Dataset) -> Self {
        Self::Accessible(DependencyDatasetResultAccessible { dataset })
    }

    pub fn not_accessible(dataset_id: odf::DatasetID) -> Self {
        Self::NotAccessible(DependencyDatasetResultNotAccessible {
            id: dataset_id.into(),
        })
    }
}

#[derive(SimpleObject, Debug, Clone)]
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

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct DependencyDatasetResultNotAccessible {
    pub id: DatasetID,
}

#[ComplexObject]
impl DependencyDatasetResultNotAccessible {
    async fn message(&self) -> String {
        "Not Accessible".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
