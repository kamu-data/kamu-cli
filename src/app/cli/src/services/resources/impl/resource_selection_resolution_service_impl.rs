// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::collect_all_pages;
use kamu_resources::{ResourceIdentityView, ResourceKindDescriptor};
use kamu_resources_facade::{
    GetResourceError,
    GetResourceRequest,
    ListAllResourceIdentitiesRequest,
    ListResourceIdentitiesRequest,
    ResourceFacade,
};

use crate::CLIError;
use crate::resources::{
    ResourceIgnoredSelector,
    ResourceSelectionItem,
    ResourceSelectionResolution,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
    ResourceTarget,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceSelectionResolutionService)]
pub struct ResourceSelectionResolutionServiceImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_PAGE_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceSelectionResolutionService for ResourceSelectionResolutionServiceImpl {
    async fn resolve(
        &self,
        selection: ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
        ignore_not_found: bool,
    ) -> Result<ResourceSelectionResolution, CLIError> {
        let mut targets = Vec::with_capacity(selection.items.len());
        let mut ignored_selectors = Vec::new();

        for item in selection.items {
            match item {
                ResourceSelectionItem::All => {
                    let identities = Self::list_all_identities(resource_facade).await?;
                    targets.extend(
                        identities
                            .into_iter()
                            .map(|identity| Self::target_from_identity(identity, "all".to_owned())),
                    );
                }
                ResourceSelectionItem::AllByKind {
                    kind_descriptor,
                    selector_input,
                } => {
                    let identities =
                        Self::list_identities_by_kind(resource_facade, &kind_descriptor).await?;
                    targets.extend(identities.into_iter().map(|identity| {
                        Self::target_from_identity(identity, selector_input.clone())
                    }));
                }
                ResourceSelectionItem::Exact(selector) => {
                    let identity = resource_facade
                        .get_identity(GetResourceRequest {
                            kind: selector.kind_descriptor.kind.clone(),
                            api_version: Some(selector.kind_descriptor.api_version.clone()),
                            account: None,
                            resource_ref: selector.resource_ref,
                        })
                        .await;

                    match identity {
                        Ok(identity) => {
                            targets.push(Self::target_from_identity(
                                identity,
                                selector.selector_input,
                            ));
                        }
                        Err(
                            GetResourceError::NameNotFound(_) | GetResourceError::UIDNotFound(_),
                        ) if ignore_not_found => {
                            ignored_selectors.push(ResourceIgnoredSelector {
                                kind_descriptor: selector.kind_descriptor,
                                selector_input: selector.selector_input,
                            });
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }

        Ok(ResourceSelectionResolution {
            targets,
            ignored_selectors,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSelectionResolutionServiceImpl {
    async fn list_identities_by_kind(
        resource_facade: &dyn ResourceFacade,
        kind_descriptor: &ResourceKindDescriptor,
    ) -> Result<Vec<ResourceIdentityView>, CLIError> {
        collect_all_pages(RESOURCE_PAGE_SIZE, |pagination| async move {
            resource_facade
                .list_identities(ListResourceIdentitiesRequest {
                    kind: kind_descriptor.kind.clone(),
                    account: None,
                    pagination,
                })
                .await
                .map_err(Into::into)
        })
        .await
    }

    async fn list_all_identities(
        resource_facade: &dyn ResourceFacade,
    ) -> Result<Vec<ResourceIdentityView>, CLIError> {
        collect_all_pages(RESOURCE_PAGE_SIZE, |pagination| async move {
            resource_facade
                .list_all_identities(ListAllResourceIdentitiesRequest {
                    account: None,
                    pagination,
                })
                .await
                .map_err(Into::into)
        })
        .await
    }

    fn target_from_identity(
        identity: ResourceIdentityView,
        selector_input: String,
    ) -> ResourceTarget {
        ResourceTarget {
            kind: identity.kind,
            api_version: identity.api_version,
            canonical_kind_name: identity.canonical_kind_name,
            uid: identity.uid,
            name: identity.name,
            selector_input,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
