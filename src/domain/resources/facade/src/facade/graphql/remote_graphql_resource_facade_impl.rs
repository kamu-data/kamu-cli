// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use domain::{
    ResourceIdentityView,
    ResourceKindDescriptor,
    ResourceListColumnDescriptor,
    ResourceListColumnValue,
    ResourceListColumnValueView,
    ResourceNameNotFoundError,
    ResourcePhaseCounts,
    ResourceStatusSummaryView,
    ResourceSummaryView,
    ResourceTypeCountSummary,
    ResourceUIDNotFoundError,
    ResourcesSummary,
};
use graphql_http::{GraphqlHttpClient, GraphqlHttpRequestError};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources as domain;
use url::Url;

use super::fragments;
use crate::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchRequest,
    BatchRequestProblem,
    BatchRequestResponse,
    DeleteResourceError,
    DeleteResourceRequest,
    GetResourceError,
    GetResourceRequest,
    ListAllResourceIdentitiesRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourceIdentitiesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ListSupportedResourceKindsError,
    RenderResourceManifestError,
    RenderResourceManifestRequest,
    RenderResourceManifestResult,
    ResourceFacade,
    ResourceKindMismatchError,
    ResourceRef,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Note: intentionally not a dill component, used via factories
pub struct RemoteGraphqlResourceFacadeImpl {
    graphql_client: GraphqlHttpClient,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RemoteGraphqlResourceFacadeImpl {
    const LIST_PAGE_SIZE: usize = 100;

    const LIST_FIELDS: &'static str = r#"
        nodes {
          id
          apiVersion
          kind {
            value
          }
          name
          description
          generation
          createdAt
          updatedAt
          status {
            phase
            observedGeneration
            ready
          }
          listValues {
            key
            stringValue
            uint64Value
            boolValue
          }
        }
    "#;

    const RESOURCE_FIELDS: &'static str = r#"
        apiVersion
        kind {
          value
        }
        metadata {
          id
          accountId
          name
          description
          labels
          annotations
          generation
          createdAt
          updatedAt
          deletedAt
          lastReconciledAt
        }
        spec
        status
    "#;

    const BATCH_RESOURCE_FIELDS: &'static str = r#"
        resources {
          apiVersion
          kind {
            value
          }
          metadata {
            id
            accountId
            name
            description
            labels
            annotations
            generation
            createdAt
            updatedAt
            deletedAt
            lastReconciledAt
          }
          spec
          status
        }
        problems {
          requestIndex
          code
          message
        }
    "#;

    const BATCH_IDENTITY_FIELDS: &'static str = r#"
        identities {
          id
          apiVersion
          kind {
            value
          }
          canonicalKindName
          name
        }
        problems {
          requestIndex
          code
          message
        }
    "#;

    const BATCH_RENDER_MANIFEST_FIELDS: &'static str = r#"
        manifests {
          manifest
          format
        }
        problems {
          requestIndex
          code
          message
        }
    "#;

    const IDENTITY_LIST_FIELDS: &'static str = r#"
        nodes {
          id
          apiVersion
          kind {
            value
          }
          canonicalKindName
          name
        }
    "#;

    pub fn new(backend_url: &Url, maybe_access_token: Option<String>) -> Self {
        Self {
            graphql_client: GraphqlHttpClient::from_backend_url(backend_url, maybe_access_token),
        }
    }

    async fn execute_graphql<T>(&self, query: &str) -> Result<T, GraphqlHttpRequestError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.graphql_client.execute(query).await
    }

    fn apply_manifest_query(
        request: &ApplyManifestRequest,
        dry_run: bool,
    ) -> Result<String, ApplyManifestError> {
        let manifest = serde_json::to_string(&request.manifest).int_err()?;
        let format = request.format.to_string();

        Ok(format!(
            r#"
            mutation {{
              resources {{
                applyManifest(manifest: {manifest}, format: {format}, dryRun: {dry_run}) {{
                  __typename
                  ... on ResourceApplySuccess {{
                    operation
                    resource {{
                      apiVersion
                      kind {{
                        value
                      }}
                      metadata {{
                        id
                        accountId
                        name
                        description
                        labels
                        annotations
                        generation
                        createdAt
                        updatedAt
                        deletedAt
                        lastReconciledAt
                      }}
                      spec
                      status
                    }}
                    changes {{
                      kind
                      path
                      before
                      after
                    }}
                    warnings {{
                      code
                      path
                      message
                    }}
                  }}
                  ... on ResourceApplyRejection {{
                    category
                    message
                  }}
                }}
              }}
            }}
            "#
        ))
    }

    fn target_account_id<E>(
        account: Option<&domain::ResourceManifestAccount>,
    ) -> Result<Option<odf::AccountID>, E>
    where
        E: From<InternalError>,
    {
        match account {
            None => Ok(None),
            Some(account) => account.id.clone().map(Some).ok_or_else(|| {
                InternalError::new("Remote admin resource requests require target account id")
                    .into()
            }),
        }
    }

    fn parse_enum<T>(value: &str, field_name: &str) -> Result<T, InternalError>
    where
        T: std::str::FromStr,
    {
        value.parse().map_err(|_| {
            InternalError::new(format!(
                "Unsupported {field_name} '{value}' in remote resource list",
            ))
        })
    }

    fn resource_summary_from_fragment<E>(
        fragment: fragments::ResourceSummaryFragment,
    ) -> Result<ResourceSummaryView, E>
    where
        E: From<InternalError>,
    {
        let status = match fragment.status {
            Some(status) => Some(ResourceStatusSummaryView {
                phase: status
                    .phase
                    .as_deref()
                    .map(|phase| Self::parse_enum(phase, "resource phase"))
                    .transpose()?,
                observed_generation: status.observed_generation,
                ready: status.ready,
            }),
            None => None,
        };

        let list_values = fragment
            .list_values
            .into_iter()
            .map(|value| {
                let key = value.key;
                let value = match (value.string_value, value.uint64_value, value.bool_value) {
                    (Some(value), None, None) => ResourceListColumnValue::String(value),
                    (None, Some(value), None) => ResourceListColumnValue::UInt64(value),
                    (None, None, Some(value)) => ResourceListColumnValue::Bool(value),
                    (None, None, None) => {
                        return Err(E::from(InternalError::new(format!(
                            "Missing list value payload for key '{key}'",
                        ))));
                    }
                    _ => {
                        return Err(E::from(InternalError::new(format!(
                            "Ambiguous list value payload for key '{key}'",
                        ))));
                    }
                };

                Ok(ResourceListColumnValueView { key, value })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResourceSummaryView {
            kind: fragment.kind.value,
            api_version: fragment.api_version,
            uid: fragment.id,
            name: fragment.name,
            description: fragment.description,
            generation: fragment.generation,
            created_at: fragment.created_at,
            updated_at: fragment.updated_at,
            status,
            list_values,
        })
    }

    fn list_resources_query(
        kind: &str,
        page: usize,
        per_page: usize,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, InternalError> {
        let kind = serde_json::to_string(kind).int_err()?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      listByKind(kind: {{ custom: {kind} }}, page: {page}, perPage: {per_page}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::LIST_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    listByKind(kind: {{ custom: {kind} }}, page: {page}, perPage: {per_page}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::LIST_FIELDS,
            )
        })
    }

    fn list_all_resources_query(
        page: usize,
        per_page: usize,
        target_account_id: Option<&odf::AccountID>,
    ) -> String {
        if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      listAll(page: {page}, perPage: {per_page}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::LIST_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    listAll(page: {page}, perPage: {per_page}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::LIST_FIELDS,
            )
        }
    }

    fn list_resource_identities_query(
        kind: &str,
        page: usize,
        per_page: usize,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, InternalError> {
        let kind = serde_json::to_string(kind).int_err()?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      listIdentitiesByKind(kind: {{ custom: {kind} }}, page: {page}, perPage: {per_page}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::IDENTITY_LIST_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    listIdentitiesByKind(kind: {{ custom: {kind} }}, page: {page}, perPage: {per_page}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::IDENTITY_LIST_FIELDS,
            )
        })
    }

    fn list_all_resource_identities_query(
        page: usize,
        per_page: usize,
        target_account_id: Option<&odf::AccountID>,
    ) -> String {
        if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      listAllIdentities(page: {page}, perPage: {per_page}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::IDENTITY_LIST_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    listAllIdentities(page: {page}, perPage: {per_page}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::IDENTITY_LIST_FIELDS,
            )
        }
    }

    fn graphql_page_params(offset: usize, limit: usize) -> (usize, usize) {
        let page = offset.checked_div(limit).unwrap_or(0);
        let per_page = if limit == 0 {
            Self::LIST_PAGE_SIZE
        } else {
            limit
        };

        (page, per_page)
    }

    async fn list_resource_summaries<E>(
        &self,
        query_kind: RemoteListQuery<'_>,
        maybe_target_account_id: Option<&odf::AccountID>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ResourceSummaryView>, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) = Self::graphql_page_params(offset, limit);

        let query = match query_kind {
            RemoteListQuery::ByKind(kind) => {
                Self::list_resources_query(kind, page, per_page, maybe_target_account_id)
                    .map_err(E::from)?
            }
            RemoteListQuery::All => {
                Self::list_all_resources_query(page, per_page, maybe_target_account_id)
            }
        };

        let nodes = match query_kind {
            RemoteListQuery::ByKind(_) if maybe_target_account_id.is_some() => {
                let response: fragments::AdminListByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.admin.resources.list_by_kind.nodes
            }
            RemoteListQuery::ByKind(_) => {
                let response: fragments::ListByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_by_kind.nodes
            }
            RemoteListQuery::All if maybe_target_account_id.is_some() => {
                let response: fragments::AdminListAllQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.admin.resources.list_all.nodes
            }
            RemoteListQuery::All => {
                let response: fragments::ListAllQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_all.nodes
            }
        };

        nodes
            .into_iter()
            .map(Self::resource_summary_from_fragment::<E>)
            .collect()
    }

    async fn list_resource_identities<E>(
        &self,
        query_kind: RemoteListQuery<'_>,
        maybe_target_account_id: Option<&odf::AccountID>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ResourceIdentityView>, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) = Self::graphql_page_params(offset, limit);

        let query = match query_kind {
            RemoteListQuery::ByKind(kind) => {
                Self::list_resource_identities_query(kind, page, per_page, maybe_target_account_id)
                    .map_err(E::from)?
            }
            RemoteListQuery::All => {
                Self::list_all_resource_identities_query(page, per_page, maybe_target_account_id)
            }
        };

        let nodes = match query_kind {
            RemoteListQuery::ByKind(_) if maybe_target_account_id.is_some() => {
                let response: fragments::AdminListIdentitiesByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.admin.resources.list_identities_by_kind.nodes
            }
            RemoteListQuery::ByKind(_) => {
                let response: fragments::ListIdentitiesByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_identities_by_kind.nodes
            }
            RemoteListQuery::All if maybe_target_account_id.is_some() => {
                let response: fragments::AdminListAllIdentitiesQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.admin.resources.list_all_identities.nodes
            }
            RemoteListQuery::All => {
                let response: fragments::ListAllIdentitiesQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_all_identities.nodes
            }
        };

        Ok(nodes.into_iter().map(Into::into).collect())
    }

    fn selector_input(
        kind: &str,
        api_version: Option<&str>,
        resource_ref: &ResourceRef,
    ) -> Result<String, InternalError> {
        let kind = serde_json::to_string(kind).int_err()?;
        let selector_ref = Self::resource_ref_input(resource_ref)?;
        let maybe_api_version = match api_version {
            Some(api_version) => format!(
                "apiVersion: {},",
                serde_json::to_string(api_version).int_err()?
            ),
            None => String::new(),
        };

        Ok(format!(
            r#"{{
                kind: {{ custom: {kind} }},
                {maybe_api_version}
                ref: {selector_ref}
            }}"#
        ))
    }

    fn selectors_input(requests: &[GetResourceRequest]) -> Result<String, InternalError> {
        let selectors = requests
            .iter()
            .map(|request| {
                Self::selector_input(
                    &request.kind,
                    request.api_version.as_deref(),
                    &request.resource_ref,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");

        Ok(format!("[{selectors}]"))
    }

    fn common_target_account_id(
        requests: &[GetResourceRequest],
    ) -> Result<Option<odf::AccountID>, GetResourceError> {
        let mut common_account_id = None;
        let mut seen_first = false;

        for request in requests {
            let target_account_id =
                Self::target_account_id::<GetResourceError>(request.account.as_ref())?;
            if seen_first {
                if target_account_id != common_account_id {
                    return Err(InternalError::new(
                        "Remote batch resource identity request cannot mix target accounts",
                    )
                    .into());
                }
            } else {
                common_account_id = target_account_id;
                seen_first = true;
            }
        }

        Ok(common_account_id)
    }

    fn common_render_manifest_target_account_id(
        requests: &[RenderResourceManifestRequest],
    ) -> Result<Option<odf::AccountID>, RenderResourceManifestError> {
        let mut common_account_id = None;
        let mut common_format = None;
        let mut seen_first = false;

        for request in requests {
            let target_account_id =
                Self::target_account_id::<RenderResourceManifestError>(request.account.as_ref())?;

            if seen_first {
                if common_account_id != target_account_id {
                    return Err(InternalError::new(
                        "Remote batch resource manifest request cannot mix target accounts",
                    )
                    .into());
                }
            } else {
                common_account_id = target_account_id;
                seen_first = true;
            }

            if let Some(format) = common_format {
                if request.format != format {
                    return Err(InternalError::new(
                        "Remote batch resource manifest request cannot mix formats",
                    )
                    .into());
                }
            } else {
                common_format = Some(request.format);
            }
        }

        Ok(common_account_id)
    }

    fn get_resource_query(
        request: &GetResourceRequest,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, GetResourceError> {
        let selector = Self::selector_input(
            &request.kind,
            request.api_version.as_deref(),
            &request.resource_ref,
        )?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      resource(selector: {selector}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::RESOURCE_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    resource(selector: {selector}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::RESOURCE_FIELDS,
            )
        })
    }

    fn get_resources_query(requests: &[GetResourceRequest]) -> Result<String, GetResourceError> {
        let target_account_id = Self::common_target_account_id(requests)?;
        let selectors = Self::selectors_input(requests)?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      resources(selectors: {selectors}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::BATCH_RESOURCE_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    resources(selectors: {selectors}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::BATCH_RESOURCE_FIELDS,
            )
        })
    }

    fn get_resource_identity_query(
        request: &GetResourceRequest,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, GetResourceError> {
        let selector = Self::selector_input(
            &request.kind,
            request.api_version.as_deref(),
            &request.resource_ref,
        )?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      resourceIdentity(selector: {selector}) {{
                        id
                        apiVersion
                        kind {{
                          value
                        }}
                        canonicalKindName
                        name
                      }}
                    }}
                  }}
                }}
                "#
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    resourceIdentity(selector: {selector}) {{
                      id
                      apiVersion
                      kind {{
                        value
                      }}
                      canonicalKindName
                      name
                    }}
                  }}
                }}
                "#
            )
        })
    }

    fn get_resource_identities_query(
        requests: &[GetResourceRequest],
    ) -> Result<String, GetResourceError> {
        let target_account_id = Self::common_target_account_id(requests)?;
        let selectors = Self::selectors_input(requests)?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      resourceIdentities(selectors: {selectors}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::BATCH_IDENTITY_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    resourceIdentities(selectors: {selectors}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::BATCH_IDENTITY_FIELDS,
            )
        })
    }

    fn render_manifest_query(
        request: &RenderResourceManifestRequest,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, RenderResourceManifestError> {
        let selector = Self::selector_input(
            &request.kind,
            request.api_version.as_deref(),
            &request.resource_ref,
        )
        .map_err(RenderResourceManifestError::Internal)?;
        let format = request.format.to_string();

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      renderManifest(
                        selector: {selector}
                        format: {format}
                      ) {{
                        manifest
                        format
                      }}
                    }}
                  }}
                }}
                "#
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    renderManifest(
                      selector: {selector}
                      format: {format}
                    ) {{
                      manifest
                      format
                    }}
                  }}
                }}
                "#
            )
        })
    }

    fn render_manifests_query(
        requests: &[RenderResourceManifestRequest],
    ) -> Result<String, RenderResourceManifestError> {
        let target_account_id = Self::common_render_manifest_target_account_id(requests)?;
        let selectors = requests
            .iter()
            .map(|request| {
                Self::selector_input(
                    &request.kind,
                    request.api_version.as_deref(),
                    &request.resource_ref,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(RenderResourceManifestError::Internal)?
            .join(", ");
        let selectors = format!("[{selectors}]");
        let format = requests[0].format.to_string();

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      renderManifests(
                        selectors: {selectors}
                        format: {format}
                      ) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::BATCH_RENDER_MANIFEST_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    renderManifests(
                      selectors: {selectors}
                      format: {format}
                    ) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::BATCH_RENDER_MANIFEST_FIELDS,
            )
        })
    }

    fn delete_resource_query(
        request: &DeleteResourceRequest,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, DeleteResourceError> {
        let selector = Self::selector_input(&request.kind, None, &request.resource_ref)
            .map_err(DeleteResourceError::Internal)?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                mutation {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      delete(selector: {selector}) {{
                        resourceId
                      }}
                    }}
                  }}
                }}
                "#
            )
        } else {
            format!(
                r#"
                mutation {{
                  resources {{
                    delete(selector: {selector}) {{
                      resourceId
                    }}
                  }}
                }}
                "#
            )
        })
    }

    fn resource_ref_input(resource_ref: &ResourceRef) -> Result<String, InternalError> {
        match resource_ref {
            ResourceRef::ById(uid) => Ok(format!(
                "{{ byId: {} }}",
                serde_json::to_string(&uid).int_err()?
            )),
            ResourceRef::ByName(name) => Ok(format!(
                "{{ byName: {{ name: {} }} }}",
                serde_json::to_string(&name).int_err()?
            )),
        }
    }

    fn map_delete_graphql_error(
        request: &DeleteResourceRequest,
        message: &str,
    ) -> Option<DeleteResourceError> {
        match &request.resource_ref {
            ResourceRef::ById(uid) => {
                let not_found = domain::ResourceUIDNotFoundError(*uid);
                if message.contains(&not_found.to_string()) {
                    return Some(DeleteResourceError::UIDNotFound(not_found));
                }

                let mismatch_prefix = format!("Resource uid {uid} refers to kind '");
                let mismatch_suffix = format!("', expected '{}'", request.kind);
                if let Some(actual_kind_start) = message.find(&mismatch_prefix) {
                    let actual_kind_start = actual_kind_start + mismatch_prefix.len();
                    if let Some(actual_kind_end) =
                        message[actual_kind_start..].find(&mismatch_suffix)
                    {
                        return Some(DeleteResourceError::KindMismatch(
                            ResourceKindMismatchError {
                                uid: *uid,
                                expected_kind: request.kind.clone(),
                                actual_kind: message
                                    [actual_kind_start..actual_kind_start + actual_kind_end]
                                    .to_string(),
                            },
                        ));
                    }
                }

                None
            }
            ResourceRef::ByName(name) => {
                let not_found = domain::ResourceNameNotFoundError {
                    kind: request.kind.clone(),
                    name: name.clone(),
                };
                message
                    .contains(&not_found.to_string())
                    .then_some(DeleteResourceError::NameNotFound(not_found))
            }
        }
    }

    fn map_delete_remote_error(
        request: &DeleteResourceRequest,
        error: GraphqlHttpRequestError,
    ) -> DeleteResourceError {
        match error {
            GraphqlHttpRequestError::Graphql {
                endpoint_url,
                message,
            } => Self::map_delete_graphql_error(request, &message).unwrap_or_else(|| {
                DeleteResourceError::RemoteRequest(GraphqlHttpRequestError::Graphql {
                    endpoint_url,
                    message,
                })
            }),
            GraphqlHttpRequestError::Internal(error) => DeleteResourceError::Internal(error),
            other => DeleteResourceError::RemoteRequest(other),
        }
    }

    fn map_render_manifest_graphql_error(
        request: &RenderResourceManifestRequest,
        message: &str,
    ) -> Option<RenderResourceManifestError> {
        match &request.resource_ref {
            ResourceRef::ById(uid) => {
                let error = domain::ResourceUIDNotFoundError(*uid);
                message
                    .contains(&error.to_string())
                    .then_some(RenderResourceManifestError::UIDNotFound(error))
            }
            ResourceRef::ByName(name) => {
                let error = domain::ResourceNameNotFoundError {
                    kind: request.kind.clone(),
                    name: name.clone(),
                };
                message
                    .contains(&error.to_string())
                    .then_some(RenderResourceManifestError::NameNotFound(error))
            }
        }
    }

    fn map_render_manifest_remote_error(
        request: &RenderResourceManifestRequest,
        error: GraphqlHttpRequestError,
    ) -> RenderResourceManifestError {
        match error {
            GraphqlHttpRequestError::Graphql {
                endpoint_url,
                message,
            } => Self::map_render_manifest_graphql_error(request, &message).unwrap_or_else(|| {
                RenderResourceManifestError::RemoteRequest(GraphqlHttpRequestError::Graphql {
                    endpoint_url,
                    message,
                })
            }),
            GraphqlHttpRequestError::Internal(error) => {
                RenderResourceManifestError::Internal(error)
            }
            other => RenderResourceManifestError::RemoteRequest(other),
        }
    }

    fn not_found_error(request: &GetResourceRequest) -> GetResourceError {
        match &request.resource_ref {
            ResourceRef::ById(uid) => GetResourceError::UIDNotFound(ResourceUIDNotFoundError(*uid)),
            ResourceRef::ByName(name) => {
                GetResourceError::NameNotFound(ResourceNameNotFoundError {
                    kind: request.kind.clone(),
                    name: name.clone(),
                })
            }
        }
    }

    fn batch_resource_problem_error(
        problem: fragments::BatchResourceProblemFragment,
        request: &GetResourceRequest,
    ) -> GetResourceError {
        use fragments::BatchResourceProblemCodeFragment as Code;
        match problem.code {
            Code::UidNotFound => match request.resource_ref {
                ResourceRef::ById(uid) => {
                    GetResourceError::UIDNotFound(ResourceUIDNotFoundError(uid))
                }
                ResourceRef::ByName(_) => {
                    GetResourceError::Internal(InternalError::new(problem.message))
                }
            },
            Code::NameNotFound => match &request.resource_ref {
                ResourceRef::ByName(name) => {
                    GetResourceError::NameNotFound(ResourceNameNotFoundError {
                        kind: request.kind.clone(),
                        name: name.clone(),
                    })
                }
                ResourceRef::ById(_) => {
                    GetResourceError::Internal(InternalError::new(problem.message))
                }
            },
            Code::ApiVersionMismatch
            | Code::KindMismatch
            | Code::UnsupportedDescriptor
            | Code::BadAccount
            | Code::RemoteRequest
            | Code::Internal => GetResourceError::Internal(InternalError::new(problem.message)),
        }
    }

    fn batch_render_manifest_problem_error(
        problem: fragments::BatchResourceProblemFragment,
        request: &RenderResourceManifestRequest,
    ) -> RenderResourceManifestError {
        use fragments::BatchResourceProblemCodeFragment as Code;
        match problem.code {
            Code::UidNotFound => match request.resource_ref {
                ResourceRef::ById(uid) => {
                    RenderResourceManifestError::UIDNotFound(ResourceUIDNotFoundError(uid))
                }
                ResourceRef::ByName(_) => {
                    RenderResourceManifestError::Internal(InternalError::new(problem.message))
                }
            },
            Code::NameNotFound => match &request.resource_ref {
                ResourceRef::ByName(name) => {
                    RenderResourceManifestError::NameNotFound(ResourceNameNotFoundError {
                        kind: request.kind.clone(),
                        name: name.clone(),
                    })
                }
                ResourceRef::ById(_) => {
                    RenderResourceManifestError::Internal(InternalError::new(problem.message))
                }
            },
            Code::ApiVersionMismatch
            | Code::KindMismatch
            | Code::UnsupportedDescriptor
            | Code::BadAccount
            | Code::RemoteRequest
            | Code::Internal => {
                RenderResourceManifestError::Internal(InternalError::new(problem.message))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for RemoteGraphqlResourceFacadeImpl {
    async fn list_supported_kinds(
        &self,
    ) -> Result<Vec<ResourceKindDescriptor>, ListSupportedResourceKindsError> {
        let response: fragments::SupportedKindsQueryDataFragment = self
            .execute_graphql(
                r#"
                query {
                  resources {
                    supportedKinds {
                      name
                      shortNames
                      kind {
                        value
                      }
                      apiVersion
                      listColumns {
                        key
                        header
                        dataType
                        visibility
                      }
                    }
                  }
                }
                "#,
            )
            .await?;

        Ok(response
            .resources
            .supported_kinds
            .into_iter()
            .map(|item| {
                Ok(ResourceKindDescriptor {
                    name: item.name,
                    short_names: item.short_names,
                    kind: item.kind.value,
                    api_version: item.api_version,
                    list_columns: item
                        .list_columns
                        .into_iter()
                        .map(|column| {
                            Ok(ResourceListColumnDescriptor {
                                key: column.key,
                                header: column.header,
                                data_type: Self::parse_enum(
                                    &column.data_type,
                                    "list column data type",
                                )?,
                                visibility: Self::parse_enum(
                                    &column.visibility,
                                    "list column visibility",
                                )?,
                            })
                        })
                        .collect::<Result<Vec<_>, InternalError>>()?,
                })
            })
            .collect::<Result<Vec<_>, InternalError>>()?)
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError> {
        let maybe_target_account_id =
            Self::target_account_id::<ResourcesSummaryError>(request.account.as_ref())?;

        let response = if let Some(target_account_id) = maybe_target_account_id {
            let query = format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      summary {{
                        resourceCounts {{
                          kind
                          name
                          apiVersion
                          totalCount
                          phaseCounts {{
                            pending
                            reconciling
                            ready
                            degraded
                            failed
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
                "#
            );

            let response: fragments::AdminSummaryQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.summary
        } else {
            let response: fragments::SummaryQueryDataFragment = self
                .execute_graphql(
                    r#"
                    query {
                      resources {
                        summary {
                          resourceCounts {
                            kind
                            name
                            apiVersion
                            totalCount
                            phaseCounts {
                              pending
                              reconciling
                              ready
                              degraded
                              failed
                            }
                          }
                        }
                      }
                    }
                    "#,
                )
                .await?;
            response.resources.summary
        };

        Ok(ResourcesSummary {
            resource_counts: response
                .resource_counts
                .into_iter()
                .map(|item| ResourceTypeCountSummary {
                    kind: item.kind,
                    name: item.name,
                    api_version: item.api_version,
                    total_count: item.total_count,
                    phase_counts: ResourcePhaseCounts {
                        pending: item.phase_counts.pending,
                        reconciling: item.phase_counts.reconciling,
                        ready: item.phase_counts.ready,
                        degraded: item.phase_counts.degraded,
                        failed: item.phase_counts.failed,
                    },
                })
                .collect(),
        })
    }

    async fn get(
        &self,
        request: GetResourceRequest,
    ) -> Result<kamu_resources::ResourceView, GetResourceError> {
        let maybe_target_account_id =
            Self::target_account_id::<GetResourceError>(request.account.as_ref())?;

        let query = Self::get_resource_query(&request, maybe_target_account_id.as_ref())?;

        let maybe_resource = if maybe_target_account_id.is_some() {
            let response: fragments::AdminGetResourceQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.resource
        } else {
            let response: fragments::GetResourceQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.resources.resource
        };

        let Some(resource) = maybe_resource else {
            return Err(Self::not_found_error(&request));
        };

        resource.try_into().map_err(GetResourceError::Internal)
    }

    async fn get_many(
        &self,
        request: BatchRequest<GetResourceRequest>,
    ) -> Result<BatchRequestResponse<domain::ResourceView, GetResourceError>, GetResourceError>
    {
        if request.requests.is_empty() {
            return Ok(BatchRequestResponse {
                items: Vec::new(),
                problems: Vec::new(),
            });
        }

        let maybe_target_account_id = Self::common_target_account_id(&request.requests)?;

        let query = Self::get_resources_query(&request.requests)?;

        let batch_result = if maybe_target_account_id.is_some() {
            let response: fragments::AdminBatchGetResourcesQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.resources
        } else {
            let response: fragments::BatchGetResourcesQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.resources.resources
        };

        let resources = batch_result
            .resources
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(GetResourceError::Internal)?;

        let problems = batch_result
            .problems
            .into_iter()
            .map(|problem| {
                let request = request.requests.get(problem.request_index).ok_or_else(|| {
                    GetResourceError::Internal(InternalError::new(format!(
                        "Remote resource problem index {} is out of bounds",
                        problem.request_index
                    )))
                })?;

                Ok(BatchRequestProblem {
                    request_index: problem.request_index,
                    error: Self::batch_resource_problem_error(problem, request),
                })
            })
            .collect::<Result<Vec<_>, GetResourceError>>()?;

        Ok(BatchRequestResponse {
            items: resources,
            problems,
        })
    }

    async fn get_identity(
        &self,
        request: GetResourceRequest,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        let maybe_target_account_id =
            Self::target_account_id::<GetResourceError>(request.account.as_ref())?;

        let query = Self::get_resource_identity_query(&request, maybe_target_account_id.as_ref())?;

        let maybe_identity = if maybe_target_account_id.is_some() {
            let response: fragments::AdminGetResourceIdentityQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.resource_identity
        } else {
            let response: fragments::GetResourceIdentityQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.resources.resource_identity
        };

        let Some(identity) = maybe_identity else {
            return Err(Self::not_found_error(&request));
        };

        Ok(identity.into())
    }

    async fn get_identities(
        &self,
        request: BatchRequest<GetResourceRequest>,
    ) -> Result<BatchRequestResponse<ResourceIdentityView, GetResourceError>, GetResourceError>
    {
        if request.requests.is_empty() {
            return Ok(BatchRequestResponse {
                items: Vec::new(),
                problems: Vec::new(),
            });
        }

        let maybe_target_account_id = Self::common_target_account_id(&request.requests)?;

        let query = Self::get_resource_identities_query(&request.requests)?;

        let batch_result = if maybe_target_account_id.is_some() {
            let response: fragments::AdminBatchGetResourceIdentitiesQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.resource_identities
        } else {
            let response: fragments::BatchGetResourceIdentitiesQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.resources.resource_identities
        };

        let identities = batch_result
            .identities
            .into_iter()
            .map(Into::into)
            .collect();

        let problems = batch_result
            .problems
            .into_iter()
            .map(|problem| {
                let request = request.requests.get(problem.request_index).ok_or_else(|| {
                    GetResourceError::Internal(InternalError::new(format!(
                        "Remote resource identity problem index {} is out of bounds",
                        problem.request_index
                    )))
                })?;

                Ok(BatchRequestProblem {
                    request_index: problem.request_index,
                    error: Self::batch_resource_problem_error(problem, request),
                })
            })
            .collect::<Result<Vec<_>, GetResourceError>>()?;

        Ok(BatchRequestResponse {
            items: identities,
            problems,
        })
    }

    async fn render_manifest(
        &self,
        request: RenderResourceManifestRequest,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let maybe_target_account_id =
            Self::target_account_id::<RenderResourceManifestError>(request.account.as_ref())?;

        let query = Self::render_manifest_query(&request, maybe_target_account_id.as_ref())?;

        let rendered = if maybe_target_account_id.is_some() {
            let response: fragments::AdminRenderManifestQueryDataFragment = self
                .execute_graphql(&query)
                .await
                .map_err(|error| Self::map_render_manifest_remote_error(&request, error))?;
            response.admin.resources.render_manifest
        } else {
            let response: fragments::RenderManifestQueryDataFragment = self
                .execute_graphql(&query)
                .await
                .map_err(|error| Self::map_render_manifest_remote_error(&request, error))?;
            response.resources.render_manifest
        };

        Ok(RenderResourceManifestResult {
            manifest: rendered.manifest,
            format: rendered.format.into(),
        })
    }

    async fn render_manifests(
        &self,
        request: BatchRequest<RenderResourceManifestRequest>,
    ) -> Result<
        BatchRequestResponse<RenderResourceManifestResult, RenderResourceManifestError>,
        RenderResourceManifestError,
    > {
        if request.requests.is_empty() {
            return Ok(BatchRequestResponse {
                items: Vec::new(),
                problems: Vec::new(),
            });
        }

        let maybe_target_account_id =
            Self::common_render_manifest_target_account_id(&request.requests)?;

        let query = Self::render_manifests_query(&request.requests)?;

        let batch_result = if maybe_target_account_id.is_some() {
            let response: fragments::AdminBatchRenderManifestsQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.render_manifests
        } else {
            let response: fragments::BatchRenderManifestsQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.resources.render_manifests
        };

        let manifests = batch_result
            .manifests
            .into_iter()
            .map(|manifest| RenderResourceManifestResult {
                manifest: manifest.manifest,
                format: manifest.format.into(),
            })
            .collect();

        let problems = batch_result
            .problems
            .into_iter()
            .map(|problem| {
                let request = request.requests.get(problem.request_index).ok_or_else(|| {
                    RenderResourceManifestError::Internal(InternalError::new(format!(
                        "Remote resource manifest problem index {} is out of bounds",
                        problem.request_index
                    )))
                })?;

                Ok(BatchRequestProblem {
                    request_index: problem.request_index,
                    error: Self::batch_render_manifest_problem_error(problem, request),
                })
            })
            .collect::<Result<Vec<_>, RenderResourceManifestError>>()?;

        Ok(BatchRequestResponse {
            items: manifests,
            problems,
        })
    }

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListResourcesError> {
        let maybe_target_account_id =
            Self::target_account_id::<ListResourcesError>(request.account.as_ref())?;

        self.list_resource_summaries::<ListResourcesError>(
            RemoteListQuery::ByKind(&request.kind),
            maybe_target_account_id.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn list_identities(
        &self,
        request: ListResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListResourcesError> {
        let maybe_target_account_id =
            Self::target_account_id::<ListResourcesError>(request.account.as_ref())?;

        self.list_resource_identities::<ListResourcesError>(
            RemoteListQuery::ByKind(&request.kind),
            maybe_target_account_id.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListAllResourcesError> {
        let maybe_target_account_id =
            Self::target_account_id::<ListAllResourcesError>(request.account.as_ref())?;

        self.list_resource_summaries::<ListAllResourcesError>(
            RemoteListQuery::All,
            maybe_target_account_id.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn list_all_identities(
        &self,
        request: ListAllResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListAllResourcesError> {
        let maybe_target_account_id =
            Self::target_account_id::<ListAllResourcesError>(request.account.as_ref())?;

        self.list_resource_identities::<ListAllResourcesError>(
            RemoteListQuery::All,
            maybe_target_account_id.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestPlanningDecision, ApplyManifestError> {
        let query = Self::apply_manifest_query(&request, true)?;
        let response: fragments::ApplyManifestMutationDataFragment =
            self.execute_graphql(&query).await?;

        response
            .resources
            .apply_manifest
            .into_planning_decision()
            .map_err(Into::into)
    }

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestApplicationDecision, ApplyManifestError> {
        let query = Self::apply_manifest_query(&request, false)?;
        let response: fragments::ApplyManifestMutationDataFragment =
            self.execute_graphql(&query).await?;

        response
            .resources
            .apply_manifest
            .into_application_decision()
            .map_err(Into::into)
    }

    async fn delete(
        &self,
        request: DeleteResourceRequest,
    ) -> Result<kamu_resources::ResourceUID, DeleteResourceError> {
        let maybe_target_account_id =
            Self::target_account_id::<DeleteResourceError>(request.account.as_ref())?;

        let query = Self::delete_resource_query(&request, maybe_target_account_id.as_ref())?;

        let resource_id = if maybe_target_account_id.is_some() {
            let response: fragments::AdminDeleteMutationDataFragment = self
                .execute_graphql(&query)
                .await
                .map_err(|error| Self::map_delete_remote_error(&request, error))?;
            response.admin.resources.delete.resource_id
        } else {
            let response: fragments::DeleteMutationDataFragment = self
                .execute_graphql(&query)
                .await
                .map_err(|error| Self::map_delete_remote_error(&request, error))?;
            response.resources.delete.resource_id
        };

        Ok(resource_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum RemoteListQuery<'a> {
    ByKind(&'a str),
    All,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
