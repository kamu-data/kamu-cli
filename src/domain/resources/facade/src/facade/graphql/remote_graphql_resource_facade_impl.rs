// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use domain::{
    ResourceAPIVersionMismatchError,
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
    BatchResourceError,
    BatchResourceProblem,
    BatchResourceResponse,
    BatchResourceSuccess,
    DeleteResourceError,
    GetResourceError,
    ListAllResourceIdentitiesRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourceIdentitiesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ListSupportedResourceKindsError,
    RenderResourceManifestError,
    RenderResourceManifestResult,
    ResourceBatchSelector,
    ResourceFacade,
    ResourceKindMismatchError,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
    SearchResourceIdentitiesRequest,
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
          requestIndex
          resource {
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
        }
        problems {
          requestIndex
          code
          message
        }
    "#;

    const BATCH_IDENTITY_FIELDS: &'static str = r#"
        identities {
          requestIndex
          identity {
            id
            apiVersion
            kind {
              value
            }
            canonicalKindName
            name
          }
        }
        problems {
          requestIndex
          code
          message
        }
    "#;

    const BATCH_RENDER_MANIFEST_FIELDS: &'static str = r#"
        manifests {
          requestIndex
          manifest {
            manifest
            format
          }
        }
        problems {
          requestIndex
          code
          message
        }
    "#;

    const BATCH_DELETE_FIELDS: &'static str = r#"
        resources {
          requestIndex
          resourceId
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

    fn account_selector_input(
        account: Option<&domain::ResourceManifestAccount>,
    ) -> Result<Option<String>, InternalError> {
        match account {
            None => Ok(None),
            Some(account) => {
                if let Some(id) = &account.id {
                    Ok(Some(format!(
                        "account: {{ byId: {} }}",
                        serde_json::to_string(&id.to_string()).int_err()?
                    )))
                } else if let Some(name) = &account.name {
                    Ok(Some(format!(
                        "account: {{ byName: {} }}",
                        serde_json::to_string(name).int_err()?
                    )))
                } else {
                    Err(InternalError::new(
                        "Remote resource request account selector must contain either id or name",
                    ))
                }
            }
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
        account: Option<&domain::ResourceManifestAccount>,
    ) -> Result<String, InternalError> {
        let kind = serde_json::to_string(kind).int_err()?;
        let account_arg = Self::account_selector_input(account)?
            .map(|s| format!("{s}, "))
            .unwrap_or_default();

        Ok(format!(
            r#"
            query {{
              resources {{
                listByKind(kind: {{ custom: {kind} }}, {account_arg}page: {page}, perPage: {per_page}) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::LIST_FIELDS,
        ))
    }

    fn list_all_resources_query(
        page: usize,
        per_page: usize,
        account: Option<&domain::ResourceManifestAccount>,
    ) -> Result<String, InternalError> {
        let account_arg = Self::account_selector_input(account)?
            .map(|s| format!("{s}, "))
            .unwrap_or_default();

        Ok(format!(
            r#"
            query {{
              resources {{
                listAll({account_arg}page: {page}, perPage: {per_page}) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::LIST_FIELDS,
        ))
    }

    fn list_resource_identities_query(
        kind: &str,
        page: usize,
        per_page: usize,
        account: Option<&domain::ResourceManifestAccount>,
    ) -> Result<String, InternalError> {
        let kind = serde_json::to_string(kind).int_err()?;
        let account_arg = Self::account_selector_input(account)?
            .map(|s| format!("{s}, "))
            .unwrap_or_default();

        Ok(format!(
            r#"
            query {{
              resources {{
                listIdentitiesByKind(kind: {{ custom: {kind} }}, {account_arg}page: {page}, perPage: {per_page}) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::IDENTITY_LIST_FIELDS,
        ))
    }

    fn list_all_resource_identities_query(
        page: usize,
        per_page: usize,
        account: Option<&domain::ResourceManifestAccount>,
    ) -> Result<String, InternalError> {
        let account_arg = Self::account_selector_input(account)?
            .map(|s| format!("{s}, "))
            .unwrap_or_default();

        Ok(format!(
            r#"
            query {{
              resources {{
                listAllIdentities({account_arg}page: {page}, perPage: {per_page}) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::IDENTITY_LIST_FIELDS,
        ))
    }

    fn search_resource_identities_query(
        request: &SearchResourceIdentitiesRequest,
        page: usize,
        per_page: usize,
    ) -> Result<String, InternalError> {
        let kinds = request
            .kinds
            .iter()
            .map(|kind| {
                Ok(format!(
                    "{{ custom: {} }}",
                    serde_json::to_string(kind).int_err()?
                ))
            })
            .collect::<Result<Vec<_>, InternalError>>()?
            .join(", ");

        let names = request
            .exact_names
            .as_ref()
            .map(|names| serde_json::to_string(names).int_err())
            .transpose()?;
        let name_pattern = request
            .name_pattern
            .as_ref()
            .map(|pattern| serde_json::to_string(pattern).int_err())
            .transpose()?;
        let account = Self::account_selector_input(request.account.as_ref())?
            .map(|account| format!("account: {account},"))
            .unwrap_or_default();
        let names = names
            .map(|names| format!("names: {names},"))
            .unwrap_or_default();
        let name_pattern = name_pattern
            .map(|name_pattern| format!("namePattern: {name_pattern},"))
            .unwrap_or_default();

        Ok(format!(
            r#"
            query {{
              resources {{
                searchIdentities(
                  query: {{ kinds: [{kinds}], {names}{name_pattern}{account} }},
                  page: {page},
                  perPage: {per_page}
                ) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::IDENTITY_LIST_FIELDS,
        ))
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
        account: Option<&domain::ResourceManifestAccount>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ResourceSummaryView>, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) = Self::graphql_page_params(offset, limit);

        let query = match query_kind {
            RemoteListQuery::ByKind(kind) => {
                Self::list_resources_query(kind, page, per_page, account).map_err(E::from)?
            }
            RemoteListQuery::All => {
                Self::list_all_resources_query(page, per_page, account).map_err(E::from)?
            }
        };

        let nodes = match query_kind {
            RemoteListQuery::ByKind(_) => {
                let response: fragments::ListByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_by_kind.nodes
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
        account: Option<&domain::ResourceManifestAccount>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ResourceIdentityView>, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) = Self::graphql_page_params(offset, limit);

        let query = match query_kind {
            RemoteListQuery::ByKind(kind) => {
                Self::list_resource_identities_query(kind, page, per_page, account)
                    .map_err(E::from)?
            }
            RemoteListQuery::All => {
                Self::list_all_resource_identities_query(page, per_page, account)
                    .map_err(E::from)?
            }
        };

        let nodes = match query_kind {
            RemoteListQuery::ByKind(_) => {
                let response: fragments::ListIdentitiesByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_identities_by_kind.nodes
            }
            RemoteListQuery::All => {
                let response: fragments::ListAllIdentitiesQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_all_identities.nodes
            }
        };

        Ok(nodes.into_iter().map(Into::into).collect())
    }

    async fn search_resource_identities<E>(
        &self,
        request: &SearchResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) =
            Self::graphql_page_params(request.pagination.offset, request.pagination.limit);
        let query =
            Self::search_resource_identities_query(request, page, per_page).map_err(E::from)?;
        let response: fragments::SearchIdentitiesQueryDataFragment =
            self.execute_graphql(&query).await?;

        Ok(response
            .resources
            .search_identities
            .nodes
            .into_iter()
            .map(Into::into)
            .collect())
    }

    fn selector_input(
        kind: &str,
        api_version: Option<&str>,
        resource_ref: &ResourceRef,
        account: Option<&domain::ResourceManifestAccount>,
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
        let account_arg = Self::account_selector_input(account)?
            .map(|s| format!("{s},"))
            .unwrap_or_default();

        Ok(format!(
            r#"{{
                kind: {{ custom: {kind} }},
                {maybe_api_version}
                ref: {selector_ref},
                {account_arg}
            }}"#
        ))
    }

    fn batch_selector_input(selector: &ResourceBatchSelector) -> Result<String, InternalError> {
        let kind = serde_json::to_string(&selector.kind).int_err()?;
        let refs = Self::resource_refs_input(&selector.resource_refs)?;
        let maybe_api_version = match selector.api_version.as_deref() {
            Some(api_version) => format!(
                "apiVersion: {},",
                serde_json::to_string(api_version).int_err()?
            ),
            None => String::new(),
        };
        let account_arg = Self::account_selector_input(selector.account.as_ref())?
            .map(|s| format!("{s},"))
            .unwrap_or_default();

        Ok(format!(
            r#"{{
                kind: {{ custom: {kind} }},
                {maybe_api_version}
                refs: {refs},
                {account_arg}
            }}"#
        ))
    }

    fn get_resource_query(selector: &ResourceSelector) -> Result<String, GetResourceError> {
        let selector_str = Self::selector_input(
            &selector.kind,
            selector.api_version.as_deref(),
            &selector.resource_ref,
            selector.account.as_ref(),
        )?;

        Ok(format!(
            r#"
            query {{
              resources {{
                resource(selector: {selector_str}) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::RESOURCE_FIELDS,
        ))
    }

    fn get_resources_query(selector: &ResourceBatchSelector) -> Result<String, BatchResourceError> {
        let selector_str = Self::batch_selector_input(selector)?;

        Ok(format!(
            r#"
            query {{
              resources {{
                resources(selector: {selector_str}) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::BATCH_RESOURCE_FIELDS,
        ))
    }

    fn get_resource_identity_query(
        selector: &ResourceSelector,
    ) -> Result<String, GetResourceError> {
        let selector_str = Self::selector_input(
            &selector.kind,
            selector.api_version.as_deref(),
            &selector.resource_ref,
            selector.account.as_ref(),
        )?;

        Ok(format!(
            r#"
            query {{
              resources {{
                resourceIdentity(selector: {selector_str}) {{
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
        ))
    }

    fn get_resource_identities_query(
        selector: &ResourceBatchSelector,
    ) -> Result<String, BatchResourceError> {
        let selector_str = Self::batch_selector_input(selector)?;

        Ok(format!(
            r#"
            query {{
              resources {{
                resourceIdentities(selector: {selector_str}) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::BATCH_IDENTITY_FIELDS,
        ))
    }

    fn render_manifest_query(
        selector: &ResourceSelector,
        format: ResourceManifestFormat,
    ) -> Result<String, RenderResourceManifestError> {
        let selector_str = Self::selector_input(
            &selector.kind,
            selector.api_version.as_deref(),
            &selector.resource_ref,
            selector.account.as_ref(),
        )
        .map_err(RenderResourceManifestError::Internal)?;
        let format = format.to_string();

        Ok(format!(
            r#"
            query {{
              resources {{
                renderManifest(
                  selector: {selector_str}
                  format: {format}
                ) {{
                  manifest
                  format
                }}
              }}
            }}
            "#
        ))
    }

    fn render_manifests_query(
        selector: &ResourceBatchSelector,
        format: ResourceManifestFormat,
    ) -> Result<String, BatchResourceError> {
        let selector_str = Self::batch_selector_input(selector)?;
        let format = format.to_string();

        Ok(format!(
            r#"
            query {{
              resources {{
                renderManifests(
                  selector: {selector_str}
                  format: {format}
                ) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::BATCH_RENDER_MANIFEST_FIELDS,
        ))
    }

    fn delete_resource_query(selector: &ResourceSelector) -> Result<String, DeleteResourceError> {
        let selector_str = Self::selector_input(
            &selector.kind,
            selector.api_version.as_deref(),
            &selector.resource_ref,
            selector.account.as_ref(),
        )
        .map_err(DeleteResourceError::Internal)?;

        Ok(format!(
            r#"
            mutation {{
              resources {{
                delete(selector: {selector_str}) {{
                  resourceId
                }}
              }}
            }}
            "#
        ))
    }

    fn delete_resources_query(
        selector: &ResourceBatchSelector,
    ) -> Result<String, BatchResourceError> {
        let selector_str = Self::batch_selector_input(selector)?;

        Ok(format!(
            r#"
            mutation {{
              resources {{
                deleteMany(selector: {selector_str}) {{
                  {fields}
                }}
              }}
            }}
            "#,
            fields = Self::BATCH_DELETE_FIELDS,
        ))
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

    fn resource_refs_input(resource_refs: &[ResourceRef]) -> Result<String, InternalError> {
        let refs = resource_refs
            .iter()
            .map(Self::resource_ref_input)
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");

        Ok(format!("[{refs}]"))
    }

    fn map_delete_graphql_error(
        selector: &ResourceSelector,
        message: &str,
    ) -> Option<DeleteResourceError> {
        match &selector.resource_ref {
            ResourceRef::ById(uid) => {
                let not_found = domain::ResourceUIDNotFoundError(*uid);
                if message.contains(&not_found.to_string()) {
                    return Some(DeleteResourceError::LookupProblem(
                        ResourceLookupProblem::UIDNotFound(not_found),
                    ));
                }

                let mismatch_prefix = format!("Resource uid {uid} refers to kind '");
                let mismatch_suffix = format!("', expected '{}'", selector.kind);
                if let Some(actual_kind_start) = message.find(&mismatch_prefix) {
                    let actual_kind_start = actual_kind_start + mismatch_prefix.len();
                    if let Some(actual_kind_end) =
                        message[actual_kind_start..].find(&mismatch_suffix)
                    {
                        return Some(DeleteResourceError::LookupProblem(
                            ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
                                uid: *uid,
                                expected_kind: selector.kind.clone(),
                                actual_kind: message
                                    [actual_kind_start..actual_kind_start + actual_kind_end]
                                    .to_string(),
                            }),
                        ));
                    }
                }

                None
            }
            ResourceRef::ByName(name) => {
                let not_found = domain::ResourceNameNotFoundError {
                    kind: selector.kind.clone(),
                    name: name.clone(),
                };
                message.contains(&not_found.to_string()).then_some(
                    DeleteResourceError::LookupProblem(ResourceLookupProblem::NameNotFound(
                        not_found,
                    )),
                )
            }
        }
    }

    fn map_delete_remote_error(
        selector: &ResourceSelector,
        error: GraphqlHttpRequestError,
    ) -> DeleteResourceError {
        match error {
            GraphqlHttpRequestError::Graphql {
                endpoint_url,
                message,
            } => Self::map_delete_graphql_error(selector, &message).unwrap_or_else(|| {
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
        selector: &ResourceSelector,
        message: &str,
    ) -> Option<RenderResourceManifestError> {
        match &selector.resource_ref {
            ResourceRef::ById(uid) => {
                let error = domain::ResourceUIDNotFoundError(*uid);
                message.contains(&error.to_string()).then_some(
                    RenderResourceManifestError::LookupProblem(ResourceLookupProblem::UIDNotFound(
                        error,
                    )),
                )
            }
            ResourceRef::ByName(name) => {
                let error = domain::ResourceNameNotFoundError {
                    kind: selector.kind.clone(),
                    name: name.clone(),
                };
                message.contains(&error.to_string()).then_some(
                    RenderResourceManifestError::LookupProblem(
                        ResourceLookupProblem::NameNotFound(error),
                    ),
                )
            }
        }
    }

    fn map_render_manifest_remote_error(
        selector: &ResourceSelector,
        error: GraphqlHttpRequestError,
    ) -> RenderResourceManifestError {
        match error {
            GraphqlHttpRequestError::Graphql {
                endpoint_url,
                message,
            } => Self::map_render_manifest_graphql_error(selector, &message).unwrap_or_else(|| {
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

    fn not_found_error(selector: &ResourceSelector) -> GetResourceError {
        match &selector.resource_ref {
            ResourceRef::ById(uid) => GetResourceError::LookupProblem(
                ResourceLookupProblem::UIDNotFound(ResourceUIDNotFoundError(*uid)),
            ),
            ResourceRef::ByName(name) => GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(ResourceNameNotFoundError {
                    kind: selector.kind.clone(),
                    name: name.clone(),
                }),
            ),
        }
    }

    fn batch_resource_problem_error(
        problem: &fragments::BatchResourceProblemFragment,
        kind: &str,
        resource_ref: &ResourceRef,
        expected_api_version: Option<&str>,
    ) -> Result<ResourceLookupProblem, BatchResourceError> {
        use fragments::BatchResourceProblemCodeFragment as Code;
        Ok(match &problem.code {
            Code::UidNotFound => match resource_ref {
                ResourceRef::ById(uid) => {
                    ResourceLookupProblem::UIDNotFound(ResourceUIDNotFoundError(*uid))
                }
                ResourceRef::ByName(_) => return Err(Self::malformed_remote_problem(problem)),
            },
            Code::NameNotFound => match resource_ref {
                ResourceRef::ByName(name) => {
                    ResourceLookupProblem::NameNotFound(ResourceNameNotFoundError {
                        kind: kind.to_string(),
                        name: name.clone(),
                    })
                }
                ResourceRef::ById(_) => return Err(Self::malformed_remote_problem(problem)),
            },
            Code::ApiVersionMismatch => {
                let Some(expected_api_version) = expected_api_version else {
                    return Err(Self::malformed_remote_problem(problem));
                };
                let Some(actual_api_version) = Self::parse_actual_api_version(&problem.message)
                else {
                    return Err(Self::malformed_remote_problem(problem));
                };

                ResourceLookupProblem::ApiVersionMismatch(ResourceAPIVersionMismatchError {
                    expected_api_version: expected_api_version.to_string(),
                    actual_api_version,
                })
            }
            Code::KindMismatch => match resource_ref {
                ResourceRef::ById(uid) => {
                    let Some(actual_kind) = Self::parse_actual_kind(*uid, kind, &problem.message)
                    else {
                        return Err(Self::malformed_remote_problem(problem));
                    };

                    ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
                        uid: *uid,
                        expected_kind: kind.to_string(),
                        actual_kind,
                    })
                }
                ResourceRef::ByName(_) => return Err(Self::malformed_remote_problem(problem)),
            },
        })
    }

    fn malformed_remote_problem(
        problem: &fragments::BatchResourceProblemFragment,
    ) -> BatchResourceError {
        BatchResourceError::Internal(InternalError::new(format!(
            "Malformed remote resource problem: code={:?}, message={}",
            problem.code, problem.message
        )))
    }

    fn parse_actual_api_version(message: &str) -> Option<String> {
        let prefix = "Resource api version mismatch: expected '";
        let (_, actual) = message.strip_prefix(prefix)?.split_once("', actual '")?;
        let actual_end = actual.find('\'')?;
        Some(actual[..actual_end].to_string())
    }

    fn parse_actual_kind(
        uid: domain::ResourceUID,
        expected_kind: &str,
        message: &str,
    ) -> Option<String> {
        let mismatch_prefix = format!("Resource uid {uid} refers to kind '");
        let mismatch_suffix = format!("', expected '{expected_kind}'");
        let actual_kind_start = message.find(&mismatch_prefix)? + mismatch_prefix.len();
        let actual_kind_end = message[actual_kind_start..].find(&mismatch_suffix)?;
        Some(message[actual_kind_start..actual_kind_start + actual_kind_end].to_string())
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
        let summary_field = match Self::account_selector_input(request.account.as_ref())
            .map_err(ResourcesSummaryError::Internal)?
        {
            None => "summary".to_string(),
            Some(account_arg) => format!("summary({account_arg})"),
        };

        let query = format!(
            r#"
            query {{
              resources {{
                {summary_field} {{
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
            "#
        );

        let response: fragments::SummaryQueryDataFragment = self.execute_graphql(&query).await?;

        Ok(ResourcesSummary {
            resource_counts: response
                .resources
                .summary
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
        selector: ResourceSelector,
    ) -> Result<kamu_resources::ResourceView, GetResourceError> {
        let query = Self::get_resource_query(&selector)?;

        let response: fragments::GetResourceQueryDataFragment =
            self.execute_graphql(&query).await?;

        let Some(resource) = response.resources.resource else {
            return Err(Self::not_found_error(&selector));
        };

        resource.try_into().map_err(GetResourceError::Internal)
    }

    async fn get_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        BatchResourceResponse<domain::ResourceView, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse {
                successes: Vec::new(),
                problems: Vec::new(),
            });
        }

        let query = Self::get_resources_query(&selector)?;

        let response: fragments::BatchGetResourcesQueryDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.resources;

        let successes = batch_result
            .resources
            .into_iter()
            .map(|success| {
                Ok(BatchResourceSuccess {
                    request_index: success.request_index,
                    item: success
                        .resource
                        .try_into()
                        .map_err(BatchResourceError::Internal)?,
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        let problems = batch_result
            .problems
            .into_iter()
            .map(|problem| {
                let resource_ref = selector
                    .resource_refs
                    .get(problem.request_index)
                    .ok_or_else(|| {
                        BatchResourceError::Internal(InternalError::new(format!(
                            "Remote resource problem index {} is out of bounds",
                            problem.request_index
                        )))
                    })?;

                let request_index = problem.request_index;
                let error = Self::batch_resource_problem_error(
                    &problem,
                    &selector.kind,
                    resource_ref,
                    selector.api_version.as_deref(),
                )?;

                Ok(BatchResourceProblem {
                    request_index,
                    error,
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn get_identity(
        &self,
        selector: ResourceSelector,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        let query = Self::get_resource_identity_query(&selector)?;

        let response: fragments::GetResourceIdentityQueryDataFragment =
            self.execute_graphql(&query).await?;

        let Some(identity) = response.resources.resource_identity else {
            return Err(Self::not_found_error(&selector));
        };

        Ok(identity.into())
    }

    async fn get_identities(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        BatchResourceResponse<ResourceIdentityView, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse {
                successes: Vec::new(),
                problems: Vec::new(),
            });
        }

        let query = Self::get_resource_identities_query(&selector)?;

        let response: fragments::BatchGetResourceIdentitiesQueryDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.resource_identities;

        let successes = batch_result
            .identities
            .into_iter()
            .map(|success| BatchResourceSuccess {
                request_index: success.request_index,
                item: success.identity.into(),
            })
            .collect();

        let problems = batch_result
            .problems
            .into_iter()
            .map(|problem| {
                let resource_ref = selector
                    .resource_refs
                    .get(problem.request_index)
                    .ok_or_else(|| {
                        BatchResourceError::Internal(InternalError::new(format!(
                            "Remote resource identity problem index {} is out of bounds",
                            problem.request_index
                        )))
                    })?;

                let request_index = problem.request_index;
                let error = Self::batch_resource_problem_error(
                    &problem,
                    &selector.kind,
                    resource_ref,
                    selector.api_version.as_deref(),
                )?;

                Ok(BatchResourceProblem {
                    request_index,
                    error,
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn render_manifest(
        &self,
        selector: ResourceSelector,
        format: ResourceManifestFormat,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let query = Self::render_manifest_query(&selector, format)?;

        let response: fragments::RenderManifestQueryDataFragment = self
            .execute_graphql(&query)
            .await
            .map_err(|error| Self::map_render_manifest_remote_error(&selector, error))?;

        let rendered = response.resources.render_manifest;

        Ok(RenderResourceManifestResult {
            manifest: rendered.manifest,
            format: rendered.format.into(),
        })
    }

    async fn render_manifests(
        &self,
        selector: ResourceBatchSelector,
        format: ResourceManifestFormat,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse {
                successes: Vec::new(),
                problems: Vec::new(),
            });
        }

        let query = Self::render_manifests_query(&selector, format)?;

        let response: fragments::BatchRenderManifestsQueryDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.render_manifests;

        let successes = batch_result
            .manifests
            .into_iter()
            .map(|success| BatchResourceSuccess {
                request_index: success.request_index,
                item: RenderResourceManifestResult {
                    manifest: success.manifest.manifest,
                    format: success.manifest.format.into(),
                },
            })
            .collect();

        let problems = batch_result
            .problems
            .into_iter()
            .map(|problem| {
                let resource_ref = selector
                    .resource_refs
                    .get(problem.request_index)
                    .ok_or_else(|| {
                        BatchResourceError::Internal(InternalError::new(format!(
                            "Remote resource manifest problem index {} is out of bounds",
                            problem.request_index
                        )))
                    })?;

                let request_index = problem.request_index;
                let error = Self::batch_resource_problem_error(
                    &problem,
                    &selector.kind,
                    resource_ref,
                    selector.api_version.as_deref(),
                )?;

                Ok(BatchResourceProblem {
                    request_index,
                    error,
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListResourcesError> {
        self.list_resource_summaries::<ListResourcesError>(
            RemoteListQuery::ByKind(&request.kind),
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn list_identities(
        &self,
        request: ListResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListResourcesError> {
        self.list_resource_identities::<ListResourcesError>(
            RemoteListQuery::ByKind(&request.kind),
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn search_identities(
        &self,
        request: SearchResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListResourcesError> {
        self.search_resource_identities::<ListResourcesError>(&request)
            .await
    }

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListAllResourcesError> {
        self.list_resource_summaries::<ListAllResourcesError>(
            RemoteListQuery::All,
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn list_all_identities(
        &self,
        request: ListAllResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListAllResourcesError> {
        self.list_resource_identities::<ListAllResourcesError>(
            RemoteListQuery::All,
            request.account.as_ref(),
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
        selector: ResourceSelector,
    ) -> Result<kamu_resources::ResourceUID, DeleteResourceError> {
        let query = Self::delete_resource_query(&selector)?;

        let response: fragments::DeleteMutationDataFragment = self
            .execute_graphql(&query)
            .await
            .map_err(|error| Self::map_delete_remote_error(&selector, error))?;

        Ok(response.resources.delete.resource_id)
    }

    async fn delete_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<domain::ResourceUID, ResourceLookupProblem>, BatchResourceError>
    {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse {
                successes: Vec::new(),
                problems: Vec::new(),
            });
        }

        let query = Self::delete_resources_query(&selector)?;

        let response: fragments::DeleteManyMutationDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.delete_many;

        let successes = batch_result
            .resources
            .into_iter()
            .map(|success| BatchResourceSuccess {
                request_index: success.request_index,
                item: success.resource_id,
            })
            .collect();

        let problems = batch_result
            .problems
            .into_iter()
            .map(|problem| {
                let resource_ref = selector
                    .resource_refs
                    .get(problem.request_index)
                    .ok_or_else(|| {
                        BatchResourceError::Internal(InternalError::new(format!(
                            "Remote resource delete problem index {} is out of bounds",
                            problem.request_index
                        )))
                    })?;

                let request_index = problem.request_index;
                let error = Self::batch_resource_problem_error(
                    &problem,
                    &selector.kind,
                    resource_ref,
                    selector.api_version.as_deref(),
                )?;

                Ok(BatchResourceProblem {
                    request_index,
                    error,
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum RemoteListQuery<'a> {
    ByKind(&'a str),
    All,
}
