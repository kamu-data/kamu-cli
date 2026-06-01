// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources as domain;

use crate::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    DeleteResourceError,
    ResourceBatchSelector,
    ResourceRef,
    ResourceSelector,
    SearchResourceIdentitiesRequest,
};

pub(super) const LIST_PAGE_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn summary_query(
    account: Option<&domain::ResourceManifestAccount>,
) -> Result<String, InternalError> {
    let summary_field = match account_selector_input(account)? {
        None => "summary".to_string(),
        Some(account_arg) => format!("summary({account_arg})"),
    };

    Ok(format!(
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
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Field selections
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) const LIST_FIELDS: &str = r#"
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

pub(super) const BATCH_DELETE_FIELDS: &str = r#"
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

pub(super) const IDENTITY_LIST_FIELDS: &str = r#"
    totalCount
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Input builders
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn account_selector_input(
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn selector_input(
    kind: &str,
    api_version: Option<&str>,
    resource_ref: &ResourceRef,
    account: Option<&domain::ResourceManifestAccount>,
) -> Result<String, InternalError> {
    let kind = serde_json::to_string(kind).int_err()?;
    let selector_ref = resource_ref_input(resource_ref)?;
    let maybe_api_version = match api_version {
        Some(api_version) => format!(
            "apiVersion: {},",
            serde_json::to_string(api_version).int_err()?
        ),
        None => String::new(),
    };
    let account_arg = account_selector_input(account)?
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn batch_selector_input(
    selector: &ResourceBatchSelector,
) -> Result<String, InternalError> {
    let kind = serde_json::to_string(&selector.kind).int_err()?;
    let refs = resource_refs_input(&selector.resource_refs)?;
    let maybe_api_version = match selector.api_version.as_deref() {
        Some(api_version) => format!(
            "apiVersion: {},",
            serde_json::to_string(api_version).int_err()?
        ),
        None => String::new(),
    };
    let account_arg = account_selector_input(selector.account.as_ref())?
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn resource_ref_input(resource_ref: &ResourceRef) -> Result<String, InternalError> {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn resource_refs_input(resource_refs: &[ResourceRef]) -> Result<String, InternalError> {
    let refs = resource_refs
        .iter()
        .map(resource_ref_input)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");

    Ok(format!("[{refs}]"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn graphql_page_params(offset: usize, limit: usize) -> (usize, usize) {
    let page = offset.checked_div(limit).unwrap_or(0);
    let per_page = if limit == 0 { LIST_PAGE_SIZE } else { limit };
    (page, per_page)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn parse_enum<T>(value: &str, field_name: &str) -> Result<T, InternalError>
where
    T: std::str::FromStr,
{
    value.parse().map_err(|_| {
        InternalError::new(format!(
            "Unsupported {field_name} '{value}' in remote resource list",
        ))
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Query / mutation builders
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn apply_manifest_query(
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn list_resources_query(
    kind: &str,
    page: usize,
    per_page: usize,
    account: Option<&domain::ResourceManifestAccount>,
) -> Result<String, InternalError> {
    let kind = serde_json::to_string(kind).int_err()?;
    let account_arg = account_selector_input(account)?
        .map(|s| format!("{s}, "))
        .unwrap_or_default();

    Ok(format!(
        r#"
        query {{
          resources {{
            listByKind(kind: {{ custom: {kind} }}, {account_arg}page: {page}, perPage: {per_page}) {{
              {LIST_FIELDS}
            }}
          }}
        }}
        "#
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn list_all_resources_query(
    page: usize,
    per_page: usize,
    account: Option<&domain::ResourceManifestAccount>,
) -> Result<String, InternalError> {
    let account_arg = account_selector_input(account)?
        .map(|s| format!("{s}, "))
        .unwrap_or_default();

    Ok(format!(
        r#"
        query {{
          resources {{
            listAll({account_arg}page: {page}, perPage: {per_page}) {{
              {LIST_FIELDS}
            }}
          }}
        }}
        "#
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn list_resource_identities_query(
    kind: &str,
    page: usize,
    per_page: usize,
    account: Option<&domain::ResourceManifestAccount>,
) -> Result<String, InternalError> {
    let kind = serde_json::to_string(kind).int_err()?;
    let account_arg = account_selector_input(account)?
        .map(|s| format!("{s}, "))
        .unwrap_or_default();

    Ok(format!(
        r#"
        query {{
          resources {{
            listIdentitiesByKind(kind: {{ custom: {kind} }}, {account_arg}page: {page}, perPage: {per_page}) {{
              {IDENTITY_LIST_FIELDS}
            }}
          }}
        }}
        "#
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn list_all_resource_identities_query(
    page: usize,
    per_page: usize,
    account: Option<&domain::ResourceManifestAccount>,
) -> Result<String, InternalError> {
    let account_arg = account_selector_input(account)?
        .map(|s| format!("{s}, "))
        .unwrap_or_default();

    Ok(format!(
        r#"
        query {{
          resources {{
            listAllIdentities({account_arg}page: {page}, perPage: {per_page}) {{
              {IDENTITY_LIST_FIELDS}
            }}
          }}
        }}
        "#
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn search_resource_identities_query(
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
    let account = account_selector_input(request.account.as_ref())?
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
              {IDENTITY_LIST_FIELDS}
            }}
          }}
        }}
        "#
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn delete_resource_query(
    selector: &ResourceSelector,
) -> Result<String, DeleteResourceError> {
    let selector_str = selector_input(
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn delete_resources_query(
    selector: &ResourceBatchSelector,
) -> Result<String, BatchResourceError> {
    let selector_str = batch_selector_input(selector)?;

    Ok(format!(
        r#"
        mutation {{
          resources {{
            deleteMany(selector: {selector_str}) {{
              {BATCH_DELETE_FIELDS}
            }}
          }}
        }}
        "#
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
