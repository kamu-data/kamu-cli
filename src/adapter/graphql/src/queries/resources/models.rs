// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;

use crate::prelude::*;
use crate::scalars::{
    AccountID,
    AccountName,
    AccountRef,
    ResourceAnnotations,
    ResourceLabels,
    ResourcePhase,
    UInt64,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Type aliases for cleaner From implementations

type BatchGetResourcesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::ResourceView,
    kamu_resources_facade::ResourceLookupProblem,
>;

type BatchGetResourceIdentitiesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::ResourceIdentityView,
    kamu_resources_facade::ResourceLookupProblem,
>;

type BatchRenderResourceManifestsResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources_facade::RenderResourceManifestResult,
    kamu_resources_facade::ResourceLookupProblem,
>;

type BatchGetResourceProblem =
    kamu_resources_facade::BatchResourceProblem<kamu_resources_facade::ResourceLookupProblem>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ResourceTypeSelectorInput {
    pub selector: ResourceTypeSelectorRaw<'static>,
}

impl ResourceTypeSelectorInput {
    pub fn into_resource_type_selector(self) -> kamu_resources::ResourceTypeSelectorRaw {
        self.selector.into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ResourceAccountByIdAndNameInput {
    pub id: AccountID<'static>,
    pub name: AccountName<'static>,
}

#[derive(OneofObject, Debug, Clone)]
pub enum ResourceAccountSelectorInput {
    ById(AccountID<'static>),
    ByName(AccountName<'static>),
    Both(ResourceAccountByIdAndNameInput),
}

impl ResourceAccountSelectorInput {
    pub fn into_manifest_account(self) -> kamu_resources::ResourceAccountRef {
        match self {
            Self::ById(id) => kamu_resources::ResourceAccountRef::Id(id.into()),
            Self::ByName(name) => kamu_resources::ResourceAccountRef::Name(name.into()),
            Self::Both(ResourceAccountByIdAndNameInput { id, name }) => {
                kamu_resources::ResourceAccountRef::Both {
                    id: id.into(),
                    name: name.into(),
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ResourceSelectorInput {
    pub resource_type: ResourceTypeSelectorInput,
    #[graphql(name = "ref")]
    pub resource_ref: ResourceRefInput,
    pub account: Option<ResourceAccountSelectorInput>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ResourceBatchSelectorInput {
    pub resource_type: ResourceTypeSelectorInput,
    #[graphql(name = "refs")]
    pub resource_refs: Vec<ResourceRefInput>,
    pub account: Option<ResourceAccountSelectorInput>,
}

impl From<ResourceSelectorInput> for kamu_resources_facade::ResourceSelector {
    fn from(value: ResourceSelectorInput) -> Self {
        Self {
            account: value
                .account
                .map(ResourceAccountSelectorInput::into_manifest_account),
            resource_type: value.resource_type.into_resource_type_selector(),
            resource_ref: value.resource_ref.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<ResourceBatchSelectorInput> for kamu_resources_facade::ResourceBatchSelector {
    fn from(value: ResourceBatchSelectorInput) -> Self {
        Self {
            account: value
                .account
                .map(ResourceAccountSelectorInput::into_manifest_account),
            resource_type: value.resource_type.into_resource_type_selector(),
            resource_refs: value.resource_refs.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct SearchResourceIdentitiesInput {
    pub resource_types: Vec<ResourceTypeSelectorInput>,
    pub names: Option<Vec<ResourceName<'static>>>,
    pub name_pattern: Option<String>,
    pub account: Option<ResourceAccountSelectorInput>,
}

impl SearchResourceIdentitiesInput {
    pub fn into_facade_request(
        self,
        pagination: PaginationOpts,
    ) -> kamu_resources_facade::SearchResourceIdentitiesRequest {
        kamu_resources_facade::SearchResourceIdentitiesRequest {
            raw_type_selectors: self
                .resource_types
                .into_iter()
                .map(ResourceTypeSelectorInput::into_resource_type_selector)
                .collect(),
            exact_names: self
                .names
                .map(|names| names.into_iter().map(Into::into).collect()),
            name_pattern: self.name_pattern,
            account: self
                .account
                .map(ResourceAccountSelectorInput::into_manifest_account),
            pagination,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Debug, Clone)]
pub enum ResourceRefInput {
    ById(ResourceID<'static>),
    ByName(ResourceByNameSelectorInput),
}

impl From<ResourceRefInput> for kamu_resources_facade::ResourceRef {
    fn from(value: ResourceRefInput) -> Self {
        match value {
            ResourceRefInput::ById(id) => Self::ById(id.into()),
            ResourceRefInput::ByName(by_name) => Self::ByName(by_name.name.clone().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ResourceByNameSelectorInput {
    pub name: ResourceName<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ResourceTypeDescriptor {
    pub canonical_selector: ResourceSelectorName<'static>,
    pub selector_aliases: Vec<ResourceSelectorName<'static>>,
    pub schema: TypeUri<'static>,
    pub list_columns: Vec<ResourceListColumnDescriptor>,
}

impl From<kamu_resources::ResourceTypeDescriptor> for ResourceTypeDescriptor {
    fn from(value: kamu_resources::ResourceTypeDescriptor) -> Self {
        Self {
            canonical_selector: value.canonical_selector.into(),
            selector_aliases: value.selector_aliases.into_iter().map(Into::into).collect(),
            schema: value.schema.into(),
            list_columns: value.list_columns.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resource type was addressed by its canonical schema URI, but no descriptor
/// is registered for it. This is reachable *only* on the apply-manifest path,
/// where the `$schema` URI comes straight from the user's manifest; selector
/// lookups surface [`ResourceUnsupportedSelectorProblem`] instead, so this
/// field never carries a short selector masquerading as a URI.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceUnsupportedDescriptorProblem {
    pub schema: TypeUri<'static>,
    pub message: String,
}

/// A resource type was addressed by a short selector (main name or alias, e.g.
/// `vs`), but no descriptor matches it. The `selector` is the raw user-supplied
/// string — never a schema URI (the public API selects resource types by
/// selector).
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceUnsupportedSelectorProblem {
    pub selector: ResourceTypeSelectorRaw<'static>,
    pub message: String,
}

/// Maps an [`UnsupportedResourceDescriptorError`] (apply/URI path) to the
/// apply-path problem.
pub(crate) fn map_unsupported_descriptor_problem(
    err: kamu_resources::UnsupportedResourceDescriptorError,
) -> ResourceUnsupportedDescriptorProblem {
    use kamu_resources::UnsupportedResourceDescriptorError as E;

    let message = err.to_string();
    match err {
        E::NotFound { schema } => ResourceUnsupportedDescriptorProblem {
            schema: schema.into(),
            message,
        },
    }
}

/// Maps an [`UnsupportedResourceSelectorError`] (selector path) to the
/// selector-path problem.
pub(crate) fn map_unsupported_selector_problem(
    err: kamu_resources::UnsupportedResourceSelectorError,
) -> ResourceUnsupportedSelectorProblem {
    use kamu_resources::UnsupportedResourceSelectorError as E;

    let message = err.to_string();
    match err {
        E::NotFound {
            raw_selector: selector,
        } => ResourceUnsupportedSelectorProblem {
            selector: selector.into(),
            message,
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceBadAccountProblem {
    pub code: ResourceBadAccountProblemCode,
    pub account_id: Option<AccountID<'static>>,
    pub account_name: Option<AccountName<'static>>,
    pub expected_name: Option<AccountName<'static>>,
    pub actual_name: Option<AccountName<'static>>,
    pub message: String,
}

impl From<kamu_resources_facade::ResolveManifestAccountError> for ResourceBadAccountProblem {
    fn from(value: kamu_resources_facade::ResolveManifestAccountError) -> Self {
        use kamu_resources_facade::ResolveManifestAccountError as E;

        let message = value.to_string();
        match value {
            E::AccountNotFoundById(error) => Self {
                code: ResourceBadAccountProblemCode::AccountNotFoundById,
                account_id: Some(error.account_id.into()),
                account_name: None,
                expected_name: None,
                actual_name: None,
                message,
            },
            E::AccountNotFoundByName(error) => Self {
                code: ResourceBadAccountProblemCode::AccountNotFoundByName,
                account_id: None,
                account_name: Some(error.account_name.into()),
                expected_name: None,
                actual_name: None,
                message,
            },
            E::IdNameMismatch {
                account_id,
                expected_name,
                actual_name,
            } => Self {
                code: ResourceBadAccountProblemCode::IdNameMismatch,
                account_id: Some(account_id.into()),
                account_name: None,
                expected_name: Some(expected_name.into()),
                actual_name: Some(actual_name.into()),
                message,
            },
            // These are non-user-facing failures: map_bad_account_problem promotes them to
            // GqlError before this From impl is ever called.
            E::AnonymousSubject | E::Access(_) | E::Internal(_) => {
                unreachable!("non-user account error must not reach ResourceBadAccountProblem")
            }
        }
    }
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceBadAccountProblemCode {
    AccountNotFoundById,
    AccountNotFoundByName,
    IdNameMismatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceIDNotFoundProblem {
    pub id: ResourceID<'static>,
    pub message: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceNameNotFoundProblem {
    pub canonical_selector: ResourceSelectorName<'static>,
    pub name: ResourceName<'static>,
    pub message: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceSchemaMismatchProblem {
    pub id: ResourceID<'static>,
    pub expected_schema: TypeUri<'static>,
    pub actual_schema: TypeUri<'static>,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceLookupProblem {
    UidNotFound(ResourceIDNotFoundProblem),
    NameNotFound(ResourceNameNotFoundProblem),
    SchemaMismatch(ResourceSchemaMismatchProblem),
}

impl From<kamu_resources_facade::ResourceLookupProblem> for ResourceLookupProblem {
    fn from(value: kamu_resources_facade::ResourceLookupProblem) -> Self {
        use kamu_resources_facade::ResourceLookupProblem as P;
        match value {
            P::IDNotFound(e) => Self::UidNotFound(ResourceIDNotFoundProblem {
                id: e.0.into(),
                message: e.to_string(),
            }),
            P::NameNotFound(e) => Self::NameNotFound(ResourceNameNotFoundProblem {
                canonical_selector: e.canonical_selector.clone().into(),
                name: e.name.clone().into(),
                message: e.to_string(),
            }),
            P::SchemaMismatch(e) => {
                let message = e.to_string();
                Self::SchemaMismatch(ResourceSchemaMismatchProblem {
                    id: e.id.into(),
                    expected_schema: e.expected_schema.into(),
                    actual_schema: e.actual_schema.into(),
                    message,
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceSelectorProblem {
    UidNotFound(ResourceIDNotFoundProblem),
    NameNotFound(ResourceNameNotFoundProblem),
    SchemaMismatch(ResourceSchemaMismatchProblem),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    BadAccount(ResourceBadAccountProblem),
}

impl From<kamu_resources_facade::ResourceLookupProblem> for ResourceSelectorProblem {
    fn from(value: kamu_resources_facade::ResourceLookupProblem) -> Self {
        match ResourceLookupProblem::from(value) {
            ResourceLookupProblem::UidNotFound(p) => Self::UidNotFound(p),
            ResourceLookupProblem::NameNotFound(p) => Self::NameNotFound(p),
            ResourceLookupProblem::SchemaMismatch(p) => Self::SchemaMismatch(p),
        }
    }
}

impl TryFrom<kamu_resources::UnsupportedResourceSelectorError> for ResourceSelectorProblem {
    type Error = GqlError;

    fn try_from(e: kamu_resources::UnsupportedResourceSelectorError) -> Result<Self, GqlError> {
        Ok(Self::UnsupportedSelector(map_unsupported_selector_problem(
            e,
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceSelectorProblemResult {
    pub problem: ResourceSelectorProblem,
}

impl From<kamu_resources_facade::ResourceLookupProblem> for ResourceSelectorProblemResult {
    fn from(value: kamu_resources_facade::ResourceLookupProblem) -> Self {
        Self {
            problem: value.into(),
        }
    }
}

impl TryFrom<kamu_resources::UnsupportedResourceSelectorError> for ResourceSelectorProblemResult {
    type Error = GqlError;

    fn try_from(e: kamu_resources::UnsupportedResourceSelectorError) -> Result<Self, GqlError> {
        Ok(Self {
            problem: e.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceInvalidSearchQueryProblem {
    pub message: String,
}

impl From<kamu_resources_facade::InvalidResourceSearchQueryError>
    for ResourceInvalidSearchQueryProblem
{
    fn from(value: kamu_resources_facade::InvalidResourceSearchQueryError) -> Self {
        Self {
            message: value.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "kamu_resources_facade::ResourceManifestFormat")]
pub enum ResourceManifestFormat {
    Json,
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceRenderManifestResult {
    pub manifest: String,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct Resource {
    pub schema: TypeUri<'static>,
    pub headers: ResourceHeaders,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
}

impl From<kamu_resources::ResourceView> for Resource {
    fn from(value: kamu_resources::ResourceView) -> Self {
        let headers = ResourceHeaders::from(value.clone());

        Self {
            schema: value.schema.into(),
            headers,
            spec: value.spec,
            status: value.status,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum BatchResourcesOutcome {
    Success(BatchResourcesResult),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    BadAccount(ResourceBadAccountProblem),
}

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourcesResult {
    pub resources: Vec<BatchResourceSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

impl From<BatchGetResourcesResponse> for BatchResourcesResult {
    fn from(value: BatchGetResourcesResponse) -> Self {
        Self {
            resources: value
                .successes
                .into_iter()
                .map(|success| BatchResourceSuccess {
                    request_index: success.request_index,
                    resource: success.item.into(),
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceSuccess {
    pub request_index: usize,
    pub resource: Resource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum BatchResourceManifestsOutcome {
    Success(BatchResourceManifestsResult),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    BadAccount(ResourceBadAccountProblem),
}

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceManifestsResult {
    pub manifests: Vec<BatchResourceManifestSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

impl From<BatchRenderResourceManifestsResponse> for BatchResourceManifestsResult {
    fn from(value: BatchRenderResourceManifestsResponse) -> Self {
        Self {
            manifests: value
                .successes
                .into_iter()
                .map(|success| BatchResourceManifestSuccess {
                    request_index: success.request_index,
                    manifest: ResourceRenderManifestResult {
                        manifest: success.item.manifest,
                        format: success.item.format.into(),
                    },
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceManifestSuccess {
    pub request_index: usize,
    pub manifest: ResourceRenderManifestResult,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceIdentity {
    pub id: ResourceID<'static>,
    pub schema: TypeUri<'static>,
    pub canonical_selector: ResourceSelectorName<'static>,
    pub name: ResourceName<'static>,
}

impl From<kamu_resources::ResourceIdentityView> for ResourceIdentity {
    fn from(value: kamu_resources::ResourceIdentityView) -> Self {
        Self {
            id: value.id.into(),
            schema: value.schema.into(),
            canonical_selector: value.canonical_selector.into(),
            name: value.name.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum BatchResourceIdentitiesOutcome {
    Success(BatchResourceIdentitiesResult),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    BadAccount(ResourceBadAccountProblem),
}

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceIdentitiesResult {
    pub identities: Vec<BatchResourceIdentitySuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

impl From<BatchGetResourceIdentitiesResponse> for BatchResourceIdentitiesResult {
    fn from(value: BatchGetResourceIdentitiesResponse) -> Self {
        Self {
            identities: value
                .successes
                .into_iter()
                .map(|success| BatchResourceIdentitySuccess {
                    request_index: success.request_index,
                    identity: success.item.into(),
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceIdentitySuccess {
    pub request_index: usize,
    pub identity: ResourceIdentity,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceProblem {
    pub request_index: usize,
    pub problem: ResourceLookupProblem,
}

impl From<BatchGetResourceProblem> for BatchResourceProblem {
    fn from(value: BatchGetResourceProblem) -> Self {
        Self {
            request_index: value.request_index,
            problem: value.error.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceHeaders {
    pub id: ResourceID<'static>,
    pub account: AccountRef,
    pub name: ResourceName<'static>,
    pub description: Option<String>,
    pub labels: ResourceLabels,
    pub annotations: ResourceAnnotations,
    pub generation: UInt64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

impl From<kamu_resources::ResourceView> for ResourceHeaders {
    fn from(value: kamu_resources::ResourceView) -> Self {
        let account = match value.headers.account.name {
            Some(name) => kamu_resources::ResourceAccountRef::Both {
                id: value.headers.account.id,
                name,
            },
            None => kamu_resources::ResourceAccountRef::Id(value.headers.account.id),
        };

        Self {
            id: value.headers.id.into(),
            account: account.into(),
            name: value.headers.name.clone().into(),
            description: value.headers.description,
            labels: value.headers.labels.into(),
            annotations: value.headers.annotations.into(),
            generation: value.headers.generation.into(),
            created_at: value.headers.created_at,
            updated_at: value.headers.updated_at,
            deleted_at: value.headers.deleted_at,
            last_reconciled_at: value.last_reconciled_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceSummary {
    pub id: ResourceID<'static>,
    pub schema: TypeUri<'static>,
    pub name: ResourceName<'static>,
    pub description: Option<String>,
    pub generation: UInt64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: Option<ResourceStatusSummary>,
    pub list_values: Vec<ResourceListColumnValueView>,
}

impl From<kamu_resources::ResourceSummaryView> for ResourceSummary {
    fn from(value: kamu_resources::ResourceSummaryView) -> Self {
        Self {
            id: value.id.into(),
            schema: value.schema.into(),
            name: value.name.clone().into(),
            description: value.description,
            generation: value.generation.into(),
            created_at: value.created_at,
            updated_at: value.updated_at,
            status: value.status.map(Into::into),
            list_values: value.list_values.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "kamu_resources::ResourceListColumnDataType")]
pub enum ResourceListColumnDataType {
    String,
    UInt64,
    Bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "kamu_resources::ResourceListColumnVisibility")]
pub enum ResourceListColumnVisibility {
    Default,
    WideOnly,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ResourceListColumnDescriptor {
    pub key: String,
    pub header: String,
    pub data_type: ResourceListColumnDataType,
    pub visibility: ResourceListColumnVisibility,
}

impl From<kamu_resources::ResourceListColumnDescriptor> for ResourceListColumnDescriptor {
    fn from(value: kamu_resources::ResourceListColumnDescriptor) -> Self {
        Self {
            key: value.key,
            header: value.header,
            data_type: value.data_type.into(),
            visibility: value.visibility.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ResourceListColumnValueView {
    pub key: String,
    pub string_value: Option<String>,
    pub uint64_value: Option<UInt64>,
    pub bool_value: Option<bool>,
}

impl From<kamu_resources::ResourceListColumnValueView> for ResourceListColumnValueView {
    fn from(value: kamu_resources::ResourceListColumnValueView) -> Self {
        let (string_value, uint64_value, bool_value) = match value.value {
            kamu_resources::ResourceListColumnValue::String(value) => (Some(value), None, None),
            kamu_resources::ResourceListColumnValue::UInt64(value) => (None, Some(value), None),
            kamu_resources::ResourceListColumnValue::Bool(value) => (None, None, Some(value)),
        };

        Self {
            key: value.key,
            string_value,
            uint64_value: uint64_value.map(Into::into),
            bool_value,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceStatusSummary {
    pub phase: Option<ResourcePhase>,
    pub observed_generation: Option<UInt64>,
    pub ready: Option<bool>,
}

impl From<kamu_resources::ResourceStatusSummaryView> for ResourceStatusSummary {
    fn from(value: kamu_resources::ResourceStatusSummaryView) -> Self {
        Self {
            phase: value.phase.map(Into::into),
            observed_generation: value.observed_generation.map(Into::into),
            ready: value.ready,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourcesSummary {
    pub resource_counts: Vec<ResourceTypeCountSummary>,
}

impl From<kamu_resources::ResourcesSummary> for ResourcesSummary {
    fn from(value: kamu_resources::ResourcesSummary) -> Self {
        Self {
            resource_counts: value.resource_counts.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceTypeCountSummary {
    pub schema: TypeUri<'static>,
    pub canonical_selector: ResourceSelectorName<'static>,
    pub total_count: UInt64,
    pub phase_counts: ResourcePhaseCounts,
}

impl From<kamu_resources::ResourceTypeCountSummary> for ResourceTypeCountSummary {
    fn from(value: kamu_resources::ResourceTypeCountSummary) -> Self {
        Self {
            schema: value.schema.into(),
            canonical_selector: value.canonical_selector.into(),
            total_count: value.total_count.into(),
            phase_counts: value.phase_counts.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourcePhaseCounts {
    pub pending: UInt64,
    pub reconciling: UInt64,
    pub ready: UInt64,
    pub failed: UInt64,
}

impl From<kamu_resources::ResourcePhaseCounts> for ResourcePhaseCounts {
    fn from(value: kamu_resources::ResourcePhaseCounts) -> Self {
        Self {
            pending: value.pending.into(),
            reconciling: value.reconciling.into(),
            ready: value.ready.into(),
            failed: value.failed.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(ResourceSummary, ResourceConnection, ResourceEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    ResourceIdentity,
    ResourceIdentityConnection,
    ResourceIdentityEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
