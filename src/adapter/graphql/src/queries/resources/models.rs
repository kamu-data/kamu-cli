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
use crate::scalars::{AccountID, AccountName, Did, ResourcePhase, TypeName, UInt64};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Type aliases for cleaner From implementations

type BatchGetResourcesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::Resource,
    kamu_resources_facade::ResourceLookupProblem,
>;

type BatchGetResourceHandlesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::ResourceHandle,
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

    /// Reinterprets the authored selector as an ODF type reference.
    ///
    /// Lossless in practice: descriptors resolve canonical selectors, aliases,
    /// ODF type names, and schema URIs alike, and `TypeRef::from_str` routes
    /// `https:`-prefixed input to `Uri` and everything else to `Name`. Both
    /// arms are matched by the same descriptor lookup.
    pub fn into_type_ref(self) -> kamu_resources::TypeRef {
        let selector: kamu_resources::ResourceTypeSelectorRaw = self.selector.into();
        selector
            .as_str()
            .parse()
            .unwrap_or_else(|_| kamu_resources::TypeName::new_unchecked(selector.as_str()).into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One authored `key = value` label predicate.
#[derive(InputObject, Debug, Clone)]
pub struct ResourceLabelFilterEntryInput {
    pub key: String,
    pub value: serde_json::Value,
}

/// Selects resources by label predicates.
/// A list preserves duplicate keys for validation.
#[derive(InputObject, Debug, Clone)]
pub struct ResourceLabelFilterInput {
    pub entries: Vec<ResourceLabelFilterEntryInput>,
}

impl ResourceLabelFilterInput {
    /// Rejects duplicate keys before entries collapse into a map.
    pub fn into_facade_filter(self) -> Result<kamu_resources::ResourceLabelFilterInput, GqlError> {
        let mut entries = std::collections::BTreeMap::new();

        for entry in self.entries {
            if entries.contains_key(&entry.key) {
                return Err(GqlError::gql(format!(
                    "label filter key '{}' is specified more than once",
                    entry.key
                )));
            }
            entries.insert(entry.key, entry.value);
        }

        Ok(kamu_resources::ResourceLabelFilterInput { entries })
    }
}

#[derive(InputObject, Debug, Clone, Default)]
pub struct AccountRefInput {
    pub id: Option<ResourceID<'static>>,
    pub did: Option<AccountID<'static>>,
    pub name: Option<AccountName<'static>>,
}

impl AccountRefInput {
    pub fn into_manifest_account(self) -> kamu_resources::ResourceAccountRef {
        kamu_resources::ResourceAccountRef {
            id: self.id.map(Into::into),
            did: self.did.map(Into::into),
            name: self.name.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Options controlling how a resource's spec is rendered. A struct rather than
/// a bare `revealed: bool` argument, so future spec-view options can be added
/// here without growing the resolver's argument list.
#[derive(InputObject, Debug, Clone, Copy, Default)]
pub struct SpecViewOptsInput {
    /// Show actual (decrypted) secret values instead of ciphertext
    /// placeholders.
    #[graphql(default)]
    pub revealed: bool,
}

impl SpecViewOptsInput {
    pub fn into_spec_view_opts(self) -> kamu_resources_facade::SpecViewOpts {
        kamu_resources_facade::SpecViewOpts {
            revealed: self.revealed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Converts a batch of authored refs, validating each.
///
/// Every ref carries its own type and account, so one call may span both.
pub(crate) fn into_resource_refs(
    resource_refs: Vec<ResourceRefInput>,
) -> Result<Vec<kamu_resources::ResourceRef>, GqlError> {
    resource_refs.into_iter().map(TryInto::try_into).collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Matches zero or many resources, mirroring the ODF `ResourceSelector`.
///
/// Several selectors in one call act as a logical OR, which is what lets a
/// single call span resource types. Every field is optional: a selector with no
/// `type` spans every type, and one narrowing by nothing matches all of them.
#[derive(InputObject, Debug, Clone)]
pub struct ResourceSelectorInput {
    /// The account this selector spans. Defaults to the call-level `account`
    /// when unset, so one call can span several accounts — subject to the
    /// caller being authorized for each; any denial fails the whole call.
    pub account: Option<AccountRefInput>,
    /// Canonical selector (`variablesets`), alias (`vs`), ODF type name
    /// (`VariableSet`), or full schema URI. `null` spans every type.
    pub r#type: Option<ResourceTypeSelectorInput>,
    pub id: Option<ResourceID<'static>>,
    /// Name pattern in SQL `LIKE` format, per the ODF schema. Only `%` acts as
    /// a wildcard — `_` is escaped to a literal underscore. A pattern with no
    /// `%` matches that name exactly.
    pub name: Option<String>,
    /// Labels this selector requires, all of which must be present. Applies to
    /// this selector alone, so one call may filter differently per type.
    /// Values must be strings — only string-valued labels are indexed.
    pub labels: Option<ResourceLabelFilterInput>,
}

impl TryFrom<ResourceSelectorInput> for kamu_resources::ResourceSelector {
    type Error = GqlError;

    fn try_from(value: ResourceSelectorInput) -> Result<Self, GqlError> {
        let selector = Self {
            account: value.account.map(AccountRefInput::into_manifest_account),
            r#type: value.r#type.map(ResourceTypeSelectorInput::into_type_ref),
            id: value.id.map(Into::into),
            // Reserved in ODF for when datasets and accounts become resources;
            // the facade rejects a populated `did`, so it is not accepted here.
            did: None,
            name: value.name,
            labels: value
                .labels
                .map(ResourceLabelFilterInput::into_facade_filter)
                .transpose()?,
        };

        kamu_resources_facade::validate_selector(&selector)
            .map_err(|e| GqlError::gql(e.to_string()))?;

        Ok(selector)
    }
}

pub(crate) fn into_resource_selectors(
    selectors: Vec<ResourceSelectorInput>,
) -> Result<Vec<kamu_resources::ResourceSelector>, GqlError> {
    selectors.into_iter().map(TryInto::try_into).collect()
}

/// The all-types, unnarrowed selector — what a listing means with no selectors
/// given.
pub(crate) fn any_resource_selector() -> kamu_resources::ResourceSelector {
    kamu_resources::ResourceSelector::default()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Selects resources to search, for both `search` and `searchHandles` — only
/// the response shape differs.
///
/// Omitting `selectors` spans every type, which is what the retired `listAll`
/// field did implicitly. Passing an empty list matches nothing — an explicit
/// "no selectors" is a narrowing to zero, not a widening to everything.
///
/// Label filtering lives on each selector's `labels`, so one call may filter
/// differently per type. There is deliberately no call-level `labelFilter`: one
/// uniform filter is the special case where every selector carries the same
/// labels, and selectors being OR'd makes that exactly equivalent.
#[derive(InputObject, Debug, Clone)]
pub struct SearchResourcesInput {
    pub selectors: Option<Vec<ResourceSelectorInput>>,
    pub account: Option<AccountRefInput>,
}

impl SearchResourcesInput {
    pub fn into_facade_request(
        self,
        pagination: PaginationOpts,
    ) -> Result<kamu_resources_facade::SearchResourcesRequest, GqlError> {
        Ok(kamu_resources_facade::SearchResourcesRequest {
            selectors: match self.selectors {
                Some(selectors) => into_resource_selectors(selectors)?,
                None => vec![any_resource_selector()],
            },
            account: self.account.map(AccountRefInput::into_manifest_account),
            pagination,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reference to exactly one resource, mirroring the ODF `ResourceRef`.
///
/// Not a `oneOf`: ODF allows `id` and `name` together as a consistency
/// assertion, so both are accepted and at least one is required. Validation
/// happens at conversion rather than in the schema, which cannot express
/// "at least one of".
#[derive(InputObject, Debug, Clone)]
pub struct ResourceRefInput {
    pub account: Option<AccountRefInput>,
    /// Canonical selector (`variablesets`), alias (`vs`), ODF type name
    /// (`VariableSet`), or full schema URI — all resolve to the same type.
    ///
    /// `null` spans every type: the resource is looked up across all of them.
    /// Since a ref names exactly one resource, a name matching in several types
    /// is an ambiguity error rather than a multi-match.
    pub r#type: Option<ResourceTypeSelectorInput>,
    pub id: Option<ResourceID<'static>>,
    /// Exact name. Never a pattern; use a selector for pattern matching.
    pub name: Option<ResourceName<'static>>,
}

impl TryFrom<ResourceRefInput> for kamu_resources::ResourceRef {
    type Error = GqlError;

    fn try_from(value: ResourceRefInput) -> Result<Self, GqlError> {
        let resource_ref = Self {
            account: value.account.map(AccountRefInput::into_manifest_account),
            r#type: value.r#type.map(ResourceTypeSelectorInput::into_type_ref),
            id: value.id.map(Into::into),
            // Reserved in ODF for when datasets and accounts become resources;
            // the facade rejects a populated `did`, so it is not accepted here.
            did: None,
            name: value.name.map(Into::into),
        };

        kamu_resources_facade::validate_ref(&resource_ref)
            .map_err(|e| GqlError::gql(e.to_string()))?;

        Ok(resource_ref)
    }
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

/// The account selector supplied for this operation could not be resolved to a
/// concrete account (empty selector, unknown id/name, or a selector whose
/// fields disagree with each other). This is an input-validation problem, not
/// an authorization outcome — a caller denied access to another account's
/// resources gets a top-level GraphQL error, never this type.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceAccountResolutionProblem {
    pub code: ResourceAccountResolutionProblemCode,
    pub message: String,
}

impl From<kamu_resources_facade::ResourceAccountResolutionError>
    for ResourceAccountResolutionProblem
{
    fn from(value: kamu_resources_facade::ResourceAccountResolutionError) -> Self {
        use kamu_resources_facade::ResourceAccountResolutionProblemCode as C;

        let code = match value.code {
            C::EmptySelector => ResourceAccountResolutionProblemCode::EmptySelector,
            C::AccountNotFoundById => ResourceAccountResolutionProblemCode::AccountNotFoundById,
            C::AccountNotFoundByName => ResourceAccountResolutionProblemCode::AccountNotFoundByName,
            C::SelectorMismatch => ResourceAccountResolutionProblemCode::SelectorMismatch,
        };

        Self {
            code,
            message: value.message,
        }
    }
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceAccountResolutionProblemCode {
    EmptySelector,
    AccountNotFoundById,
    AccountNotFoundByName,
    SelectorMismatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceIDNotFoundProblem {
    pub id: ResourceID<'static>,
    pub message: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceNameNotFoundProblem {
    pub type_name: TypeName<'static>,
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

/// A reference supplied both an `id` and a `name` that name different
/// resources. The pair is a consistency assertion, so the mismatch fails the
/// entry rather than letting the `id` silently win.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceNameMismatchProblem {
    pub id: ResourceID<'static>,
    pub expected_name: ResourceName<'static>,
    pub actual_name: ResourceName<'static>,
    pub message: String,
}

/// A type-less reference whose name matched nothing in any type. Distinct from
/// `ResourceNameNotFoundProblem`, which can name the single type it searched.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceAnyTypeNameNotFoundProblem {
    pub name: ResourceName<'static>,
    pub message: String,
}

/// A type-less reference whose name matched in several types. A reference names
/// exactly one resource, so this is an addressing failure rather than a
/// multi-match — the caller must say which type they meant.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceAmbiguousTypeProblem {
    pub name: ResourceName<'static>,
    pub type_names: Vec<TypeName<'static>>,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A reference that named neither an id nor a name.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceEmptyRefProblem {
    pub message: String,
}

#[derive(Union, Debug, Clone)]
pub enum ResourceLookupProblem {
    UidNotFound(ResourceIDNotFoundProblem),
    NameNotFound(ResourceNameNotFoundProblem),
    AnyTypeNameNotFound(ResourceAnyTypeNameNotFoundProblem),
    AmbiguousType(ResourceAmbiguousTypeProblem),
    SchemaMismatch(ResourceSchemaMismatchProblem),
    NameMismatch(ResourceNameMismatchProblem),
    EmptyRef(ResourceEmptyRefProblem),
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
                type_name: e.type_name.clone().into(),
                name: e.name.clone().into(),
                message: e.to_string(),
            }),
            P::AnyTypeNameNotFound(e) => {
                Self::AnyTypeNameNotFound(ResourceAnyTypeNameNotFoundProblem {
                    name: e.name.clone().into(),
                    message: e.to_string(),
                })
            }
            P::AmbiguousType(e) => {
                let message = e.to_string();
                Self::AmbiguousType(ResourceAmbiguousTypeProblem {
                    name: e.name.clone().into(),
                    type_names: e.type_names.iter().cloned().map(Into::into).collect(),
                    message,
                })
            }
            P::SchemaMismatch(e) => {
                let message = e.to_string();
                Self::SchemaMismatch(ResourceSchemaMismatchProblem {
                    id: e.id.into(),
                    expected_schema: e.expected_schema.into(),
                    actual_schema: e.actual_schema.into(),
                    message,
                })
            }
            P::NameMismatch(e) => {
                let message = e.to_string();
                Self::NameMismatch(ResourceNameMismatchProblem {
                    id: e.id.into(),
                    expected_name: e.expected_name.clone().into(),
                    actual_name: e.actual_name.clone().into(),
                    message,
                })
            }
            P::EmptyRef => Self::EmptyRef(ResourceEmptyRefProblem {
                message: P::EmptyRef.to_string(),
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A selector's `labels` could not be resolved into a matchable filter.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceInvalidLabelFilterProblem {
    pub code: ResourceLabelFilterProblemCode,
    pub message: String,
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceLabelFilterProblemCode {
    InvalidKey,
    ResourceExtensionSchema,
    NonStringValue,
    DuplicateAfterCanonicalization,
    UnsupportedExpression,
}

impl From<kamu_resources_facade::ResourceInvalidLabelFilterError>
    for ResourceInvalidLabelFilterProblem
{
    fn from(value: kamu_resources_facade::ResourceInvalidLabelFilterError) -> Self {
        use kamu_resources_facade::ResourceLabelFilterProblemCode as C;

        let code = match value.code {
            C::InvalidKey => ResourceLabelFilterProblemCode::InvalidKey,
            C::ResourceExtensionSchema => ResourceLabelFilterProblemCode::ResourceExtensionSchema,
            C::NonStringValue => ResourceLabelFilterProblemCode::NonStringValue,
            C::DuplicateAfterCanonicalization => {
                ResourceLabelFilterProblemCode::DuplicateAfterCanonicalization
            }
            C::UnsupportedExpression => ResourceLabelFilterProblemCode::UnsupportedExpression,
        };

        Self {
            code,
            message: value.message,
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

pub use crate::scalars::Resource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum BatchResourcesOutcome {
    Success(BatchResourcesResult),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    AccountResolution(ResourceAccountResolutionProblem),
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
    AccountResolution(ResourceAccountResolutionProblem),
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
pub struct ResourceHandle {
    pub id: ResourceID<'static>,
    #[graphql(name = "type")]
    pub r#type: TypeUri<'static>,
    // Always `None` until we support DID-aware resource types (see
    // handle_support.rs).
    pub did: Option<Did<'static>>,
    pub name: ResourceName<'static>,
    pub account: AccountHandle,
}

impl From<kamu_resources::ResourceHandle> for ResourceHandle {
    fn from(value: kamu_resources::ResourceHandle) -> Self {
        Self {
            id: value.id.into(),
            r#type: value.r#type.into(),
            did: value.did.map(Into::into),
            name: value.name.into(),
            account: value.account.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum BatchResourceHandlesOutcome {
    Success(BatchResourceHandlesResult),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    AccountResolution(ResourceAccountResolutionProblem),
}

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceHandlesResult {
    pub handles: Vec<BatchResourceHandleSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

impl From<BatchGetResourceHandlesResponse> for BatchResourceHandlesResult {
    fn from(value: BatchGetResourceHandlesResponse) -> Self {
        Self {
            handles: value
                .successes
                .into_iter()
                .map(|success| BatchResourceHandleSuccess {
                    request_index: success.request_index,
                    handle: success.item.into(),
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct BatchResourceHandleSuccess {
    pub request_index: usize,
    pub handle: ResourceHandle,
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
    pub type_name: TypeName<'static>,
    pub total_count: UInt64,
    pub phase_counts: ResourcePhaseCounts,
}

impl From<kamu_resources::ResourceTypeCountSummary> for ResourceTypeCountSummary {
    fn from(value: kamu_resources::ResourceTypeCountSummary) -> Self {
        Self {
            schema: value.schema.into(),
            type_name: value.type_name.into(),
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

page_based_connection!(ResourceHandle, ResourceHandleConnection, ResourceHandleEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
