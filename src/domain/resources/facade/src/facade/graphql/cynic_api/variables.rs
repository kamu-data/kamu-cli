use internal_error::InternalError;

use super::fragments::ResourceManifestFormat;
use super::inputs::{
    ResourceAccountSelectorInput,
    ResourceBatchSelectorInput,
    ResourceKindInput,
    ResourceSelectorInput,
    SearchResourceIdentitiesInput,
};
use super::schema;
use crate::{
    ResourceBatchSelector,
    ResourceSelector,
    SearchResourceIdentitiesRequest,
    SpecViewMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceSelectorVariables {
    pub selector: ResourceSelectorInput,
    pub revealed: bool,
}

impl ResourceSelectorVariables {
    pub(crate) fn new(
        selector: &ResourceSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
            revealed: spec_view_mode == SpecViewMode::Revealed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceBatchSelectorVariables {
    pub selector: ResourceBatchSelectorInput,
    pub revealed: bool,
}

impl ResourceBatchSelectorVariables {
    pub(crate) fn new(
        selector: &ResourceBatchSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
            revealed: spec_view_mode == SpecViewMode::Revealed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceIdentitySelectorVariables {
    pub selector: ResourceSelectorInput,
}

impl ResourceIdentitySelectorVariables {
    pub(crate) fn new(selector: &ResourceSelector) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceIdentityBatchSelectorVariables {
    pub selector: ResourceBatchSelectorInput,
}

impl ResourceIdentityBatchSelectorVariables {
    pub(crate) fn new(selector: &ResourceBatchSelector) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct RenderResourceManifestVariables {
    pub selector: ResourceSelectorInput,
    pub format: ResourceManifestFormat,
    pub revealed: bool,
}

impl RenderResourceManifestVariables {
    pub(crate) fn new(
        selector: &ResourceSelector,
        format: crate::ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
            format: format.into(),
            revealed: spec_view_mode == SpecViewMode::Revealed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct RenderResourceManifestsVariables {
    pub selector: ResourceBatchSelectorInput,
    pub format: ResourceManifestFormat,
    pub revealed: bool,
}

impl RenderResourceManifestsVariables {
    pub(crate) fn new(
        selector: &ResourceBatchSelector,
        format: crate::ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
            format: format.into(),
            revealed: spec_view_mode == SpecViewMode::Revealed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ListByKindVariables {
    pub kind: ResourceKindInput,
    pub account: Option<ResourceAccountSelectorInput>,
    pub page: i32,
    pub per_page: i32,
}

impl ListByKindVariables {
    pub(crate) fn new(
        kind: &str,
        account: Option<&kamu_resources::ResourceManifestAccount>,
        offset: usize,
        limit: usize,
    ) -> Result<Self, InternalError> {
        let (page, per_page) = graphql_page_params(offset, limit);
        Ok(Self {
            kind: ResourceKindInput::custom(kind.to_string()),
            account: account.map(TryInto::try_into).transpose()?,
            page,
            per_page,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ListAllVariables {
    pub account: Option<ResourceAccountSelectorInput>,
    pub page: i32,
    pub per_page: i32,
}

impl ListAllVariables {
    pub(crate) fn new(
        account: Option<&kamu_resources::ResourceManifestAccount>,
        offset: usize,
        limit: usize,
    ) -> Result<Self, InternalError> {
        let (page, per_page) = graphql_page_params(offset, limit);
        Ok(Self {
            account: account.map(TryInto::try_into).transpose()?,
            page,
            per_page,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct SearchIdentitiesVariables {
    pub query: SearchResourceIdentitiesInput,
    pub page: i32,
    pub per_page: i32,
}

impl SearchIdentitiesVariables {
    pub(crate) fn new(request: &SearchResourceIdentitiesRequest) -> Result<Self, InternalError> {
        let (page, per_page) =
            graphql_page_params(request.pagination.offset, request.pagination.limit);
        Ok(Self {
            query: request.try_into()?,
            page,
            per_page,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const LIST_PAGE_SIZE: usize = 100;

fn graphql_page_params(offset: usize, limit: usize) -> (i32, i32) {
    let per_page = if limit == 0 { LIST_PAGE_SIZE } else { limit };
    let page = offset.checked_div(per_page).unwrap_or(0);
    // GraphQL Int is i32; page counts are always small so this will not overflow
    (page as i32, per_page as i32)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
