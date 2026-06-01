use internal_error::InternalError;

use super::fragments::ResourceManifestFormat;
use super::inputs::{ResourceBatchSelectorInput, ResourceSelectorInput};
use super::schema;
use crate::{ResourceBatchSelector, ResourceSelector, SpecViewMode};

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
