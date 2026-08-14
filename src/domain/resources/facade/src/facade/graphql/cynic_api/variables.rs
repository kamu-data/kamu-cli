// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;

use crate::facade::graphql::cynic_api::inputs::{
    AccountRefInput,
    ResourceLabelFilterInput,
    ResourceSelectorInput,
    resource_selector_inputs,
};
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ListByResourceTypeVariables {
    pub selectors: Vec<ResourceSelectorInput>,
    pub account: Option<AccountRefInput>,
    pub label_filter: Option<ResourceLabelFilterInput>,
    pub page: i32,
    pub per_page: i32,
}

impl ListByResourceTypeVariables {
    pub(crate) fn new(
        selectors: &[kamu_resources::ResourceSelector],
        account: Option<&kamu_resources::ResourceAccountRef>,
        label_filter: Option<&kamu_resources::ResourceLabelFilterInput>,
        pagination: PaginationOpts,
    ) -> Result<Self, InternalError> {
        let (page, per_page) = pagination.as_page_params(Self::DEFAULT_PAGE_SIZE)?;
        Ok(Self {
            selectors: resource_selector_inputs(selectors),
            account: account.map(Into::into),
            label_filter: label_filter.map(Into::into),
            page,
            per_page,
        })
    }

    const DEFAULT_PAGE_SIZE: usize = 100;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ListAllVariables {
    pub account: Option<AccountRefInput>,
    pub label_filter: Option<ResourceLabelFilterInput>,
    pub selectors: Option<Vec<ResourceSelectorInput>>,
    pub page: i32,
    pub per_page: i32,
}

impl ListAllVariables {
    pub(crate) fn new(
        account: Option<&kamu_resources::ResourceAccountRef>,
        label_filter: Option<&kamu_resources::ResourceLabelFilterInput>,
        pagination: PaginationOpts,
        selectors: &[kamu_resources::ResourceSelector],
    ) -> Result<Self, InternalError> {
        let (page, per_page) = pagination.as_page_params(Self::DEFAULT_PAGE_SIZE)?;
        Ok(Self {
            account: account.map(Into::into),
            label_filter: label_filter.map(Into::into),
            selectors: Some(resource_selector_inputs(selectors)),
            page,
            per_page,
        })
    }

    const DEFAULT_PAGE_SIZE: usize = 100;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
