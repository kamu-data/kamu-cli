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

use crate::facade::graphql::cynic_api::inputs::{ResourceAccountSelectorInput, ResourceKindInput};
use crate::facade::graphql::cynic_api::schema;

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
        account: Option<&kamu_resources::ResourceAccountRef>,
        pagination: PaginationOpts,
    ) -> Result<Self, InternalError> {
        let (page, per_page) = pagination.as_page_params(Self::DEFAULT_PAGE_SIZE)?;
        Ok(Self {
            kind: ResourceKindInput::from_kind(kind),
            account: account.map(TryInto::try_into).transpose()?,
            page,
            per_page,
        })
    }

    const DEFAULT_PAGE_SIZE: usize = 100;
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
        account: Option<&kamu_resources::ResourceAccountRef>,
        pagination: PaginationOpts,
    ) -> Result<Self, InternalError> {
        let (page, per_page) = pagination.as_page_params(Self::DEFAULT_PAGE_SIZE)?;
        Ok(Self {
            account: account.map(TryInto::try_into).transpose()?,
            page,
            per_page,
        })
    }

    const DEFAULT_PAGE_SIZE: usize = 100;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
