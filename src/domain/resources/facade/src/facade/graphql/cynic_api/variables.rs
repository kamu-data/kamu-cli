// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

pub(crate) fn graphql_page_params(offset: usize, limit: usize) -> (i32, i32) {
    const LIST_PAGE_SIZE: usize = 100;
    let per_page = if limit == 0 { LIST_PAGE_SIZE } else { limit };
    let page = offset.checked_div(per_page).unwrap_or(0);
    // GraphQL Int is i32; page counts are always small so this will not overflow
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    (page as i32, per_page as i32)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
