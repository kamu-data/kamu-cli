// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{
    ResourceHeaders,
    ResourceID,
    ResourceName,
    ResourceSelectorName,
    ResourceStatus,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceIdentityView {
    pub schema: TypeUri,
    pub canonical_selector: ResourceSelectorName,
    pub id: ResourceID,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceView {
    pub schema: TypeUri,
    #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceHeaders")]
    pub headers: ResourceHeaders,
    pub spec: serde_json::Value,
    #[serde_as(as = "Option<odf::metadata::serde::yaml::resource::ResourceStatus>")]
    pub status: Option<ResourceStatus>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
