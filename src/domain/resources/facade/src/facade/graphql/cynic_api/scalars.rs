// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_resources as domain;

use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

cynic::impl_scalar!(DateTime<Utc>, schema::DateTime);
cynic::impl_scalar!(odf::AccountID, schema::AccountID);
cynic::impl_scalar!(domain::ResourceUID, schema::ResourceID);
cynic::impl_scalar!(serde_json::Value, schema::JSON);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::Scalar, Debug, Clone)]
#[cynic(graphql_type = "AccountName")]
pub(crate) struct AccountName(pub(crate) String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
