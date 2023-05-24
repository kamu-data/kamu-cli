// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use dill::*;

use crate::domain::{InternalError, QueryDataAccessor};

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct QueryDataAccessorLocalFs {}

impl QueryDataAccessorLocalFs {
    pub fn new() -> Self {
        Self {}
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl QueryDataAccessor for QueryDataAccessorLocalFs {
    fn bind_object_store(&self, _session_context: &SessionContext) -> Result<(), InternalError> {
        // File-system object store is built-in
        Ok(())
    }

    fn object_store_url(&self) -> url::Url {
        url::Url::parse(ObjectStoreUrl::local_filesystem().as_str()).unwrap()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
