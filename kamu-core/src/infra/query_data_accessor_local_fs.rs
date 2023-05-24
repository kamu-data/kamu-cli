// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::{SessionConfig, SessionContext};
use dill::*;
use opendatafabric::{DataSlice, DatasetHandle};

use super::WorkspaceLayout;
use crate::domain::{InternalError, QueryDataAccessor};

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct QueryDataAccessorLocalFs {
    workspace_layout: Arc<WorkspaceLayout>,
}

impl QueryDataAccessorLocalFs {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        Self { workspace_layout }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl QueryDataAccessor for QueryDataAccessorLocalFs {
    fn session_context(&self) -> Result<SessionContext, InternalError> {
        let cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("kamu", "kamu");

        Ok(SessionContext::with_config(cfg))
    }

    fn object_store_url(&self) -> url::Url {
        url::Url::parse(ObjectStoreUrl::local_filesystem().as_str()).unwrap()
    }

    fn data_object_store_path(
        &self,
        dataset_handle: &DatasetHandle,
        data_slice: &DataSlice,
    ) -> object_store::path::Path {
        let dataset_layout = self.workspace_layout.dataset_layout(&dataset_handle.alias);
        let data_slice_path = dataset_layout.data_slice_path(data_slice);
        object_store::path::Path::from_filesystem_path(data_slice_path).unwrap()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
