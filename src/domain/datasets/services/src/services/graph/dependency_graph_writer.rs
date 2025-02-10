// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait DependencyGraphWriter: Send + Sync {
    async fn create_dataset_node(&self, dataset_id: &odf::DatasetID) -> Result<(), InternalError>;

    async fn remove_dataset_node(&self, dataset_id: &odf::DatasetID) -> Result<(), InternalError>;

    async fn update_dataset_node_dependencies(
        &self,
        catalog: &Catalog,
        dataset_id: &odf::DatasetID,
        new_upstream_ids: Vec<odf::DatasetID>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
