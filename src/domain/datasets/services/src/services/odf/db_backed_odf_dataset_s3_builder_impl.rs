// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use s3_utils::S3Context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedOdfDatasetS3BuilderImpl {}

#[component(pub)]
#[interface(dyn odf::dataset::DatasetS3Builder)]
impl DatabaseBackedOdfDatasetS3BuilderImpl {
    pub fn new() -> Self {
        Self {}
    }
}

impl odf::dataset::DatasetS3Builder for DatabaseBackedOdfDatasetS3BuilderImpl {
    fn build_s3_dataset(
        &self,
        _dataset_id: &odf::DatasetID,
        _s3_context: S3Context,
    ) -> std::sync::Arc<dyn odf::Dataset> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
