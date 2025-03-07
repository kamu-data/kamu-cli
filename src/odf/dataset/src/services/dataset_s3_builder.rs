// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use s3_utils::S3Context;

use crate::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatasetS3Builder: Send + Sync {
    fn build_s3_dataset(
        &self,
        dataset_id: &odf_metadata::DatasetID,
        s3_context: S3Context,
    ) -> Arc<dyn Dataset>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
