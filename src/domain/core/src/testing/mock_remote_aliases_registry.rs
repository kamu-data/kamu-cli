// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetHandle;

use crate::{GetAliasesError, RemoteAliases, RemoteAliasesRegistry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub RemoteAliasesRegistry {}

    #[async_trait::async_trait]
    impl RemoteAliasesRegistry for RemoteAliasesRegistry {
        async fn get_remote_aliases(&self, dataset_handle: &DatasetHandle) -> Result<Box<dyn RemoteAliases>, GetAliasesError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
