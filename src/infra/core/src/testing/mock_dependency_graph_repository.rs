// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{DatasetDependenciesIDStream, DependencyGraphRepository};

////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DependencyGraphRepository {}
    #[async_trait::async_trait]
    impl DependencyGraphRepository for DependencyGraphRepository {
        fn list_dependencies_of_all_datasets(&self) -> DatasetDependenciesIDStream<'_>;
    }
}

////////////////////////////////////////////////////////////////////////////////

impl MockDependencyGraphRepository {
    pub fn no_dependencies() -> Self {
        let mut dependency_graph_repo_mock = MockDependencyGraphRepository::default();
        dependency_graph_repo_mock
            .expect_list_dependencies_of_all_datasets()
            .return_once(|| Box::pin(futures::stream::empty()));
        dependency_graph_repo_mock
    }
}

////////////////////////////////////////////////////////////////////////////////
