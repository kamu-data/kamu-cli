// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(assert_matches)]

mod dataset_dependencies_repository_test_suite;
mod dataset_entry_repository_test_suite;
mod dataset_env_var_repository_test_suite;
mod dataset_reference_repository_test_suite;
mod dataset_statistics_repository_test_suite;

mod helpers;

pub mod dataset_dependency_repo {
    pub use crate::dataset_dependencies_repository_test_suite::*;
}
pub mod dataset_entry_repo {
    pub use crate::dataset_entry_repository_test_suite::*;
}
pub mod dataset_env_var_repo {
    pub use crate::dataset_env_var_repository_test_suite::*;
}
pub mod dataset_reference_repo {
    pub use crate::dataset_reference_repository_test_suite::*;
}
pub mod dataset_statistics_repo {
    pub use crate::dataset_statistics_repository_test_suite::*;
}
