// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(assert_matches)]

mod access_token_repository_test_utils;
mod account_quota_event_store_test_suite;
mod accounts_repository_test_utils;

mod access_token_repository_test_suite;
mod accounts_repository_test_suite;
mod did_secret_key_repository_suite;
mod oauth_device_code_repository_test_suite;
mod password_hash_repository_test_suite;

pub use access_token_repository_test_suite::*;
pub(crate) use access_token_repository_test_utils::*;
pub use account_quota_event_store_test_suite::*;
pub use accounts_repository_test_suite::*;
pub(crate) use accounts_repository_test_utils::*;
pub use password_hash_repository_test_suite::*;
pub mod oauth_device_code_repository {
    pub use super::oauth_device_code_repository_test_suite::*;
}
pub mod did_secret_key_repository {
    pub use super::did_secret_key_repository_suite::*;
}
