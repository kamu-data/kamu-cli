// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! test_for_each_tenancy {
    ($test_name: expr) => {
        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st">]() {
                $test_name(::kamu_core::TenancyConfig::SingleTenant).await;
            }

            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt">]() {
                $test_name(::kamu_core::TenancyConfig::MultiTenant).await;
            }
        }
    };
}

pub use test_for_each_tenancy;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
