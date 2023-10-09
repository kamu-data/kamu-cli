// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod test_authentication_layer;
mod test_dataset_authorization_layer;
mod test_routing;

mod tests_pull;
mod tests_push;

/////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! test_client_server_local_fs_harness_permutations {
    ($test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_st_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(false),
                    ServerSideLocalFsHarness::new(false).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(false),
                    ServerSideLocalFsHarness::new(true).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt_client_st_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(true),
                    ServerSideLocalFsHarness::new(false).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(true),
                    ServerSideLocalFsHarness::new(true).await,
                )
                .await;
            }
        }
    };
}

#[macro_export]
macro_rules! test_client_server_s3_harness_permutations {
    ($test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(containerized)]
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_st_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(false),
                    ServerSideS3Harness::new(false).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_group::group(containerized)]
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(false),
                    ServerSideS3Harness::new(true).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_group::group(containerized)]
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt_client_st_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(true),
                    ServerSideS3Harness::new(false).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_group::group(containerized)]
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(true),
                    ServerSideS3Harness::new(true).await,
                )
                .await;
            }
        }
    };
}

pub(crate) use {
    test_client_server_local_fs_harness_permutations,
    test_client_server_s3_harness_permutations,
};

/////////////////////////////////////////////////////////////////////////////////////////
