// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod test_authentication_layer;
mod test_data_ingest;
mod test_data_query;
mod test_dataset_authorization_layer;
mod test_platform_login_validate;
mod test_protocol_dataset_helpers;
mod test_routing;
mod tests_pull;
mod tests_push;

/////////////////////////////////////////////////////////////////////////////////////////

/** Test scenario in the following permutations:
 *    - client: local fs, single-tenant x multi-tenant
 *    x
 *    - server: local fs, single-tenant x multi-tenant
 */
macro_rules! test_client_server_local_fs_harness_permutations {
    ($test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_st_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions { multi_tenant: false, authenticated_remotely: true }),
                    ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                        multi_tenant: false,
                        authorized_writes: true,
                        base_catalog: None,
                    }),
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions { multi_tenant: false, authenticated_remotely: true }),
                    ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                        multi_tenant: true,
                        authorized_writes: true,
                        base_catalog: None,
                    }),
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt_client_st_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions { multi_tenant: true, authenticated_remotely: true }),
                    ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                        multi_tenant: false,
                        authorized_writes: true,
                        base_catalog: None,
                    }),
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions { multi_tenant: true, authenticated_remotely: true }),
                    ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                        multi_tenant: true,
                        authorized_writes: true,
                        base_catalog: None,
                    }),
                )
                .await;
            }
        }
    };
}

/** Test scenario in the following permutations:
 *    - client: local fs, single-tenant
 *    x
 *    - server: s3, single-tenant x multi-tenant
 */
macro_rules! test_client_server_s3_harness_permutations {
    ($test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(containerized)]
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_st_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions { multi_tenant: false, authenticated_remotely: true }),
                    ServerSideS3Harness::new(ServerSideHarnessOptions {
                        multi_tenant: false,
                        authorized_writes: true,
                        base_catalog: None,
                    }).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_group::group(containerized)]
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions { multi_tenant: false, authenticated_remotely: true }),
                    ServerSideS3Harness::new(ServerSideHarnessOptions {
                        multi_tenant: true,
                        authorized_writes: true,
                        base_catalog: None,
                    }).await,
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
