// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod test_account_info;
mod test_authentication_layer;
mod test_data_ingest;
mod test_data_query;
mod test_dataset_authorization_layer;
mod test_dataset_info;
mod test_node_info;
mod test_platform_login_validate;
mod test_protocol_dataset_helpers;
mod test_routing;
mod test_unknown_handler;
mod test_upload_local;
mod test_upload_s3;
mod tests_pull;
mod tests_push;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
                    ClientSideHarness::new(ClientSideHarnessOptions {
                        tenancy_config: TenancyConfig::SingleTenant,
                        authenticated_remotely: true
                    }).await,
                    ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                        tenancy_config: TenancyConfig::SingleTenant,
                        authorized_writes: true,
                        base_catalog: None,
                    }).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_st_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions {
                        tenancy_config: TenancyConfig::SingleTenant,
                        authenticated_remotely: true
                    }).await,
                    ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                        tenancy_config: TenancyConfig::MultiTenant,
                        authorized_writes: true,
                        base_catalog: None,
                    }).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt_client_st_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions {
                        tenancy_config: TenancyConfig::MultiTenant,
                        authenticated_remotely: true
                    }).await,
                    ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                        tenancy_config: TenancyConfig::SingleTenant,
                        authorized_writes: true,
                        base_catalog: None,
                    }).await,
                )
                .await;
            }
        }

        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name "_mt_client_mt_local_fs_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions {
                        tenancy_config: TenancyConfig::MultiTenant,
                        authenticated_remotely: true
                    }).await,
                    ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                        tenancy_config: TenancyConfig::MultiTenant,
                        authorized_writes: true,
                        base_catalog: None,
                    }).await,
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
            async fn [<$test_name "_st_client_st_s3_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions {
                        tenancy_config: TenancyConfig::SingleTenant,
                        authenticated_remotely: true
                    }).await,
                    ServerSideS3Harness::new(ServerSideHarnessOptions {
                        tenancy_config: TenancyConfig::SingleTenant,
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
            async fn [<$test_name "_st_client_mt_s3_server">] () {
                $test_package::$test_name(
                    ClientSideHarness::new(ClientSideHarnessOptions {
                        tenancy_config: TenancyConfig::SingleTenant,
                         authenticated_remotely: true
                    }).await,
                    ServerSideS3Harness::new(ServerSideHarnessOptions {
                        tenancy_config: TenancyConfig::MultiTenant,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
