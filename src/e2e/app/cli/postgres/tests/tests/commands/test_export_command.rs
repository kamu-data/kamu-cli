// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_csv_file,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_csv_file_partitioning_ignored_for_single_file,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_csv_files,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_json_file,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_json_file_partitioning_ignored_for_single_file,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_json_files,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_parquet_file,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_parquet_file_partitioning_ignored_for_single_file,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_parquet_files,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_to_unsupported_format,
    extra_test_groups = "engine, datafusion"
);

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_export_non_existent_dataset,
    extra_test_groups = "engine, datafusion"
);
