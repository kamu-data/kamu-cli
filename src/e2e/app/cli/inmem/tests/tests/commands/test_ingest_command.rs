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
    storage = inmem,
    fixture = kamu_cli_e2e_repo_tests::test_push_ingest_from_file_ledger,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = inmem,
    fixture = kamu_cli_e2e_repo_tests::test_push_ingest_from_file_snapshot_with_event_time,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = inmem,
    fixture = kamu_cli_e2e_repo_tests::test_ingest_from_stdin,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = inmem,
    fixture = kamu_cli_e2e_repo_tests::test_ingest_recursive,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = inmem,
    fixture = kamu_cli_e2e_repo_tests::test_ingest_with_source_name,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
