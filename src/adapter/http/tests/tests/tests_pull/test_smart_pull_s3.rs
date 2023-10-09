// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::harness::{ClientSideHarness, ClientSideHarnessOptions, ServerSideS3Harness};
use crate::tests::test_client_server_s3_harness_permutations;
use crate::tests::tests_pull::test_smart_pull_shared;

/////////////////////////////////////////////////////////////////////////////////////////

test_client_server_s3_harness_permutations!(test_smart_pull_shared, test_smart_pull_new_dataset);

/////////////////////////////////////////////////////////////////////////////////////////

test_client_server_s3_harness_permutations!(
    test_smart_pull_shared,
    test_smart_pull_existing_up_to_date_dataset
);

/////////////////////////////////////////////////////////////////////////////////////////

test_client_server_s3_harness_permutations!(
    test_smart_pull_shared,
    test_smart_pull_existing_evolved_dataset
);

/////////////////////////////////////////////////////////////////////////////////////////

test_client_server_s3_harness_permutations!(
    test_smart_pull_shared,
    test_smart_pull_existing_advanced_dataset_fails
);

/////////////////////////////////////////////////////////////////////////////////////////

test_client_server_s3_harness_permutations!(
    test_smart_pull_shared,
    test_smart_pull_aborted_read_of_new_reread_succeeds
);

/////////////////////////////////////////////////////////////////////////////////////////

test_client_server_s3_harness_permutations!(
    test_smart_pull_shared,
    test_smart_pull_aborted_read_of_existing_evolved_dataset_reread_succeeds
);

/////////////////////////////////////////////////////////////////////////////////////////
