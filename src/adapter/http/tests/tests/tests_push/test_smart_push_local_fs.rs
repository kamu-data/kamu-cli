// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::harness::{
    ClientSideHarness,
    ClientSideHarnessOptions,
    ServerSideHarnessOptions,
    ServerSideHarnessOverrides,
    ServerSideLocalFsHarness,
};
use crate::tests::test_client_server_local_fs_harness_permutations;
use crate::tests::tests_push::test_smart_push_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_new_dataset
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_new_dataset_as_public
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_new_empty_dataset
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_existing_up_to_date_dataset
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_existing_evolved_dataset
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_existing_diverged_dataset
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_existing_dataset_fails_as_server_advanced
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_aborted_write_of_new_rewrite_succeeds
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_client_server_local_fs_harness_permutations!(
    test_smart_push_shared,
    test_smart_push_aborted_write_of_updated_rewrite_succeeds
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
