// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ApplyResourceParams;
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{TestResource, TestResourceSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Remove when actually used in tests
#[expect(dead_code)]
pub fn make_account_id() -> odf::AccountID {
    odf::AccountID::new_generated_ed25519().1
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Remove when actually used in tests
#[expect(dead_code)]
pub fn make_resource_params(
    account_id: odf::AccountID,
    name: &str,
) -> ApplyResourceParams<TestResource> {
    ApplyResourceParams {
        uid: None,
        metadata: BaseResourceServiceHarness::make_metadata(account_id, name),
        spec: TestResourceSpec {
            value: name.to_string(),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
