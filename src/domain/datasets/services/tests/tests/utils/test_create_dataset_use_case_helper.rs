// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[rstest::rstest]
// kamu
#[case("kamu", "kamu.project", true)]
#[case("kamu", "user", false)]
// molecule
#[case("molecule", "molecule.project", true)]
#[case("molecule", "molecule.devops", true)]
#[case("molecule", "molecule.sandboxes", true)]
#[case("molecule", "molecule.dev", false)]
#[case("molecule", "molecule.dev.project", false)]
#[case("molecule", "molecule.sandbox", false)]
#[case("molecule", "molecule.sandbox.project", false)]
#[case("molecule", "moleculex", false)]
// molecule.dev
#[case("molecule.dev", "molecule.dev.project", true)]
#[case("molecule.dev", "molecule.devops", false)]
#[case("molecule.dev", "molecule.project", false)]
#[case("molecule.dev", "molecule.sandbox.project", false)]
// molecule.sandbox
#[case("molecule.sandbox", "molecule.sandbox.project", true)]
#[case("molecule.sandbox", "molecule.sandboxes", false)]
#[case("molecule.sandbox", "molecule", false)]
#[case("molecule.sandbox", "molecule.project", false)]
#[case("molecule.sandbox", "molecule.dev.project", false)]
// Account names compare case-insensitively
#[case("molecule", "Molecule.Project", true)]
#[case("molecule", "molecule.DEV.project", false)]
#[case("molecule", "Molecule.Sandbox", false)]
#[case("molecule.sandbox", "MOLECULE.SANDBOX.project", true)]
// Other accounts
#[case("user", "user.project", false)]
#[case("user", "molecule.project", false)]
fn test_has_cross_account_create_access(
    #[case] subject_account_name: &str,
    #[case] target_account_name: &str,
    #[case] expected: bool,
) {
    assert_eq!(
        expected,
        CreateDatasetUseCaseHelper::has_cross_account_create_access(
            subject_account_name,
            target_account_name
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
