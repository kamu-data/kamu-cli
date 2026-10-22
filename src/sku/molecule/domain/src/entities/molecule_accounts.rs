// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MOLECULE_ORG_ACCOUNT_NAME: &str = "molecule";
pub const MOLECULE_DEV_ORG_ACCOUNT_NAME: &str = "molecule.dev";
pub const MOLECULE_SANDBOX_ORG_ACCOUNT_NAME: &str = "molecule.sandbox";
pub const MOLECULE_ORG_ACCOUNTS: [&str; 3] = [
    MOLECULE_ORG_ACCOUNT_NAME,
    MOLECULE_DEV_ORG_ACCOUNT_NAME,
    MOLECULE_SANDBOX_ORG_ACCOUNT_NAME,
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The Molecule org account that owns `account_name`: the longest org name
/// `<org>` for which the account is named `<org>.<suffix>`. Org accounts are
/// never owned by another org, so `molecule.dev` is not a project of `molecule`.
/// Names compare case-insensitively, as account names do.
pub fn owning_molecule_org_account(account_name: &str) -> Option<&'static str> {
    if MOLECULE_ORG_ACCOUNTS
        .iter()
        .any(|org| org.eq_ignore_ascii_case(account_name))
    {
        return None;
    }

    MOLECULE_ORG_ACCOUNTS
        .into_iter()
        .filter(|org| {
            account_name.as_bytes().get(org.len()) == Some(&b'.')
                && account_name
                    .get(..org.len())
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case(org))
        })
        .max_by_key(|org| org.len())
}

/// Whether `subject_account_name` may act for `target_account_name`: an
/// account always may for itself, and a Molecule org account may for the
/// project accounts it owns (see [`owning_molecule_org_account`]).
pub fn molecule_org_account_owns(subject_account_name: &str, target_account_name: &str) -> bool {
    subject_account_name.eq_ignore_ascii_case(target_account_name)
        || owning_molecule_org_account(target_account_name)
            .is_some_and(|org| org.eq_ignore_ascii_case(subject_account_name))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
