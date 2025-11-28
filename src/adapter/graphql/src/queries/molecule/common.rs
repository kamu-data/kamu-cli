// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{CurrentAccountSubject, LoggedAccount};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MOLECULE_ORG_ACCOUNTS: [&str; 2] = ["molecule", "molecule.dev"];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn molecule_subject(ctx: &Context<'_>) -> Result<LoggedAccount> {
    // Check auth
    let subject = from_catalog_n!(ctx, CurrentAccountSubject);
    let subject_molecule = match subject.as_ref() {
        CurrentAccountSubject::Logged(subj)
            if MOLECULE_ORG_ACCOUNTS.contains(&subj.account_name.as_str()) =>
        {
            subj
        }
        _ => {
            return Err(GqlError::Access(odf::AccessError::Unauthorized(
                format!(
                    "Only accounts {} can provision projects",
                    MOLECULE_ORG_ACCOUNTS
                        .iter()
                        .map(|account_name| format!("'{account_name}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
                .as_str()
                .into(),
            )));
        }
    };
    Ok(subject_molecule.clone())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
