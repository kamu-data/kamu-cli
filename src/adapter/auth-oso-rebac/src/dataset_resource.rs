// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use kamu_auth_rebac::AccountToDatasetRelation;
use oso::PolarClass;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ROLE_READER: &str = "Reader";
const ROLE_EDITOR: &str = "Editor";
const ROLE_MAINTAINER: &str = "Maintainer";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(PolarClass, Debug, Clone)]
pub struct DatasetResource {
    #[polar(attribute)]
    pub owner_account_id: String,
    #[polar(attribute)]
    pub allows_public_read: bool,
    #[polar(attribute)]
    pub authorized_users: HashMap<String, &'static str>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetResource {
    pub fn new(owner_account_id: &odf::AccountID, allows_public_read: bool) -> Self {
        Self {
            owner_account_id: owner_account_id.to_string(),
            allows_public_read,
            authorized_users: HashMap::new(),
        }
    }

    pub fn authorize_account(
        &mut self,
        account_id: &odf::AccountID,
        relation: AccountToDatasetRelation,
    ) {
        let role = match relation {
            AccountToDatasetRelation::Reader => ROLE_READER,
            AccountToDatasetRelation::Editor => ROLE_EDITOR,
            AccountToDatasetRelation::Maintainer => ROLE_MAINTAINER,
        };

        self.authorized_users.insert(account_id.to_string(), role);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for DatasetResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset(owner_account_id='{}', allows_public_read={}, num_authorizations={})",
            &self.owner_account_id,
            self.allows_public_read,
            self.authorized_users.len(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
