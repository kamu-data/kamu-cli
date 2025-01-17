// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use oso::PolarClass;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ROLE_READER: &str = "Reader";
const ROLE_EDITOR: &str = "Editor";

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

    // TODO: Private Datasets: use for relations
    pub fn authorize_reader(&mut self, reader_account_id: &odf::AccountID) {
        self.authorized_users
            .insert(reader_account_id.to_string(), ROLE_READER);
    }

    pub fn authorize_editor(&mut self, editor_account_id: &odf::AccountID) {
        self.authorized_users
            .insert(editor_account_id.to_string(), ROLE_EDITOR);
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
