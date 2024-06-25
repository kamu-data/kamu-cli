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

////////////////////////////////////////////////////////////////////////////////

const ROLE_READER: &str = "Reader";
const ROLE_EDITOR: &str = "Editor";

#[derive(PolarClass, Debug, Clone)]
pub struct DatasetResource {
    #[polar(attribute)]
    pub created_by: String,
    #[polar(attribute)]
    pub allows_public_read: bool,
    #[polar(attribute)]
    pub authorized_users: HashMap<String, &'static str>,
}

////////////////////////////////////////////////////////////////////////////////

impl DatasetResource {
    pub fn new(created_by: &str, allows_public_read: bool) -> Self {
        Self {
            created_by: created_by.to_string(),
            allows_public_read,
            authorized_users: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn authorize_reader(&mut self, reader: &str) {
        self.authorized_users
            .insert(reader.to_string(), ROLE_READER);
    }

    #[allow(dead_code)]
    pub fn authorize_editor(&mut self, editor: &str) {
        self.authorized_users
            .insert(editor.to_string(), ROLE_EDITOR);
    }
}

////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for DatasetResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset(created_by='{}', allows_public_read={}, num_authorizations={})",
            &self.created_by,
            self.allows_public_read,
            self.authorized_users.len(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////
