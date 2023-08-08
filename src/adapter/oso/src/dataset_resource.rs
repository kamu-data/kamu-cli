// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use oso::PolarClass;

///////////////////////////////////////////////////////////////////////////////

#[derive(PolarClass, Debug, Clone)]
pub struct DatasetResource {
    #[polar(attribute)]
    pub created_by: String,
    #[polar(attribute)]
    pub allows_public_read: bool,
    #[polar(attribute)]
    pub authorized_readers: HashSet<String>,
    #[polar(attribute)]
    pub authorized_editors: HashSet<String>,
}

///////////////////////////////////////////////////////////////////////////////

impl DatasetResource {
    pub fn new(created_by: &str, allows_public_read: bool) -> Self {
        Self {
            created_by: created_by.to_string(),
            allows_public_read,
            authorized_readers: HashSet::new(),
            authorized_editors: HashSet::new(),
        }
    }

    #[allow(dead_code)]
    pub fn authorize_reader(&mut self, reader: &str) {
        self.authorized_readers.insert(reader.to_string());
    }

    #[allow(dead_code)]
    pub fn authorize_editor(&mut self, editor: &str) {
        self.authorized_editors.insert(editor.to_string());
    }
}

///////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for DatasetResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset(created_by='{}', allows_public_read={}, num_readers={}, num_editors={})",
            &self.created_by,
            self.allows_public_read,
            self.authorized_readers.len(),
            self.authorized_editors.len()
        )
    }
}

///////////////////////////////////////////////////////////////////////////////
