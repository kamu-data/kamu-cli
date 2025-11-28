// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetColumn {
    pub name: String,
    pub data_type_ddl: String,
}

impl DatasetColumn {
    pub fn int(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type_ddl: "INT".into(),
        }
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type_ddl: "STRING".into(),
        }
    }

    pub fn string_array(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type_ddl: "ARRAY<STRING>".into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
