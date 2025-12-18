// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod fields {
    pub const EVENT_TIME: &str = "event_time";
    pub const SYSTEM_TIME: &str = "system_time";
    pub const MOLECULE_ACCOUNT_ID: &str = "molecule_account_id";
    pub const IPNFT_UID: &str = "ipnft_uid";

    pub const PATH: &str = "path";
    pub const REF: &str = "ref";
    pub const VERSION: &str = "version";
    pub const CONTENT_TYPE: &str = "content_type";
    pub const CONTENT_HASH: &str = "content_hash";
    pub const CONTENT_LENGTH: &str = "content_length";

    pub const ACCESS_LEVEL: &str = "molecule_access_level";
    pub const CHANGE_BY: &str = "molecule_change_by";
    pub const DESCRIPTION: &str = "description";
    pub const TAGS: &str = "tags";
    pub const CATEGORIES: &str = "categories";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod field_definitions {

    use kamu_search::*;

    use super::fields;

    //

    pub const EVENT_TIME: FullTextSchemaField = FullTextSchemaField {
        path: fields::EVENT_TIME,
        role: FullTextSchemaFieldRole::DateTime,
    };

    pub const SYSTEM_TIME: FullTextSchemaField = FullTextSchemaField {
        path: fields::SYSTEM_TIME,
        role: FullTextSchemaFieldRole::DateTime,
    };

    pub const MOLECULE_ACCOUNT_ID: FullTextSchemaField = FullTextSchemaField {
        path: fields::MOLECULE_ACCOUNT_ID,
        role: FullTextSchemaFieldRole::Keyword,
    };

    pub const IPNFT_UID: FullTextSchemaField = FullTextSchemaField {
        path: fields::IPNFT_UID,
        role: FullTextSchemaFieldRole::Keyword,
    };

    //

    // Note: no PATH definition, varies depending on the index

    pub const REF: FullTextSchemaField = FullTextSchemaField {
        path: fields::REF,
        role: FullTextSchemaFieldRole::Keyword,
    };

    pub const VERSION: FullTextSchemaField = FullTextSchemaField {
        path: fields::VERSION,
        role: FullTextSchemaFieldRole::Integer,
    };

    pub const CONTENT_TYPE: FullTextSchemaField = FullTextSchemaField {
        path: fields::CONTENT_TYPE,
        role: FullTextSchemaFieldRole::Keyword,
    };

    pub const CONTENT_HASH: FullTextSchemaField = FullTextSchemaField {
        path: fields::CONTENT_HASH,
        role: FullTextSchemaFieldRole::Keyword,
    };

    pub const CONTENT_LENGTH: FullTextSchemaField = FullTextSchemaField {
        path: fields::CONTENT_LENGTH,
        role: FullTextSchemaFieldRole::Integer,
    };

    //

    pub const ACCESS_LEVEL: FullTextSchemaField = FullTextSchemaField {
        path: fields::ACCESS_LEVEL,
        role: FullTextSchemaFieldRole::Keyword,
    };

    pub const CHANGE_BY: FullTextSchemaField = FullTextSchemaField {
        path: fields::CHANGE_BY,
        role: FullTextSchemaFieldRole::Keyword,
    };

    pub const DESCRIPTION: FullTextSchemaField = FullTextSchemaField {
        path: fields::DESCRIPTION,
        role: FullTextSchemaFieldRole::Prose {
            enable_positions: true,
        },
    };

    pub const CATEGORIES: FullTextSchemaField = FullTextSchemaField {
        path: fields::CATEGORIES,
        role: FullTextSchemaFieldRole::Keyword,
    };

    pub const TAGS: FullTextSchemaField = FullTextSchemaField {
        path: fields::TAGS,
        role: FullTextSchemaFieldRole::Keyword,
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
