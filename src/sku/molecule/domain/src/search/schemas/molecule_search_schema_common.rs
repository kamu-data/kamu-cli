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

    pub const EVENT_TIME: SearchSchemaField = SearchSchemaField {
        path: fields::EVENT_TIME,
        role: SearchSchemaFieldRole::DateTime,
    };

    pub const SYSTEM_TIME: SearchSchemaField = SearchSchemaField {
        path: fields::SYSTEM_TIME,
        role: SearchSchemaFieldRole::DateTime,
    };

    pub const MOLECULE_ACCOUNT_ID: SearchSchemaField = SearchSchemaField {
        path: fields::MOLECULE_ACCOUNT_ID,
        role: SearchSchemaFieldRole::Keyword,
    };

    pub const IPNFT_UID: SearchSchemaField = SearchSchemaField {
        path: fields::IPNFT_UID,
        role: SearchSchemaFieldRole::Keyword,
    };

    //

    // Note: no PATH definition, varies depending on the index

    pub const REF: SearchSchemaField = SearchSchemaField {
        path: fields::REF,
        role: SearchSchemaFieldRole::Keyword,
    };

    pub const VERSION: SearchSchemaField = SearchSchemaField {
        path: fields::VERSION,
        role: SearchSchemaFieldRole::Integer,
    };

    pub const CONTENT_TYPE: SearchSchemaField = SearchSchemaField {
        path: fields::CONTENT_TYPE,
        role: SearchSchemaFieldRole::Keyword,
    };

    pub const CONTENT_HASH: SearchSchemaField = SearchSchemaField {
        path: fields::CONTENT_HASH,
        role: SearchSchemaFieldRole::Keyword,
    };

    pub const CONTENT_LENGTH: SearchSchemaField = SearchSchemaField {
        path: fields::CONTENT_LENGTH,
        role: SearchSchemaFieldRole::Integer,
    };

    //

    pub const ACCESS_LEVEL: SearchSchemaField = SearchSchemaField {
        path: fields::ACCESS_LEVEL,
        role: SearchSchemaFieldRole::Keyword,
    };

    pub const CHANGE_BY: SearchSchemaField = SearchSchemaField {
        path: fields::CHANGE_BY,
        role: SearchSchemaFieldRole::Keyword,
    };

    pub const DESCRIPTION: SearchSchemaField = SearchSchemaField {
        path: fields::DESCRIPTION,
        role: SearchSchemaFieldRole::Description { add_keyword: false },
    };

    pub const CATEGORIES: SearchSchemaField = SearchSchemaField {
        path: fields::CATEGORIES,
        role: SearchSchemaFieldRole::Keyword,
    };

    pub const TAGS: SearchSchemaField = SearchSchemaField {
        path: fields::TAGS,
        role: SearchSchemaFieldRole::Keyword,
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
