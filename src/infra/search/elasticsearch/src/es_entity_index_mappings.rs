// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{FullTextSchemaFieldKind, FullTextSearchEntitySchema};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ElasticSearchIndexMappings {
    pub mappings_json: serde_json::Value,
    pub mappings_hash: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ElasticSearchIndexMappings {
    pub(crate) fn from_entity_schema(entity_schema: &FullTextSearchEntitySchema) -> Self {
        let mut mappings = serde_json::Map::new();
        for field in entity_schema.fields {
            let field_mapping = match field.kind {
                FullTextSchemaFieldKind::Text => serde_json::json!({"type": "text"}),
                FullTextSchemaFieldKind::Keyword => serde_json::json!({"type": "keyword"}),
                FullTextSchemaFieldKind::DateTime => serde_json::json!({"type": "date"}),
            };
            mappings.insert(field.path.to_string(), field_mapping);
        }

        let mappings_json = serde_json::json!({ "properties": mappings });
        let mappings_hash = Self::hash_json_normalized(&mappings_json);

        Self {
            mappings_json,
            mappings_hash,
        }
    }

    fn hash_json_normalized(value: &serde_json::Value) -> String {
        // Step 1: normalize to canonical JSON string
        let normalized = canonical_json::to_string(value).unwrap();

        // Step 2: hash it
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(normalized.as_bytes());
        let result = hasher.finalize();

        // Step 3: hex-encode
        format!("sha256:{}", hex::encode(result))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
