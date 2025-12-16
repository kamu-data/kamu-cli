// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{
    FULL_TEXT_SEARCH_ALIAS_TITLE,
    FULL_TEXT_SEARCH_FIELD_IS_BANNED,
    FullTextSchemaFieldRole,
    FullTextSearchEntitySchema,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FIELD_SUFFIX_KEYWORD: &str = "keyword";
pub const FIELD_SUFFIX_NGRAM: &str = "ngram";
pub const FIELD_SUFFIX_SUBSTR: &str = "substr";
pub const FIELD_SUFFIX_TOKENS: &str = "tokens";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchIndexMappings {
    pub mappings_json: serde_json::Value,
    pub mappings_hash: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ElasticSearchIndexMappings {
    pub fn build_analysis_settings_json() -> serde_json::Value {
        serde_json::json!({
            "filter": {
                "kamu_edge_ngram": {
                    "type": "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 10,
                },
                "kamu_inner_ngram": {
                    "type": "ngram",
                    "min_gram": 3,
                    "max_gram": 6
                },
                "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english",
                },
                "english_stemmer": {
                    "type": "stemmer",
                    "language": "english",
                },
                "english_stop": {
                    "type": "stop",
                    "language": "english",
                }
            },
            "normalizer": {
                "kamu_keyword_norm": {
                    "type": "custom",
                    "char_filter": [],
                    "filter": [
                        "lowercase",
                        "asciifolding",
                    ]
                }
            },
            "tokenizer": {
                "kamu_ident_pattern": {
                    "type": "pattern",
                    "pattern": "[^A-Za-z0-9]+",
                }
            },
            "analyzer": {
                "kamu_ident_parts": {
                    "type": "custom",
                    "tokenizer": "kamu_ident_pattern",
                    "filter": [
                        "lowercase",
                        "asciifolding",
                    ],
                },
                "kamu_ident_edge_ngram": {
                    "type": "custom",
                    "tokenizer": "kamu_ident_pattern",
                    "filter": [
                        "lowercase",
                        "asciifolding",
                        "kamu_edge_ngram",
                    ],
                },
                "kamu_ident_inner_ngram": {
                    "type": "custom",
                    "tokenizer": "kamu_ident_pattern",
                    "filter": [
                        "lowercase",
                        "asciifolding",
                        "kamu_inner_ngram"
                    ]
                },
                "kamu_name": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "asciifolding"
                    ]
                },
                "kamu_english_html": {
                    "type": "custom",
                    "char_filter": [
                        "html_strip",
                    ],
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "asciifolding",
                        "english_possessive_stemmer",
                        "english_stemmer",
                        "english_stop",
                    ],
                },
            }
        })
    }

    pub fn from_entity_schema(entity_schema: &FullTextSearchEntitySchema) -> Self {
        let mut mappings = serde_json::Map::new();
        for field in entity_schema.fields {
            let field_mapping = match field.role {
                FullTextSchemaFieldRole::Identifier {
                    hierarchical,
                    enable_edge_ngrams,
                    enable_inner_ngrams,
                } => Self::map_identifier_field(
                    hierarchical,
                    enable_edge_ngrams,
                    enable_inner_ngrams,
                ),

                FullTextSchemaFieldRole::Name => Self::map_name_field(),

                FullTextSchemaFieldRole::Prose { enable_positions } => {
                    Self::map_prose_field(enable_positions)
                }

                FullTextSchemaFieldRole::Keyword => Self::map_keyword_field(),

                FullTextSchemaFieldRole::DateTime => serde_json::json!({
                    "type": "date"
                }),

                FullTextSchemaFieldRole::Boolean => serde_json::json!({
                    "type": "boolean"
                }),

                FullTextSchemaFieldRole::Integer => serde_json::json!({
                    "type": "integer"
                }),

                FullTextSchemaFieldRole::UnprocessedObject => serde_json::json!({
                    "type": "object",
                    "enabled": false
                }),
            };
            mappings.insert(field.path.to_string(), field_mapping);
        }

        mappings.insert(
            FULL_TEXT_SEARCH_ALIAS_TITLE.to_string(),
            serde_json::json!({
                "type": "alias",
                "path": entity_schema.title_field
            }),
        );

        if entity_schema.enable_banning {
            mappings.insert(
                FULL_TEXT_SEARCH_FIELD_IS_BANNED.to_string(),
                serde_json::json!({
                    "type": "boolean"
                }),
            );
        }

        let mappings_json = serde_json::json!({ "properties": mappings });
        let mappings_hash = Self::hash_json_normalized(&mappings_json);

        Self {
            mappings_json,
            mappings_hash,
        }
    }

    fn map_identifier_field(
        hierarchical: bool,
        edge_ngrams: bool,
        inner_ngrams: bool,
    ) -> serde_json::Value {
        let mut base_mapping = serde_json::json!({
            "type": "keyword",
            "normalizer": "kamu_keyword_norm",
            "ignore_above": 1024,
        });

        let mut fields = serde_json::Map::new();

        if hierarchical {
            fields.insert(
                FIELD_SUFFIX_TOKENS.to_string(),
                serde_json::json!({
                    "type": "text",
                    "analyzer": "kamu_ident_parts",
                    "search_analyzer": "kamu_ident_parts"
                }),
            );
        }

        if edge_ngrams {
            fields.insert(
                FIELD_SUFFIX_NGRAM.to_string(),
                serde_json::json!({
                    "type": "text",
                    "analyzer": "kamu_ident_edge_ngram",
                    "search_analyzer": "kamu_ident_parts"
                }),
            );
        }

        if inner_ngrams {
            fields.insert(
                FIELD_SUFFIX_SUBSTR.to_string(),
                serde_json::json!({
                    "type": "text",
                    "analyzer": "kamu_ident_inner_ngram",
                    "search_analyzer": "kamu_ident_parts"
                }),
            );
        }

        if !fields.is_empty() {
            base_mapping["fields"] = serde_json::Value::Object(fields);
        }

        base_mapping
    }

    fn map_prose_field(enable_positions: bool) -> serde_json::Value {
        let mut mapping = serde_json::json!({
            "type": "text",
            "analyzer": "kamu_english_html",
        });

        if enable_positions {
            mapping["term_vector"] =
                serde_json::Value::String("with_positions_offsets".to_string());
        }

        mapping
    }

    fn map_name_field() -> serde_json::Value {
        serde_json::json!({
            "type": "text",
            "analyzer": "kamu_name",
            "search_analyzer": "kamu_name",
            "fields": {
                FIELD_SUFFIX_KEYWORD: {
                    "type": "keyword",
                    "normalizer": "kamu_keyword_norm",
                    "ignore_above": 1024
                },
                FIELD_SUFFIX_NGRAM: {
                    "type": "text",
                    "analyzer": "kamu_ident_edge_ngram",
                    "search_analyzer": "kamu_name"
                }
            }
        })
    }

    fn map_keyword_field() -> serde_json::Value {
        serde_json::json!({
            "type": "keyword",
            "normalizer": "kamu_keyword_norm",
            "ignore_above": 256
        })
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
