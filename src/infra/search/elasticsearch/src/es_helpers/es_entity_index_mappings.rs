// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{SearchEntitySchema, SearchSchemaFieldRole};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FIELD_SUFFIX_KEYWORD: &str = "keyword";
pub const FIELD_SUFFIX_NGRAM: &str = "ngram";
pub const FIELD_SUFFIX_SUBSTR: &str = "substr";
pub const FIELD_SUFFIX_TOKENS: &str = "tokens";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticsearchIndexMappings {
    pub mappings_json: serde_json::Value,
    pub mappings_hash: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ElasticsearchIndexMappings {
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
                "kamu_english_stop": {
                    "type": "stop",
                    "stopwords": [
                        // a, an, and, are, as, at, be, but, by, for, if, in, into,
                        // is, it, no, not, of, on, or, such, that, the, their, then,
                        // there, these, they, this, to, was, will, with
                        "_english_",
                        "than",
                        "then",
                        "however",
                        "therefore",
                        "thus",
                        "also",
                        "just",
                        "very",
                        "quite"
                    ]
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
                        "kamu_english_stop",
                        "english_stemmer",  // keep after stop to avoid stemming stop words
                    ],
                },
            }
        })
    }

    pub fn from_entity_schema(
        entity_schema: &SearchEntitySchema,
        embedding_dimensions: usize,
    ) -> Self {
        let mut mappings = serde_json::Map::new();
        for field in entity_schema.fields {
            let field_mapping = match field.role {
                SearchSchemaFieldRole::Identifier {
                    hierarchical,
                    enable_edge_ngrams,
                    enable_inner_ngrams,
                } => Self::map_identifier_field(
                    hierarchical,
                    enable_edge_ngrams,
                    enable_inner_ngrams,
                ),

                SearchSchemaFieldRole::Name => Self::map_name_field(),

                SearchSchemaFieldRole::Description { add_keyword } => {
                    Self::map_description_field(add_keyword)
                }

                SearchSchemaFieldRole::Prose => Self::map_prose_field(),

                SearchSchemaFieldRole::Keyword => Self::map_keyword_field(),

                SearchSchemaFieldRole::DateTime => serde_json::json!({
                    "type": "date"
                }),

                SearchSchemaFieldRole::Boolean => serde_json::json!({
                    "type": "boolean"
                }),

                SearchSchemaFieldRole::Integer => serde_json::json!({
                    "type": "integer"
                }),

                SearchSchemaFieldRole::UnprocessedObject => serde_json::json!({
                    "type": "object",
                    "enabled": false
                }),
            };
            mappings.insert(field.path.to_string(), field_mapping);
        }

        mappings.insert(
            kamu_search::fields::TITLE.to_string(),
            serde_json::json!({
                "type": "alias",
                "path": entity_schema.title_field
            }),
        );

        if entity_schema.flags.enable_banning {
            mappings.insert(
                kamu_search::fields::IS_BANNED.to_string(),
                serde_json::json!({
                    "type": "boolean"
                }),
            );
        }

        if entity_schema.flags.enable_security {
            mappings.insert(
                kamu_search::fields::VISIBILITY.to_string(),
                Self::map_keyword_field(),
            );

            mappings.insert(
                kamu_search::fields::PRINCIPAL_IDS.to_string(),
                Self::map_keyword_field(),
            );
        }

        if entity_schema.flags.enable_embeddings {
            mappings.insert(
                kamu_search::fields::SEMANTIC_EMBEDDINGS.to_string(),
                Self::map_embedding_chunks_field(embedding_dimensions),
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

    fn map_description_field(add_keyword: bool) -> serde_json::Value {
        let mut base_mapping = serde_json::json!({
            "type": "text",
            "analyzer": "kamu_english_html",
            "search_analyzer": "kamu_english_html",
        });

        if add_keyword {
            let mut fields = serde_json::Map::new();
            fields.insert(
                FIELD_SUFFIX_KEYWORD.to_string(),
                serde_json::json!({
                    "type": "keyword",
                    "normalizer": "kamu_keyword_norm",
                    "ignore_above": 1024
                }),
            );
            base_mapping["fields"] = serde_json::Value::Object(fields);
        }

        base_mapping
    }

    fn map_prose_field() -> serde_json::Value {
        serde_json::json!({
            "type": "text",
            "analyzer": "kamu_english_html",
            "search_analyzer": "kamu_english_html",
            "term_vector": "with_positions_offsets"
        })
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

    fn map_embedding_chunks_field(embedding_dimensions: usize) -> serde_json::Value {
        serde_json::json!({
            "type": "nested",
            "properties": {
                "chunk_id": {
                    "type": "keyword"
                },
                "embedding": {
                    "type": "dense_vector",
                    "dims": embedding_dimensions,
                    // Elastic’s documentation explicitly recommends cosine for transformer-based text embeddings
                    "similarity": "cosine",
                    "index_options": {
                        // Index for approximate nearest neighbor search (ANN)
                        "type": "hnsw",
                        // Maximum number of outgoing connections per node in the HNSW graph.
                        // Higher values lead to better accuracy, but slow down  indexing and enlarge memory usage.
                        "m": 16,
                        // Size of the candidate pool / beam used while selecting those neighbors
                        "ef_construction": 128
                    }
                }
            },

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

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use kamu_search::{
        SearchEntitySchema,
        SearchEntitySchemaFlags,
        SearchEntitySchemaUpgradeMode,
        SearchSchemaField,
        SearchSchemaFieldRole,
    };
    use pretty_assertions::assert_eq;

    use super::*;

    const ALL_FIELDS: &[SearchSchemaField] = &[
        SearchSchemaField {
            path: "id",
            role: SearchSchemaFieldRole::Identifier {
                hierarchical: true,
                enable_edge_ngrams: true,
                enable_inner_ngrams: true,
            },
        },
        SearchSchemaField {
            path: "name",
            role: SearchSchemaFieldRole::Name,
        },
        SearchSchemaField {
            path: "description",
            role: SearchSchemaFieldRole::Description { add_keyword: true },
        },
        SearchSchemaField {
            path: "body",
            role: SearchSchemaFieldRole::Prose,
        },
        SearchSchemaField {
            path: "tag",
            role: SearchSchemaFieldRole::Keyword,
        },
        SearchSchemaField {
            path: "created_at",
            role: SearchSchemaFieldRole::DateTime,
        },
        SearchSchemaField {
            path: "is_active",
            role: SearchSchemaFieldRole::Boolean,
        },
        SearchSchemaField {
            path: "rank",
            role: SearchSchemaFieldRole::Integer,
        },
        SearchSchemaField {
            path: "metadata",
            role: SearchSchemaFieldRole::UnprocessedObject,
        },
    ];

    fn assert_json(actual: &serde_json::Value, expected_json: &str) {
        let expected: serde_json::Value = serde_json::from_str(expected_json).unwrap();
        assert_eq!(expected, *actual);
    }

    #[test]
    fn test_from_entity_schema_covers_all_identifier_and_description_variations() {
        let schema = SearchEntitySchema {
            schema_name: "id-variants",
            version: 1,
            upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
            fields: &[
                SearchSchemaField {
                    path: "id_none",
                    role: SearchSchemaFieldRole::Identifier {
                        hierarchical: false,
                        enable_edge_ngrams: false,
                        enable_inner_ngrams: false,
                    },
                },
                SearchSchemaField {
                    path: "id_tokens",
                    role: SearchSchemaFieldRole::Identifier {
                        hierarchical: true,
                        enable_edge_ngrams: false,
                        enable_inner_ngrams: false,
                    },
                },
                SearchSchemaField {
                    path: "id_edge",
                    role: SearchSchemaFieldRole::Identifier {
                        hierarchical: false,
                        enable_edge_ngrams: true,
                        enable_inner_ngrams: false,
                    },
                },
                SearchSchemaField {
                    path: "id_inner",
                    role: SearchSchemaFieldRole::Identifier {
                        hierarchical: false,
                        enable_edge_ngrams: false,
                        enable_inner_ngrams: true,
                    },
                },
                SearchSchemaField {
                    path: "id_tokens_edge",
                    role: SearchSchemaFieldRole::Identifier {
                        hierarchical: true,
                        enable_edge_ngrams: true,
                        enable_inner_ngrams: false,
                    },
                },
                SearchSchemaField {
                    path: "id_tokens_inner",
                    role: SearchSchemaFieldRole::Identifier {
                        hierarchical: true,
                        enable_edge_ngrams: false,
                        enable_inner_ngrams: true,
                    },
                },
                SearchSchemaField {
                    path: "id_edge_inner",
                    role: SearchSchemaFieldRole::Identifier {
                        hierarchical: false,
                        enable_edge_ngrams: true,
                        enable_inner_ngrams: true,
                    },
                },
                SearchSchemaField {
                    path: "id_all",
                    role: SearchSchemaFieldRole::Identifier {
                        hierarchical: true,
                        enable_edge_ngrams: true,
                        enable_inner_ngrams: true,
                    },
                },
                SearchSchemaField {
                    path: "desc_plain",
                    role: SearchSchemaFieldRole::Description { add_keyword: false },
                },
                SearchSchemaField {
                    path: "desc_keyword",
                    role: SearchSchemaFieldRole::Description { add_keyword: true },
                },
            ],
            title_field: "desc_plain",
            flags: SearchEntitySchemaFlags {
                enable_banning: false,
                enable_security: false,
                enable_embeddings: false,
            },
        };

        let mappings = ElasticsearchIndexMappings::from_entity_schema(&schema, 128);

        assert_json(
            &mappings.mappings_json,
            indoc!(
                r#"
                {
                  "properties": {
                    "id_none": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024
                    },
                    "id_tokens": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "tokens": {
                          "type": "text",
                          "analyzer": "kamu_ident_parts",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "id_edge": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "ngram": {
                          "type": "text",
                          "analyzer": "kamu_ident_edge_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "id_inner": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "substr": {
                          "type": "text",
                          "analyzer": "kamu_ident_inner_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "id_tokens_edge": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "tokens": {
                          "type": "text",
                          "analyzer": "kamu_ident_parts",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "ngram": {
                          "type": "text",
                          "analyzer": "kamu_ident_edge_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "id_tokens_inner": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "tokens": {
                          "type": "text",
                          "analyzer": "kamu_ident_parts",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "substr": {
                          "type": "text",
                          "analyzer": "kamu_ident_inner_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "id_edge_inner": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "ngram": {
                          "type": "text",
                          "analyzer": "kamu_ident_edge_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "substr": {
                          "type": "text",
                          "analyzer": "kamu_ident_inner_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "id_all": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "tokens": {
                          "type": "text",
                          "analyzer": "kamu_ident_parts",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "ngram": {
                          "type": "text",
                          "analyzer": "kamu_ident_edge_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "substr": {
                          "type": "text",
                          "analyzer": "kamu_ident_inner_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "desc_plain": {
                      "type": "text",
                      "analyzer": "kamu_english_html",
                      "search_analyzer": "kamu_english_html"
                    },
                    "desc_keyword": {
                      "type": "text",
                      "analyzer": "kamu_english_html",
                      "search_analyzer": "kamu_english_html",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "normalizer": "kamu_keyword_norm",
                          "ignore_above": 1024
                        }
                      }
                    },
                    "title": {
                      "type": "alias",
                      "path": "desc_plain"
                    }
                  }
                }
                "#
            ),
        );
        assert!(mappings.mappings_hash.starts_with("sha256:"));
    }

    #[test]
    fn test_from_entity_schema_covers_all_field_types_and_all_flag_variations() {
        let schema_no_flags = SearchEntitySchema {
            schema_name: "all-fields-no-flags",
            version: 1,
            upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
            fields: ALL_FIELDS,
            title_field: "name",
            flags: SearchEntitySchemaFlags {
                enable_banning: false,
                enable_security: false,
                enable_embeddings: false,
            },
        };

        let mappings_no_flags =
            ElasticsearchIndexMappings::from_entity_schema(&schema_no_flags, 384);
        assert_json(
            &mappings_no_flags.mappings_json,
            indoc!(
                r#"
                {
                  "properties": {
                    "id": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "tokens": {
                          "type": "text",
                          "analyzer": "kamu_ident_parts",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "ngram": {
                          "type": "text",
                          "analyzer": "kamu_ident_edge_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "substr": {
                          "type": "text",
                          "analyzer": "kamu_ident_inner_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "name": {
                      "type": "text",
                      "analyzer": "kamu_name",
                      "search_analyzer": "kamu_name",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "normalizer": "kamu_keyword_norm",
                          "ignore_above": 1024
                        },
                        "ngram": {
                          "type": "text",
                          "analyzer": "kamu_ident_edge_ngram",
                          "search_analyzer": "kamu_name"
                        }
                      }
                    },
                    "description": {
                      "type": "text",
                      "analyzer": "kamu_english_html",
                      "search_analyzer": "kamu_english_html",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "normalizer": "kamu_keyword_norm",
                          "ignore_above": 1024
                        }
                      }
                    },
                    "body": {
                      "type": "text",
                      "analyzer": "kamu_english_html",
                      "search_analyzer": "kamu_english_html",
                      "term_vector": "with_positions_offsets"
                    },
                    "tag": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 256
                    },
                    "created_at": {
                      "type": "date"
                    },
                    "is_active": {
                      "type": "boolean"
                    },
                    "rank": {
                      "type": "integer"
                    },
                    "metadata": {
                      "type": "object",
                      "enabled": false
                    },
                    "title": {
                      "type": "alias",
                      "path": "name"
                    }
                  }
                }
                "#
            ),
        );

        let schema_all_flags = SearchEntitySchema {
            schema_name: "all-fields-all-flags",
            version: 1,
            upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
            fields: ALL_FIELDS,
            title_field: "name",
            flags: SearchEntitySchemaFlags {
                enable_banning: true,
                enable_security: true,
                enable_embeddings: true,
            },
        };

        let mappings_all_flags =
            ElasticsearchIndexMappings::from_entity_schema(&schema_all_flags, 384);
        assert_json(
            &mappings_all_flags.mappings_json,
            indoc!(
                r#"
                {
                  "properties": {
                    "id": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 1024,
                      "fields": {
                        "tokens": {
                          "type": "text",
                          "analyzer": "kamu_ident_parts",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "ngram": {
                          "type": "text",
                          "analyzer": "kamu_ident_edge_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        },
                        "substr": {
                          "type": "text",
                          "analyzer": "kamu_ident_inner_ngram",
                          "search_analyzer": "kamu_ident_parts"
                        }
                      }
                    },
                    "name": {
                      "type": "text",
                      "analyzer": "kamu_name",
                      "search_analyzer": "kamu_name",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "normalizer": "kamu_keyword_norm",
                          "ignore_above": 1024
                        },
                        "ngram": {
                          "type": "text",
                          "analyzer": "kamu_ident_edge_ngram",
                          "search_analyzer": "kamu_name"
                        }
                      }
                    },
                    "description": {
                      "type": "text",
                      "analyzer": "kamu_english_html",
                      "search_analyzer": "kamu_english_html",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "normalizer": "kamu_keyword_norm",
                          "ignore_above": 1024
                        }
                      }
                    },
                    "body": {
                      "type": "text",
                      "analyzer": "kamu_english_html",
                      "search_analyzer": "kamu_english_html",
                      "term_vector": "with_positions_offsets"
                    },
                    "tag": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 256
                    },
                    "created_at": {
                      "type": "date"
                    },
                    "is_active": {
                      "type": "boolean"
                    },
                    "rank": {
                      "type": "integer"
                    },
                    "metadata": {
                      "type": "object",
                      "enabled": false
                    },
                    "title": {
                      "type": "alias",
                      "path": "name"
                    },
                    "is_banned": {
                      "type": "boolean"
                    },
                    "visibility": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 256
                    },
                    "principal_ids": {
                      "type": "keyword",
                      "normalizer": "kamu_keyword_norm",
                      "ignore_above": 256
                    },
                    "semantic_embeddings": {
                      "type": "nested",
                      "properties": {
                        "chunk_id": {
                          "type": "keyword"
                        },
                        "embedding": {
                          "type": "dense_vector",
                          "dims": 384,
                          "similarity": "cosine",
                          "index_options": {
                            "type": "hnsw",
                            "m": 16,
                            "ef_construction": 128
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ),
        );

        assert!(mappings_no_flags.mappings_hash.starts_with("sha256:"));
        assert!(mappings_all_flags.mappings_hash.starts_with("sha256:"));
        assert_ne!(
            mappings_no_flags.mappings_hash,
            mappings_all_flags.mappings_hash
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
