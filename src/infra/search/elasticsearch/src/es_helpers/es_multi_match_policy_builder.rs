// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{SearchEntitySchema, SearchSchemaFieldRole, TextBoostingOverrides};

use crate::es_helpers::{FIELD_SUFFIX_NGRAM, FIELD_SUFFIX_SUBSTR, FIELD_SUFFIX_TOKENS};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MultiMatchPolicyBuilder {}

impl MultiMatchPolicyBuilder {
    pub fn build_full_text_policy(
        schema: &SearchEntitySchema,
        text_boosting_overrides: TextBoostingOverrides,
    ) -> MultiMatchPolicy {
        let mut specs = Vec::new();

        for field in schema.fields {
            match &field.role {
                SearchSchemaFieldRole::Name => {
                    specs.push(MultiMatchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: 6.0 * text_boosting_overrides.name_boost,
                    });
                    specs.push(MultiMatchFieldSpec {
                        field_name: format!("{}.{}", field.path, FIELD_SUFFIX_NGRAM),
                        boost: 1.5 * text_boosting_overrides.name_boost,
                    });
                }

                SearchSchemaFieldRole::Identifier {
                    hierarchical,
                    enable_edge_ngrams,
                    enable_inner_ngrams,
                } => {
                    if *hierarchical {
                        specs.push(MultiMatchFieldSpec {
                            field_name: format!("{}.{}", field.path, FIELD_SUFFIX_TOKENS),
                            boost: 4.0 * text_boosting_overrides.identifier_boost,
                        });
                    } else {
                        specs.push(MultiMatchFieldSpec {
                            field_name: field.path.to_string(),
                            boost: 4.0 * text_boosting_overrides.identifier_boost,
                        });
                    }

                    if *enable_edge_ngrams {
                        specs.push(MultiMatchFieldSpec {
                            field_name: format!("{}.{}", field.path, FIELD_SUFFIX_NGRAM),
                            boost: 1.0 * text_boosting_overrides.identifier_boost,
                        });
                    }

                    if *enable_inner_ngrams {
                        specs.push(MultiMatchFieldSpec {
                            field_name: format!("{}.{}", field.path, FIELD_SUFFIX_SUBSTR),
                            boost: 0.3 * text_boosting_overrides.identifier_boost,
                        });
                    }
                }

                SearchSchemaFieldRole::Description { .. } => {
                    specs.push(MultiMatchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: 3.5 * text_boosting_overrides.description_boost,
                    });
                }

                SearchSchemaFieldRole::Prose => {
                    specs.push(MultiMatchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: 1.0 * text_boosting_overrides.prose_boost,
                    });
                }

                SearchSchemaFieldRole::Boolean
                | SearchSchemaFieldRole::Integer
                | SearchSchemaFieldRole::DateTime
                | SearchSchemaFieldRole::Keyword
                | SearchSchemaFieldRole::UnprocessedObject => {
                    // Skip non-text fields
                }
            }
        }

        MultiMatchPolicy { specs }
    }

    pub fn build_autocomplete_policy(
        schema: &SearchEntitySchema,
        text_boosting_overrides: TextBoostingOverrides,
    ) -> MultiMatchPolicy {
        let mut specs = Vec::new();

        for field in schema.fields {
            match &field.role {
                SearchSchemaFieldRole::Name => {
                    specs.push(MultiMatchFieldSpec {
                        field_name: format!("{}.{}", field.path, FIELD_SUFFIX_NGRAM),
                        boost: 8.0 * text_boosting_overrides.name_boost,
                    });
                    specs.push(MultiMatchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: 2.0 * text_boosting_overrides.name_boost,
                    });
                }

                SearchSchemaFieldRole::Identifier {
                    hierarchical,
                    enable_edge_ngrams,
                    ..
                } => {
                    if *enable_edge_ngrams {
                        specs.push(MultiMatchFieldSpec {
                            field_name: format!("{}.{}", field.path, FIELD_SUFFIX_NGRAM),
                            boost: 5.0 * text_boosting_overrides.identifier_boost,
                        });
                    }
                    if *hierarchical {
                        specs.push(MultiMatchFieldSpec {
                            field_name: format!("{}.{}", field.path, FIELD_SUFFIX_TOKENS),
                            boost: 1.0 * text_boosting_overrides.identifier_boost,
                        });
                    } else {
                        specs.push(MultiMatchFieldSpec {
                            field_name: field.path.to_string(),
                            boost: 1.0 * text_boosting_overrides.identifier_boost,
                        });
                    }
                }

                // Note: no autocomplete on prose and description
                SearchSchemaFieldRole::Description { .. }
                | SearchSchemaFieldRole::Prose
                | SearchSchemaFieldRole::Boolean
                | SearchSchemaFieldRole::Integer
                | SearchSchemaFieldRole::DateTime
                | SearchSchemaFieldRole::Keyword
                | SearchSchemaFieldRole::UnprocessedObject => {
                    // Skip non-autocomplete fields
                }
            }
        }

        MultiMatchPolicy { specs }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct MultiMatchPolicy {
    pub specs: Vec<MultiMatchFieldSpec>,
}

impl MultiMatchPolicy {
    pub fn merge(policies: &[MultiMatchPolicy]) -> MultiMatchPolicy {
        use std::collections::HashMap;

        let mut field_boost_map: HashMap<String, f32> = HashMap::new();

        for policy in policies {
            for spec in &policy.specs {
                field_boost_map
                    .entry(spec.field_name.clone())
                    .and_modify(|boost| *boost = boost.max(spec.boost))
                    .or_insert(spec.boost);
            }
        }

        let specs = field_boost_map
            .into_iter()
            .map(|(field_name, boost)| MultiMatchFieldSpec { field_name, boost })
            .collect();

        MultiMatchPolicy { specs }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct MultiMatchFieldSpec {
    pub field_name: String, // might include nested fields
    pub boost: f32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use kamu_search::{
        SearchEntitySchema,
        SearchEntitySchemaFlags,
        SearchEntitySchemaUpgradeMode,
        SearchSchemaField,
        SearchSchemaFieldRole,
        TextBoostingOverrides,
    };

    use super::*;

    const TEST_FIELDS: &[SearchSchemaField] = &[
        SearchSchemaField {
            path: "name",
            role: SearchSchemaFieldRole::Name,
        },
        SearchSchemaField {
            path: "id_hier",
            role: SearchSchemaFieldRole::Identifier {
                hierarchical: true,
                enable_edge_ngrams: true,
                enable_inner_ngrams: true,
            },
        },
        SearchSchemaField {
            path: "id_flat",
            role: SearchSchemaFieldRole::Identifier {
                hierarchical: false,
                enable_edge_ngrams: false,
                enable_inner_ngrams: false,
            },
        },
        SearchSchemaField {
            path: "description",
            role: SearchSchemaFieldRole::Description { add_keyword: false },
        },
        SearchSchemaField {
            path: "body",
            role: SearchSchemaFieldRole::Prose,
        },
        SearchSchemaField {
            path: "created_at",
            role: SearchSchemaFieldRole::DateTime,
        },
    ];

    fn make_schema() -> SearchEntitySchema {
        SearchEntitySchema {
            schema_name: "schema",
            version: 1,
            upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
            fields: TEST_FIELDS,
            title_field: "name",
            flags: SearchEntitySchemaFlags {
                enable_banning: false,
                enable_security: false,
                enable_embeddings: false,
            },
        }
    }

    fn as_boost_map(policy: &MultiMatchPolicy) -> HashMap<&str, f32> {
        policy
            .specs
            .iter()
            .map(|s| (s.field_name.as_str(), s.boost))
            .collect()
    }

    fn assert_boost_eq(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "actual={actual}, expected={expected}"
        );
    }

    #[test]
    fn test_build_full_text_policy() {
        let policy = MultiMatchPolicyBuilder::build_full_text_policy(
            &make_schema(),
            TextBoostingOverrides {
                name_boost: 2.0,
                identifier_boost: 1.5,
                description_boost: 1.2,
                prose_boost: 0.5,
            },
        );
        let map = as_boost_map(&policy);

        assert_boost_eq(*map.get("name").expect("name boost"), 12.0);
        assert_boost_eq(*map.get("name.ngram").expect("name ngram boost"), 3.0);
        assert_boost_eq(
            *map.get("id_hier.tokens").expect("id_hier tokens boost"),
            6.0,
        );
        assert_boost_eq(*map.get("id_hier.ngram").expect("id_hier ngram boost"), 1.5);
        assert_boost_eq(
            *map.get("id_hier.substr").expect("id_hier substr boost"),
            0.45,
        );
        assert_boost_eq(*map.get("id_flat").expect("id_flat boost"), 6.0);
        assert_boost_eq(*map.get("description").expect("description boost"), 4.2);
        assert_boost_eq(*map.get("body").expect("body boost"), 0.5);
        assert!(!map.contains_key("created_at"));
    }

    #[test]
    fn test_build_autocomplete_policy() {
        let policy = MultiMatchPolicyBuilder::build_autocomplete_policy(
            &make_schema(),
            TextBoostingOverrides::default(),
        );
        let map = as_boost_map(&policy);

        assert_boost_eq(*map.get("name.ngram").expect("name.ngram boost"), 8.0);
        assert_boost_eq(*map.get("name").expect("name boost"), 2.0);
        assert_boost_eq(*map.get("id_hier.ngram").expect("id_hier ngram boost"), 5.0);
        assert_boost_eq(
            *map.get("id_hier.tokens").expect("id_hier tokens boost"),
            1.0,
        );
        assert_boost_eq(*map.get("id_flat").expect("id_flat boost"), 1.0);
        assert!(!map.contains_key("description"));
        assert!(!map.contains_key("body"));
    }

    #[test]
    fn test_merge_keeps_max_boost_per_field() {
        let merged = MultiMatchPolicy::merge(&[
            MultiMatchPolicy {
                specs: vec![
                    MultiMatchFieldSpec {
                        field_name: "name".to_string(),
                        boost: 2.0,
                    },
                    MultiMatchFieldSpec {
                        field_name: "description".to_string(),
                        boost: 1.0,
                    },
                ],
            },
            MultiMatchPolicy {
                specs: vec![
                    MultiMatchFieldSpec {
                        field_name: "name".to_string(),
                        boost: 7.0,
                    },
                    MultiMatchFieldSpec {
                        field_name: "body".to_string(),
                        boost: 0.5,
                    },
                ],
            },
        ]);

        let map = as_boost_map(&merged);
        assert_boost_eq(*map.get("name").expect("name boost"), 7.0);
        assert_boost_eq(*map.get("description").expect("description boost"), 1.0);
        assert_boost_eq(*map.get("body").expect("body boost"), 0.5);
    }
}
