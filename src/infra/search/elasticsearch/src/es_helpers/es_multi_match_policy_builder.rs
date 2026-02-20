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

const FULL_TEXT_NAME_BOOST_COEFF: f32 = 6.0;
const FULL_TEXT_NAME_NGRAM_BOOST_COEFF: f32 = 1.5;
const FULL_TEXT_IDENTIFIER_MAIN_BOOST_COEFF: f32 = 4.0;
const FULL_TEXT_IDENTIFIER_NGRAM_BOOST_COEFF: f32 = 1.0;
const FULL_TEXT_IDENTIFIER_SUBSTR_BOOST_COEFF: f32 = 0.3;
const FULL_TEXT_DESCRIPTION_BOOST_COEFF: f32 = 3.5;
const FULL_TEXT_PROSE_BOOST_COEFF: f32 = 1.0;

const AUTOCOMPLETE_NAME_NGRAM_BOOST_COEFF: f32 = 8.0;
const AUTOCOMPLETE_NAME_BOOST_COEFF: f32 = 2.0;
const AUTOCOMPLETE_IDENTIFIER_NGRAM_BOOST_COEFF: f32 = 5.0;
const AUTOCOMPLETE_IDENTIFIER_MAIN_BOOST_COEFF: f32 = 1.0;

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
                        boost: FULL_TEXT_NAME_BOOST_COEFF * text_boosting_overrides.name_boost,
                    });
                    specs.push(MultiMatchFieldSpec {
                        field_name: format!("{}.{}", field.path, FIELD_SUFFIX_NGRAM),
                        boost: FULL_TEXT_NAME_NGRAM_BOOST_COEFF
                            * text_boosting_overrides.name_boost,
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
                            boost: FULL_TEXT_IDENTIFIER_MAIN_BOOST_COEFF
                                * text_boosting_overrides.identifier_boost,
                        });
                    } else {
                        specs.push(MultiMatchFieldSpec {
                            field_name: field.path.to_string(),
                            boost: FULL_TEXT_IDENTIFIER_MAIN_BOOST_COEFF
                                * text_boosting_overrides.identifier_boost,
                        });
                    }

                    if *enable_edge_ngrams {
                        specs.push(MultiMatchFieldSpec {
                            field_name: format!("{}.{}", field.path, FIELD_SUFFIX_NGRAM),
                            boost: FULL_TEXT_IDENTIFIER_NGRAM_BOOST_COEFF
                                * text_boosting_overrides.identifier_boost,
                        });
                    }

                    if *enable_inner_ngrams {
                        specs.push(MultiMatchFieldSpec {
                            field_name: format!("{}.{}", field.path, FIELD_SUFFIX_SUBSTR),
                            boost: FULL_TEXT_IDENTIFIER_SUBSTR_BOOST_COEFF
                                * text_boosting_overrides.identifier_boost,
                        });
                    }
                }

                SearchSchemaFieldRole::Description { .. } => {
                    specs.push(MultiMatchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: FULL_TEXT_DESCRIPTION_BOOST_COEFF
                            * text_boosting_overrides.description_boost,
                    });
                }

                SearchSchemaFieldRole::Prose => {
                    specs.push(MultiMatchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: FULL_TEXT_PROSE_BOOST_COEFF * text_boosting_overrides.prose_boost,
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
                        boost: AUTOCOMPLETE_NAME_NGRAM_BOOST_COEFF
                            * text_boosting_overrides.name_boost,
                    });
                    specs.push(MultiMatchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: AUTOCOMPLETE_NAME_BOOST_COEFF * text_boosting_overrides.name_boost,
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
                            boost: AUTOCOMPLETE_IDENTIFIER_NGRAM_BOOST_COEFF
                                * text_boosting_overrides.identifier_boost,
                        });
                    }
                    if *hierarchical {
                        specs.push(MultiMatchFieldSpec {
                            field_name: format!("{}.{}", field.path, FIELD_SUFFIX_TOKENS),
                            boost: AUTOCOMPLETE_IDENTIFIER_MAIN_BOOST_COEFF
                                * text_boosting_overrides.identifier_boost,
                        });
                    } else {
                        specs.push(MultiMatchFieldSpec {
                            field_name: field.path.to_string(),
                            boost: AUTOCOMPLETE_IDENTIFIER_MAIN_BOOST_COEFF
                                * text_boosting_overrides.identifier_boost,
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

    use kamu_search::*;

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
        SearchSchemaField {
            path: "kind",
            role: SearchSchemaFieldRole::Keyword,
        },
        SearchSchemaField {
            path: "is_public",
            role: SearchSchemaFieldRole::Boolean,
        },
        SearchSchemaField {
            path: "rank",
            role: SearchSchemaFieldRole::Integer,
        },
        SearchSchemaField {
            path: "raw_payload",
            role: SearchSchemaFieldRole::UnprocessedObject,
        },
    ];

    const FULL_TEXT_EXCLUDED_FIELDS: &[&str] =
        &["created_at", "kind", "is_public", "rank", "raw_payload"];

    const AUTOCOMPLETE_EXCLUDED_FIELDS: &[&str] = &[
        "description",
        "body",
        "created_at",
        "kind",
        "is_public",
        "rank",
        "raw_payload",
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
        const NAME_BOOST_OVERRIDE: f32 = 2.0;
        const IDENTIFIER_BOOST_OVERRIDE: f32 = 1.5;
        const DESCRIPTION_BOOST_OVERRIDE: f32 = 1.2;
        const PROSE_BOOST_OVERRIDE: f32 = 0.5;

        let policy = MultiMatchPolicyBuilder::build_full_text_policy(
            &make_schema(),
            TextBoostingOverrides {
                name_boost: NAME_BOOST_OVERRIDE,
                identifier_boost: IDENTIFIER_BOOST_OVERRIDE,
                description_boost: DESCRIPTION_BOOST_OVERRIDE,
                prose_boost: PROSE_BOOST_OVERRIDE,
            },
        );
        let map = as_boost_map(&policy);

        assert_boost_eq(
            *map.get("name").expect("name boost"),
            FULL_TEXT_NAME_BOOST_COEFF * NAME_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("name.ngram").expect("name ngram boost"),
            FULL_TEXT_NAME_NGRAM_BOOST_COEFF * NAME_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.tokens").expect("id_hier tokens boost"),
            FULL_TEXT_IDENTIFIER_MAIN_BOOST_COEFF * IDENTIFIER_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.ngram").expect("id_hier ngram boost"),
            FULL_TEXT_IDENTIFIER_NGRAM_BOOST_COEFF * IDENTIFIER_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.substr").expect("id_hier substr boost"),
            FULL_TEXT_IDENTIFIER_SUBSTR_BOOST_COEFF * IDENTIFIER_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_flat").expect("id_flat boost"),
            FULL_TEXT_IDENTIFIER_MAIN_BOOST_COEFF * IDENTIFIER_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("description").expect("description boost"),
            FULL_TEXT_DESCRIPTION_BOOST_COEFF * DESCRIPTION_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("body").expect("body boost"),
            FULL_TEXT_PROSE_BOOST_COEFF * PROSE_BOOST_OVERRIDE,
        );

        for field_name in FULL_TEXT_EXCLUDED_FIELDS {
            assert!(
                !map.contains_key(field_name),
                "unexpected policy for {field_name}"
            );
        }
    }

    #[test]
    fn test_build_full_text_policy_without_overrides() {
        const DEFAULT_BOOST_OVERRIDE: f32 = 1.0;

        let policy = MultiMatchPolicyBuilder::build_full_text_policy(
            &make_schema(),
            TextBoostingOverrides::default(),
        );
        let map = as_boost_map(&policy);

        assert_boost_eq(
            *map.get("name").expect("name boost"),
            FULL_TEXT_NAME_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("name.ngram").expect("name ngram boost"),
            FULL_TEXT_NAME_NGRAM_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.tokens").expect("id_hier tokens boost"),
            FULL_TEXT_IDENTIFIER_MAIN_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.ngram").expect("id_hier ngram boost"),
            FULL_TEXT_IDENTIFIER_NGRAM_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.substr").expect("id_hier substr boost"),
            FULL_TEXT_IDENTIFIER_SUBSTR_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_flat").expect("id_flat boost"),
            FULL_TEXT_IDENTIFIER_MAIN_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("description").expect("description boost"),
            FULL_TEXT_DESCRIPTION_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("body").expect("body boost"),
            FULL_TEXT_PROSE_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );

        for field_name in FULL_TEXT_EXCLUDED_FIELDS {
            assert!(
                !map.contains_key(field_name),
                "unexpected policy for {field_name}"
            );
        }
    }

    #[test]
    fn test_build_autocomplete_policy() {
        const DEFAULT_BOOST_OVERRIDE: f32 = 1.0;

        let policy = MultiMatchPolicyBuilder::build_autocomplete_policy(
            &make_schema(),
            TextBoostingOverrides::default(),
        );
        let map = as_boost_map(&policy);

        assert_boost_eq(
            *map.get("name.ngram").expect("name.ngram boost"),
            AUTOCOMPLETE_NAME_NGRAM_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("name").expect("name boost"),
            AUTOCOMPLETE_NAME_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.ngram").expect("id_hier ngram boost"),
            AUTOCOMPLETE_IDENTIFIER_NGRAM_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.tokens").expect("id_hier tokens boost"),
            AUTOCOMPLETE_IDENTIFIER_MAIN_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_flat").expect("id_flat boost"),
            AUTOCOMPLETE_IDENTIFIER_MAIN_BOOST_COEFF * DEFAULT_BOOST_OVERRIDE,
        );

        for field_name in AUTOCOMPLETE_EXCLUDED_FIELDS {
            assert!(
                !map.contains_key(field_name),
                "unexpected policy for {field_name}"
            );
        }
    }

    #[test]
    fn test_build_autocomplete_policy_with_overrides() {
        const NAME_BOOST_OVERRIDE: f32 = 1.7;
        const IDENTIFIER_BOOST_OVERRIDE: f32 = 2.5;
        const DESCRIPTION_BOOST_OVERRIDE: f32 = 0.3;
        const PROSE_BOOST_OVERRIDE: f32 = 0.2;

        let policy = MultiMatchPolicyBuilder::build_autocomplete_policy(
            &make_schema(),
            TextBoostingOverrides {
                name_boost: NAME_BOOST_OVERRIDE,
                identifier_boost: IDENTIFIER_BOOST_OVERRIDE,
                description_boost: DESCRIPTION_BOOST_OVERRIDE,
                prose_boost: PROSE_BOOST_OVERRIDE,
            },
        );
        let map = as_boost_map(&policy);

        assert_boost_eq(
            *map.get("name.ngram").expect("name.ngram boost"),
            AUTOCOMPLETE_NAME_NGRAM_BOOST_COEFF * NAME_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("name").expect("name boost"),
            AUTOCOMPLETE_NAME_BOOST_COEFF * NAME_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.ngram").expect("id_hier ngram boost"),
            AUTOCOMPLETE_IDENTIFIER_NGRAM_BOOST_COEFF * IDENTIFIER_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_hier.tokens").expect("id_hier tokens boost"),
            AUTOCOMPLETE_IDENTIFIER_MAIN_BOOST_COEFF * IDENTIFIER_BOOST_OVERRIDE,
        );
        assert_boost_eq(
            *map.get("id_flat").expect("id_flat boost"),
            AUTOCOMPLETE_IDENTIFIER_MAIN_BOOST_COEFF * IDENTIFIER_BOOST_OVERRIDE,
        );

        for field_name in AUTOCOMPLETE_EXCLUDED_FIELDS {
            assert!(
                !map.contains_key(field_name),
                "unexpected policy for {field_name}"
            );
        }
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
