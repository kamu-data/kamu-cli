// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{SearchEntitySchema, SearchSchemaFieldRole, TextBoostingOverrides};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PhraseSearchPolicyBuilder {}

impl PhraseSearchPolicyBuilder {
    pub fn build_policy(
        schema: &SearchEntitySchema,
        user_slop: u32,
        text_boosting_overrides: TextBoostingOverrides,
    ) -> PhraseSearchPolicy {
        let mut specs = Vec::new();

        for field in schema.fields {
            match &field.role {
                SearchSchemaFieldRole::Name => {
                    specs.push(PhraseSearchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: 8.0 * text_boosting_overrides.name_boost,
                        slop: std::cmp::min(user_slop, 1),
                    });
                }

                SearchSchemaFieldRole::Description { .. } => {
                    specs.push(PhraseSearchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: 4.0 * text_boosting_overrides.description_boost,
                        slop: std::cmp::min(user_slop, 2),
                    });
                }

                SearchSchemaFieldRole::Prose => {
                    specs.push(PhraseSearchFieldSpec {
                        field_name: field.path.to_string(),
                        boost: 1.0 * text_boosting_overrides.prose_boost,
                        slop: std::cmp::min(user_slop, 6),
                    });
                }

                SearchSchemaFieldRole::Boolean
                | SearchSchemaFieldRole::Integer
                | SearchSchemaFieldRole::DateTime
                | SearchSchemaFieldRole::Keyword
                | SearchSchemaFieldRole::Identifier { .. }
                | SearchSchemaFieldRole::UnprocessedObject => {
                    // No phrase matching
                }
            }
        }

        PhraseSearchPolicy { specs }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PhraseSearchPolicy {
    pub specs: Vec<PhraseSearchFieldSpec>,
}

impl PhraseSearchPolicy {
    pub fn merge(policies: &[PhraseSearchPolicy]) -> PhraseSearchPolicy {
        use std::collections::HashMap;

        struct FieldAcc {
            boost: f32,
            slop: u32,
        }

        let mut field_map: HashMap<String, FieldAcc> = HashMap::new();

        for policy in policies {
            for spec in &policy.specs {
                field_map
                    .entry(spec.field_name.clone())
                    .and_modify(|acc| {
                        acc.boost = acc.boost.max(spec.boost);
                        acc.slop = acc.slop.max(spec.slop);
                    })
                    .or_insert(FieldAcc {
                        boost: spec.boost,
                        slop: spec.slop,
                    });
            }
        }

        let specs = field_map
            .into_iter()
            .map(|(field_name, acc)| PhraseSearchFieldSpec {
                field_name,
                boost: acc.boost,
                slop: acc.slop,
            })
            .collect();

        PhraseSearchPolicy { specs }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PhraseSearchFieldSpec {
    pub field_name: String, // might include nested fields
    pub boost: f32,
    pub slop: u32,
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
            path: "description",
            role: SearchSchemaFieldRole::Description { add_keyword: false },
        },
        SearchSchemaField {
            path: "body",
            role: SearchSchemaFieldRole::Prose,
        },
        SearchSchemaField {
            path: "id",
            role: SearchSchemaFieldRole::Identifier {
                hierarchical: true,
                enable_edge_ngrams: true,
                enable_inner_ngrams: true,
            },
        },
        SearchSchemaField {
            path: "kind",
            role: SearchSchemaFieldRole::Keyword,
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

    fn as_map(policy: &PhraseSearchPolicy) -> HashMap<&str, (f32, u32)> {
        policy
            .specs
            .iter()
            .map(|s| (s.field_name.as_str(), (s.boost, s.slop)))
            .collect()
    }

    fn assert_boost_eq(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "actual={actual}, expected={expected}"
        );
    }

    #[test]
    fn test_build_policy_respects_role_selection_and_slop_caps() {
        let policy = PhraseSearchPolicyBuilder::build_policy(
            &make_schema(),
            10,
            TextBoostingOverrides {
                name_boost: 1.5,
                description_boost: 2.0,
                prose_boost: 0.5,
                identifier_boost: 3.0,
            },
        );

        let map = as_map(&policy);

        let (name_boost, name_slop) = map.get("name").expect("name policy");
        assert_boost_eq(*name_boost, 12.0);
        assert_eq!(*name_slop, 1);

        let (description_boost, description_slop) =
            map.get("description").expect("description policy");
        assert_boost_eq(*description_boost, 8.0);
        assert_eq!(*description_slop, 2);

        let (body_boost, body_slop) = map.get("body").expect("body policy");
        assert_boost_eq(*body_boost, 0.5);
        assert_eq!(*body_slop, 6);

        assert!(!map.contains_key("id"));
        assert!(!map.contains_key("kind"));
    }

    #[test]
    fn test_merge_uses_max_boost_and_max_slop() {
        let merged = PhraseSearchPolicy::merge(&[
            PhraseSearchPolicy {
                specs: vec![
                    PhraseSearchFieldSpec {
                        field_name: "name".to_string(),
                        boost: 5.0,
                        slop: 1,
                    },
                    PhraseSearchFieldSpec {
                        field_name: "body".to_string(),
                        boost: 1.0,
                        slop: 4,
                    },
                ],
            },
            PhraseSearchPolicy {
                specs: vec![
                    PhraseSearchFieldSpec {
                        field_name: "name".to_string(),
                        boost: 8.0,
                        slop: 0,
                    },
                    PhraseSearchFieldSpec {
                        field_name: "body".to_string(),
                        boost: 0.8,
                        slop: 6,
                    },
                ],
            },
        ]);

        let map = as_map(&merged);
        let (name_boost, name_slop) = map.get("name").expect("name policy");
        assert_boost_eq(*name_boost, 8.0);
        assert_eq!(*name_slop, 1);

        let (body_boost, body_slop) = map.get("body").expect("body policy");
        assert_boost_eq(*body_boost, 1.0);
        assert_eq!(*body_slop, 6);
    }
}
