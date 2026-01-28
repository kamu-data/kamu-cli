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

                SearchSchemaFieldRole::Description => {
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
                SearchSchemaFieldRole::Description
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
