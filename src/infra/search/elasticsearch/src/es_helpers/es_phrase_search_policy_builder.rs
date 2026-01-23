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

                SearchSchemaFieldRole::Description => {
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
                | SearchSchemaFieldRole::UnprocessedObject
                | SearchSchemaFieldRole::EmbeddingChunks => {
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
