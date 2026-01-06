// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use nonempty::NonEmpty;

use crate::MoleculeAccessLevelRule;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn molecule_fields_filter(
    by_ipnft_uids: Option<Vec<String>>,
    by_tags: Option<Vec<String>>,
    by_categories: Option<Vec<String>>,
) -> Option<kamu_datasets::ExtraDataFieldsFilter> {
    use kamu_datasets::ExtraDataFieldFilter as Filter;

    let maybe_ipnft_uids_filter = by_ipnft_uids.and_then(|values| {
        NonEmpty::from_vec(values).map(|values| Filter {
            field_name: "ipnft_uid".to_string(),
            values,
            is_array: false,
        })
    });
    let maybe_tags_filter = by_tags.and_then(|values| {
        NonEmpty::from_vec(values).map(|values| Filter {
            field_name: "tags".to_string(),
            values,
            is_array: true,
        })
    });
    let maybe_categories_filter = by_categories.and_then(|values| {
        NonEmpty::from_vec(values).map(|values| Filter {
            field_name: "categories".to_string(),
            values,
            is_array: true,
        })
    });

    let filters = maybe_ipnft_uids_filter
        .into_iter()
        .chain(maybe_tags_filter)
        .chain(maybe_categories_filter)
        .collect::<Vec<_>>();

    NonEmpty::from_vec(filters)
}

pub fn normalize_access_level_rules(
    by_access_levels: Option<Vec<String>>,
    by_access_level_rules: Option<Vec<MoleculeAccessLevelRule>>,
) -> Vec<MoleculeAccessLevelRule> {
    let mut rules = Vec::new();

    if let Some(access_levels) = by_access_levels
        && let Some(access_levels) = NonEmpty::from_vec(access_levels)
    {
        rules.push(MoleculeAccessLevelRule {
            ipnft_uid: None,
            access_levels: access_levels.into(),
        });
    }

    if let Some(mut access_rules) = by_access_level_rules {
        access_rules.retain(|rule| !rule.access_levels.is_empty());
        rules.extend(access_rules);
    }

    let mut deduplicated_rules: Vec<MoleculeAccessLevelRule> = Vec::new();

    for rule in rules {
        let mut unique_access_levels = Vec::new();
        let mut seen_levels = HashSet::new();

        for access_level in rule.access_levels {
            if seen_levels.contains(access_level.as_str()) {
                continue;
            }

            seen_levels.insert(access_level.clone());
            unique_access_levels.push(access_level);
        }

        if let Some(existing_rule) = deduplicated_rules
            .iter_mut()
            .find(|existing_rule| existing_rule.ipnft_uid == rule.ipnft_uid)
        {
            let mut existing_levels = existing_rule
                .access_levels
                .iter()
                .cloned()
                .collect::<HashSet<_>>();

            for access_level in unique_access_levels {
                if existing_levels.contains(access_level.as_str()) {
                    continue;
                }

                existing_levels.insert(access_level.clone());
                existing_rule.access_levels.push(access_level);
            }
        } else {
            deduplicated_rules.push(MoleculeAccessLevelRule {
                ipnft_uid: rule.ipnft_uid,
                access_levels: unique_access_levels,
            });
        }
    }

    deduplicated_rules
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_access_level_rules_dedups_and_merges_by_ipnft() {
        let rules = normalize_access_level_rules(
            Some(vec!["public".to_string(), "public".to_string()]),
            Some(vec![
                MoleculeAccessLevelRule {
                    ipnft_uid: None,
                    access_levels: vec!["private".to_string(), "public".to_string()],
                },
                MoleculeAccessLevelRule {
                    ipnft_uid: Some("ipnft-1".to_string()),
                    access_levels: vec!["project".to_string()],
                },
                MoleculeAccessLevelRule {
                    ipnft_uid: Some("ipnft-1".to_string()),
                    access_levels: vec!["team".to_string(), "project".to_string()],
                },
            ]),
        );

        assert_eq!(
            rules,
            vec![
                MoleculeAccessLevelRule {
                    ipnft_uid: None,
                    access_levels: vec!["public".to_string(), "private".to_string()],
                },
                MoleculeAccessLevelRule {
                    ipnft_uid: Some("ipnft-1".to_string()),
                    access_levels: vec!["project".to_string(), "team".to_string()],
                }
            ]
        );
    }
}
