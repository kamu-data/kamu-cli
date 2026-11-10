// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ResourceConditions, TypeRef, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sets or removes named conditions of a resource and leaves the others as
/// they are. Each key appears once, so the last change to it wins
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceConditionsPatch {
    changes: BTreeMap<TypeUri, ResourceConditionPatchChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceConditionPatchChange {
    Set(serde_json::Value),
    Remove,
}

impl ResourceConditionsPatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(mut self, key: TypeUri, value: serde_json::Value) -> Self {
        self.changes
            .insert(key, ResourceConditionPatchChange::Set(value));
        self
    }

    pub fn remove(mut self, key: TypeUri) -> Self {
        self.changes
            .insert(key, ResourceConditionPatchChange::Remove);
        self
    }

    pub fn changes(&self) -> impl Iterator<Item = (&TypeUri, &ResourceConditionPatchChange)> {
        self.changes.iter()
    }

    /// Applies the patch and returns the keys whose condition it changed:
    /// setting a condition to the value it has, or removing an absent one,
    /// changes nothing
    pub fn apply_to(&self, conditions: &mut ResourceConditions) -> BTreeSet<TypeUri> {
        let mut changed = BTreeSet::new();

        for (key, change) in &self.changes {
            let entry_key = TypeRef::Uri(key.clone());
            let previous = match change {
                ResourceConditionPatchChange::Set(value) => {
                    conditions.entries.insert(entry_key, value.clone())
                }
                ResourceConditionPatchChange::Remove => conditions.entries.remove(&entry_key),
            };

            let unchanged = match change {
                ResourceConditionPatchChange::Set(value) => previous.as_ref() == Some(value),
                ResourceConditionPatchChange::Remove => previous.is_none(),
            };
            if !unchanged {
                changed.insert(key.clone());
            }
        }

        changed
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::empty_resource_conditions;

    fn key(name: &str) -> TypeUri {
        TypeUri::new_unchecked(format!("https://test.kamu.dev/schemas/conditions/{name}"))
    }

    fn conditions(entries: &[(&str, serde_json::Value)]) -> ResourceConditions {
        let mut conditions = empty_resource_conditions();
        for (name, value) in entries {
            conditions
                .entries
                .insert(TypeRef::Uri(key(name)), value.clone());
        }
        conditions
    }

    #[test]
    fn patch_changes_only_the_named_conditions() {
        let mut current = conditions(&[
            ("Kept", serde_json::json!("a")),
            ("Replaced", serde_json::json!("b")),
            ("Removed", serde_json::json!("c")),
        ]);

        let changed = ResourceConditionsPatch::new()
            .set(key("Replaced"), serde_json::json!("b2"))
            .set(key("Added"), serde_json::json!("d"))
            .remove(key("Removed"))
            .apply_to(&mut current);

        assert_eq!(
            current,
            conditions(&[
                ("Kept", serde_json::json!("a")),
                ("Replaced", serde_json::json!("b2")),
                ("Added", serde_json::json!("d")),
            ])
        );
        assert_eq!(
            changed,
            [key("Added"), key("Removed"), key("Replaced")].into()
        );
    }

    #[test]
    fn patch_to_the_current_conditions_changes_nothing() {
        let mut current = conditions(&[("Kept", serde_json::json!("a"))]);

        let changed = ResourceConditionsPatch::new()
            .set(key("Kept"), serde_json::json!("a"))
            .remove(key("Absent"))
            .apply_to(&mut current);

        assert_eq!(current, conditions(&[("Kept", serde_json::json!("a"))]));
        assert_eq!(changed, BTreeSet::new());
    }

    #[test]
    fn last_change_to_a_key_wins() {
        let mut current = conditions(&[("Stage", serde_json::json!("a"))]);

        ResourceConditionsPatch::new()
            .set(key("Stage"), serde_json::json!("b"))
            .remove(key("Stage"))
            .apply_to(&mut current);

        assert_eq!(current, conditions(&[]));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
