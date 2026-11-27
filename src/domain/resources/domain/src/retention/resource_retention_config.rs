// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::{ResourceRetentionPolicy, TypeName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The node's retention policies. A type's policy is, first found: its entry in
/// `types`, the type's own default, `default`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRetentionConfig {
    pub default: ResourceRetentionPolicy,
    pub types: BTreeMap<TypeName, ResourceRetentionPolicy>,
}

impl ResourceRetentionConfig {
    pub fn policy_for(
        &self,
        type_name: &TypeName,
        type_default: Option<ResourceRetentionPolicy>,
    ) -> ResourceRetentionPolicy {
        self.types
            .get(type_name)
            .copied()
            .or(type_default)
            .unwrap_or(self.default)
    }

    /// Fails on a type entry that names none of the registered types
    pub fn check_types<'a>(
        &self,
        registered: impl IntoIterator<Item = &'a TypeName>,
    ) -> Result<(), UnknownRetentionTypesError> {
        let mut registered = registered.into_iter().cloned().collect::<Vec<_>>();
        let unknown = self
            .types
            .keys()
            .filter(|type_name| !registered.contains(type_name))
            .cloned()
            .collect::<Vec<_>>();

        if unknown.is_empty() {
            Ok(())
        } else {
            registered.sort();
            Err(UnknownRetentionTypesError {
                unknown,
                registered,
            })
        }
    }
}

impl Default for ResourceRetentionConfig {
    fn default() -> Self {
        Self {
            default: ResourceRetentionPolicy::FRAMEWORK_DEFAULT,
            types: BTreeMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error(
    "Retention configured for unknown resource types: {}; known types: {}",
    join(.unknown),
    join(.registered)
)]
pub struct UnknownRetentionTypesError {
    pub unknown: Vec<TypeName>,
    pub registered: Vec<TypeName>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn join(type_names: &[TypeName]) -> String {
    type_names
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    #[test]
    fn test_policy_for_takes_override_then_type_default_then_default() {
        use ResourceRetentionPolicy::{PurgeImmediately, RetainFor, RetainForever};

        let listed: TypeName = "Listed".parse().unwrap();
        let unlisted: TypeName = "Unlisted".parse().unwrap();
        let config = ResourceRetentionConfig {
            default: RetainFor(TimeDelta::days(3)),
            types: BTreeMap::from([(listed.clone(), RetainForever)]),
        };

        for (type_name, type_default, expected) in [
            (&listed, Some(PurgeImmediately), RetainForever),
            (&unlisted, Some(PurgeImmediately), PurgeImmediately),
            (&unlisted, None, RetainFor(TimeDelta::days(3))),
        ] {
            assert_eq!(
                config.policy_for(type_name, type_default),
                expected,
                "{type_name}, {type_default:?}"
            );
        }
    }

    #[test]
    fn test_check_types_names_unknown_types() {
        let known: TypeName = "Known".parse().unwrap();
        let unknown: TypeName = "Unknown".parse().unwrap();
        let config = ResourceRetentionConfig {
            types: BTreeMap::from([
                (known.clone(), ResourceRetentionPolicy::RetainForever),
                (unknown.clone(), ResourceRetentionPolicy::RetainForever),
            ]),
            ..Default::default()
        };

        assert!(
            config
                .check_types([&known])
                .is_err_and(|err| err.unknown == [unknown])
        );
        assert!(
            ResourceRetentionConfig::default()
                .check_types([&known])
                .is_ok()
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
