// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::{
    RESOURCE_CONDITION_CREATION_PROVENANCE_SCHEMA_URI,
    ResourceConditions,
    TypeRef,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Who created a resource, and through which boundary. The server records it
/// in the conditions of the `Created` event, and no condition patch can set or
/// remove it, so it can be trusted as the creator's identity
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreationProvenance {
    /// The acting principal
    #[serde_as(as = "odf::metadata::serde::yaml::auth::PrincipalHandle")]
    pub principal: odf::metadata::auth::PrincipalHandle,
    pub origin: CreationOrigin,
}

impl CreationProvenance {
    /// The provenance recorded in `conditions`; `None` for a resource created
    /// before provenance was recorded. A value that does not decode is an
    /// error, never an unknown creator
    pub fn from_conditions(conditions: &ResourceConditions) -> Result<Option<Self>, InternalError> {
        conditions
            .entries
            .get(&creation_provenance_type_ref())
            .map(|value| {
                Self::deserialize(value).map_err(|err| {
                    err.int_err()
                        .with_context("Stored creation provenance does not decode")
                })
            })
            .transpose()
    }

    pub fn insert_into(&self, conditions: &mut ResourceConditions) {
        conditions.entries.insert(
            creation_provenance_type_ref(),
            serde_json::to_value(self).unwrap(),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The boundary a resource was created through
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all_fields = "camelCase")]
pub enum CreationOrigin {
    /// A request to the node's API
    Api { api: CreationOriginApi },
    /// A local CLI command
    Cli,
    /// Work of the node itself
    Internal {
        /// Stable name of the producing component
        producer: String,
        /// Defined by the producer, such as the ID of the flow that created
        /// the resource
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The API a resource was created through
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreationOriginApi {
    GraphQl,
    /// Any HTTP route other than GraphQL
    Rest,
    FlightSql,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn creation_provenance_type_ref() -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(
        RESOURCE_CONDITION_CREATION_PROVENANCE_SCHEMA_URI,
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::empty_resource_conditions;

    #[test]
    fn test_from_conditions_tells_absent_from_malformed() {
        let provenance = CreationProvenance {
            principal: odf::metadata::auth::PrincipalHandle {
                account: None,
                r#type: odf::metadata::auth::Node::type_uri().clone(),
                id: odf::ResourceID::new(uuid::Uuid::nil()),
                did: None,
                name: odf::metadata::resources::ResourceName::new_unchecked("node"),
            },
            origin: CreationOrigin::Cli,
        };
        let mut conditions = empty_resource_conditions();

        assert_eq!(
            CreationProvenance::from_conditions(&conditions).unwrap(),
            None
        );

        provenance.insert_into(&mut conditions);
        assert_eq!(
            CreationProvenance::from_conditions(&conditions).unwrap(),
            Some(provenance)
        );

        conditions.entries.insert(
            creation_provenance_type_ref(),
            serde_json::json!({ "origin": { "kind": "Cli" } }),
        );
        assert!(CreationProvenance::from_conditions(&conditions).is_err());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
