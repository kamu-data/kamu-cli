// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

use crate::TypeUri;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A parsed lens over a [`TypeUri`]: the same schema identity value, plus its
/// decomposed `base` / `context` / `version` / `name` segments.
///
/// [`TypeUri`] is the opaque identity carried through fields, storage, and the
/// wire; `ResourceSchemaId` is obtained on demand (by parsing a `TypeUri`) when
/// the individual URL segments are needed. The inner `typ` is the single source
/// of truth for the schema string — `as_str`, `Display`, and serde all delegate
/// to it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceSchemaId {
    typ: TypeUri,
    base: String,
    context: String,
    version: String,
    name: String,
}

impl ResourceSchemaId {
    pub fn parse(schema: &str) -> Result<Self, ParseResourceSchemaError> {
        if schema.trim() != schema || schema.is_empty() {
            return Err(ParseResourceSchemaError::new(schema));
        }

        let url = Url::parse(schema).map_err(|_| ParseResourceSchemaError::new(schema))?;

        if !url.has_host()
            || url.cannot_be_a_base()
            || url.query().is_some()
            || url.fragment().is_some()
            || schema.ends_with('/')
        {
            return Err(ParseResourceSchemaError::new(schema));
        }

        let segments: Vec<&str> = url
            .path_segments()
            .ok_or_else(|| ParseResourceSchemaError::new(schema))?
            .collect();

        if segments.len() < 3 || segments.iter().any(|segment| segment.is_empty()) {
            return Err(ParseResourceSchemaError::new(schema));
        }

        let context = segments[segments.len() - 3].to_owned();
        let version = segments[segments.len() - 2].to_owned();
        let name = segments[segments.len() - 1].to_owned();
        let base_path = &segments[..segments.len() - 3];
        let mut base = format!(
            "{}://{}",
            url.scheme(),
            url.host_str()
                .ok_or_else(|| ParseResourceSchemaError::new(schema))?
        );

        if let Some(port) = url.port() {
            base.push(':');
            base.push_str(&port.to_string());
        }

        for segment in base_path {
            base.push('/');
            base.push_str(segment);
        }

        Ok(Self {
            typ: TypeUri::new_unchecked(schema),
            base,
            context,
            version,
            name,
        })
    }

    /// The schema identity as a [`TypeUri`] (the single source of truth).
    pub fn typ(&self) -> &TypeUri {
        &self.typ
    }

    pub fn as_str(&self) -> &str {
        self.typ.as_str()
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn context(&self) -> &str {
        &self.context
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// The short display name (RFC-018 CRD-style type name, e.g. `VariableSet`) of
/// a schema [`TypeUri`], for human-facing output and error messages.
///
/// Callers pass a `TypeUri` that is expected to already be a valid canonical
/// schema (stored snapshots, apply results, schemas resolved from registered
/// selectors). A parse failure therefore means the identity is corrupt, which
/// is a data-integrity catastrophe — it is surfaced as an [`InternalError`],
/// never silently rendered as the raw URL.
// TODO(typeuri-followup): when RFC-018 `TypeName` is adopted for the schema's
// last segment, return `&TypeName` from a `ResourceSchemaId::type_name()`
// method and/or source known kinds from `ResourcePresentation` instead of URL
// parsing.
pub fn resource_kind_display_name(
    schema: &TypeUri,
) -> Result<String, internal_error::InternalError> {
    ResourceSchemaId::try_from(schema)
        .map(|schema_id| schema_id.name().to_string())
        .map_err(|err| {
            internal_error::InternalError::new(format!(
                "Stored resource schema '{schema}' is not a valid canonical schema URL: {err}"
            ))
        })
}

impl std::fmt::Display for ResourceSchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ResourceSchemaId {
    type Err = ParseResourceSchemaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<&TypeUri> for ResourceSchemaId {
    type Error = ParseResourceSchemaError;

    fn try_from(typ: &TypeUri) -> Result<Self, Self::Error> {
        Self::parse(typ.as_str())
    }
}

impl Serialize for ResourceSchemaId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ResourceSchemaId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let schema = String::deserialize(deserializer)?;
        Self::parse(&schema).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_schema_with_path_base() {
        let schema = ResourceSchemaId::parse(
            "https://opendatafabric.org/schemas/config/v1alpha1/VariableSet",
        )
        .unwrap();

        assert_eq!(
            schema.as_str(),
            "https://opendatafabric.org/schemas/config/v1alpha1/VariableSet"
        );
        assert_eq!(schema.base(), "https://opendatafabric.org/schemas");
        assert_eq!(schema.context(), "config");
        assert_eq!(schema.version(), "v1alpha1");
        assert_eq!(schema.name(), "VariableSet");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn parses_schema_with_host_only_base() {
        let schema =
            ResourceSchemaId::parse("https://schemas.partner.org/billing/v1/Invoice").unwrap();

        assert_eq!(
            schema.as_str(),
            "https://schemas.partner.org/billing/v1/Invoice"
        );
        assert_eq!(schema.base(), "https://schemas.partner.org");
        assert_eq!(schema.context(), "billing");
        assert_eq!(schema.version(), "v1");
        assert_eq!(schema.name(), "Invoice");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn rejects_non_canonical_schema_strings() {
        for invalid_schema in [
            "",
            " https://schemas.partner.org/billing/v1/Invoice",
            "https://schemas.partner.org/billing/v1/Invoice ",
            "schemas.partner.org/billing/v1/Invoice",
            "urn:schemas:billing:v1:Invoice",
            "https://schemas.partner.org/billing/v1",
            "https://schemas.partner.org/billing/v1/Invoice/",
            "https://schemas.partner.org/billing/v1/Invoice?format=json",
            "https://schemas.partner.org/billing/v1/Invoice#latest",
            "https://schemas.partner.org/billing//v1/Invoice",
        ] {
            assert!(
                ResourceSchemaId::parse(invalid_schema).is_err(),
                "schema should be rejected: {invalid_schema}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("Invalid resource schema '{schema}'")]
pub struct ParseResourceSchemaError {
    pub schema: String,
}

impl ParseResourceSchemaError {
    pub fn new(schema: &str) -> Self {
        Self {
            schema: schema.to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
