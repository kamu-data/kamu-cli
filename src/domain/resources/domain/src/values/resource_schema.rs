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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceSchema {
    schema: String,
    base: String,
    context: String,
    version: String,
    name: String,
}

impl ResourceSchema {
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
            schema: schema.to_owned(),
            base,
            context,
            version,
            name,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.schema
    }

    pub fn display_name(schema: &str) -> &str {
        schema.rsplit('/').next().unwrap_or(schema)
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

impl std::fmt::Display for ResourceSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.schema)
    }
}

impl FromStr for ResourceSchema {
    type Err = ParseResourceSchemaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for ResourceSchema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ResourceSchema {
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
        let schema =
            ResourceSchema::parse("https://opendatafabric.org/schemas/config/v1alpha1/VariableSet")
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
            ResourceSchema::parse("https://schemas.partner.org/billing/v1/Invoice").unwrap();

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
                ResourceSchema::parse(invalid_schema).is_err(),
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
