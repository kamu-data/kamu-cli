// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! resource_selector_value {
    ($typ:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $typ(Cow<'static, str>);

        impl $typ {
            pub const fn new_unchecked_static(value: &'static str) -> Self {
                Self(Cow::Borrowed(value))
            }

            pub fn new(value: impl Into<String>) -> Result<Self, ParseResourceSelectorValueError> {
                let value = value.into();
                validate_selector_value(&value)?;
                Ok(Self(Cow::Owned(value)))
            }

            pub fn new_unchecked(value: impl Into<String>) -> Self {
                Self(Cow::Owned(value.into()))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_ref()
            }

            pub fn into_string(self) -> String {
                self.0.into_owned()
            }
        }

        impl std::fmt::Display for $typ {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl AsRef<str> for $typ {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $typ {
            type Err = ParseResourceSelectorValueError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl TryFrom<String> for $typ {
            type Error = ParseResourceSelectorValueError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $typ {
            type Error = ParseResourceSelectorValueError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$typ> for String {
            fn from(value: $typ) -> Self {
                value.into_string()
            }
        }

        impl From<&$typ> for String {
            fn from(value: &$typ) -> Self {
                value.as_str().to_owned()
            }
        }

        impl Serialize for $typ {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $typ {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

resource_selector_value!(
    ResourceSelectorName,
    "User-facing resource selector: a canonical name, e.g. `variablesets`, or a short alias, e.g. \
     `vs`."
);
resource_selector_value!(
    ResourceTypeSelectorRaw,
    "Raw user/API selector for a resource type before it is resolved to a schema."
);

impl From<ResourceSelectorName> for ResourceTypeSelectorRaw {
    fn from(value: ResourceSelectorName) -> Self {
        Self(value.0)
    }
}

impl From<&ResourceSelectorName> for ResourceTypeSelectorRaw {
    fn from(value: &ResourceSelectorName) -> Self {
        Self(Cow::Owned(value.as_str().to_owned()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("Invalid resource selector value '{value}'")]
pub struct ParseResourceSelectorValueError {
    value: String,
}

impl ParseResourceSelectorValueError {
    fn new(value: &str) -> Self {
        Self {
            value: value.to_owned(),
        }
    }
}

fn validate_selector_value(value: &str) -> Result<(), ParseResourceSelectorValueError> {
    if value.is_empty() || value.trim() != value {
        return Err(ParseResourceSelectorValueError::new(value));
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn selector_values_accept_current_resource_selectors() {
        for value in [
            "variablesets",
            "secretsets",
            "storages",
            "vs",
            "ss",
            "st",
            "storage",
        ] {
            ResourceSelectorName::new(value).unwrap();
            ResourceTypeSelectorRaw::new(value).unwrap();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn selector_values_reject_empty_and_whitespace() {
        for value in ["", " variablesets", "variablesets ", " "] {
            assert!(ResourceSelectorName::new(value).is_err());
            assert!(ResourceTypeSelectorRaw::new(value).is_err());
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn selector_values_preserve_serialized_string() {
        let selector = ResourceTypeSelectorRaw::new("Vs").unwrap();

        assert_eq!(selector.as_str(), "Vs");
        assert_eq!(serde_json::to_string(&selector).unwrap(), "\"Vs\"");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
