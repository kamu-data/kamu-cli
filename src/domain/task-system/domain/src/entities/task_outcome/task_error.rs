// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskError {
    pub error_type: String,
    pub payload: serde_json::Value,
    pub recoverable: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskError {
    pub const TASK_ERROR_EMPTY: &str = "Empty";

    pub fn empty_recoverable() -> Self {
        TaskError {
            error_type: Self::TASK_ERROR_EMPTY.to_string(),
            payload: serde_json::Value::Null,
            recoverable: true,
        }
    }

    pub fn empty_unrecoverable() -> Self {
        TaskError {
            error_type: Self::TASK_ERROR_EMPTY.to_string(),
            payload: serde_json::Value::Null,
            recoverable: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.error_type == Self::TASK_ERROR_EMPTY
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Serialize for TaskError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry(self.error_type.as_str(), &self.payload)?;
        map.serialize_entry("recoverable", &self.recoverable)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for TaskError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TaskErrorVisitor;

        impl<'de> Visitor<'de> for TaskErrorVisitor {
            type Value = TaskError;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "either a string \"Empty\" or a map with error type and optional recoverable \
                     flag",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // Backward compatibility: old empty results are recoverable
                if v == TaskError::TASK_ERROR_EMPTY {
                    Ok(TaskError::empty_recoverable())
                } else {
                    Err(E::custom(format!("Unknown string variant: {v}")))
                }
            }

            fn visit_map<M>(self, mut map: M) -> Result<TaskError, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut error_type: Option<String> = None;
                let mut payload: Option<serde_json::Value> = None;
                let mut recoverable = false; // Default to unrecoverable for backwards compatibility

                while let Some(key) = map.next_key::<String>()? {
                    if key == "recoverable" {
                        recoverable = map.next_value()?;
                    } else {
                        // This is the error type key
                        if error_type.is_some() {
                            return Err(serde::de::Error::custom("Multiple error type keys found"));
                        }
                        error_type = Some(key);
                        payload = Some(map.next_value()?);
                    }
                }

                let error_type =
                    error_type.ok_or_else(|| serde::de::Error::custom("No error type found"))?;
                let payload =
                    payload.ok_or_else(|| serde::de::Error::custom("No payload found"))?;

                Ok(TaskError {
                    error_type,
                    payload,
                    recoverable,
                })
            }
        }

        deserializer.deserialize_any(TaskErrorVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Macro to generate task error enums with serialization helpers
#[macro_export]
macro_rules! task_error_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($variant:tt)*
        }
        => $type_id:expr, recoverable: $recoverable:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        $vis enum $name {
            $($variant)*
        }

        impl $name {
            pub const TYPE_ID: &'static str = $type_id;
            pub const RECOVERABLE: bool = $recoverable;

            pub fn into_task_error(self) -> $crate::TaskError {
                $crate::TaskError {
                    error_type: Self::TYPE_ID.to_string(),
                    payload: serde_json::to_value(self)
                        .expect(concat!("Failed to serialize ", stringify!($name), " into JSON")),
                    recoverable: Self::RECOVERABLE,
                }
            }

            pub fn from_task_error(
                task_error: &$crate::TaskError,
            ) -> Result<Self, internal_error::InternalError> {
                use internal_error::ResultIntoInternal;
                serde_json::from_value(task_error.payload.clone()).int_err()
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
