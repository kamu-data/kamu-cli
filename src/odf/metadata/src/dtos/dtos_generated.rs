// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#![allow(clippy::all)]
#![allow(clippy::pedantic)]

use std::path::PathBuf;

use bitflags::bitflags;
use chrono::{DateTime, Utc};
use enum_variants::*;
use multiformats::*;
use serde::{Deserialize, Serialize};
use setty::types::{ByteSize, DurationString};

use crate::auth::*;
use crate::dataset::*;
use crate::resource::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// auth
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod auth {
    #[allow(unused_imports)]
    use super::*;

    /// Registers an account in an predefined account provider.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Account
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Account {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the resource.
        pub spec: auth::AccountSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl Account {
        pub fn schema() -> &'static TypeUri {
            &ACCOUNT_SCHEMA
        }
    }

    static ACCOUNT_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/auth/v1alpha1/Account";

    static ACCOUNT_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(ACCOUNT_SCHEMA_STR));

    pub use crate::auth::AccountRef;

    /// Predefined account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountSpec
    #[derive(Clone, Debug, Eq)]
    pub struct AccountSpec {
        /// DID associated with the account by ODF or an external system
        pub did: Option<AccountID>,
        /// Type of the account.
        ///
        /// Defaults to: "User"
        pub account_type: Option<auth::AccountType>,
        /// Human-friendly display name.
        pub display_name: Option<String>,
        /// Email address of the account.
        pub email: String,
        /// URL of the account's avatar image.
        pub avatar_url: Option<String>,
        /// Password for local authentication. Absent for SSO or DID-based
        /// accounts.
        pub password: Option<config::Secret>,
    }

    impl AccountSpec {
        pub fn default_account_type() -> auth::AccountType {
            auth::AccountType::User
        }
        pub fn account_type(&self) -> auth::AccountType {
            self.account_type.unwrap_or(Self::default_account_type())
        }
    }

    impl PartialEq for AccountSpec {
        fn eq(&self, other: &Self) -> bool {
            self.did == other.did
                && self
                    .account_type
                    .or_else(|| Some(Self::default_account_type()))
                    == other
                        .account_type
                        .or_else(|| Some(Self::default_account_type()))
                && self.display_name == other.display_name
                && self.email == other.email
                && self.avatar_url == other.avatar_url
                && self.password == other.password
        }
    }

    /// Represents the type of an account.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountType
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum AccountType {
        User,
        Organization,
    }

    /// A named attribute attached to a resource, used by auth policies for
    /// access control decisions.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Attribute
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Attribute {
        /// The resource this attribute is attached to.
        pub object: resource::ResourceRef,
        /// Name of the attribute e.g. `allowPublicRead`.
        pub name: String,
        /// Value of the attribute.
        pub value: serde_json::Value,
    }

    /// A directed relationship between two resources, optionally carrying a
    /// typed value.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Relation
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Relation {
        /// The resource that holds the relation.
        pub subject: resource::ResourceRef,
        /// Name of the relation e.g. `role`, `member`, `owner`.
        pub relation: String,
        /// Optional value associated with the relation e.g. `maintainer` for a
        /// `role` relation.
        pub value: Option<serde_json::Value>,
        /// The resource that is the target of the relation.
        pub object: resource::ResourceRef,
    }

    /// Specified relations between resources on which auth policies act upon.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Relations
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Relations {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the resource.
        pub spec: auth::RelationsSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl Relations {
        pub fn schema() -> &'static TypeUri {
            &RELATIONS_SCHEMA
        }
    }

    static RELATIONS_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/auth/v1alpha1/Relations";

    static RELATIONS_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(RELATIONS_SCHEMA_STR));

    /// Specifies resource attributes and relations between resources on which
    /// auth policies act upon.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/RelationsSpec
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct RelationsSpec {
        /// Relations between resources.
        pub relations: Option<Vec<auth::Relation>>,
        /// Resource attributes.
        pub attributes: Option<Vec<auth::Attribute>>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// config
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod config {
    #[allow(unused_imports)]
    use super::*;

    /// Individual secret in raw or encrypted form.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secret
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Secret {
        /// A secret value in raw or encoded form.
        pub value: String,
        /// Represents the encoding of the value. Typically will be `jwe` after
        /// a raw secret gets encrypted.
        pub content_encoding: Option<String>,
    }

    /// Defines a set of secrets stored and managed by the ODF node.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSet
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SecretSet {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the secret set.
        pub spec: config::SecretSetSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl SecretSet {
        pub fn schema() -> &'static TypeUri {
            &SECRET_SET_SCHEMA
        }
    }

    static SECRET_SET_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/config/v1alpha1/SecretSet";

    static SECRET_SET_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(SECRET_SET_SCHEMA_STR));

    /// Defines a set of secrets stored and managed by the ODF node and
    /// accessible via embedded sercets provider.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSetSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SecretSetSpec {
        /// Key value pairs of secrets.
        pub secrets: config::Secrets,
    }

    /// Container for key-value secrets. Every key must be a string. Values may
    /// be strings with raw unencrypted data or objects that signify the
    /// encoding.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secrets
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct Secrets {
        pub entries: std::collections::BTreeMap<String, config::Secret>,
    }

    pub use crate::config::ValueRef;

    /// Container for key-value variables. Every key must be a string. Values
    /// shoud reference fields in `SecretSet`s and `VariableSet`s.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueRefs
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ValueRefs {
        pub entries: std::collections::BTreeMap<String, config::ValueRef>,
    }

    /// Individual variable.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variable
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Variable {
        /// A value in raw or encoded form.
        pub value: String,
    }

    /// Defines a set of variables stored and managed by the ODF node.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSet
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct VariableSet {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the variable set.
        pub spec: config::VariableSetSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl VariableSet {
        pub fn schema() -> &'static TypeUri {
            &VARIABLE_SET_SCHEMA
        }
    }

    static VARIABLE_SET_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/config/v1alpha1/VariableSet";

    static VARIABLE_SET_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(VARIABLE_SET_SCHEMA_STR));

    /// Defines a set of variables stored and managed by the ODF node and
    /// accessible via embedded variables provider.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSetSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct VariableSetSpec {
        /// Key value pairs of variables.
        pub variables: config::Variables,
    }

    /// Container for key-value variables. Every key must be a string. Values
    /// may be raw strings or objects that incorporate the encoding.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variables
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct Variables {
        pub entries: std::collections::BTreeMap<String, config::Variable>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// data
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod data {
    #[allow(unused_imports)]
    use super::*;

    /// Represents a named field (column) in a root or nested struct schema
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataField
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataField {
        /// Name of the field
        pub name: String,
        /// Logical type of the field that defines its semantic behavior and
        /// value ranges
        pub r#type: data::DataType,
        /// ODF extensions
        pub extra: Option<data::ExtraAttributes>,
    }

    /// This schema aims to be a human-friendly variant of Arrow. Arrow currently specifies only the [flatbuffer format](https://github.com/apache/arrow/blob/f9301c0ba8a7ed1b0b63275cfdd4c44c26b04675/format/Schema.fbs) which has many legacy to it and is not suited to be defined by humans, so we had to define our own schema format. While inspired by Arrow - this format makes a clear separation between logical data types and encoding (physical layout) of data in the chunks.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataSchema
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataSchema {
        /// Top-level fields (columns) of the schema.
        pub fields: Vec<data::DataField>,
        /// ODF extensions
        pub extra: Option<data::ExtraAttributes>,
    }

    /// Defines a logical type of the field. Logical type determines the
    /// semantics and boudaries of a type and how it can be operated on, without
    /// a concern about encoding and physical layout of the data in chunks.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum DataType {
        Binary(data::DataTypeBinary),
        Bool(data::DataTypeBool),
        Date(data::DataTypeDate),
        Decimal(data::DataTypeDecimal),
        Duration(data::DataTypeDuration),
        Float16(data::DataTypeFloat16),
        Float32(data::DataTypeFloat32),
        Float64(data::DataTypeFloat64),
        Int8(data::DataTypeInt8),
        Int16(data::DataTypeInt16),
        Int32(data::DataTypeInt32),
        Int64(data::DataTypeInt64),
        UInt8(data::DataTypeUInt8),
        UInt16(data::DataTypeUInt16),
        UInt32(data::DataTypeUInt32),
        UInt64(data::DataTypeUInt64),
        List(data::DataTypeList),
        Map(data::DataTypeMap),
        Null(data::DataTypeNull),
        Option(data::DataTypeOption),
        Struct(data::DataTypeStruct),
        Time(data::DataTypeTime),
        Timestamp(data::DataTypeTimestamp),
        String(data::DataTypeString),
    }

    impl_enum_with_variants!(DataType);
    impl_enum_variant!(DataType::Binary(data::DataTypeBinary));
    impl_enum_variant!(DataType::Bool(data::DataTypeBool));
    impl_enum_variant!(DataType::Date(data::DataTypeDate));
    impl_enum_variant!(DataType::Decimal(data::DataTypeDecimal));
    impl_enum_variant!(DataType::Duration(data::DataTypeDuration));
    impl_enum_variant!(DataType::Float16(data::DataTypeFloat16));
    impl_enum_variant!(DataType::Float32(data::DataTypeFloat32));
    impl_enum_variant!(DataType::Float64(data::DataTypeFloat64));
    impl_enum_variant!(DataType::Int8(data::DataTypeInt8));
    impl_enum_variant!(DataType::Int16(data::DataTypeInt16));
    impl_enum_variant!(DataType::Int32(data::DataTypeInt32));
    impl_enum_variant!(DataType::Int64(data::DataTypeInt64));
    impl_enum_variant!(DataType::UInt8(data::DataTypeUInt8));
    impl_enum_variant!(DataType::UInt16(data::DataTypeUInt16));
    impl_enum_variant!(DataType::UInt32(data::DataTypeUInt32));
    impl_enum_variant!(DataType::UInt64(data::DataTypeUInt64));
    impl_enum_variant!(DataType::List(data::DataTypeList));
    impl_enum_variant!(DataType::Map(data::DataTypeMap));
    impl_enum_variant!(DataType::Null(data::DataTypeNull));
    impl_enum_variant!(DataType::Option(data::DataTypeOption));
    impl_enum_variant!(DataType::Struct(data::DataTypeStruct));
    impl_enum_variant!(DataType::Time(data::DataTypeTime));
    impl_enum_variant!(DataType::Timestamp(data::DataTypeTimestamp));
    impl_enum_variant!(DataType::String(data::DataTypeString));

    /// A sequence of bytes. Used for arbitrary binary data.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Binary
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeBinary {
        /// Number of bytes per value for fixed-size binary. If omitted, the
        /// binary is variable-length.
        pub fixed_length: Option<u64>,
    }

    /// A boolean value representing true or false.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Bool
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeBool {}

    /// A calendar date.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Date
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeDate {}

    /// A fixed-point decimal number with a specified precision and scale.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Decimal
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeDecimal {
        /// Total number of decimal digits that can be stored.
        pub precision: u32,
        /// Number of digits after the decimal point. In certain situations,
        /// scale could be negative number. For negative scale, it is the number
        /// of padding 0 to the right of the digits.
        ///
        /// For example the number 12300 could be treated as a decimal has
        /// precision 3 and scale -2.
        pub scale: i32,
    }

    /// An elapsed time interval with a specified time unit.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Duration
    #[derive(Clone, Debug, Eq, Default)]
    pub struct DataTypeDuration {
        /// The unit of the duration measurement.
        ///
        /// Defaults to: "Millisecond"
        pub unit: Option<data::TimeUnit>,
    }

    impl DataTypeDuration {
        pub fn default_unit() -> data::TimeUnit {
            data::TimeUnit::Millisecond
        }
        pub fn unit(&self) -> data::TimeUnit {
            self.unit.unwrap_or(Self::default_unit())
        }
    }

    impl PartialEq for DataTypeDuration {
        fn eq(&self, other: &Self) -> bool {
            self.unit.or_else(|| Some(Self::default_unit()))
                == other.unit.or_else(|| Some(Self::default_unit()))
        }
    }

    /// A floating-point number.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float16
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeFloat16 {}

    /// A floating-point number.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float32
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeFloat32 {}

    /// A floating-point number.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float64
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeFloat64 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int16
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeInt16 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int32
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeInt32 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int64
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeInt64 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int8
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeInt8 {}

    /// A list of values, all having the same data type.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/List
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeList {
        /// Data type of list items.
        pub item_type: Box<data::DataType>,
        /// Number of list items per value for fixed-size lists. If omitted, the
        /// list is variable-length.
        pub fixed_length: Option<u64>,
    }

    /// A map of key-value pairs, represented as a list of entries (structs with
    /// key and value fields).
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Map
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeMap {
        /// Data type of the map's keys.
        pub key_type: Box<data::DataType>,
        /// Data type of the map's values.
        pub value_type: Box<data::DataType>,
        /// Set to true if the keys within each value are sorted.
        pub keys_sorted: Option<bool>,
    }

    /// A type representing the absence of a value (null).
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Null
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeNull {}

    /// A type representing an optional (nullable) value of another data type.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Option
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeOption {
        /// Inner data type for the optional value.
        pub inner: Box<data::DataType>,
    }

    /// A Unicode string.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/String
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeString {}

    /// A collection of named fields, each with its own data type.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Struct
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeStruct {
        /// Fields that make up the struct.
        pub fields: Vec<data::DataField>,
    }

    /// A time of day value, without a date, with a specified unit of
    /// granularity.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Time
    #[derive(Clone, Debug, Eq, Default)]
    pub struct DataTypeTime {
        /// The unit of the time value.
        ///
        /// Defaults to: "Millisecond"
        pub unit: Option<data::TimeUnit>,
    }

    impl DataTypeTime {
        pub fn default_unit() -> data::TimeUnit {
            data::TimeUnit::Millisecond
        }
        pub fn unit(&self) -> data::TimeUnit {
            self.unit.unwrap_or(Self::default_unit())
        }
    }

    impl PartialEq for DataTypeTime {
        fn eq(&self, other: &Self) -> bool {
            self.unit.or_else(|| Some(Self::default_unit()))
                == other.unit.or_else(|| Some(Self::default_unit()))
        }
    }

    /// A point in time, represented as an offset from the Unix epoch in a
    /// specific timezone.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Timestamp
    #[derive(Clone, Debug, Eq, Default)]
    pub struct DataTypeTimestamp {
        /// The unit of the timestamp value that determines its precision.
        ///
        /// Defaults to: "Millisecond"
        pub unit: Option<data::TimeUnit>,
        /// The timezone is an optional string indicating the name of a timezone
        /// one of
        ///
        /// * As used in the Olson timezone database (the "tz database" or
        ///   "tzdata"), such as "America/New_York".
        /// * An absolute timezone offset of the form "+XX:XX" or "-XX:XX", such
        ///   as "+07:30".
        ///
        /// Defaults to: "UTC"
        pub timezone: Option<String>,
    }

    impl DataTypeTimestamp {
        pub fn default_unit() -> data::TimeUnit {
            data::TimeUnit::Millisecond
        }
        pub fn unit(&self) -> data::TimeUnit {
            self.unit.unwrap_or(Self::default_unit())
        }
        pub fn default_timezone() -> &'static str {
            "UTC"
        }
        pub fn timezone(&self) -> &str {
            self.timezone.as_deref().unwrap_or(Self::default_timezone())
        }
    }

    impl PartialEq for DataTypeTimestamp {
        fn eq(&self, other: &Self) -> bool {
            self.unit.or_else(|| Some(Self::default_unit()))
                == other.unit.or_else(|| Some(Self::default_unit()))
                && self
                    .timezone
                    .as_deref()
                    .or_else(|| Some(Self::default_timezone()))
                    == other
                        .timezone
                        .as_deref()
                        .or_else(|| Some(Self::default_timezone()))
        }
    }

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt16
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeUInt16 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt32
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeUInt32 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt64
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeUInt64 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt8
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeUInt8 {}

    /// Container for custom key-value extension attributes. Every key must be
    /// in the form of `<domain>/<path>` (e.g. `kamu.dev/archetype`) in order to
    /// fully disambiguate the value in the face of multiple extensions. Values
    /// may be any valid JSON including nested objects.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/ExtraAttributes
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ExtraAttributes {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    pub use crate::data::OperationType;

    /// Defines the unit of measurement of time
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/TimeUnit
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum TimeUnit {
        Second,
        Millisecond,
        Microsecond,
        Nanosecond,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// dataset
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod dataset {
    #[allow(unused_imports)]
    use super::*;

    /// Indicates that data has been ingested into a root dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/AddData
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct AddData {
        /// Hash of the checkpoint file used to restore ingestion state, if any.
        pub prev_checkpoint: Option<Multihash>,
        /// Last offset of the previous data slice, if any. Must be equal to the
        /// last non-empty `newData.offsetInterval.end`.
        pub prev_offset: Option<u64>,
        /// Describes output data written during this transaction, if any.
        pub new_data: Option<dataset::DataSlice>,
        /// Describes checkpoint written during this transaction, if any. If an
        /// engine operation resulted in no updates to the checkpoint, but
        /// checkpoint is still relevant for subsequent runs - a hash of the
        /// previous checkpoint should be specified.
        pub new_checkpoint: Option<dataset::Checkpoint>,
        /// Last watermark of the output data stream, if any. Initial blocks may
        /// not have watermarks, but once watermark is set - all subsequent
        /// blocks should either carry the same watermark or specify a new
        /// (greater) one. Thus, watermarks are monotonically non-decreasing.
        pub new_watermark: Option<DateTime<Utc>>,
        /// The state of the source the data was added from to allow fast
        /// resuming. If the state did not change but is still relevant for
        /// subsequent runs it should be carried, i.e. only the last state per
        /// source is considered when resuming.
        pub new_source_state: Option<source::SourceState>,
        /// ODF extensions.
        pub extra: Option<data::ExtraAttributes>,
    }

    /// Embedded attachment item.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/AttachmentEmbedded
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AttachmentEmbedded {
        /// Path to an attachment if it was materialized into a file.
        pub path: String,
        /// Content of the attachment.
        pub content: String,
    }

    /// Defines the source of attachment files.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Attachments
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum Attachments {
        Embedded(dataset::AttachmentsEmbedded),
    }

    impl_enum_with_variants!(Attachments);
    impl_enum_variant!(Attachments::Embedded(dataset::AttachmentsEmbedded));

    /// For attachments that are specified inline and are embedded in the
    /// metadata.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Attachments#/$defs/Embedded
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AttachmentsEmbedded {
        /// List of embedded items.
        pub items: Vec<dataset::AttachmentEmbedded>,
    }

    /// Describes a checkpoint produced by an engine
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Checkpoint
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Checkpoint {
        /// Hash sum of the checkpoint file.
        pub physical_hash: Multihash,
        /// Size of checkpoint file in bytes.
        pub size: u64,
    }

    /// Optional parameters to control ingestion behavior.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/CompactionParams
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct CompactionParams {
        /// Target maximum size of each compacted data slice e.g. `100MiB`.
        pub max_slice_size: Option<ByteSize>,
        /// Target maximum number of records per compacted data slice.
        pub max_slice_records: Option<u64>,
    }

    /// Describes a slice of data added to a dataset or produced via
    /// transformation
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DataSlice
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataSlice {
        /// Logical hash sum of the data in this slice.
        pub logical_hash: Multihash,
        /// Hash sum of the data part file.
        pub physical_hash: Multihash,
        /// Data slice produced by the transaction.
        pub offset_interval: dataset::OffsetInterval,
        /// Size of data file in bytes.
        pub size: u64,
    }

    /// Represents a desired state of a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Dataset
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Dataset {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the resource.
        pub spec: dataset::DatasetSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl Dataset {
        pub fn schema() -> &'static TypeUri {
            &DATASET_SCHEMA
        }
    }

    static DATASET_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/dataset/v1alpha1/Dataset";

    static DATASET_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(DATASET_SCHEMA_STR));

    /// Represents type of the dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetKind
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub enum DatasetKind {
        Root,
        Derivative,
    }

    pub use crate::dataset::DatasetSelector;

    /// Represents a desired state of the dataset metadata.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DatasetSpec {
        /// DID of the dataset in global ODF network
        pub did: Option<DatasetID>,
        /// Type of the dataset.
        pub kind: dataset::DatasetKind,
        /// An array of metadata events that will be used to populate the chain.
        /// Here you can define polling and push sources, set licenses, add
        /// attachments etc.
        pub metadata: Vec<dataset::MetadataEvent>,
        /// Reference to a storage volume where dataset data will be stored. If
        /// omitted, the node's default storage is used.
        pub volume: Option<storage::PersistentVolumeRef>,
    }

    /// Specifies the mapping of system columns onto dataset schema.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetVocabulary
    #[derive(Clone, Debug, Eq, Default)]
    pub struct DatasetVocabulary {
        /// Name of the offset column.
        ///
        /// Defaults to: "offset"
        pub offset_column: Option<String>,
        /// Name of the operation type column.
        ///
        /// Defaults to: "op"
        pub operation_type_column: Option<String>,
        /// Name of the system time column.
        ///
        /// Defaults to: "system_time"
        pub system_time_column: Option<String>,
        /// Name of the event time column.
        ///
        /// Defaults to: "event_time"
        pub event_time_column: Option<String>,
    }

    impl DatasetVocabulary {
        pub fn default_offset_column() -> &'static str {
            "offset"
        }
        pub fn offset_column(&self) -> &str {
            self.offset_column
                .as_deref()
                .unwrap_or(Self::default_offset_column())
        }
        pub fn default_operation_type_column() -> &'static str {
            "op"
        }
        pub fn operation_type_column(&self) -> &str {
            self.operation_type_column
                .as_deref()
                .unwrap_or(Self::default_operation_type_column())
        }
        pub fn default_system_time_column() -> &'static str {
            "system_time"
        }
        pub fn system_time_column(&self) -> &str {
            self.system_time_column
                .as_deref()
                .unwrap_or(Self::default_system_time_column())
        }
        pub fn default_event_time_column() -> &'static str {
            "event_time"
        }
        pub fn event_time_column(&self) -> &str {
            self.event_time_column
                .as_deref()
                .unwrap_or(Self::default_event_time_column())
        }
    }

    impl PartialEq for DatasetVocabulary {
        fn eq(&self, other: &Self) -> bool {
            self.offset_column
                .as_deref()
                .or_else(|| Some(Self::default_offset_column()))
                == other
                    .offset_column
                    .as_deref()
                    .or_else(|| Some(Self::default_offset_column()))
                && self
                    .operation_type_column
                    .as_deref()
                    .or_else(|| Some(Self::default_operation_type_column()))
                    == other
                        .operation_type_column
                        .as_deref()
                        .or_else(|| Some(Self::default_operation_type_column()))
                && self
                    .system_time_column
                    .as_deref()
                    .or_else(|| Some(Self::default_system_time_column()))
                    == other
                        .system_time_column
                        .as_deref()
                        .or_else(|| Some(Self::default_system_time_column()))
                && self
                    .event_time_column
                    .as_deref()
                    .or_else(|| Some(Self::default_event_time_column()))
                    == other
                        .event_time_column
                        .as_deref()
                        .or_else(|| Some(Self::default_event_time_column()))
        }
    }

    /// Indicates that derivative transformation has been performed.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ExecuteTransform
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ExecuteTransform {
        /// Defines inputs used in this transaction. Slices corresponding to
        /// every input dataset must be present.
        pub query_inputs: Vec<dataset::ExecuteTransformInput>,
        /// Hash of the checkpoint file used to restore transformation state, if
        /// any.
        pub prev_checkpoint: Option<Multihash>,
        /// Last offset of the previous data slice, if any. Must be equal to the
        /// last non-empty `newData.offsetInterval.end`.
        pub prev_offset: Option<u64>,
        /// Describes output data written during this transaction, if any.
        pub new_data: Option<dataset::DataSlice>,
        /// Describes checkpoint written during this transaction, if any. If an
        /// engine operation resulted in no updates to the checkpoint, but
        /// checkpoint is still relevant for subsequent runs - a hash of the
        /// previous checkpoint should be specified.
        pub new_checkpoint: Option<dataset::Checkpoint>,
        /// Last watermark of the output data stream, if any. Initial blocks may
        /// not have watermarks, but once watermark is set - all subsequent
        /// blocks should either carry the same watermark or specify a new
        /// (greater) one. Thus, watermarks are monotonically non-decreasing.
        pub new_watermark: Option<DateTime<Utc>>,
    }

    /// Describes a slice of the input dataset used during a transformation
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ExecuteTransformInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ExecuteTransformInput {
        /// Input dataset identifier.
        pub dataset_id: DatasetID,
        /// Last block of the input dataset that was previously incorporated
        /// into the derivative transformation, if any. Must be equal to the
        /// last non-empty `newBlockHash`. Together with `newBlockHash` defines
        /// a half-open `(prevBlockHash, newBlockHash]` interval of blocks that
        /// will be considered in this transaction.
        pub prev_block_hash: Option<Multihash>,
        /// Hash of the last block that will be incorporated into the derivative
        /// transformation. When present, defines a half-open `(prevBlockHash,
        /// newBlockHash]` interval of blocks that will be considered in this
        /// transaction.
        pub new_block_hash: Option<Multihash>,
        /// Last data record offset in the input dataset that was previously
        /// incorporated into the derivative transformation, if any. Must be
        /// equal to the last non-empty `newOffset`. Together with `newOffset`
        /// defines a half-open `(prevOffset, newOffset]` interval of data
        /// records that will be considered in this transaction.
        pub prev_offset: Option<u64>,
        /// Offset of the last data record that will be incorporated into the
        /// derivative transformation, if any. When present, defines a half-open
        /// `(prevOffset, newOffset]` interval of data records that will be
        /// considered in this transaction.
        pub new_offset: Option<u64>,
    }

    /// An individual block in the metadata chain that captures the history of
    /// modifications of a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/MetadataBlock
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MetadataBlock {
        /// System time when this block was written.
        pub system_time: DateTime<Utc>,
        /// Hash sum of the preceding block.
        pub prev_block_hash: Option<Multihash>,
        /// Block sequence number, starting from zero at the seed block.
        pub sequence_number: u64,
        /// Event data.
        pub event: dataset::MetadataEvent,
    }

    /// Represents a transaction that occurred on a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/MetadataEvent
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum MetadataEvent {
        AddData(dataset::AddData),
        ExecuteTransform(dataset::ExecuteTransform),
        Seed(dataset::Seed),
        SetPollingSource(legacy::SetPollingSource),
        SetTransform(dataset::SetTransform),
        SetVocab(dataset::SetVocab),
        SetAttachments(dataset::SetAttachments),
        SetInfo(dataset::SetInfo),
        SetLicense(dataset::SetLicense),
        SetDataSchema(dataset::SetDataSchema),
        AddPushSource(legacy::AddPushSource),
        DisablePushSource(legacy::DisablePushSource),
        DisablePollingSource(legacy::DisablePollingSource),
    }

    impl_enum_with_variants!(MetadataEvent);
    impl_enum_variant!(MetadataEvent::AddData(dataset::AddData));
    impl_enum_variant!(MetadataEvent::ExecuteTransform(dataset::ExecuteTransform));
    impl_enum_variant!(MetadataEvent::Seed(dataset::Seed));
    impl_enum_variant!(MetadataEvent::SetPollingSource(legacy::SetPollingSource));
    impl_enum_variant!(MetadataEvent::SetTransform(dataset::SetTransform));
    impl_enum_variant!(MetadataEvent::SetVocab(dataset::SetVocab));
    impl_enum_variant!(MetadataEvent::SetAttachments(dataset::SetAttachments));
    impl_enum_variant!(MetadataEvent::SetInfo(dataset::SetInfo));
    impl_enum_variant!(MetadataEvent::SetLicense(dataset::SetLicense));
    impl_enum_variant!(MetadataEvent::SetDataSchema(dataset::SetDataSchema));
    impl_enum_variant!(MetadataEvent::AddPushSource(legacy::AddPushSource));
    impl_enum_variant!(MetadataEvent::DisablePushSource(legacy::DisablePushSource));
    impl_enum_variant!(MetadataEvent::DisablePollingSource(
        legacy::DisablePollingSource
    ));

    bitflags! {
        #[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        pub struct MetadataEventTypeFlags: u32 {
            const ADD_DATA = 1 << 0;
            const EXECUTE_TRANSFORM = 1 << 1;
            const SEED = 1 << 2;
            const SET_POLLING_SOURCE = 1 << 3;
            const SET_TRANSFORM = 1 << 4;
            const SET_VOCAB = 1 << 5;
            const SET_ATTACHMENTS = 1 << 6;
            const SET_INFO = 1 << 7;
            const SET_LICENSE = 1 << 8;
            const SET_DATA_SCHEMA = 1 << 9;
            const ADD_PUSH_SOURCE = 1 << 10;
            const DISABLE_PUSH_SOURCE = 1 << 11;
            const DISABLE_POLLING_SOURCE = 1 << 12;
        }
    }

    impl From<&MetadataEvent> for MetadataEventTypeFlags {
        fn from(v: &MetadataEvent) -> Self {
            match v {
                MetadataEvent::AddData(_) => Self::ADD_DATA,
                MetadataEvent::ExecuteTransform(_) => Self::EXECUTE_TRANSFORM,
                MetadataEvent::Seed(_) => Self::SEED,
                MetadataEvent::SetPollingSource(_) => Self::SET_POLLING_SOURCE,
                MetadataEvent::SetTransform(_) => Self::SET_TRANSFORM,
                MetadataEvent::SetVocab(_) => Self::SET_VOCAB,
                MetadataEvent::SetAttachments(_) => Self::SET_ATTACHMENTS,
                MetadataEvent::SetInfo(_) => Self::SET_INFO,
                MetadataEvent::SetLicense(_) => Self::SET_LICENSE,
                MetadataEvent::SetDataSchema(_) => Self::SET_DATA_SCHEMA,
                MetadataEvent::AddPushSource(_) => Self::ADD_PUSH_SOURCE,
                MetadataEvent::DisablePushSource(_) => Self::DISABLE_PUSH_SOURCE,
                MetadataEvent::DisablePollingSource(_) => Self::DISABLE_POLLING_SOURCE,
            }
        }
    }

    /// Describes a range of data as a closed arithmetic interval of offsets
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/OffsetInterval
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct OffsetInterval {
        /// Start of the closed interval [start; end].
        pub start: u64,
        /// End of the closed interval [start; end].
        pub end: u64,
    }

    /// Represents a projection of a dataaset history into a state for fast
    /// lookups.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Projection
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Projection {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the resource.
        pub spec: dataset::ProjectionSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl Projection {
        pub fn schema() -> &'static TypeUri {
            &PROJECTION_SCHEMA
        }
    }

    static PROJECTION_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/dataset/v1alpha1/Projection";

    static PROJECTION_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(PROJECTION_SCHEMA_STR));

    /// Represents a projection of a dataaset history into a state for fast
    /// lookups.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ProjectionSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ProjectionSpec {
        /// Datasets that will be used as sources.
        pub inputs: Vec<dataset::TransformInput>,
        /// Transformation that will be applied to produce new data.
        pub project: dataset::Transform,
    }

    /// Establishes the identity of the dataset. Always the first metadata event
    /// in the chain.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Seed
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Seed {
        /// Unique identity of the dataset.
        pub dataset_id: DatasetID,
        /// Type of the dataset.
        pub dataset_kind: dataset::DatasetKind,
    }

    /// Associates a set of files with this dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetAttachments
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SetAttachments {
        /// One of the supported attachment sources.
        pub attachments: dataset::Attachments,
    }

    /// Specifies the complete schema of Data Slices added to the Dataset
    /// following this event.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetDataSchema
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SetDataSchema {
        /// DEPRECATED: Apache Arrow schema encoded in its native flatbuffers
        /// representation.
        pub raw_arrow_schema: Option<Vec<u8>>,
        /// Defines the logical schema of the data files that follow this event.
        /// Will become a required field after migration.
        pub schema: Option<data::DataSchema>,
    }

    /// Provides basic human-readable information about a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetInfo
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SetInfo {
        /// Brief single-sentence summary of a dataset.
        pub description: Option<String>,
        /// Keywords, search terms, or tags used to describe the dataset.
        pub keywords: Option<Vec<String>>,
    }

    /// Defines a license that applies to this dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetLicense
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SetLicense {
        /// Abbreviated name of the license.
        pub short_name: String,
        /// Full name of the license.
        pub name: String,
        /// License identifier from the SPDX License List.
        pub spdx_id: Option<String>,
        /// URL where licensing terms can be found.
        pub website_url: String,
    }

    /// Defines a transformation that produces data in a derivative dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetTransform
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SetTransform {
        /// Datasets that will be used as sources.
        pub inputs: Vec<dataset::TransformInput>,
        /// Transformation that will be applied to produce new data.
        pub transform: dataset::Transform,
    }

    /// Lets you manipulate names of the system columns to avoid conflicts.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetVocab
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SetVocab {
        /// Name of the offset column.
        pub offset_column: Option<String>,
        /// Name of the operation type column.
        pub operation_type_column: Option<String>,
        /// Name of the system time column.
        pub system_time_column: Option<String>,
        /// Name of the event time column.
        pub event_time_column: Option<String>,
    }

    /// Defines a query in a multi-step SQL transformation.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SqlQueryStep
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SqlQueryStep {
        /// Name of the temporary view that will be created from result of the
        /// query. Step without this alias will be treated as an output of the
        /// transformation.
        pub alias: Option<String>,
        /// SQL query the result of which will be exposed under the alias.
        pub query: String,
    }

    /// Temporary Flink-specific extension for creating temporal tables from
    /// streams.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/TemporalTable
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TemporalTable {
        /// Name of the dataset to be converted into a temporal table.
        pub name: String,
        /// Column names used as the primary key for creating a table.
        pub primary_key: Vec<String>,
    }

    /// Engine-specific processing queries that shape the resulting data.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Transform
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum Transform {
        Sql(dataset::TransformSql),
    }

    impl_enum_with_variants!(Transform);
    impl_enum_variant!(Transform::Sql(dataset::TransformSql));

    /// Describes a derivative transformation input
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/TransformInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformInput {
        /// A local or remote dataset reference. When block is accepted this
        /// MUST be in the form of a DatasetId to guarantee reproducibility, as
        /// aliases can change over time.
        pub dataset_ref: DatasetRef,
        /// An alias under which this input will be available in queries. Will
        /// be populated from `datasetRef` if not provided before resolving it
        /// to DatasetId.
        pub alias: Option<String>,
    }

    /// Transform using one of the SQL dialects.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Transform#/$defs/Sql
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformSql {
        /// Identifier of the engine used for this transformation.
        pub engine: String,
        /// Version of the engine to use.
        pub version: Option<String>,
        /// SQL query the result of which will be used as an output. This is a
        /// convenience property meant only for defining queries by hand. When
        /// stored in the metadata this property will never be set and instead
        /// will be converted into a single-iter `queries` array.
        pub query: Option<String>,
        /// Specifies multi-step SQL transformations. Each step acts as a
        /// shorthand for `CREATE TEMPORARY VIEW <alias> AS (<query>)`. Last
        /// query in the array should have no alias and will be treated as an
        /// output.
        pub queries: Option<Vec<dataset::SqlQueryStep>>,
        /// Temporary Flink-specific extension for creating temporal tables from
        /// streams.
        pub temporal_tables: Option<Vec<dataset::TemporalTable>>,
    }

    /// Represents a watermark in the event stream.
    ///
    /// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Watermark
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Watermark {
        /// Moment in processing time when watermark was emitted.
        pub system_time: DateTime<Utc>,
        /// Moment in event time which watermark has reached.
        pub event_time: DateTime<Utc>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// engine
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod engine {
    #[allow(unused_imports)]
    use super::*;

    /// Sent by the coordinator to an engine to perform query on raw input data,
    /// usually as part of ingest preprocessing step
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryRequest
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RawQueryRequest {
        /// Paths to input data files to perform query over. Must all have
        /// identical schema.
        pub input_data_paths: Vec<PathBuf>,
        /// Transformation that will be applied to produce new data.
        pub transform: dataset::Transform,
        /// Path where query result will be written.
        pub output_data_path: PathBuf,
    }

    /// Sent by an engine to coordinator when performing the raw query operation
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RawQueryResponse {
        Progress(engine::RawQueryResponseProgress),
        Success(engine::RawQueryResponseSuccess),
        InvalidQuery(engine::RawQueryResponseInvalidQuery),
        InternalError(engine::RawQueryResponseInternalError),
    }

    impl_enum_with_variants!(RawQueryResponse);
    impl_enum_variant!(RawQueryResponse::Progress(engine::RawQueryResponseProgress));
    impl_enum_variant!(RawQueryResponse::Success(engine::RawQueryResponseSuccess));
    impl_enum_variant!(RawQueryResponse::InvalidQuery(
        engine::RawQueryResponseInvalidQuery
    ));
    impl_enum_variant!(RawQueryResponse::InternalError(
        engine::RawQueryResponseInternalError
    ));

    /// Internal error during query execution
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/InternalError
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RawQueryResponseInternalError {
        /// Brief description of an error
        pub message: String,
        /// Details of an error (e.g. a backtrace)
        pub backtrace: Option<String>,
    }

    /// Query did not pass validation
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/InvalidQuery
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RawQueryResponseInvalidQuery {
        /// Explanation of an error
        pub message: String,
    }

    /// Reports query progress
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/Progress
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct RawQueryResponseProgress {}

    /// Query executed successfully
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/Success
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RawQueryResponseSuccess {
        /// Number of records produced by the query
        pub num_records: u64,
    }

    /// Sent by the coordinator to an engine to perform the next step of data
    /// transformation
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformRequest
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformRequest {
        /// Unique identifier of the output dataset.
        pub dataset_id: DatasetID,
        /// Alias of the output dataset, for logging purposes only.
        pub dataset_alias: DatasetAlias,
        /// System time to use for new records.
        pub system_time: DateTime<Utc>,
        /// Vocabulary of the output dataset.
        pub vocab: dataset::DatasetVocabulary,
        /// Transformation that will be applied to produce new data.
        pub transform: dataset::Transform,
        /// Defines inputs used in this transaction. Slices corresponding to
        /// every input dataset must be present.
        pub query_inputs: Vec<engine::TransformRequestInput>,
        /// Starting offset to use for new data records.
        pub next_offset: u64,
        /// TODO: This will be removed when coordinator will be speaking to
        /// engines purely through Arrow.
        pub prev_checkpoint_path: Option<PathBuf>,
        /// TODO: This will be removed when coordinator will be speaking to
        /// engines purely through Arrow.
        pub new_checkpoint_path: PathBuf,
        /// TODO: This will be removed when coordinator will be speaking to
        /// engines purely through Arrow.
        pub new_data_path: PathBuf,
    }

    /// Sent as part of the engine transform request operation to describe the
    /// input
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformRequestInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformRequestInput {
        /// Unique identifier of the dataset.
        pub dataset_id: DatasetID,
        /// Alias of the output dataset, for logging purposes only.
        pub dataset_alias: DatasetAlias,
        /// An alias of this input to be used in queries.
        pub query_alias: String,
        /// Vocabulary of the input dataset.
        pub vocab: dataset::DatasetVocabulary,
        /// Subset of data that goes into this transaction.
        pub offset_interval: Option<dataset::OffsetInterval>,
        /// TODO: This will be removed when coordinator will be slicing data for
        /// the engine.
        pub data_paths: Vec<PathBuf>,
        /// TODO: replace with actual DDL or Parquet schema.
        pub schema_file: PathBuf,
        /// Watermarks that should be injected into the stream to separate micro
        /// batches for reproducibility.
        pub explicit_watermarks: Vec<dataset::Watermark>,
    }

    /// Sent by an engine to coordinator when performing the data transformation
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum TransformResponse {
        Progress(engine::TransformResponseProgress),
        Success(engine::TransformResponseSuccess),
        InvalidQuery(engine::TransformResponseInvalidQuery),
        InternalError(engine::TransformResponseInternalError),
    }

    impl_enum_with_variants!(TransformResponse);
    impl_enum_variant!(TransformResponse::Progress(
        engine::TransformResponseProgress
    ));
    impl_enum_variant!(TransformResponse::Success(engine::TransformResponseSuccess));
    impl_enum_variant!(TransformResponse::InvalidQuery(
        engine::TransformResponseInvalidQuery
    ));
    impl_enum_variant!(TransformResponse::InternalError(
        engine::TransformResponseInternalError
    ));

    /// Internal error during query execution
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/InternalError
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformResponseInternalError {
        /// Brief description of an error
        pub message: String,
        /// Details of an error (e.g. a backtrace)
        pub backtrace: Option<String>,
    }

    /// Query did not pass validation
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/InvalidQuery
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformResponseInvalidQuery {
        /// Explanation of an error
        pub message: String,
    }

    /// Reports query progress
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/Progress
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct TransformResponseProgress {}

    /// Query executed successfully
    ///
    /// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/Success
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct TransformResponseSuccess {
        /// Data slice produced by the transaction, if any.
        pub new_offset_interval: Option<dataset::OffsetInterval>,
        /// Watermark advanced by the transaction, if any.
        pub new_watermark: Option<DateTime<Utc>>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// event
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod event {
    #[allow(unused_imports)]
    use super::*;

    /// Filters that work on domain event types and fields.
    ///
    /// Schema: https://opendatafabric.org/schemas/event/v1alpha1/EventFilter
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct EventFilter {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// flow
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod flow {
    #[allow(unused_imports)]
    use super::*;

    /// Defines a sequence of tasks to be executed upon certain trigger
    /// conditions.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/Flow
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Flow {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the flow.
        pub spec: flow::FlowSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl Flow {
        pub fn schema() -> &'static TypeUri {
            &FLOW_SCHEMA
        }
    }

    static FLOW_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/flow/v1alpha1/Flow";

    static FLOW_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(FLOW_SCHEMA_STR));

    /// Defines a sequence of tasks to be executed upon certain trigger
    /// conditions.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowSpec {
        /// Defines resources for which this flow will be instantiated.
        pub target: resource::ResourceSelector,
        /// Conditions that cause this flow to execute.
        pub triggers: Vec<flow::FlowTrigger>,
        /// List of tasks to run consecutively.
        pub tasks: Vec<flow::TaskSpec>,
    }

    /// Condition that causes a flow to be executed.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum FlowTrigger {
        Schedule(flow::FlowTriggerSchedule),
        Event(flow::FlowTriggerEvent),
        Source(flow::FlowTriggerSource),
        Dataset(flow::FlowTriggerDataset),
    }

    impl_enum_with_variants!(FlowTrigger);
    impl_enum_variant!(FlowTrigger::Schedule(flow::FlowTriggerSchedule));
    impl_enum_variant!(FlowTrigger::Event(flow::FlowTriggerEvent));
    impl_enum_variant!(FlowTrigger::Source(flow::FlowTriggerSource));
    impl_enum_variant!(FlowTrigger::Dataset(flow::FlowTriggerDataset));

    /// Triggers the flow when matching datasets are updated.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Dataset
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowTriggerDataset {
        /// Selector that identifies which datasets can trigger this flow.
        pub dataset: dataset::DatasetSelector,
        /// Set of event bus event IDs that this trigger will react to
        pub events: Option<Vec<String>>,
    }

    /// Triggers the flow when an event bus event matching one of the filters is
    /// observed.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Event
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowTriggerEvent {
        /// Filters the event by type and fields.
        pub events: event::EventFilter,
        /// The trigger will fire upon first observed event. If another event
        /// arrives withing the `cooldown` interval the firing will be postponed
        /// until `cooldown` interval ends. I.e. trigger is guaranteed to fire,
        /// but may batch multiple events together into one flow run.
        pub cooldown: Option<DurationString>,
        /// If an event is observed a `cooldownMaxBatch` number of times during
        /// the `cooldown` interval it will fire the trigger without waiting for
        /// cooldown to finish.
        pub cooldown_max_batch: Option<u64>,
    }

    /// Triggers the flow on a cron schedule.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Schedule
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowTriggerSchedule {
        /// Cron5 expression defining the schedule e.g. `@daily` or `*/30 * * *
        /// *`.
        pub cron: String,
    }

    /// Triggers the flow when a source receives new data, with optional
    /// batching controls.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Source
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowTriggerSource {
        /// Reference to the source resource that drives this trigger.
        pub source: resource::ResourceRef,
        /// Minimum number of new records to accumulate before triggering.
        pub min_records_to_await: Option<u64>,
        /// Maximum time to wait for `minRecordsToAwait` before triggering
        /// anyway e.g. `1h`.
        pub max_await_interval: Option<DurationString>,
    }

    /// An individual work item to be executed.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/Task
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Task {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the task.
        pub spec: Option<flow::TaskSpec>,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl Task {
        pub fn schema() -> &'static TypeUri {
            &TASK_SCHEMA
        }
    }

    static TASK_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/flow/v1alpha1/Task";

    static TASK_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(TASK_SCHEMA_STR));

    /// An individual work item to be executed as part of a flow.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum TaskSpec {
        Ingest(flow::TaskSpecIngest),
        Compaction(flow::TaskSpecCompaction),
        GarbageCollection(flow::TaskSpecGarbageCollection),
        WebhookCall(flow::TaskSpecWebhookCall),
    }

    impl_enum_with_variants!(TaskSpec);
    impl_enum_variant!(TaskSpec::Ingest(flow::TaskSpecIngest));
    impl_enum_variant!(TaskSpec::Compaction(flow::TaskSpecCompaction));
    impl_enum_variant!(TaskSpec::GarbageCollection(flow::TaskSpecGarbageCollection));
    impl_enum_variant!(TaskSpec::WebhookCall(flow::TaskSpecWebhookCall));

    /// Compacts data files in matching datasets to improve query performance.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/Compaction
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct TaskSpecCompaction {
        /// Optional parameters to control ingestion behavior.
        pub params: Option<dataset::CompactionParams>,
    }

    /// Removes unreferenced data files from matching datasets.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/GarbageCollection
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct TaskSpecGarbageCollection {}

    /// Fetches data from a source and appends it to a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/Ingest
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TaskSpecIngest {
        /// Reference to the source resource that defines how to fetch data.
        pub source: resource::ResourceRef,
        /// Optional parameters to control ingestion behavior.
        pub params: Option<source::IngestParams>,
    }

    /// Dispatches a certain payload to a specific `WebhookTarget`.
    ///
    /// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/WebhookCall
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TaskSpecWebhookCall {
        /// Reference to the `WebhookTarget`.
        pub target: resource::ResourceRef,
        /// The payload to send. May include templating.
        pub payload: Option<String>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// legacy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod legacy {
    #[allow(unused_imports)]
    use super::*;

    /// Describes how to ingest data into a root dataset from a certain logical
    /// source.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/AddPushSource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AddPushSource {
        /// Identifies the source within this dataset.
        pub source_name: String,
        /// Defines how data is read into structured format.
        pub read: source::ReadStep,
        /// Pre-processing query that shapes the data.
        pub preprocess: Option<dataset::Transform>,
        /// Determines how newly-ingested data should be merged with existing
        /// history.
        pub merge: source::MergeStrategy,
    }

    /// Represents a projection of the dataset metadata at a single point in
    /// time. This type is typically used for defining new datasets and
    /// changing the existing ones.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/DatasetSnapshot
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DatasetSnapshot {
        /// Alias of the dataset.
        pub name: DatasetAlias,
        /// Type of the dataset.
        pub kind: dataset::DatasetKind,
        /// An array of metadata events that will be used to populate the chain.
        /// Here you can define polling and push sources, set licenses, add
        /// attachments etc.
        pub metadata: Vec<dataset::MetadataEvent>,
    }

    /// Disables the previously defined polling source.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePollingSource
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DisablePollingSource {}

    /// Disables the previously defined source.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePushSource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DisablePushSource {
        /// Identifies the source to be disabled.
        pub source_name: String,
    }

    /// Defines the external source of data.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum FetchStep {
        Url(legacy::FetchStepUrl),
        FilesGlob(legacy::FetchStepFilesGlob),
        Container(legacy::FetchStepContainer),
        Mqtt(legacy::FetchStepMqtt),
        EthereumLogs(legacy::FetchStepEthereumLogs),
    }

    impl_enum_with_variants!(FetchStep);
    impl_enum_variant!(FetchStep::Url(legacy::FetchStepUrl));
    impl_enum_variant!(FetchStep::FilesGlob(legacy::FetchStepFilesGlob));
    impl_enum_variant!(FetchStep::Container(legacy::FetchStepContainer));
    impl_enum_variant!(FetchStep::Mqtt(legacy::FetchStepMqtt));
    impl_enum_variant!(FetchStep::EthereumLogs(legacy::FetchStepEthereumLogs));

    /// Runs the specified OCI container to fetch data from an arbitrary source.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Container
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FetchStepContainer {
        /// Image name and and an optional tag.
        pub image: String,
        /// Specifies the entrypoint. Not executed within a shell. The default
        /// OCI image's ENTRYPOINT is used if this is not provided.
        pub command: Option<Vec<String>>,
        /// Arguments to the entrypoint. The OCI image's CMD is used if this is
        /// not provided.
        pub args: Option<Vec<String>>,
        /// Environment variables to propagate into or set in the container.
        pub env: Option<Vec<source::EnvVar>>,
    }

    /// Connects to an Ethereum node to stream transaction logs.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/EthereumLogs
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct FetchStepEthereumLogs {
        /// Identifier of the chain to scan logs from. This parameter may be
        /// used for RPC endpoint lookup as well as asserting that provided
        /// `nodeUrl` corresponds to the expected chain.
        pub chain_id: Option<u64>,
        /// Url of the node.
        pub node_url: Option<String>,
        /// An SQL WHERE clause that can be used to pre-filter the logs before
        /// fetching them from the ETH node.
        ///
        /// Examples:
        /// - "block_number > 123 and address =
        ///   X'5fbdb2315678afecb367f032d93f642f64180aa3' and topic1 =
        ///   X'000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266'
        ///   "
        pub filter: Option<String>,
        /// Solidity log event signature to use for decoding. Using this field
        /// adds `event` to the output containing decoded log as JSON.
        pub signature: Option<String>,
    }

    /// Uses glob operator to match files on the local file system.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/FilesGlob
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FetchStepFilesGlob {
        /// Path with a glob pattern.
        pub path: String,
        /// Describes how event time is extracted from the source metadata.
        pub event_time: Option<source::EventTimeSource>,
        /// Describes the caching settings used for this source.
        pub cache: Option<source::SourceCaching>,
        /// Specifies how input files should be ordered before ingestion.
        /// Order is important as every file will be processed individually
        /// and will advance the dataset's watermark.
        pub order: Option<source::SourceOrdering>,
    }

    /// Connects to an MQTT broker to fetch events from the specified topic.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Mqtt
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FetchStepMqtt {
        /// Hostname of the MQTT broker.
        pub host: String,
        /// Port of the MQTT broker.
        pub port: i32,
        /// Username to use for auth with the broker.
        pub username: Option<String>,
        /// Password to use for auth with the broker (can be templated).
        pub password: Option<String>,
        /// List of topic subscription parameters.
        pub topics: Vec<source::MqttTopicSubscription>,
    }

    /// Pulls data from one of the supported sources by its URL.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Url
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FetchStepUrl {
        /// URL of the data source
        pub url: String,
        /// Describes how event time is extracted from the source metadata.
        pub event_time: Option<source::EventTimeSource>,
        /// Describes the caching settings used for this source.
        pub cache: Option<source::SourceCaching>,
        /// Headers to pass during the request (e.g. HTTP Authorization)
        pub headers: Option<Vec<source::RequestHeader>>,
    }

    /// An object that wraps the metadata resources providing versioning and
    /// type identification. All root-level resources are wrapped with a
    /// manifest when serialized to disk.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/Manifest
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Manifest<ContentT> {
        /// Type of the resource.
        pub kind: String,
        /// Major version number of the resource contained in this manifest. It
        /// provides the mechanism for introducing compatibility breaking
        /// changes.
        pub version: i32,
        /// Resource data.
        pub content: ContentT,
    }

    /// Contains information on how externally-hosted data can be ingested into
    /// the root dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/SetPollingSource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SetPollingSource {
        /// Determines where data is sourced from.
        pub fetch: legacy::FetchStep,
        /// Defines how raw data is prepared before reading.
        pub prepare: Option<Vec<source::PrepStep>>,
        /// Defines how data is read into structured format.
        pub read: source::ReadStep,
        /// Pre-processing query that shapes the data.
        pub preprocess: Option<dataset::Transform>,
        /// Determines how newly-ingested data should be merged with existing
        /// history.
        pub merge: source::MergeStrategy,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// resource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod resource {
    #[allow(unused_imports)]
    use super::*;

    /// Filters that work on resource labels and identity headers.
    ///
    /// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/LabelFilter
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct LabelFilter {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    /// Top-level container for resources that specifies the type and version of
    /// the resource it's specifying and carries identity, ownership, and status
    /// information.
    ///
    /// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/Resource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Resource<SpecT> {
        /// Identifies the controlling entity, a bounded context that this resource belongs to, and the version. Url should follow the pattern `{base-url}/{context}/{version}/{name}.json` e.g. `https://opendatafabric.org/schemas/dataset/v1/Dataset.json`.
        pub schema: TypeUri,
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of a resource.
        pub spec: SpecT,
        /// Resource lifecycle and reconciliation inforamtion.
        pub status: Option<resource::ResourceStatus>,
    }

    /// Annotations is an unstructured key value map stored with a resource that
    /// may be set by external tools to store and retrieve arbitrary metadata.
    /// Unlike labels, annotations are not indexed and cannot be queried by.
    ///
    /// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceAnnotations
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ResourceAnnotations {
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    /// Container of feneric contditions that can be added by contollers to provide additional information about the state of a resource. Keys uniquely identify the condition and should be in the form of URL to a schema describing this condition, e.g. `https://opendatafabric.org/schemas/resource/ConditionReady.json`.
    ///
    /// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceConditions
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ResourceConditions {
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    /// Container for identity and ownership information of a resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceHeaders
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResourceHeaders {
        /// Unique identifier of a resource within entire ODF node.
        /// Automatically assigned upon resource creation.
        pub id: Option<ResourceID>,
        /// Symbolic name of a resource that identifies it within a scope of an
        /// onwing account.
        pub name: ResourceName,
        /// Reference to an account that owns the resource.
        pub account: Option<auth::AccountRef>,
        /// Map of string keys and values that can be used to organize,
        /// categorize, and query resources.
        pub labels: Option<resource::ResourceLabels>,
        /// Annotations is an unstructured key value map stored with a resource
        /// that may be set by external tools to store and retrieve arbitrary
        /// metadata. Unlike labels, annotations are not indexed and cannot be
        /// queried by.
        pub annotations: Option<resource::ResourceAnnotations>,
        /// A sequential number that changes every time the resource header and
        /// spec are updated. Does not increment on status changes, thus
        /// signifying changes to the desired state. Populated by the system.
        /// Starts with `1`.
        pub generation: Option<u64>,
        /// Time when the resource was first applied and assigned an identity.
        pub created_at: Option<DateTime<Utc>>,
        /// Time when the resource was last updated, including header, spec, and
        /// status updates.
        pub updated_at: Option<DateTime<Utc>>,
        /// Time when the resource was deleted.
        pub deleted_at: Option<DateTime<Utc>>,
    }

    /// Map of string keys and values that can be used to organize, categorize,
    /// and query resources.
    ///
    /// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceLabels
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ResourceLabels {
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    /// Represents the lifecycle stage of a resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourcePhase
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum ResourcePhase {
        Pending,
        Reconciling,
        Ready,
        Failed,
    }

    pub use crate::resource::{ResourceRef, ResourceSelector};

    /// Resource lifecycle and reconciliation inforamtion.
    ///
    /// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceStatus
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResourceStatus {
        /// Represents the lifecycle stage of a resource.
        pub phase: resource::ResourcePhase,
        /// Resource generation that was last processed by the main resource
        /// controller.
        pub observed_generation: Option<u64>,
        /// Time when the controller last reconciled the desired resource state
        /// as defined in `observedGeneration`.
        pub reconciled_at: Option<DateTime<Utc>>,
        /// Detailed conditions describing the state of the resource that are
        /// added by controllers.
        pub conditions: Option<resource::ResourceConditions>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// sink
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod sink {
    #[allow(unused_imports)]
    use super::*;

    /// Defines a webhook target endpoint that can receive event notifications
    /// and data.
    ///
    /// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTarget
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct WebhookTarget {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the resource.
        pub spec: sink::WebhookTargetSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl WebhookTarget {
        pub fn schema() -> &'static TypeUri {
            &WEBHOOK_TARGET_SCHEMA
        }
    }

    static WEBHOOK_TARGET_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTarget";

    static WEBHOOK_TARGET_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(WEBHOOK_TARGET_SCHEMA_STR));

    /// Defines a webhook target endpoint that can receive event notifications
    /// and data.
    ///
    /// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct WebhookTargetSpec {
        /// Target url of the webhook.
        pub url: String,
        /// Shared secret used for HMAC signature of the request payload for
        /// authentication.
        pub secret: Option<config::Secret>,
    }

    /// Represents the status of the webhook target endpoint.
    ///
    /// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetStatus
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct WebhookTargetStatus {
        /// Status value.
        pub value: sink::WebhookTargetStatusValue,
    }

    /// Status of the target endpoint
    ///
    /// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetStatus#/$defs/Value
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum WebhookTargetStatusValue {
        Ready,
        Failed,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// source
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod source {
    #[allow(unused_imports)]
    use super::*;

    /// Defines a compression algorithm.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/CompressionFormat
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum CompressionFormat {
        Gzip,
        Zip,
    }

    /// Defines an environment variable passed into some job.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EnvVar
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct EnvVar {
        /// Name of the variable.
        pub name: String,
        /// Value of the variable.
        pub value: Option<String>,
    }

    /// Defines the external source of data.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum EventTimeSource {
        FromMetadata(source::EventTimeSourceFromMetadata),
        FromPath(source::EventTimeSourceFromPath),
        FromSystemTime(source::EventTimeSourceFromSystemTime),
    }

    impl_enum_with_variants!(EventTimeSource);
    impl_enum_variant!(EventTimeSource::FromMetadata(
        source::EventTimeSourceFromMetadata
    ));
    impl_enum_variant!(EventTimeSource::FromPath(source::EventTimeSourceFromPath));
    impl_enum_variant!(EventTimeSource::FromSystemTime(
        source::EventTimeSourceFromSystemTime
    ));

    /// Extracts event time from the source's metadata.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromMetadata
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct EventTimeSourceFromMetadata {}

    /// Extracts event time from the path component of the source.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromPath
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct EventTimeSourceFromPath {
        /// Regular expression where first group contains the timestamp string.
        pub pattern: String,
        /// Format of the expected timestamp in java.text.SimpleDateFormat form.
        pub timestamp_format: Option<String>,
    }

    /// Assigns event time from the system time source.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromSystemTime
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct EventTimeSourceFromSystemTime {}

    /// Optional parameters to control ingestion behavior.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngestParams
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct IngestParams {
        /// Target number of records to ingest per data slice.
        pub target_slice_records: Option<u64>,
    }

    /// Defines the point where data enters the system.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum Ingress {
        Url(source::IngressUrl),
        FilesGlob(source::IngressFilesGlob),
        Container(source::IngressContainer),
        Mqtt(source::IngressMqtt),
        EvmLogs(source::IngressEvmLogs),
        RestEndpoint(source::IngressRestEndpoint),
    }

    impl_enum_with_variants!(Ingress);
    impl_enum_variant!(Ingress::Url(source::IngressUrl));
    impl_enum_variant!(Ingress::FilesGlob(source::IngressFilesGlob));
    impl_enum_variant!(Ingress::Container(source::IngressContainer));
    impl_enum_variant!(Ingress::Mqtt(source::IngressMqtt));
    impl_enum_variant!(Ingress::EvmLogs(source::IngressEvmLogs));
    impl_enum_variant!(Ingress::RestEndpoint(source::IngressRestEndpoint));

    /// Buffer configuration for holding pushed records until they are ingested.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngressBuffer
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum IngressBuffer {
        Memory(source::IngressBufferMemory),
    }

    impl_enum_with_variants!(IngressBuffer);
    impl_enum_variant!(IngressBuffer::Memory(source::IngressBufferMemory));

    /// An in-memory buffer.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngressBuffer#/$defs/Memory
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct IngressBufferMemory {
        /// Maximum number of records to hold in the buffer.
        pub buffer_size: Option<u64>,
        /// Policy applied when the buffer is full.
        pub overflow_policy: Option<String>,
    }

    /// Runs the specified OCI container to fetch data from an arbitrary source.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Container
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IngressContainer {
        /// Image name and and an optional tag.
        pub image: String,
        /// Specifies the entrypoint. Not executed within a shell. The default
        /// OCI image's ENTRYPOINT is used if this is not provided.
        pub command: Option<Vec<String>>,
        /// Arguments to the entrypoint. The OCI image's CMD is used if this is
        /// not provided.
        pub args: Option<Vec<String>>,
        /// Environment variables to propagate into or set in the container.
        pub env: Option<Vec<source::EnvVar>>,
    }

    /// Connects to an EVM (Ethereum) node to stream transaction logs.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/EvmLogs
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct IngressEvmLogs {
        /// Identifier of the chain to scan logs from. This parameter may be
        /// used for RPC endpoint lookup as well as asserting that provided
        /// `nodeUrl` corresponds to the expected chain.
        pub chain_id: Option<u64>,
        /// Url of the node.
        pub node_url: Option<String>,
        /// An SQL WHERE clause that can be used to pre-filter the logs before
        /// fetching them from the ETH node.
        ///
        /// Examples:
        /// - "block_number > 123 and address =
        ///   X'5fbdb2315678afecb367f032d93f642f64180aa3' and topic1 =
        ///   X'000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266'
        ///   "
        pub filter: Option<String>,
        /// Solidity log event signature to use for decoding. Using this field
        /// adds `event` to the output containing decoded log as JSON.
        pub signature: Option<String>,
    }

    /// Uses glob operator to match files on the local file system.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/FilesGlob
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IngressFilesGlob {
        /// Path with a glob pattern.
        pub path: String,
        /// Describes how event time is extracted from the source metadata.
        pub event_time: Option<source::EventTimeSource>,
        /// Describes the caching settings used for this source.
        pub cache: Option<source::SourceCaching>,
        /// Specifies how input files should be ordered before ingestion.
        /// Order is important as every file will be processed individually
        /// and will advance the dataset's watermark.
        pub order: Option<source::SourceOrdering>,
    }

    /// Connects to an MQTT broker to fetch events from the specified topic.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Mqtt
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IngressMqtt {
        /// Hostname of the MQTT broker.
        pub host: String,
        /// Port of the MQTT broker.
        pub port: i32,
        /// Username to use for auth with the broker.
        pub username: Option<String>,
        /// Password to use for auth with the broker (can be templated).
        pub password: Option<String>,
        /// List of topic subscription parameters.
        pub topics: Vec<source::MqttTopicSubscription>,
    }

    /// Exposes a REST HTTP endpoint that accepts pushed data records.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/RestEndpoint
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct IngressRestEndpoint {
        /// Buffer configuration for holding records until they are ingested.
        pub buffer: Option<source::IngressBuffer>,
    }

    /// Pulls data from one of the supported sources by its URL.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Url
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IngressUrl {
        /// URL of the data source
        pub url: String,
        /// Describes how event time is extracted from the source metadata.
        pub event_time: Option<source::EventTimeSource>,
        /// Describes the caching settings used for this source.
        pub cache: Option<source::SourceCaching>,
        /// Headers to pass during the request (e.g. HTTP Authorization)
        pub headers: Option<Vec<source::RequestHeader>>,
    }

    /// Merge strategy determines how newly ingested data should be combined
    /// with the data that already exists in the dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum MergeStrategy {
        Append(source::MergeStrategyAppend),
        Ledger(source::MergeStrategyLedger),
        Snapshot(source::MergeStrategySnapshot),
        ChangelogStream(source::MergeStrategyChangelogStream),
        UpsertStream(source::MergeStrategyUpsertStream),
    }

    impl_enum_with_variants!(MergeStrategy);
    impl_enum_variant!(MergeStrategy::Append(source::MergeStrategyAppend));
    impl_enum_variant!(MergeStrategy::Ledger(source::MergeStrategyLedger));
    impl_enum_variant!(MergeStrategy::Snapshot(source::MergeStrategySnapshot));
    impl_enum_variant!(MergeStrategy::ChangelogStream(
        source::MergeStrategyChangelogStream
    ));
    impl_enum_variant!(MergeStrategy::UpsertStream(
        source::MergeStrategyUpsertStream
    ));

    /// Append merge strategy.
    ///
    /// Under this strategy new data will be appended to the dataset in its
    /// entirety, without any deduplication.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Append
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct MergeStrategyAppend {}

    /// Changelog stream merge strategy.
    ///
    /// This is the native stream format for ODF that accurately describes the
    /// evolution of all event records including appends, retractions, and
    /// corrections as per RFC-015. No pre-processing except for format
    /// validation is done.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/ChangelogStream
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeStrategyChangelogStream {
        /// Names of the columns that uniquely identify the record throughout
        /// its lifetime
        pub primary_key: Vec<String>,
    }

    /// Ledger merge strategy.
    ///
    /// This strategy should be used for data sources containing ledgers of
    /// events. Currently this strategy will only perform deduplication of
    /// events using user-specified primary key columns. This means that the
    /// source data can contain partially overlapping set of records and only
    /// those records that were not previously seen will be appended.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Ledger
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeStrategyLedger {
        /// Names of the columns that uniquely identify the record throughout
        /// its lifetime
        pub primary_key: Vec<String>,
    }

    /// Snapshot merge strategy.
    ///
    /// This strategy can be used for data state snapshots that are taken
    /// periodically and contain only the latest state of the observed entity or
    /// system. Over time such snapshots can have new rows added, and old rows
    /// either removed or modified.
    ///
    /// This strategy transforms snapshot data into an append-only event stream
    /// where data already added is immutable. It does so by performing Change
    /// Data Capture - essentially diffing the current state of data against the
    /// reconstructed previous state and recording differences as retractions or
    /// corrections. The Operation Type "op" column will contain:
    ///   - append (`+A`) when a row appears for the first time
    ///   - retraction (`-D`) when row disappears
    ///   - correction (`-C`, `+C`) when row data has changed, with `-C` event
    ///     carrying the old value of the row and `+C` carrying the new value.
    ///
    /// To correctly associate rows between old and new snapshots this strategy
    /// relies on user-specified primary key columns.
    ///
    /// To identify whether a row has changed this strategy will compare all
    /// other columns one by one. If the data contains a column that is
    /// guaranteed to change whenever any of the data columns changes (for
    /// example a last modification timestamp, an incremental version, or a data
    /// hash), then it can be specified in `compareColumns` property to speed up
    /// the detection of modified rows.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Snapshot
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeStrategySnapshot {
        /// Names of the columns that uniquely identify the record throughout
        /// its lifetime.
        pub primary_key: Vec<String>,
        /// Names of the columns to compared to determine if a row has changed
        /// between two snapshots.
        pub compare_columns: Option<Vec<String>>,
    }

    /// Upsert stream merge strategy.
    ///
    /// This strategy should be used for data sources containing ledgers of
    /// insert-or-update and delete events. Unlike ChangelogStream the
    /// insert-or-update events only carry the new values, so this strategy will
    /// use primary key to re-classify the events into an append or a correction
    /// from/to pair, looking up the previous values.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/UpsertStream
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeStrategyUpsertStream {
        /// Names of the columns that uniquely identify the record throughout
        /// its lifetime
        pub primary_key: Vec<String>,
    }

    /// MQTT quality of service class.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MqttQos
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum MqttQos {
        AtMostOnce,
        AtLeastOnce,
        ExactlyOnce,
    }

    /// MQTT topic subscription parameters.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MqttTopicSubscription
    #[derive(Clone, Debug, Eq)]
    pub struct MqttTopicSubscription {
        /// Name of the topic (may include patterns).
        pub path: String,
        /// Quality of service class.
        ///
        /// Defaults to: "AtMostOnce"
        pub qos: Option<source::MqttQos>,
    }

    impl MqttTopicSubscription {
        pub fn default_qos() -> source::MqttQos {
            source::MqttQos::AtMostOnce
        }
        pub fn qos(&self) -> source::MqttQos {
            self.qos.unwrap_or(Self::default_qos())
        }
    }

    impl PartialEq for MqttTopicSubscription {
        fn eq(&self, other: &Self) -> bool {
            self.path == other.path
                && self.qos.or_else(|| Some(Self::default_qos()))
                    == other.qos.or_else(|| Some(Self::default_qos()))
        }
    }

    /// Defines the steps to prepare raw data for ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum PrepStep {
        Decompress(source::PrepStepDecompress),
        Pipe(source::PrepStepPipe),
    }

    impl_enum_with_variants!(PrepStep);
    impl_enum_variant!(PrepStep::Decompress(source::PrepStepDecompress));
    impl_enum_variant!(PrepStep::Pipe(source::PrepStepPipe));

    /// Pulls data from one of the supported sources by its URL.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep#/$defs/Decompress
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PrepStepDecompress {
        /// Name of a compression algorithm used on data.
        pub format: source::CompressionFormat,
        /// Path to a data file within a multi-file archive. Can contain glob
        /// patterns.
        pub sub_path: Option<String>,
    }

    /// Executes external command to process the data using piped input/output.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep#/$defs/Pipe
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PrepStepPipe {
        /// Command to execute and its arguments.
        pub command: Vec<String>,
    }

    /// Defines how raw data should be read into the structured form.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum ReadStep {
        Csv(source::ReadStepCsv),
        GeoJson(source::ReadStepGeoJson),
        EsriShapefile(source::ReadStepEsriShapefile),
        Parquet(source::ReadStepParquet),
        Json(source::ReadStepJson),
        NdJson(source::ReadStepNdJson),
        NdGeoJson(source::ReadStepNdGeoJson),
    }

    impl_enum_with_variants!(ReadStep);
    impl_enum_variant!(ReadStep::Csv(source::ReadStepCsv));
    impl_enum_variant!(ReadStep::GeoJson(source::ReadStepGeoJson));
    impl_enum_variant!(ReadStep::EsriShapefile(source::ReadStepEsriShapefile));
    impl_enum_variant!(ReadStep::Parquet(source::ReadStepParquet));
    impl_enum_variant!(ReadStep::Json(source::ReadStepJson));
    impl_enum_variant!(ReadStep::NdJson(source::ReadStepNdJson));
    impl_enum_variant!(ReadStep::NdGeoJson(source::ReadStepNdGeoJson));

    /// Reader for comma-separated files.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Csv
    #[derive(Clone, Debug, Eq, Default)]
    pub struct ReadStepCsv {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce
        /// values into more appropriate data types.
        ///
        /// Examples:
        /// - ["date TIMESTAMP","city STRING","population INT"]
        pub ddl_schema: Option<Vec<String>>,
        /// Sets a single character as a separator for each field and value.
        ///
        /// Defaults to: ","
        pub separator: Option<String>,
        /// Decodes the CSV files by the given encoding type.
        ///
        /// Defaults to: "utf8"
        pub encoding: Option<String>,
        /// Sets a single character used for escaping quoted values where the
        /// separator can be part of the value. Set an empty string to turn off
        /// quotations.
        ///
        /// Defaults to: "\""
        pub quote: Option<String>,
        /// Sets a single character used for escaping quotes inside an already
        /// quoted value.
        ///
        /// Defaults to: "\\"
        pub escape: Option<String>,
        /// Use the first line as names of columns.
        ///
        /// Defaults to: false
        pub header: Option<bool>,
        /// Infers the input schema automatically from data. It requires one
        /// extra pass over the data.
        ///
        /// Defaults to: false
        pub infer_schema: Option<bool>,
        /// Sets the string representation of a null value.
        ///
        /// Defaults to: ""
        pub null_value: Option<String>,
        /// Sets the string that indicates a date format. The `rfc3339` is the
        /// only required format, the other format strings are
        /// implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub date_format: Option<String>,
        /// Sets the string that indicates a timestamp format. The `rfc3339` is
        /// the only required format, the other format strings are
        /// implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub timestamp_format: Option<String>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    impl ReadStepCsv {
        pub fn default_separator() -> &'static str {
            ","
        }
        pub fn separator(&self) -> &str {
            self.separator
                .as_deref()
                .unwrap_or(Self::default_separator())
        }
        pub fn default_encoding() -> &'static str {
            "utf8"
        }
        pub fn encoding(&self) -> &str {
            self.encoding.as_deref().unwrap_or(Self::default_encoding())
        }
        pub fn default_quote() -> &'static str {
            "\""
        }
        pub fn quote(&self) -> &str {
            self.quote.as_deref().unwrap_or(Self::default_quote())
        }
        pub fn default_escape() -> &'static str {
            "\\"
        }
        pub fn escape(&self) -> &str {
            self.escape.as_deref().unwrap_or(Self::default_escape())
        }
        pub fn default_header() -> bool {
            false
        }
        pub fn header(&self) -> bool {
            self.header.unwrap_or(Self::default_header())
        }
        pub fn default_infer_schema() -> bool {
            false
        }
        pub fn infer_schema(&self) -> bool {
            self.infer_schema.unwrap_or(Self::default_infer_schema())
        }
        pub fn default_null_value() -> &'static str {
            ""
        }
        pub fn null_value(&self) -> &str {
            self.null_value
                .as_deref()
                .unwrap_or(Self::default_null_value())
        }
        pub fn default_date_format() -> &'static str {
            "rfc3339"
        }
        pub fn date_format(&self) -> &str {
            self.date_format
                .as_deref()
                .unwrap_or(Self::default_date_format())
        }
        pub fn default_timestamp_format() -> &'static str {
            "rfc3339"
        }
        pub fn timestamp_format(&self) -> &str {
            self.timestamp_format
                .as_deref()
                .unwrap_or(Self::default_timestamp_format())
        }
    }

    impl PartialEq for ReadStepCsv {
        fn eq(&self, other: &Self) -> bool {
            self.ddl_schema == other.ddl_schema
                && self
                    .separator
                    .as_deref()
                    .or_else(|| Some(Self::default_separator()))
                    == other
                        .separator
                        .as_deref()
                        .or_else(|| Some(Self::default_separator()))
                && self
                    .encoding
                    .as_deref()
                    .or_else(|| Some(Self::default_encoding()))
                    == other
                        .encoding
                        .as_deref()
                        .or_else(|| Some(Self::default_encoding()))
                && self
                    .quote
                    .as_deref()
                    .or_else(|| Some(Self::default_quote()))
                    == other
                        .quote
                        .as_deref()
                        .or_else(|| Some(Self::default_quote()))
                && self
                    .escape
                    .as_deref()
                    .or_else(|| Some(Self::default_escape()))
                    == other
                        .escape
                        .as_deref()
                        .or_else(|| Some(Self::default_escape()))
                && self.header.or_else(|| Some(Self::default_header()))
                    == other.header.or_else(|| Some(Self::default_header()))
                && self
                    .infer_schema
                    .or_else(|| Some(Self::default_infer_schema()))
                    == other
                        .infer_schema
                        .or_else(|| Some(Self::default_infer_schema()))
                && self
                    .null_value
                    .as_deref()
                    .or_else(|| Some(Self::default_null_value()))
                    == other
                        .null_value
                        .as_deref()
                        .or_else(|| Some(Self::default_null_value()))
                && self
                    .date_format
                    .as_deref()
                    .or_else(|| Some(Self::default_date_format()))
                    == other
                        .date_format
                        .as_deref()
                        .or_else(|| Some(Self::default_date_format()))
                && self
                    .timestamp_format
                    .as_deref()
                    .or_else(|| Some(Self::default_timestamp_format()))
                    == other
                        .timestamp_format
                        .as_deref()
                        .or_else(|| Some(Self::default_timestamp_format()))
                && self.schema == other.schema
        }
    }

    /// Reader for ESRI Shapefile format.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/EsriShapefile
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ReadStepEsriShapefile {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce
        /// values into more appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// If the ZIP archive contains multiple shapefiles use this field to
        /// specify a sub-path to the desired `.shp` file. Can contain glob
        /// patterns to act as a filter.
        pub sub_path: Option<String>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    /// Reader for GeoJSON files. It expects one `FeatureCollection` object in
    /// the root and will create a record per each `Feature` inside it
    /// extracting the properties into individual columns and leaving the
    /// feature geometry in its own column.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/GeoJson
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ReadStepGeoJson {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce
        /// values into more appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    /// Reader for JSON files that contain an array of objects within them.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Json
    #[derive(Clone, Debug, Eq, Default)]
    pub struct ReadStepJson {
        /// Path in the form of `a.b.c` to a sub-element of the root JSON object
        /// that is an array or objects. If not specified it is assumed that the
        /// root element is an array.
        pub sub_path: Option<String>,
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce
        /// values into more appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Sets the string that indicates a date format. The `rfc3339` is the
        /// only required format, the other format strings are
        /// implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub date_format: Option<String>,
        /// Allows to forcibly set one of standard basic or extended encodings.
        ///
        /// Defaults to: "utf8"
        pub encoding: Option<String>,
        /// Sets the string that indicates a timestamp format. The `rfc3339` is
        /// the only required format, the other format strings are
        /// implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub timestamp_format: Option<String>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    impl ReadStepJson {
        pub fn default_date_format() -> &'static str {
            "rfc3339"
        }
        pub fn date_format(&self) -> &str {
            self.date_format
                .as_deref()
                .unwrap_or(Self::default_date_format())
        }
        pub fn default_encoding() -> &'static str {
            "utf8"
        }
        pub fn encoding(&self) -> &str {
            self.encoding.as_deref().unwrap_or(Self::default_encoding())
        }
        pub fn default_timestamp_format() -> &'static str {
            "rfc3339"
        }
        pub fn timestamp_format(&self) -> &str {
            self.timestamp_format
                .as_deref()
                .unwrap_or(Self::default_timestamp_format())
        }
    }

    impl PartialEq for ReadStepJson {
        fn eq(&self, other: &Self) -> bool {
            self.sub_path == other.sub_path
                && self.ddl_schema == other.ddl_schema
                && self
                    .date_format
                    .as_deref()
                    .or_else(|| Some(Self::default_date_format()))
                    == other
                        .date_format
                        .as_deref()
                        .or_else(|| Some(Self::default_date_format()))
                && self
                    .encoding
                    .as_deref()
                    .or_else(|| Some(Self::default_encoding()))
                    == other
                        .encoding
                        .as_deref()
                        .or_else(|| Some(Self::default_encoding()))
                && self
                    .timestamp_format
                    .as_deref()
                    .or_else(|| Some(Self::default_timestamp_format()))
                    == other
                        .timestamp_format
                        .as_deref()
                        .or_else(|| Some(Self::default_timestamp_format()))
                && self.schema == other.schema
        }
    }

    /// Reader for Newline-delimited GeoJSON files. It is similar to `GeoJson`
    /// format but instead of `FeatureCollection` object in the root it expects
    /// every individual feature object to appear on its own line.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/NdGeoJson
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ReadStepNdGeoJson {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce
        /// values into more appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    /// Reader for files containing multiple newline-delimited JSON objects with
    /// the same schema.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/NdJson
    #[derive(Clone, Debug, Eq, Default)]
    pub struct ReadStepNdJson {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce
        /// values into more appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Sets the string that indicates a date format. The `rfc3339` is the
        /// only required format, the other format strings are
        /// implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub date_format: Option<String>,
        /// Allows to forcibly set one of standard basic or extended encodings.
        ///
        /// Defaults to: "utf8"
        pub encoding: Option<String>,
        /// Sets the string that indicates a timestamp format. The `rfc3339` is
        /// the only required format, the other format strings are
        /// implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub timestamp_format: Option<String>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    impl ReadStepNdJson {
        pub fn default_date_format() -> &'static str {
            "rfc3339"
        }
        pub fn date_format(&self) -> &str {
            self.date_format
                .as_deref()
                .unwrap_or(Self::default_date_format())
        }
        pub fn default_encoding() -> &'static str {
            "utf8"
        }
        pub fn encoding(&self) -> &str {
            self.encoding.as_deref().unwrap_or(Self::default_encoding())
        }
        pub fn default_timestamp_format() -> &'static str {
            "rfc3339"
        }
        pub fn timestamp_format(&self) -> &str {
            self.timestamp_format
                .as_deref()
                .unwrap_or(Self::default_timestamp_format())
        }
    }

    impl PartialEq for ReadStepNdJson {
        fn eq(&self, other: &Self) -> bool {
            self.ddl_schema == other.ddl_schema
                && self
                    .date_format
                    .as_deref()
                    .or_else(|| Some(Self::default_date_format()))
                    == other
                        .date_format
                        .as_deref()
                        .or_else(|| Some(Self::default_date_format()))
                && self
                    .encoding
                    .as_deref()
                    .or_else(|| Some(Self::default_encoding()))
                    == other
                        .encoding
                        .as_deref()
                        .or_else(|| Some(Self::default_encoding()))
                && self
                    .timestamp_format
                    .as_deref()
                    .or_else(|| Some(Self::default_timestamp_format()))
                    == other
                        .timestamp_format
                        .as_deref()
                        .or_else(|| Some(Self::default_timestamp_format()))
                && self.schema == other.schema
        }
    }

    /// Reader for Apache Parquet format.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Parquet
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ReadStepParquet {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce
        /// values into more appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    /// Defines a header (e.g. HTTP) to be passed into some request.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/RequestHeader
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RequestHeader {
        /// Name of the header.
        pub name: String,
        /// Value of the header.
        pub value: String,
    }

    /// Defines an external source of data for ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Source
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Source {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the resource.
        pub spec: source::SourceSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl Source {
        pub fn schema() -> &'static TypeUri {
            &SOURCE_SCHEMA
        }
    }

    static SOURCE_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/source/v1alpha1/Source";

    static SOURCE_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(SOURCE_SCHEMA_STR));

    /// Defines how external data should be cached.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceCaching
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum SourceCaching {
        Forever(source::SourceCachingForever),
    }

    impl_enum_with_variants!(SourceCaching);
    impl_enum_variant!(SourceCaching::Forever(source::SourceCachingForever));

    /// After source was processed once it will never be ingested again.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceCaching#/$defs/Forever
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SourceCachingForever {}

    /// Specifies how input files should be ordered before ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceOrdering
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum SourceOrdering {
        ByEventTime,
        ByName,
    }

    /// Specifies an external source of data for ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SourceSpec {
        /// Brings the configuration values into the local `config` context.
        pub config: Option<config::ValueRefs>,
        /// Determines where data is sourced from.
        pub ingress: Option<source::Ingress>,
        /// Defines how raw data is prepared before reading.
        pub prepare: Option<Vec<source::PrepStep>>,
        /// Defines how data is read into structured format.
        pub read: source::ReadStep,
        /// Pre-processing query that shapes the data.
        pub preprocess: Option<dataset::Transform>,
        /// Determines how newly-ingested data should be merged with existing
        /// history.
        pub merge: Option<source::MergeStrategy>,
        /// Defines the mapping of system fields to dataset column names.
        pub vocab: Option<dataset::DatasetVocabulary>,
    }

    /// The state of the source the data was added from to allow fast resuming.
    ///
    /// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceState
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SourceState {
        /// Identifies the source that the state corresponds to.
        pub source_name: String,
        /// Identifies the type of the state. Standard types include:
        /// `odf/etag`, `odf/last-modified`.
        pub kind: String,
        /// Opaque value representing the state.
        pub value: String,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// storage
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod storage {
    #[allow(unused_imports)]
    use super::*;

    /// Access credentials for AWS or an AWS-compatible service.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/AwsCredentials
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct AwsCredentials {
        /// Reference to a secret containing the AWS access key ID.
        pub access_key: Option<config::ValueRef>,
        /// Reference to a secret containing the AWS secret access key.
        pub secret_key: Option<config::ValueRef>,
    }

    /// Defines a storage volume where data can be stored and its access
    /// credentials.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolume
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PersistentVolume {
        /// Container for identity and ownership information of a resource.
        pub headers: resource::ResourceHeaders,
        /// Specifies the desired state of the resource.
        pub spec: storage::PersistentVolumeSpec,
        /// Resource lifecycle and reconciliation information.
        pub status: Option<resource::ResourceStatus>,
    }

    impl PersistentVolume {
        pub fn schema() -> &'static TypeUri {
            &PERSISTENT_VOLUME_SCHEMA
        }
    }

    static PERSISTENT_VOLUME_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolume";

    static PERSISTENT_VOLUME_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(PERSISTENT_VOLUME_SCHEMA_STR));

    pub use crate::storage::PersistentVolumeRef;

    /// Defines a storage volume where data can be stored and its access
    /// credentials.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum PersistentVolumeSpec {
        S3(storage::PersistentVolumeSpecS3),
    }

    impl_enum_with_variants!(PersistentVolumeSpec);
    impl_enum_variant!(PersistentVolumeSpec::S3(storage::PersistentVolumeSpecS3));

    /// An Amazon S3 or S3-compatible object storage bucket.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec#/$defs/S3
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PersistentVolumeSpecS3 {
        /// S3 endpoint URL. If omitted, defaults to AWS S3. Use for S3-compatible stores e.g. `https://s3.amazonaws.com`.
        pub endpoint: Option<String>,
        /// AWS region where the bucket is located e.g. `us-west-2`.
        pub region: Option<String>,
        /// Name of the S3 bucket.
        pub bucket: String,
        /// Optional path prefix within the bucket.
        pub prefix: Option<String>,
        /// Storage capacity allocation.
        pub capacity: Option<storage::VolumeCapacity>,
        /// Access credentials for the bucket.
        pub credentials: Option<storage::AwsCredentials>,
    }

    /// Storage capacity allocation.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/VolumeCapacity
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct VolumeCapacity {
        /// Maximum storage size e.g. `10Gi`.
        pub storage: Option<ByteSize>,
    }
}
