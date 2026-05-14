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
#![allow(unused_variables)]

use std::path::PathBuf;

use ::serde::{Deserialize, Deserializer, Serialize, Serializer};
use chrono::{DateTime, Utc};
use multiformats::*;
use setty::types::{ByteSize, DurationString};

use super::formats::*;
use crate::auth::{AccountID, AccountName};
use crate::dataset::{DatasetAlias, DatasetID, DatasetRef};
use crate::dtos;
use crate::errors::ValidationError;
use crate::resource::{ResourceID, ResourceName, TypeRef, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait IntoDto {
    type Dto;
    fn into_dto(self) -> Result<Self::Dto, ValidationError>;
}

impl IntoDto for ::serde::de::IgnoredAny {
    type Dto = Self;
    fn into_dto(self) -> Result<Self::Dto, ValidationError> {
        Ok(self)
    }
}

impl IntoDto for ::serde_json::Value {
    type Dto = Self;
    fn into_dto(self) -> Result<Self::Dto, ValidationError> {
        Ok(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! implement_serde_as {
    ($dto:ty, $proxy:ty) => {
        impl ::serde_with::SerializeAs<$dto> for $proxy {
            fn serialize_as<S>(value: &$dto, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                // TODO: PERF: Avoid cloning on serialize
                let value: $proxy = value.clone().into();
                value.serialize(serializer)
            }
        }

        impl<'de> serde_with::DeserializeAs<'de, $dto> for $proxy {
            fn deserialize_as<D>(deserializer: D) -> Result<$dto, D::Error>
            where
                D: Deserializer<'de>,
            {
                use ::serde::de::Error;
                let proxy = <$proxy>::deserialize(deserializer)?;
                proxy.try_into().map_err(D::Error::custom)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// auth
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod auth {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountRef
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AccountRef {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<AccountID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<AccountName>,
    }

    impl IntoDto for AccountRef {
        type Dto = dtos::auth::AccountRef;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::AccountRef> for StructOrString<AccountRef> {
        fn from(v: dtos::auth::AccountRef) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<AccountRef>> for dtos::auth::AccountRef {
        type Error = ValidationError;
        fn try_from(v: StructOrString<AccountRef>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    implement_serde_as!(dtos::auth::AccountRef, AccountRef);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AccountSpec {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<AccountID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account_type: Option<auth::AccountType>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub display_name: Option<String>,
        pub email: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub avatar_url: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub password: Option<StructOrString<config::Secret>>,
    }

    impl IntoDto for AccountSpec {
        type Dto = dtos::auth::AccountSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::AccountSpec> for AccountSpec {
        fn from(v: dtos::auth::AccountSpec) -> Self {
            Self {
                did: v.did,
                account_type: v.account_type.map(|v| v.into()),
                display_name: v.display_name,
                email: v.email,
                avatar_url: v.avatar_url,
                password: v.password.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<AccountSpec> for dtos::auth::AccountSpec {
        type Error = ValidationError;
        fn try_from(v: AccountSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                did: v.did,
                account_type: v
                    .account_type
                    .map(|v| dtos::auth::AccountType::try_from(v))
                    .transpose()?,
                display_name: v.display_name,
                email: v.email,
                avatar_url: v.avatar_url,
                password: v
                    .password
                    .map(|v| dtos::config::Secret::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::auth::AccountSpec, AccountSpec);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountType
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum AccountType {
        #[serde(alias = "user")]
        User,
        #[serde(alias = "organization")]
        Organization,
    }

    impl IntoDto for AccountType {
        type Dto = dtos::auth::AccountType;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::AccountType> for AccountType {
        fn from(v: dtos::auth::AccountType) -> Self {
            match v {
                dtos::auth::AccountType::User => Self::User,
                dtos::auth::AccountType::Organization => Self::Organization,
            }
        }
    }

    impl TryFrom<AccountType> for dtos::auth::AccountType {
        type Error = ValidationError;
        fn try_from(v: AccountType) -> Result<Self, Self::Error> {
            match v {
                AccountType::User => Ok(Self::User),
                AccountType::Organization => Ok(Self::Organization),
            }
        }
    }

    implement_serde_as!(dtos::auth::AccountType, AccountType);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Attribute
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Attribute {
        pub object: StructOrString<resource::ResourceRef>,
        pub name: String,
        pub value: serde_json::Value,
    }

    impl IntoDto for Attribute {
        type Dto = dtos::auth::Attribute;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::Attribute> for Attribute {
        fn from(v: dtos::auth::Attribute) -> Self {
            Self {
                object: v.object.into(),
                name: v.name,
                value: v.value,
            }
        }
    }

    impl TryFrom<Attribute> for dtos::auth::Attribute {
        type Error = ValidationError;
        fn try_from(v: Attribute) -> Result<Self, ValidationError> {
            Ok(Self {
                object: dtos::resource::ResourceRef::try_from(v.object)?,
                name: v.name,
                value: v.value,
            })
        }
    }

    implement_serde_as!(dtos::auth::Attribute, Attribute);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Relation
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Relation {
        pub subject: StructOrString<resource::ResourceRef>,
        pub relation: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub value: Option<serde_json::Value>,
        pub object: StructOrString<resource::ResourceRef>,
    }

    impl IntoDto for Relation {
        type Dto = dtos::auth::Relation;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::Relation> for Relation {
        fn from(v: dtos::auth::Relation) -> Self {
            Self {
                subject: v.subject.into(),
                relation: v.relation,
                value: v.value,
                object: v.object.into(),
            }
        }
    }

    impl TryFrom<Relation> for dtos::auth::Relation {
        type Error = ValidationError;
        fn try_from(v: Relation) -> Result<Self, ValidationError> {
            Ok(Self {
                subject: dtos::resource::ResourceRef::try_from(v.subject)?,
                relation: v.relation,
                value: v.value,
                object: dtos::resource::ResourceRef::try_from(v.object)?,
            })
        }
    }

    implement_serde_as!(dtos::auth::Relation, Relation);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/RelationsSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RelationsSpec {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub relations: Option<Vec<auth::Relation>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub attributes: Option<Vec<auth::Attribute>>,
    }

    impl IntoDto for RelationsSpec {
        type Dto = dtos::auth::RelationsSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::RelationsSpec> for RelationsSpec {
        fn from(v: dtos::auth::RelationsSpec) -> Self {
            Self {
                relations: v.relations.map(|v| v.into_iter().map(Into::into).collect()),
                attributes: v
                    .attributes
                    .map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<RelationsSpec> for dtos::auth::RelationsSpec {
        type Error = ValidationError;
        fn try_from(v: RelationsSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                relations: v
                    .relations
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::auth::Relation::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                attributes: v
                    .attributes
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::auth::Attribute::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::auth::RelationsSpec, RelationsSpec);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// config
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod config {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secret
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Secret {
        pub value: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub content_encoding: Option<String>,
    }

    impl IntoDto for Secret {
        type Dto = dtos::config::Secret;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::Secret> for StructOrString<Secret> {
        fn from(v: dtos::config::Secret) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<Secret>> for dtos::config::Secret {
        type Error = ValidationError;
        fn try_from(v: StructOrString<Secret>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    impl From<dtos::config::Secret> for Secret {
        fn from(v: dtos::config::Secret) -> Self {
            Self {
                value: v.value,
                content_encoding: v.content_encoding,
            }
        }
    }

    impl TryFrom<Secret> for dtos::config::Secret {
        type Error = ValidationError;
        fn try_from(v: Secret) -> Result<Self, ValidationError> {
            Ok(Self {
                value: v.value,
                content_encoding: v.content_encoding,
            })
        }
    }

    implement_serde_as!(dtos::config::Secret, Secret);

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSetSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SecretSetSpec {
        pub secrets: config::Secrets,
    }

    impl IntoDto for SecretSetSpec {
        type Dto = dtos::config::SecretSetSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::SecretSetSpec> for SecretSetSpec {
        fn from(v: dtos::config::SecretSetSpec) -> Self {
            Self {
                secrets: v.secrets.into(),
            }
        }
    }

    impl TryFrom<SecretSetSpec> for dtos::config::SecretSetSpec {
        type Error = ValidationError;
        fn try_from(v: SecretSetSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                secrets: dtos::config::Secrets::try_from(v.secrets)?,
            })
        }
    }

    implement_serde_as!(dtos::config::SecretSetSpec, SecretSetSpec);

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secrets
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Secrets {
        #[serde(flatten)]
        pub entries: std::collections::BTreeMap<String, StructOrString<config::Secret>>,
    }

    impl IntoDto for Secrets {
        type Dto = dtos::config::Secrets;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::Secrets> for Secrets {
        fn from(v: dtos::config::Secrets) -> Self {
            Self {
                entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
            }
        }
    }

    impl TryFrom<Secrets> for dtos::config::Secrets {
        type Error = ValidationError;
        fn try_from(v: Secrets) -> Result<Self, Self::Error> {
            Ok(Self {
                entries: v
                    .entries
                    .into_iter()
                    .map(|(k, v)| -> Result<_, ValidationError> { Ok((k, v.try_into()?)) })
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::config::Secrets, Secrets);

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueRef
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ValueRef {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        pub r#type: TypeRef,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<ResourceName>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub path: Option<String>,
    }

    impl IntoDto for ValueRef {
        type Dto = dtos::config::ValueRef;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::ValueRef> for StructOrString<ValueRef> {
        fn from(v: dtos::config::ValueRef) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<ValueRef>> for dtos::config::ValueRef {
        type Error = ValidationError;
        fn try_from(v: StructOrString<ValueRef>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    implement_serde_as!(dtos::config::ValueRef, ValueRef);

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueRefs
    #[derive(Debug, Serialize, Deserialize)]
    pub struct ValueRefs {
        #[serde(flatten)]
        pub entries: std::collections::BTreeMap<String, StructOrString<config::ValueRef>>,
    }

    impl IntoDto for ValueRefs {
        type Dto = dtos::config::ValueRefs;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::ValueRefs> for ValueRefs {
        fn from(v: dtos::config::ValueRefs) -> Self {
            Self {
                entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
            }
        }
    }

    impl TryFrom<ValueRefs> for dtos::config::ValueRefs {
        type Error = ValidationError;
        fn try_from(v: ValueRefs) -> Result<Self, Self::Error> {
            Ok(Self {
                entries: v
                    .entries
                    .into_iter()
                    .map(|(k, v)| -> Result<_, ValidationError> { Ok((k, v.try_into()?)) })
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::config::ValueRefs, ValueRefs);

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variable
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Variable {
        pub value: String,
    }

    impl IntoDto for Variable {
        type Dto = dtos::config::Variable;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::Variable> for StructOrString<Variable> {
        fn from(v: dtos::config::Variable) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<Variable>> for dtos::config::Variable {
        type Error = ValidationError;
        fn try_from(v: StructOrString<Variable>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    impl From<dtos::config::Variable> for Variable {
        fn from(v: dtos::config::Variable) -> Self {
            Self { value: v.value }
        }
    }

    impl TryFrom<Variable> for dtos::config::Variable {
        type Error = ValidationError;
        fn try_from(v: Variable) -> Result<Self, ValidationError> {
            Ok(Self { value: v.value })
        }
    }

    implement_serde_as!(dtos::config::Variable, Variable);

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSetSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct VariableSetSpec {
        pub variables: config::Variables,
    }

    impl IntoDto for VariableSetSpec {
        type Dto = dtos::config::VariableSetSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::VariableSetSpec> for VariableSetSpec {
        fn from(v: dtos::config::VariableSetSpec) -> Self {
            Self {
                variables: v.variables.into(),
            }
        }
    }

    impl TryFrom<VariableSetSpec> for dtos::config::VariableSetSpec {
        type Error = ValidationError;
        fn try_from(v: VariableSetSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                variables: dtos::config::Variables::try_from(v.variables)?,
            })
        }
    }

    implement_serde_as!(dtos::config::VariableSetSpec, VariableSetSpec);

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variables
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Variables {
        #[serde(flatten)]
        pub entries: std::collections::BTreeMap<String, StructOrString<config::Variable>>,
    }

    impl IntoDto for Variables {
        type Dto = dtos::config::Variables;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::Variables> for Variables {
        fn from(v: dtos::config::Variables) -> Self {
            Self {
                entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
            }
        }
    }

    impl TryFrom<Variables> for dtos::config::Variables {
        type Error = ValidationError;
        fn try_from(v: Variables) -> Result<Self, Self::Error> {
            Ok(Self {
                entries: v
                    .entries
                    .into_iter()
                    .map(|(k, v)| -> Result<_, ValidationError> { Ok((k, v.try_into()?)) })
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::config::Variables, Variables);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// data
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod data {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataField
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataField {
        pub name: String,
        pub r#type: UnionOrString<data::DataType>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub extra: Option<data::ExtraAttributes>,
    }

    impl IntoDto for DataField {
        type Dto = dtos::data::DataField;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataField> for DataField {
        fn from(v: dtos::data::DataField) -> Self {
            Self {
                name: v.name,
                r#type: v.r#type.into(),
                extra: v.extra.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<DataField> for dtos::data::DataField {
        type Error = ValidationError;
        fn try_from(v: DataField) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                r#type: dtos::data::DataType::try_from(v.r#type)?,
                extra: v
                    .extra
                    .map(|v| dtos::data::ExtraAttributes::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::data::DataField, DataField);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataSchema
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataSchema {
        pub fields: Vec<data::DataField>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub extra: Option<data::ExtraAttributes>,
    }

    impl IntoDto for DataSchema {
        type Dto = dtos::data::DataSchema;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataSchema> for DataSchema {
        fn from(v: dtos::data::DataSchema) -> Self {
            Self {
                fields: v.fields.into_iter().map(Into::into).collect(),
                extra: v.extra.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<DataSchema> for dtos::data::DataSchema {
        type Error = ValidationError;
        fn try_from(v: DataSchema) -> Result<Self, ValidationError> {
            Ok(Self {
                fields: v
                    .fields
                    .into_iter()
                    .map(|i| dtos::data::DataField::try_from(i))
                    .collect::<Result<_, _>>()?,
                extra: v
                    .extra
                    .map(|v| dtos::data::ExtraAttributes::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::data::DataSchema, DataSchema);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum DataType {
        #[serde(alias = "binary")]
        Binary(data::DataTypeBinary),
        #[serde(alias = "bool")]
        Bool(data::DataTypeBool),
        #[serde(alias = "date")]
        Date(data::DataTypeDate),
        #[serde(alias = "decimal")]
        Decimal(data::DataTypeDecimal),
        #[serde(alias = "duration")]
        Duration(data::DataTypeDuration),
        #[serde(alias = "float16")]
        Float16(data::DataTypeFloat16),
        #[serde(alias = "float32")]
        Float32(data::DataTypeFloat32),
        #[serde(alias = "float64")]
        Float64(data::DataTypeFloat64),
        #[serde(alias = "int8")]
        Int8(data::DataTypeInt8),
        #[serde(alias = "int16")]
        Int16(data::DataTypeInt16),
        #[serde(alias = "int32")]
        Int32(data::DataTypeInt32),
        #[serde(alias = "int64")]
        Int64(data::DataTypeInt64),
        #[serde(alias = "uInt8", alias = "uint8")]
        UInt8(data::DataTypeUInt8),
        #[serde(alias = "uInt16", alias = "uint16")]
        UInt16(data::DataTypeUInt16),
        #[serde(alias = "uInt32", alias = "uint32")]
        UInt32(data::DataTypeUInt32),
        #[serde(alias = "uInt64", alias = "uint64")]
        UInt64(data::DataTypeUInt64),
        #[serde(alias = "list")]
        List(data::DataTypeList),
        #[serde(alias = "map")]
        Map(data::DataTypeMap),
        #[serde(alias = "null")]
        Null(data::DataTypeNull),
        #[serde(alias = "option")]
        Option(data::DataTypeOption),
        #[serde(alias = "struct")]
        Struct(data::DataTypeStruct),
        #[serde(alias = "time")]
        Time(data::DataTypeTime),
        #[serde(alias = "timestamp")]
        Timestamp(data::DataTypeTimestamp),
        #[serde(alias = "string")]
        String(data::DataTypeString),
    }

    impl From<dtos::data::DataType> for UnionOrString<DataType> {
        fn from(v: dtos::data::DataType) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<UnionOrString<DataType>> for dtos::data::DataType {
        type Error = ValidationError;
        fn try_from(v: UnionOrString<DataType>) -> Result<Self, Self::Error> {
            v.0.try_into()
        }
    }

    impl IntoDto for DataType {
        type Dto = dtos::data::DataType;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataType> for DataType {
        fn from(v: dtos::data::DataType) -> Self {
            match v {
                dtos::data::DataType::Binary(v) => Self::Binary(v.into()),
                dtos::data::DataType::Bool(v) => Self::Bool(v.into()),
                dtos::data::DataType::Date(v) => Self::Date(v.into()),
                dtos::data::DataType::Decimal(v) => Self::Decimal(v.into()),
                dtos::data::DataType::Duration(v) => Self::Duration(v.into()),
                dtos::data::DataType::Float16(v) => Self::Float16(v.into()),
                dtos::data::DataType::Float32(v) => Self::Float32(v.into()),
                dtos::data::DataType::Float64(v) => Self::Float64(v.into()),
                dtos::data::DataType::Int8(v) => Self::Int8(v.into()),
                dtos::data::DataType::Int16(v) => Self::Int16(v.into()),
                dtos::data::DataType::Int32(v) => Self::Int32(v.into()),
                dtos::data::DataType::Int64(v) => Self::Int64(v.into()),
                dtos::data::DataType::UInt8(v) => Self::UInt8(v.into()),
                dtos::data::DataType::UInt16(v) => Self::UInt16(v.into()),
                dtos::data::DataType::UInt32(v) => Self::UInt32(v.into()),
                dtos::data::DataType::UInt64(v) => Self::UInt64(v.into()),
                dtos::data::DataType::List(v) => Self::List(v.into()),
                dtos::data::DataType::Map(v) => Self::Map(v.into()),
                dtos::data::DataType::Null(v) => Self::Null(v.into()),
                dtos::data::DataType::Option(v) => Self::Option(v.into()),
                dtos::data::DataType::Struct(v) => Self::Struct(v.into()),
                dtos::data::DataType::Time(v) => Self::Time(v.into()),
                dtos::data::DataType::Timestamp(v) => Self::Timestamp(v.into()),
                dtos::data::DataType::String(v) => Self::String(v.into()),
            }
        }
    }

    impl TryFrom<DataType> for dtos::data::DataType {
        type Error = ValidationError;
        fn try_from(v: DataType) -> Result<Self, Self::Error> {
            match v {
                DataType::Binary(v) => Ok(Self::Binary(v.try_into()?)),
                DataType::Bool(v) => Ok(Self::Bool(v.try_into()?)),
                DataType::Date(v) => Ok(Self::Date(v.try_into()?)),
                DataType::Decimal(v) => Ok(Self::Decimal(v.try_into()?)),
                DataType::Duration(v) => Ok(Self::Duration(v.try_into()?)),
                DataType::Float16(v) => Ok(Self::Float16(v.try_into()?)),
                DataType::Float32(v) => Ok(Self::Float32(v.try_into()?)),
                DataType::Float64(v) => Ok(Self::Float64(v.try_into()?)),
                DataType::Int8(v) => Ok(Self::Int8(v.try_into()?)),
                DataType::Int16(v) => Ok(Self::Int16(v.try_into()?)),
                DataType::Int32(v) => Ok(Self::Int32(v.try_into()?)),
                DataType::Int64(v) => Ok(Self::Int64(v.try_into()?)),
                DataType::UInt8(v) => Ok(Self::UInt8(v.try_into()?)),
                DataType::UInt16(v) => Ok(Self::UInt16(v.try_into()?)),
                DataType::UInt32(v) => Ok(Self::UInt32(v.try_into()?)),
                DataType::UInt64(v) => Ok(Self::UInt64(v.try_into()?)),
                DataType::List(v) => Ok(Self::List(v.try_into()?)),
                DataType::Map(v) => Ok(Self::Map(v.try_into()?)),
                DataType::Null(v) => Ok(Self::Null(v.try_into()?)),
                DataType::Option(v) => Ok(Self::Option(v.try_into()?)),
                DataType::Struct(v) => Ok(Self::Struct(v.try_into()?)),
                DataType::Time(v) => Ok(Self::Time(v.try_into()?)),
                DataType::Timestamp(v) => Ok(Self::Timestamp(v.try_into()?)),
                DataType::String(v) => Ok(Self::String(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::data::DataType, DataType);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Binary
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeBinary {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub fixed_length: Option<u64>,
    }

    impl IntoDto for DataTypeBinary {
        type Dto = dtos::data::DataTypeBinary;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeBinary> for DataTypeBinary {
        fn from(v: dtos::data::DataTypeBinary) -> Self {
            Self {
                fixed_length: v.fixed_length,
            }
        }
    }

    impl TryFrom<DataTypeBinary> for dtos::data::DataTypeBinary {
        type Error = ValidationError;
        fn try_from(v: DataTypeBinary) -> Result<Self, ValidationError> {
            Ok(Self {
                fixed_length: v.fixed_length,
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeBinary, DataTypeBinary);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Bool
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeBool {}

    impl IntoDto for DataTypeBool {
        type Dto = dtos::data::DataTypeBool;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeBool> for DataTypeBool {
        fn from(v: dtos::data::DataTypeBool) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeBool> for dtos::data::DataTypeBool {
        type Error = ValidationError;
        fn try_from(v: DataTypeBool) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeBool, DataTypeBool);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Date
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeDate {}

    impl IntoDto for DataTypeDate {
        type Dto = dtos::data::DataTypeDate;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeDate> for DataTypeDate {
        fn from(v: dtos::data::DataTypeDate) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeDate> for dtos::data::DataTypeDate {
        type Error = ValidationError;
        fn try_from(v: DataTypeDate) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeDate, DataTypeDate);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Decimal
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeDecimal {
        pub precision: u32,
        pub scale: i32,
    }

    impl IntoDto for DataTypeDecimal {
        type Dto = dtos::data::DataTypeDecimal;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeDecimal> for DataTypeDecimal {
        fn from(v: dtos::data::DataTypeDecimal) -> Self {
            Self {
                precision: v.precision,
                scale: v.scale,
            }
        }
    }

    impl TryFrom<DataTypeDecimal> for dtos::data::DataTypeDecimal {
        type Error = ValidationError;
        fn try_from(v: DataTypeDecimal) -> Result<Self, ValidationError> {
            Ok(Self {
                precision: v.precision,
                scale: v.scale,
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeDecimal, DataTypeDecimal);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Duration
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeDuration {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub unit: Option<data::TimeUnit>,
    }

    impl IntoDto for DataTypeDuration {
        type Dto = dtos::data::DataTypeDuration;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeDuration> for DataTypeDuration {
        fn from(v: dtos::data::DataTypeDuration) -> Self {
            Self {
                unit: v.unit.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<DataTypeDuration> for dtos::data::DataTypeDuration {
        type Error = ValidationError;
        fn try_from(v: DataTypeDuration) -> Result<Self, ValidationError> {
            Ok(Self {
                unit: v
                    .unit
                    .map(|v| dtos::data::TimeUnit::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeDuration, DataTypeDuration);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float16
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeFloat16 {}

    impl IntoDto for DataTypeFloat16 {
        type Dto = dtos::data::DataTypeFloat16;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeFloat16> for DataTypeFloat16 {
        fn from(v: dtos::data::DataTypeFloat16) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeFloat16> for dtos::data::DataTypeFloat16 {
        type Error = ValidationError;
        fn try_from(v: DataTypeFloat16) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeFloat16, DataTypeFloat16);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float32
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeFloat32 {}

    impl IntoDto for DataTypeFloat32 {
        type Dto = dtos::data::DataTypeFloat32;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeFloat32> for DataTypeFloat32 {
        fn from(v: dtos::data::DataTypeFloat32) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeFloat32> for dtos::data::DataTypeFloat32 {
        type Error = ValidationError;
        fn try_from(v: DataTypeFloat32) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeFloat32, DataTypeFloat32);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float64
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeFloat64 {}

    impl IntoDto for DataTypeFloat64 {
        type Dto = dtos::data::DataTypeFloat64;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeFloat64> for DataTypeFloat64 {
        fn from(v: dtos::data::DataTypeFloat64) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeFloat64> for dtos::data::DataTypeFloat64 {
        type Error = ValidationError;
        fn try_from(v: DataTypeFloat64) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeFloat64, DataTypeFloat64);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int16
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeInt16 {}

    impl IntoDto for DataTypeInt16 {
        type Dto = dtos::data::DataTypeInt16;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeInt16> for DataTypeInt16 {
        fn from(v: dtos::data::DataTypeInt16) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeInt16> for dtos::data::DataTypeInt16 {
        type Error = ValidationError;
        fn try_from(v: DataTypeInt16) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeInt16, DataTypeInt16);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int32
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeInt32 {}

    impl IntoDto for DataTypeInt32 {
        type Dto = dtos::data::DataTypeInt32;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeInt32> for DataTypeInt32 {
        fn from(v: dtos::data::DataTypeInt32) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeInt32> for dtos::data::DataTypeInt32 {
        type Error = ValidationError;
        fn try_from(v: DataTypeInt32) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeInt32, DataTypeInt32);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int64
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeInt64 {}

    impl IntoDto for DataTypeInt64 {
        type Dto = dtos::data::DataTypeInt64;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeInt64> for DataTypeInt64 {
        fn from(v: dtos::data::DataTypeInt64) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeInt64> for dtos::data::DataTypeInt64 {
        type Error = ValidationError;
        fn try_from(v: DataTypeInt64) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeInt64, DataTypeInt64);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int8
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeInt8 {}

    impl IntoDto for DataTypeInt8 {
        type Dto = dtos::data::DataTypeInt8;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeInt8> for DataTypeInt8 {
        fn from(v: dtos::data::DataTypeInt8) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeInt8> for dtos::data::DataTypeInt8 {
        type Error = ValidationError;
        fn try_from(v: DataTypeInt8) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeInt8, DataTypeInt8);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/List
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeList {
        pub item_type: Box<UnionOrString<data::DataType>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub fixed_length: Option<u64>,
    }

    impl IntoDto for DataTypeList {
        type Dto = dtos::data::DataTypeList;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeList> for DataTypeList {
        fn from(v: dtos::data::DataTypeList) -> Self {
            Self {
                item_type: Box::new((*v.item_type).into()),
                fixed_length: v.fixed_length,
            }
        }
    }

    impl TryFrom<DataTypeList> for dtos::data::DataTypeList {
        type Error = ValidationError;
        fn try_from(v: DataTypeList) -> Result<Self, ValidationError> {
            Ok(Self {
                item_type: Box::new(dtos::data::DataType::try_from(*v.item_type)?),
                fixed_length: v.fixed_length,
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeList, DataTypeList);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Map
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeMap {
        pub key_type: Box<UnionOrString<data::DataType>>,
        pub value_type: Box<UnionOrString<data::DataType>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub keys_sorted: Option<bool>,
    }

    impl IntoDto for DataTypeMap {
        type Dto = dtos::data::DataTypeMap;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeMap> for DataTypeMap {
        fn from(v: dtos::data::DataTypeMap) -> Self {
            Self {
                key_type: Box::new((*v.key_type).into()),
                value_type: Box::new((*v.value_type).into()),
                keys_sorted: v.keys_sorted,
            }
        }
    }

    impl TryFrom<DataTypeMap> for dtos::data::DataTypeMap {
        type Error = ValidationError;
        fn try_from(v: DataTypeMap) -> Result<Self, ValidationError> {
            Ok(Self {
                key_type: Box::new(dtos::data::DataType::try_from(*v.key_type)?),
                value_type: Box::new(dtos::data::DataType::try_from(*v.value_type)?),
                keys_sorted: v.keys_sorted,
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeMap, DataTypeMap);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Null
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeNull {}

    impl IntoDto for DataTypeNull {
        type Dto = dtos::data::DataTypeNull;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeNull> for DataTypeNull {
        fn from(v: dtos::data::DataTypeNull) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeNull> for dtos::data::DataTypeNull {
        type Error = ValidationError;
        fn try_from(v: DataTypeNull) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeNull, DataTypeNull);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Option
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeOption {
        pub inner: Box<UnionOrString<data::DataType>>,
    }

    impl IntoDto for DataTypeOption {
        type Dto = dtos::data::DataTypeOption;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeOption> for DataTypeOption {
        fn from(v: dtos::data::DataTypeOption) -> Self {
            Self {
                inner: Box::new((*v.inner).into()),
            }
        }
    }

    impl TryFrom<DataTypeOption> for dtos::data::DataTypeOption {
        type Error = ValidationError;
        fn try_from(v: DataTypeOption) -> Result<Self, ValidationError> {
            Ok(Self {
                inner: Box::new(dtos::data::DataType::try_from(*v.inner)?),
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeOption, DataTypeOption);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/String
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeString {}

    impl IntoDto for DataTypeString {
        type Dto = dtos::data::DataTypeString;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeString> for DataTypeString {
        fn from(v: dtos::data::DataTypeString) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeString> for dtos::data::DataTypeString {
        type Error = ValidationError;
        fn try_from(v: DataTypeString) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeString, DataTypeString);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Struct
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeStruct {
        pub fields: Vec<data::DataField>,
    }

    impl IntoDto for DataTypeStruct {
        type Dto = dtos::data::DataTypeStruct;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeStruct> for DataTypeStruct {
        fn from(v: dtos::data::DataTypeStruct) -> Self {
            Self {
                fields: v.fields.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<DataTypeStruct> for dtos::data::DataTypeStruct {
        type Error = ValidationError;
        fn try_from(v: DataTypeStruct) -> Result<Self, ValidationError> {
            Ok(Self {
                fields: v
                    .fields
                    .into_iter()
                    .map(|i| dtos::data::DataField::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeStruct, DataTypeStruct);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Time
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeTime {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub unit: Option<data::TimeUnit>,
    }

    impl IntoDto for DataTypeTime {
        type Dto = dtos::data::DataTypeTime;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeTime> for DataTypeTime {
        fn from(v: dtos::data::DataTypeTime) -> Self {
            Self {
                unit: v.unit.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<DataTypeTime> for dtos::data::DataTypeTime {
        type Error = ValidationError;
        fn try_from(v: DataTypeTime) -> Result<Self, ValidationError> {
            Ok(Self {
                unit: v
                    .unit
                    .map(|v| dtos::data::TimeUnit::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeTime, DataTypeTime);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Timestamp
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeTimestamp {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub unit: Option<data::TimeUnit>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub timezone: Option<String>,
    }

    impl IntoDto for DataTypeTimestamp {
        type Dto = dtos::data::DataTypeTimestamp;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeTimestamp> for DataTypeTimestamp {
        fn from(v: dtos::data::DataTypeTimestamp) -> Self {
            Self {
                unit: v.unit.map(|v| v.into()),
                timezone: v.timezone,
            }
        }
    }

    impl TryFrom<DataTypeTimestamp> for dtos::data::DataTypeTimestamp {
        type Error = ValidationError;
        fn try_from(v: DataTypeTimestamp) -> Result<Self, ValidationError> {
            Ok(Self {
                unit: v
                    .unit
                    .map(|v| dtos::data::TimeUnit::try_from(v))
                    .transpose()?,
                timezone: v.timezone,
            })
        }
    }

    implement_serde_as!(dtos::data::DataTypeTimestamp, DataTypeTimestamp);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt16
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeUInt16 {}

    impl IntoDto for DataTypeUInt16 {
        type Dto = dtos::data::DataTypeUInt16;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeUInt16> for DataTypeUInt16 {
        fn from(v: dtos::data::DataTypeUInt16) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeUInt16> for dtos::data::DataTypeUInt16 {
        type Error = ValidationError;
        fn try_from(v: DataTypeUInt16) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeUInt16, DataTypeUInt16);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt32
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeUInt32 {}

    impl IntoDto for DataTypeUInt32 {
        type Dto = dtos::data::DataTypeUInt32;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeUInt32> for DataTypeUInt32 {
        fn from(v: dtos::data::DataTypeUInt32) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeUInt32> for dtos::data::DataTypeUInt32 {
        type Error = ValidationError;
        fn try_from(v: DataTypeUInt32) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeUInt32, DataTypeUInt32);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt64
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeUInt64 {}

    impl IntoDto for DataTypeUInt64 {
        type Dto = dtos::data::DataTypeUInt64;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeUInt64> for DataTypeUInt64 {
        fn from(v: dtos::data::DataTypeUInt64) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeUInt64> for dtos::data::DataTypeUInt64 {
        type Error = ValidationError;
        fn try_from(v: DataTypeUInt64) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeUInt64, DataTypeUInt64);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt8
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataTypeUInt8 {}

    impl IntoDto for DataTypeUInt8 {
        type Dto = dtos::data::DataTypeUInt8;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::DataTypeUInt8> for DataTypeUInt8 {
        fn from(v: dtos::data::DataTypeUInt8) -> Self {
            Self {}
        }
    }

    impl TryFrom<DataTypeUInt8> for dtos::data::DataTypeUInt8 {
        type Error = ValidationError;
        fn try_from(v: DataTypeUInt8) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::data::DataTypeUInt8, DataTypeUInt8);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/ExtraAttributes
    #[derive(Debug, Serialize, Deserialize)]
    pub struct ExtraAttributes {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    impl IntoDto for ExtraAttributes {
        type Dto = dtos::data::ExtraAttributes;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::ExtraAttributes> for ExtraAttributes {
        fn from(v: dtos::data::ExtraAttributes) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<ExtraAttributes> for dtos::data::ExtraAttributes {
        type Error = ValidationError;
        fn try_from(v: ExtraAttributes) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::data::ExtraAttributes, ExtraAttributes);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/OperationType
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum OperationType {
        #[serde(alias = "append")]
        Append,
        #[serde(alias = "retract")]
        Retract,
        #[serde(alias = "correctFrom", alias = "correctfrom")]
        CorrectFrom,
        #[serde(alias = "correctTo", alias = "correctto")]
        CorrectTo,
    }

    impl IntoDto for OperationType {
        type Dto = dtos::data::OperationType;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::OperationType> for OperationType {
        fn from(v: dtos::data::OperationType) -> Self {
            match v {
                dtos::data::OperationType::Append => Self::Append,
                dtos::data::OperationType::Retract => Self::Retract,
                dtos::data::OperationType::CorrectFrom => Self::CorrectFrom,
                dtos::data::OperationType::CorrectTo => Self::CorrectTo,
            }
        }
    }

    impl TryFrom<OperationType> for dtos::data::OperationType {
        type Error = ValidationError;
        fn try_from(v: OperationType) -> Result<Self, Self::Error> {
            match v {
                OperationType::Append => Ok(Self::Append),
                OperationType::Retract => Ok(Self::Retract),
                OperationType::CorrectFrom => Ok(Self::CorrectFrom),
                OperationType::CorrectTo => Ok(Self::CorrectTo),
            }
        }
    }

    implement_serde_as!(dtos::data::OperationType, OperationType);

    // Schema: https://opendatafabric.org/schemas/data/v1alpha1/TimeUnit
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum TimeUnit {
        #[serde(alias = "second")]
        Second,
        #[serde(alias = "millisecond")]
        Millisecond,
        #[serde(alias = "microsecond")]
        Microsecond,
        #[serde(alias = "nanosecond")]
        Nanosecond,
    }

    impl IntoDto for TimeUnit {
        type Dto = dtos::data::TimeUnit;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::data::TimeUnit> for TimeUnit {
        fn from(v: dtos::data::TimeUnit) -> Self {
            match v {
                dtos::data::TimeUnit::Second => Self::Second,
                dtos::data::TimeUnit::Millisecond => Self::Millisecond,
                dtos::data::TimeUnit::Microsecond => Self::Microsecond,
                dtos::data::TimeUnit::Nanosecond => Self::Nanosecond,
            }
        }
    }

    impl TryFrom<TimeUnit> for dtos::data::TimeUnit {
        type Error = ValidationError;
        fn try_from(v: TimeUnit) -> Result<Self, Self::Error> {
            match v {
                TimeUnit::Second => Ok(Self::Second),
                TimeUnit::Millisecond => Ok(Self::Millisecond),
                TimeUnit::Microsecond => Ok(Self::Microsecond),
                TimeUnit::Nanosecond => Ok(Self::Nanosecond),
            }
        }
    }

    implement_serde_as!(dtos::data::TimeUnit, TimeUnit);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// dataset
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod dataset {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/AddData
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AddData {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_checkpoint: Option<Multihash>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_offset: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_data: Option<dataset::DataSlice>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_checkpoint: Option<dataset::Checkpoint>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub new_watermark: Option<DateTime<Utc>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_source_state: Option<source::SourceState>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub extra: Option<data::ExtraAttributes>,
    }

    impl IntoDto for AddData {
        type Dto = dtos::dataset::AddData;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::AddData> for AddData {
        fn from(v: dtos::dataset::AddData) -> Self {
            Self {
                prev_checkpoint: v.prev_checkpoint,
                prev_offset: v.prev_offset,
                new_data: v.new_data.map(|v| v.into()),
                new_checkpoint: v.new_checkpoint.map(|v| v.into()),
                new_watermark: v.new_watermark,
                new_source_state: v.new_source_state.map(|v| v.into()),
                extra: v.extra.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<AddData> for dtos::dataset::AddData {
        type Error = ValidationError;
        fn try_from(v: AddData) -> Result<Self, ValidationError> {
            Ok(Self {
                prev_checkpoint: v.prev_checkpoint,
                prev_offset: v.prev_offset,
                new_data: v
                    .new_data
                    .map(|v| dtos::dataset::DataSlice::try_from(v))
                    .transpose()?,
                new_checkpoint: v
                    .new_checkpoint
                    .map(|v| dtos::dataset::Checkpoint::try_from(v))
                    .transpose()?,
                new_watermark: v.new_watermark,
                new_source_state: v
                    .new_source_state
                    .map(|v| dtos::source::SourceState::try_from(v))
                    .transpose()?,
                extra: v
                    .extra
                    .map(|v| dtos::data::ExtraAttributes::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::AddData, AddData);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/AttachmentEmbedded
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AttachmentEmbedded {
        pub path: String,
        pub content: String,
    }

    impl IntoDto for AttachmentEmbedded {
        type Dto = dtos::dataset::AttachmentEmbedded;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::AttachmentEmbedded> for AttachmentEmbedded {
        fn from(v: dtos::dataset::AttachmentEmbedded) -> Self {
            Self {
                path: v.path,
                content: v.content,
            }
        }
    }

    impl TryFrom<AttachmentEmbedded> for dtos::dataset::AttachmentEmbedded {
        type Error = ValidationError;
        fn try_from(v: AttachmentEmbedded) -> Result<Self, ValidationError> {
            Ok(Self {
                path: v.path,
                content: v.content,
            })
        }
    }

    implement_serde_as!(dtos::dataset::AttachmentEmbedded, AttachmentEmbedded);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Attachments
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum Attachments {
        #[serde(alias = "embedded")]
        Embedded(dataset::AttachmentsEmbedded),
    }

    impl IntoDto for Attachments {
        type Dto = dtos::dataset::Attachments;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::Attachments> for Attachments {
        fn from(v: dtos::dataset::Attachments) -> Self {
            match v {
                dtos::dataset::Attachments::Embedded(v) => Self::Embedded(v.into()),
            }
        }
    }

    impl TryFrom<Attachments> for dtos::dataset::Attachments {
        type Error = ValidationError;
        fn try_from(v: Attachments) -> Result<Self, Self::Error> {
            match v {
                Attachments::Embedded(v) => Ok(Self::Embedded(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::dataset::Attachments, Attachments);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Attachments#/$defs/Embedded
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AttachmentsEmbedded {
        pub items: Vec<dataset::AttachmentEmbedded>,
    }

    impl IntoDto for AttachmentsEmbedded {
        type Dto = dtos::dataset::AttachmentsEmbedded;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::AttachmentsEmbedded> for AttachmentsEmbedded {
        fn from(v: dtos::dataset::AttachmentsEmbedded) -> Self {
            Self {
                items: v.items.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<AttachmentsEmbedded> for dtos::dataset::AttachmentsEmbedded {
        type Error = ValidationError;
        fn try_from(v: AttachmentsEmbedded) -> Result<Self, ValidationError> {
            Ok(Self {
                items: v
                    .items
                    .into_iter()
                    .map(|i| dtos::dataset::AttachmentEmbedded::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::AttachmentsEmbedded, AttachmentsEmbedded);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Checkpoint
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Checkpoint {
        pub physical_hash: Multihash,
        pub size: u64,
    }

    impl IntoDto for Checkpoint {
        type Dto = dtos::dataset::Checkpoint;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::Checkpoint> for Checkpoint {
        fn from(v: dtos::dataset::Checkpoint) -> Self {
            Self {
                physical_hash: v.physical_hash,
                size: v.size,
            }
        }
    }

    impl TryFrom<Checkpoint> for dtos::dataset::Checkpoint {
        type Error = ValidationError;
        fn try_from(v: Checkpoint) -> Result<Self, ValidationError> {
            Ok(Self {
                physical_hash: v.physical_hash,
                size: v.size,
            })
        }
    }

    implement_serde_as!(dtos::dataset::Checkpoint, Checkpoint);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/CompactionParams
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct CompactionParams {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_slice_size: Option<ByteSize>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_slice_records: Option<u64>,
    }

    impl IntoDto for CompactionParams {
        type Dto = dtos::dataset::CompactionParams;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::CompactionParams> for CompactionParams {
        fn from(v: dtos::dataset::CompactionParams) -> Self {
            Self {
                max_slice_size: v.max_slice_size,
                max_slice_records: v.max_slice_records,
            }
        }
    }

    impl TryFrom<CompactionParams> for dtos::dataset::CompactionParams {
        type Error = ValidationError;
        fn try_from(v: CompactionParams) -> Result<Self, ValidationError> {
            Ok(Self {
                max_slice_size: v.max_slice_size,
                max_slice_records: v.max_slice_records,
            })
        }
    }

    implement_serde_as!(dtos::dataset::CompactionParams, CompactionParams);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DataSlice
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataSlice {
        pub logical_hash: Multihash,
        pub physical_hash: Multihash,
        pub offset_interval: dataset::OffsetInterval,
        pub size: u64,
    }

    impl IntoDto for DataSlice {
        type Dto = dtos::dataset::DataSlice;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::DataSlice> for DataSlice {
        fn from(v: dtos::dataset::DataSlice) -> Self {
            Self {
                logical_hash: v.logical_hash,
                physical_hash: v.physical_hash,
                offset_interval: v.offset_interval.into(),
                size: v.size,
            }
        }
    }

    impl TryFrom<DataSlice> for dtos::dataset::DataSlice {
        type Error = ValidationError;
        fn try_from(v: DataSlice) -> Result<Self, ValidationError> {
            Ok(Self {
                logical_hash: v.logical_hash,
                physical_hash: v.physical_hash,
                offset_interval: dtos::dataset::OffsetInterval::try_from(v.offset_interval)?,
                size: v.size,
            })
        }
    }

    implement_serde_as!(dtos::dataset::DataSlice, DataSlice);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetKind
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum DatasetKind {
        #[serde(alias = "root")]
        Root,
        #[serde(alias = "derivative")]
        Derivative,
    }

    impl IntoDto for DatasetKind {
        type Dto = dtos::dataset::DatasetKind;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::DatasetKind> for DatasetKind {
        fn from(v: dtos::dataset::DatasetKind) -> Self {
            match v {
                dtos::dataset::DatasetKind::Root => Self::Root,
                dtos::dataset::DatasetKind::Derivative => Self::Derivative,
            }
        }
    }

    impl TryFrom<DatasetKind> for dtos::dataset::DatasetKind {
        type Error = ValidationError;
        fn try_from(v: DatasetKind) -> Result<Self, Self::Error> {
            match v {
                DatasetKind::Root => Ok(Self::Root),
                DatasetKind::Derivative => Ok(Self::Derivative),
            }
        }
    }

    implement_serde_as!(dtos::dataset::DatasetKind, DatasetKind);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetSelector
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetSelector {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub labels: Option<resource::LabelFilter>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub kind: Option<dataset::DatasetKind>,
    }

    impl IntoDto for DatasetSelector {
        type Dto = dtos::dataset::DatasetSelector;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::DatasetSelector> for StructOrString<DatasetSelector> {
        fn from(v: dtos::dataset::DatasetSelector) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<DatasetSelector>> for dtos::dataset::DatasetSelector {
        type Error = ValidationError;
        fn try_from(v: StructOrString<DatasetSelector>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    implement_serde_as!(dtos::dataset::DatasetSelector, DatasetSelector);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetSpec {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<DatasetID>,
        pub kind: dataset::DatasetKind,
        pub metadata: Vec<dataset::MetadataEvent>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub volume: Option<StructOrString<storage::PersistentVolumeRef>>,
    }

    impl IntoDto for DatasetSpec {
        type Dto = dtos::dataset::DatasetSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::DatasetSpec> for DatasetSpec {
        fn from(v: dtos::dataset::DatasetSpec) -> Self {
            Self {
                did: v.did,
                kind: v.kind.into(),
                metadata: v.metadata.into_iter().map(Into::into).collect(),
                volume: v.volume.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<DatasetSpec> for dtos::dataset::DatasetSpec {
        type Error = ValidationError;
        fn try_from(v: DatasetSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                did: v.did,
                kind: dtos::dataset::DatasetKind::try_from(v.kind)?,
                metadata: v
                    .metadata
                    .into_iter()
                    .map(|i| dtos::dataset::MetadataEvent::try_from(i))
                    .collect::<Result<_, _>>()?,
                volume: v
                    .volume
                    .map(|v| dtos::storage::PersistentVolumeRef::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::DatasetSpec, DatasetSpec);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetVocabulary
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetVocabulary {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub offset_column: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub operation_type_column: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub system_time_column: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_time_column: Option<String>,
    }

    impl IntoDto for DatasetVocabulary {
        type Dto = dtos::dataset::DatasetVocabulary;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::DatasetVocabulary> for DatasetVocabulary {
        fn from(v: dtos::dataset::DatasetVocabulary) -> Self {
            Self {
                offset_column: v.offset_column,
                operation_type_column: v.operation_type_column,
                system_time_column: v.system_time_column,
                event_time_column: v.event_time_column,
            }
        }
    }

    impl TryFrom<DatasetVocabulary> for dtos::dataset::DatasetVocabulary {
        type Error = ValidationError;
        fn try_from(v: DatasetVocabulary) -> Result<Self, ValidationError> {
            Ok(Self {
                offset_column: v.offset_column,
                operation_type_column: v.operation_type_column,
                system_time_column: v.system_time_column,
                event_time_column: v.event_time_column,
            })
        }
    }

    implement_serde_as!(dtos::dataset::DatasetVocabulary, DatasetVocabulary);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ExecuteTransform
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ExecuteTransform {
        pub query_inputs: Vec<dataset::ExecuteTransformInput>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_checkpoint: Option<Multihash>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_offset: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_data: Option<dataset::DataSlice>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_checkpoint: Option<dataset::Checkpoint>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub new_watermark: Option<DateTime<Utc>>,
    }

    impl IntoDto for ExecuteTransform {
        type Dto = dtos::dataset::ExecuteTransform;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::ExecuteTransform> for ExecuteTransform {
        fn from(v: dtos::dataset::ExecuteTransform) -> Self {
            Self {
                query_inputs: v.query_inputs.into_iter().map(Into::into).collect(),
                prev_checkpoint: v.prev_checkpoint,
                prev_offset: v.prev_offset,
                new_data: v.new_data.map(|v| v.into()),
                new_checkpoint: v.new_checkpoint.map(|v| v.into()),
                new_watermark: v.new_watermark,
            }
        }
    }

    impl TryFrom<ExecuteTransform> for dtos::dataset::ExecuteTransform {
        type Error = ValidationError;
        fn try_from(v: ExecuteTransform) -> Result<Self, ValidationError> {
            Ok(Self {
                query_inputs: v
                    .query_inputs
                    .into_iter()
                    .map(|i| dtos::dataset::ExecuteTransformInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                prev_checkpoint: v.prev_checkpoint,
                prev_offset: v.prev_offset,
                new_data: v
                    .new_data
                    .map(|v| dtos::dataset::DataSlice::try_from(v))
                    .transpose()?,
                new_checkpoint: v
                    .new_checkpoint
                    .map(|v| dtos::dataset::Checkpoint::try_from(v))
                    .transpose()?,
                new_watermark: v.new_watermark,
            })
        }
    }

    implement_serde_as!(dtos::dataset::ExecuteTransform, ExecuteTransform);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ExecuteTransformInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ExecuteTransformInput {
        pub dataset_id: DatasetID,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_block_hash: Option<Multihash>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_block_hash: Option<Multihash>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_offset: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_offset: Option<u64>,
    }

    impl IntoDto for ExecuteTransformInput {
        type Dto = dtos::dataset::ExecuteTransformInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::ExecuteTransformInput> for ExecuteTransformInput {
        fn from(v: dtos::dataset::ExecuteTransformInput) -> Self {
            Self {
                dataset_id: v.dataset_id,
                prev_block_hash: v.prev_block_hash,
                new_block_hash: v.new_block_hash,
                prev_offset: v.prev_offset,
                new_offset: v.new_offset,
            }
        }
    }

    impl TryFrom<ExecuteTransformInput> for dtos::dataset::ExecuteTransformInput {
        type Error = ValidationError;
        fn try_from(v: ExecuteTransformInput) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_id: v.dataset_id,
                prev_block_hash: v.prev_block_hash,
                new_block_hash: v.new_block_hash,
                prev_offset: v.prev_offset,
                new_offset: v.new_offset,
            })
        }
    }

    implement_serde_as!(dtos::dataset::ExecuteTransformInput, ExecuteTransformInput);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/MetadataBlock
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MetadataBlock {
        #[serde(with = "datetime_rfc3339")]
        pub system_time: DateTime<Utc>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_block_hash: Option<Multihash>,
        pub sequence_number: u64,
        pub event: dataset::MetadataEvent,
    }

    impl IntoDto for MetadataBlock {
        type Dto = dtos::dataset::MetadataBlock;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::MetadataBlock> for MetadataBlock {
        fn from(v: dtos::dataset::MetadataBlock) -> Self {
            Self {
                system_time: v.system_time,
                prev_block_hash: v.prev_block_hash,
                sequence_number: v.sequence_number,
                event: v.event.into(),
            }
        }
    }

    impl TryFrom<MetadataBlock> for dtos::dataset::MetadataBlock {
        type Error = ValidationError;
        fn try_from(v: MetadataBlock) -> Result<Self, ValidationError> {
            Ok(Self {
                system_time: v.system_time,
                prev_block_hash: v.prev_block_hash,
                sequence_number: v.sequence_number,
                event: dtos::dataset::MetadataEvent::try_from(v.event)?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::MetadataBlock, MetadataBlock);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/MetadataEvent
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum MetadataEvent {
        #[serde(alias = "addData", alias = "adddata")]
        AddData(dataset::AddData),
        #[serde(alias = "executeTransform", alias = "executetransform")]
        ExecuteTransform(dataset::ExecuteTransform),
        #[serde(alias = "seed")]
        Seed(dataset::Seed),
        #[serde(alias = "setPollingSource", alias = "setpollingsource")]
        SetPollingSource(legacy::SetPollingSource),
        #[serde(alias = "setTransform", alias = "settransform")]
        SetTransform(dataset::SetTransform),
        #[serde(alias = "setVocab", alias = "setvocab")]
        SetVocab(dataset::SetVocab),
        #[serde(alias = "setAttachments", alias = "setattachments")]
        SetAttachments(dataset::SetAttachments),
        #[serde(alias = "setInfo", alias = "setinfo")]
        SetInfo(dataset::SetInfo),
        #[serde(alias = "setLicense", alias = "setlicense")]
        SetLicense(dataset::SetLicense),
        #[serde(alias = "setDataSchema", alias = "setdataschema")]
        SetDataSchema(dataset::SetDataSchema),
        #[serde(alias = "addPushSource", alias = "addpushsource")]
        AddPushSource(legacy::AddPushSource),
        #[serde(alias = "disablePushSource", alias = "disablepushsource")]
        DisablePushSource(legacy::DisablePushSource),
        #[serde(alias = "disablePollingSource", alias = "disablepollingsource")]
        DisablePollingSource(legacy::DisablePollingSource),
    }

    impl IntoDto for MetadataEvent {
        type Dto = dtos::dataset::MetadataEvent;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::MetadataEvent> for MetadataEvent {
        fn from(v: dtos::dataset::MetadataEvent) -> Self {
            match v {
                dtos::dataset::MetadataEvent::AddData(v) => Self::AddData(v.into()),
                dtos::dataset::MetadataEvent::ExecuteTransform(v) => {
                    Self::ExecuteTransform(v.into())
                }
                dtos::dataset::MetadataEvent::Seed(v) => Self::Seed(v.into()),
                dtos::dataset::MetadataEvent::SetPollingSource(v) => {
                    Self::SetPollingSource(v.into())
                }
                dtos::dataset::MetadataEvent::SetTransform(v) => Self::SetTransform(v.into()),
                dtos::dataset::MetadataEvent::SetVocab(v) => Self::SetVocab(v.into()),
                dtos::dataset::MetadataEvent::SetAttachments(v) => Self::SetAttachments(v.into()),
                dtos::dataset::MetadataEvent::SetInfo(v) => Self::SetInfo(v.into()),
                dtos::dataset::MetadataEvent::SetLicense(v) => Self::SetLicense(v.into()),
                dtos::dataset::MetadataEvent::SetDataSchema(v) => Self::SetDataSchema(v.into()),
                dtos::dataset::MetadataEvent::AddPushSource(v) => Self::AddPushSource(v.into()),
                dtos::dataset::MetadataEvent::DisablePushSource(v) => {
                    Self::DisablePushSource(v.into())
                }
                dtos::dataset::MetadataEvent::DisablePollingSource(v) => {
                    Self::DisablePollingSource(v.into())
                }
            }
        }
    }

    impl TryFrom<MetadataEvent> for dtos::dataset::MetadataEvent {
        type Error = ValidationError;
        fn try_from(v: MetadataEvent) -> Result<Self, Self::Error> {
            match v {
                MetadataEvent::AddData(v) => Ok(Self::AddData(v.try_into()?)),
                MetadataEvent::ExecuteTransform(v) => Ok(Self::ExecuteTransform(v.try_into()?)),
                MetadataEvent::Seed(v) => Ok(Self::Seed(v.try_into()?)),
                MetadataEvent::SetPollingSource(v) => Ok(Self::SetPollingSource(v.try_into()?)),
                MetadataEvent::SetTransform(v) => Ok(Self::SetTransform(v.try_into()?)),
                MetadataEvent::SetVocab(v) => Ok(Self::SetVocab(v.try_into()?)),
                MetadataEvent::SetAttachments(v) => Ok(Self::SetAttachments(v.try_into()?)),
                MetadataEvent::SetInfo(v) => Ok(Self::SetInfo(v.try_into()?)),
                MetadataEvent::SetLicense(v) => Ok(Self::SetLicense(v.try_into()?)),
                MetadataEvent::SetDataSchema(v) => Ok(Self::SetDataSchema(v.try_into()?)),
                MetadataEvent::AddPushSource(v) => Ok(Self::AddPushSource(v.try_into()?)),
                MetadataEvent::DisablePushSource(v) => Ok(Self::DisablePushSource(v.try_into()?)),
                MetadataEvent::DisablePollingSource(v) => {
                    Ok(Self::DisablePollingSource(v.try_into()?))
                }
            }
        }
    }

    implement_serde_as!(dtos::dataset::MetadataEvent, MetadataEvent);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/OffsetInterval
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct OffsetInterval {
        pub start: u64,
        pub end: u64,
    }

    impl IntoDto for OffsetInterval {
        type Dto = dtos::dataset::OffsetInterval;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::OffsetInterval> for OffsetInterval {
        fn from(v: dtos::dataset::OffsetInterval) -> Self {
            Self {
                start: v.start,
                end: v.end,
            }
        }
    }

    impl TryFrom<OffsetInterval> for dtos::dataset::OffsetInterval {
        type Error = ValidationError;
        fn try_from(v: OffsetInterval) -> Result<Self, ValidationError> {
            Ok(Self {
                start: v.start,
                end: v.end,
            })
        }
    }

    implement_serde_as!(dtos::dataset::OffsetInterval, OffsetInterval);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ProjectionSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ProjectionSpec {
        pub inputs: Vec<dataset::TransformInput>,
        pub project: dataset::Transform,
    }

    impl IntoDto for ProjectionSpec {
        type Dto = dtos::dataset::ProjectionSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::ProjectionSpec> for ProjectionSpec {
        fn from(v: dtos::dataset::ProjectionSpec) -> Self {
            Self {
                inputs: v.inputs.into_iter().map(Into::into).collect(),
                project: v.project.into(),
            }
        }
    }

    impl TryFrom<ProjectionSpec> for dtos::dataset::ProjectionSpec {
        type Error = ValidationError;
        fn try_from(v: ProjectionSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                inputs: v
                    .inputs
                    .into_iter()
                    .map(|i| dtos::dataset::TransformInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                project: dtos::dataset::Transform::try_from(v.project)?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::ProjectionSpec, ProjectionSpec);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Seed
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Seed {
        pub dataset_id: DatasetID,
        pub dataset_kind: dataset::DatasetKind,
    }

    impl IntoDto for Seed {
        type Dto = dtos::dataset::Seed;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::Seed> for Seed {
        fn from(v: dtos::dataset::Seed) -> Self {
            Self {
                dataset_id: v.dataset_id,
                dataset_kind: v.dataset_kind.into(),
            }
        }
    }

    impl TryFrom<Seed> for dtos::dataset::Seed {
        type Error = ValidationError;
        fn try_from(v: Seed) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_id: v.dataset_id,
                dataset_kind: dtos::dataset::DatasetKind::try_from(v.dataset_kind)?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::Seed, Seed);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetAttachments
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetAttachments {
        pub attachments: dataset::Attachments,
    }

    impl IntoDto for SetAttachments {
        type Dto = dtos::dataset::SetAttachments;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::SetAttachments> for SetAttachments {
        fn from(v: dtos::dataset::SetAttachments) -> Self {
            Self {
                attachments: v.attachments.into(),
            }
        }
    }

    impl TryFrom<SetAttachments> for dtos::dataset::SetAttachments {
        type Error = ValidationError;
        fn try_from(v: SetAttachments) -> Result<Self, ValidationError> {
            Ok(Self {
                attachments: dtos::dataset::Attachments::try_from(v.attachments)?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::SetAttachments, SetAttachments);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetDataSchema
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetDataSchema {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "base64_opt")]
        pub raw_arrow_schema: Option<Vec<u8>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<data::DataSchema>,
    }

    impl IntoDto for SetDataSchema {
        type Dto = dtos::dataset::SetDataSchema;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::SetDataSchema> for SetDataSchema {
        fn from(v: dtos::dataset::SetDataSchema) -> Self {
            Self {
                raw_arrow_schema: v.raw_arrow_schema,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<SetDataSchema> for dtos::dataset::SetDataSchema {
        type Error = ValidationError;
        fn try_from(v: SetDataSchema) -> Result<Self, ValidationError> {
            Ok(Self {
                raw_arrow_schema: v.raw_arrow_schema,
                schema: v
                    .schema
                    .map(|v| dtos::data::DataSchema::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::SetDataSchema, SetDataSchema);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetInfo
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetInfo {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub keywords: Option<Vec<String>>,
    }

    impl IntoDto for SetInfo {
        type Dto = dtos::dataset::SetInfo;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::SetInfo> for SetInfo {
        fn from(v: dtos::dataset::SetInfo) -> Self {
            Self {
                description: v.description,
                keywords: v.keywords,
            }
        }
    }

    impl TryFrom<SetInfo> for dtos::dataset::SetInfo {
        type Error = ValidationError;
        fn try_from(v: SetInfo) -> Result<Self, ValidationError> {
            Ok(Self {
                description: v.description,
                keywords: v.keywords,
            })
        }
    }

    implement_serde_as!(dtos::dataset::SetInfo, SetInfo);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetLicense
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetLicense {
        pub short_name: String,
        pub name: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub spdx_id: Option<String>,
        pub website_url: String,
    }

    impl IntoDto for SetLicense {
        type Dto = dtos::dataset::SetLicense;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::SetLicense> for SetLicense {
        fn from(v: dtos::dataset::SetLicense) -> Self {
            Self {
                short_name: v.short_name,
                name: v.name,
                spdx_id: v.spdx_id,
                website_url: v.website_url,
            }
        }
    }

    impl TryFrom<SetLicense> for dtos::dataset::SetLicense {
        type Error = ValidationError;
        fn try_from(v: SetLicense) -> Result<Self, ValidationError> {
            Ok(Self {
                short_name: v.short_name,
                name: v.name,
                spdx_id: v.spdx_id,
                website_url: v.website_url,
            })
        }
    }

    implement_serde_as!(dtos::dataset::SetLicense, SetLicense);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetTransform
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetTransform {
        pub inputs: Vec<dataset::TransformInput>,
        pub transform: dataset::Transform,
    }

    impl IntoDto for SetTransform {
        type Dto = dtos::dataset::SetTransform;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::SetTransform> for SetTransform {
        fn from(v: dtos::dataset::SetTransform) -> Self {
            Self {
                inputs: v.inputs.into_iter().map(Into::into).collect(),
                transform: v.transform.into(),
            }
        }
    }

    impl TryFrom<SetTransform> for dtos::dataset::SetTransform {
        type Error = ValidationError;
        fn try_from(v: SetTransform) -> Result<Self, ValidationError> {
            Ok(Self {
                inputs: v
                    .inputs
                    .into_iter()
                    .map(|i| dtos::dataset::TransformInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                transform: dtos::dataset::Transform::try_from(v.transform)?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::SetTransform, SetTransform);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetVocab
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetVocab {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub offset_column: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub operation_type_column: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub system_time_column: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_time_column: Option<String>,
    }

    impl IntoDto for SetVocab {
        type Dto = dtos::dataset::SetVocab;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::SetVocab> for SetVocab {
        fn from(v: dtos::dataset::SetVocab) -> Self {
            Self {
                offset_column: v.offset_column,
                operation_type_column: v.operation_type_column,
                system_time_column: v.system_time_column,
                event_time_column: v.event_time_column,
            }
        }
    }

    impl TryFrom<SetVocab> for dtos::dataset::SetVocab {
        type Error = ValidationError;
        fn try_from(v: SetVocab) -> Result<Self, ValidationError> {
            Ok(Self {
                offset_column: v.offset_column,
                operation_type_column: v.operation_type_column,
                system_time_column: v.system_time_column,
                event_time_column: v.event_time_column,
            })
        }
    }

    implement_serde_as!(dtos::dataset::SetVocab, SetVocab);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SqlQueryStep
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SqlQueryStep {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub alias: Option<String>,
        pub query: String,
    }

    impl IntoDto for SqlQueryStep {
        type Dto = dtos::dataset::SqlQueryStep;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::SqlQueryStep> for SqlQueryStep {
        fn from(v: dtos::dataset::SqlQueryStep) -> Self {
            Self {
                alias: v.alias,
                query: v.query,
            }
        }
    }

    impl TryFrom<SqlQueryStep> for dtos::dataset::SqlQueryStep {
        type Error = ValidationError;
        fn try_from(v: SqlQueryStep) -> Result<Self, ValidationError> {
            Ok(Self {
                alias: v.alias,
                query: v.query,
            })
        }
    }

    implement_serde_as!(dtos::dataset::SqlQueryStep, SqlQueryStep);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/TemporalTable
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TemporalTable {
        pub name: String,
        pub primary_key: Vec<String>,
    }

    impl IntoDto for TemporalTable {
        type Dto = dtos::dataset::TemporalTable;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::TemporalTable> for TemporalTable {
        fn from(v: dtos::dataset::TemporalTable) -> Self {
            Self {
                name: v.name,
                primary_key: v.primary_key,
            }
        }
    }

    impl TryFrom<TemporalTable> for dtos::dataset::TemporalTable {
        type Error = ValidationError;
        fn try_from(v: TemporalTable) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                primary_key: v.primary_key,
            })
        }
    }

    implement_serde_as!(dtos::dataset::TemporalTable, TemporalTable);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Transform
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum Transform {
        #[serde(alias = "sql")]
        Sql(dataset::TransformSql),
    }

    impl IntoDto for Transform {
        type Dto = dtos::dataset::Transform;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::Transform> for Transform {
        fn from(v: dtos::dataset::Transform) -> Self {
            match v {
                dtos::dataset::Transform::Sql(v) => Self::Sql(v.into()),
            }
        }
    }

    impl TryFrom<Transform> for dtos::dataset::Transform {
        type Error = ValidationError;
        fn try_from(v: Transform) -> Result<Self, Self::Error> {
            match v {
                Transform::Sql(v) => Ok(Self::Sql(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::dataset::Transform, Transform);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/TransformInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformInput {
        pub dataset_ref: DatasetRef,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub alias: Option<String>,
    }

    impl IntoDto for TransformInput {
        type Dto = dtos::dataset::TransformInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::TransformInput> for TransformInput {
        fn from(v: dtos::dataset::TransformInput) -> Self {
            Self {
                dataset_ref: v.dataset_ref,
                alias: v.alias,
            }
        }
    }

    impl TryFrom<TransformInput> for dtos::dataset::TransformInput {
        type Error = ValidationError;
        fn try_from(v: TransformInput) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_ref: v.dataset_ref,
                alias: v.alias,
            })
        }
    }

    implement_serde_as!(dtos::dataset::TransformInput, TransformInput);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Transform#/$defs/Sql
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformSql {
        pub engine: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub version: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub query: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub queries: Option<Vec<dataset::SqlQueryStep>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub temporal_tables: Option<Vec<dataset::TemporalTable>>,
    }

    impl IntoDto for TransformSql {
        type Dto = dtos::dataset::TransformSql;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::TransformSql> for TransformSql {
        fn from(v: dtos::dataset::TransformSql) -> Self {
            Self {
                engine: v.engine,
                version: v.version,
                query: v.query,
                queries: v.queries.map(|v| v.into_iter().map(Into::into).collect()),
                temporal_tables: v
                    .temporal_tables
                    .map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<TransformSql> for dtos::dataset::TransformSql {
        type Error = ValidationError;
        fn try_from(v: TransformSql) -> Result<Self, ValidationError> {
            Ok(Self {
                engine: v.engine,
                version: v.version,
                query: v.query,
                queries: v
                    .queries
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::dataset::SqlQueryStep::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                temporal_tables: v
                    .temporal_tables
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::dataset::TemporalTable::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::dataset::TransformSql, TransformSql);

    // Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Watermark
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Watermark {
        #[serde(with = "datetime_rfc3339")]
        pub system_time: DateTime<Utc>,
        #[serde(with = "datetime_rfc3339")]
        pub event_time: DateTime<Utc>,
    }

    impl IntoDto for Watermark {
        type Dto = dtos::dataset::Watermark;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::dataset::Watermark> for Watermark {
        fn from(v: dtos::dataset::Watermark) -> Self {
            Self {
                system_time: v.system_time,
                event_time: v.event_time,
            }
        }
    }

    impl TryFrom<Watermark> for dtos::dataset::Watermark {
        type Error = ValidationError;
        fn try_from(v: Watermark) -> Result<Self, ValidationError> {
            Ok(Self {
                system_time: v.system_time,
                event_time: v.event_time,
            })
        }
    }

    implement_serde_as!(dtos::dataset::Watermark, Watermark);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// engine
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod engine {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryRequest
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryRequest {
        pub input_data_paths: Vec<PathBuf>,
        pub transform: dataset::Transform,
        pub output_data_path: PathBuf,
    }

    impl IntoDto for RawQueryRequest {
        type Dto = dtos::engine::RawQueryRequest;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::RawQueryRequest> for RawQueryRequest {
        fn from(v: dtos::engine::RawQueryRequest) -> Self {
            Self {
                input_data_paths: v.input_data_paths,
                transform: v.transform.into(),
                output_data_path: v.output_data_path,
            }
        }
    }

    impl TryFrom<RawQueryRequest> for dtos::engine::RawQueryRequest {
        type Error = ValidationError;
        fn try_from(v: RawQueryRequest) -> Result<Self, ValidationError> {
            Ok(Self {
                input_data_paths: v.input_data_paths,
                transform: dtos::dataset::Transform::try_from(v.transform)?,
                output_data_path: v.output_data_path,
            })
        }
    }

    implement_serde_as!(dtos::engine::RawQueryRequest, RawQueryRequest);

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum RawQueryResponse {
        #[serde(alias = "progress")]
        Progress(engine::RawQueryResponseProgress),
        #[serde(alias = "success")]
        Success(engine::RawQueryResponseSuccess),
        #[serde(alias = "invalidQuery", alias = "invalidquery")]
        InvalidQuery(engine::RawQueryResponseInvalidQuery),
        #[serde(alias = "internalError", alias = "internalerror")]
        InternalError(engine::RawQueryResponseInternalError),
    }

    impl IntoDto for RawQueryResponse {
        type Dto = dtos::engine::RawQueryResponse;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::RawQueryResponse> for RawQueryResponse {
        fn from(v: dtos::engine::RawQueryResponse) -> Self {
            match v {
                dtos::engine::RawQueryResponse::Progress(v) => Self::Progress(v.into()),
                dtos::engine::RawQueryResponse::Success(v) => Self::Success(v.into()),
                dtos::engine::RawQueryResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
                dtos::engine::RawQueryResponse::InternalError(v) => Self::InternalError(v.into()),
            }
        }
    }

    impl TryFrom<RawQueryResponse> for dtos::engine::RawQueryResponse {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponse) -> Result<Self, Self::Error> {
            match v {
                RawQueryResponse::Progress(v) => Ok(Self::Progress(v.try_into()?)),
                RawQueryResponse::Success(v) => Ok(Self::Success(v.try_into()?)),
                RawQueryResponse::InvalidQuery(v) => Ok(Self::InvalidQuery(v.try_into()?)),
                RawQueryResponse::InternalError(v) => Ok(Self::InternalError(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::engine::RawQueryResponse, RawQueryResponse);

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/InternalError
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryResponseInternalError {
        pub message: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub backtrace: Option<String>,
    }

    impl IntoDto for RawQueryResponseInternalError {
        type Dto = dtos::engine::RawQueryResponseInternalError;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::RawQueryResponseInternalError> for RawQueryResponseInternalError {
        fn from(v: dtos::engine::RawQueryResponseInternalError) -> Self {
            Self {
                message: v.message,
                backtrace: v.backtrace,
            }
        }
    }

    impl TryFrom<RawQueryResponseInternalError> for dtos::engine::RawQueryResponseInternalError {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponseInternalError) -> Result<Self, ValidationError> {
            Ok(Self {
                message: v.message,
                backtrace: v.backtrace,
            })
        }
    }

    implement_serde_as!(
        dtos::engine::RawQueryResponseInternalError,
        RawQueryResponseInternalError
    );

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/InvalidQuery
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryResponseInvalidQuery {
        pub message: String,
    }

    impl IntoDto for RawQueryResponseInvalidQuery {
        type Dto = dtos::engine::RawQueryResponseInvalidQuery;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::RawQueryResponseInvalidQuery> for RawQueryResponseInvalidQuery {
        fn from(v: dtos::engine::RawQueryResponseInvalidQuery) -> Self {
            Self { message: v.message }
        }
    }

    impl TryFrom<RawQueryResponseInvalidQuery> for dtos::engine::RawQueryResponseInvalidQuery {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponseInvalidQuery) -> Result<Self, ValidationError> {
            Ok(Self { message: v.message })
        }
    }

    implement_serde_as!(
        dtos::engine::RawQueryResponseInvalidQuery,
        RawQueryResponseInvalidQuery
    );

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/Progress
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryResponseProgress {}

    impl IntoDto for RawQueryResponseProgress {
        type Dto = dtos::engine::RawQueryResponseProgress;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::RawQueryResponseProgress> for RawQueryResponseProgress {
        fn from(v: dtos::engine::RawQueryResponseProgress) -> Self {
            Self {}
        }
    }

    impl TryFrom<RawQueryResponseProgress> for dtos::engine::RawQueryResponseProgress {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponseProgress) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::engine::RawQueryResponseProgress,
        RawQueryResponseProgress
    );

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/Success
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryResponseSuccess {
        pub num_records: u64,
    }

    impl IntoDto for RawQueryResponseSuccess {
        type Dto = dtos::engine::RawQueryResponseSuccess;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::RawQueryResponseSuccess> for RawQueryResponseSuccess {
        fn from(v: dtos::engine::RawQueryResponseSuccess) -> Self {
            Self {
                num_records: v.num_records,
            }
        }
    }

    impl TryFrom<RawQueryResponseSuccess> for dtos::engine::RawQueryResponseSuccess {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponseSuccess) -> Result<Self, ValidationError> {
            Ok(Self {
                num_records: v.num_records,
            })
        }
    }

    implement_serde_as!(
        dtos::engine::RawQueryResponseSuccess,
        RawQueryResponseSuccess
    );

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformRequest
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformRequest {
        pub dataset_id: DatasetID,
        pub dataset_alias: DatasetAlias,
        #[serde(with = "datetime_rfc3339")]
        pub system_time: DateTime<Utc>,
        pub vocab: dataset::DatasetVocabulary,
        pub transform: dataset::Transform,
        pub query_inputs: Vec<engine::TransformRequestInput>,
        pub next_offset: u64,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_checkpoint_path: Option<PathBuf>,
        pub new_checkpoint_path: PathBuf,
        pub new_data_path: PathBuf,
    }

    impl IntoDto for TransformRequest {
        type Dto = dtos::engine::TransformRequest;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::TransformRequest> for TransformRequest {
        fn from(v: dtos::engine::TransformRequest) -> Self {
            Self {
                dataset_id: v.dataset_id,
                dataset_alias: v.dataset_alias,
                system_time: v.system_time,
                vocab: v.vocab.into(),
                transform: v.transform.into(),
                query_inputs: v.query_inputs.into_iter().map(Into::into).collect(),
                next_offset: v.next_offset,
                prev_checkpoint_path: v.prev_checkpoint_path,
                new_checkpoint_path: v.new_checkpoint_path,
                new_data_path: v.new_data_path,
            }
        }
    }

    impl TryFrom<TransformRequest> for dtos::engine::TransformRequest {
        type Error = ValidationError;
        fn try_from(v: TransformRequest) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_id: v.dataset_id,
                dataset_alias: v.dataset_alias,
                system_time: v.system_time,
                vocab: dtos::dataset::DatasetVocabulary::try_from(v.vocab)?,
                transform: dtos::dataset::Transform::try_from(v.transform)?,
                query_inputs: v
                    .query_inputs
                    .into_iter()
                    .map(|i| dtos::engine::TransformRequestInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                next_offset: v.next_offset,
                prev_checkpoint_path: v.prev_checkpoint_path,
                new_checkpoint_path: v.new_checkpoint_path,
                new_data_path: v.new_data_path,
            })
        }
    }

    implement_serde_as!(dtos::engine::TransformRequest, TransformRequest);

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformRequestInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformRequestInput {
        pub dataset_id: DatasetID,
        pub dataset_alias: DatasetAlias,
        pub query_alias: String,
        pub vocab: dataset::DatasetVocabulary,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub offset_interval: Option<dataset::OffsetInterval>,
        pub data_paths: Vec<PathBuf>,
        pub schema_file: PathBuf,
        pub explicit_watermarks: Vec<dataset::Watermark>,
    }

    impl IntoDto for TransformRequestInput {
        type Dto = dtos::engine::TransformRequestInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::TransformRequestInput> for TransformRequestInput {
        fn from(v: dtos::engine::TransformRequestInput) -> Self {
            Self {
                dataset_id: v.dataset_id,
                dataset_alias: v.dataset_alias,
                query_alias: v.query_alias,
                vocab: v.vocab.into(),
                offset_interval: v.offset_interval.map(|v| v.into()),
                data_paths: v.data_paths,
                schema_file: v.schema_file,
                explicit_watermarks: v.explicit_watermarks.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<TransformRequestInput> for dtos::engine::TransformRequestInput {
        type Error = ValidationError;
        fn try_from(v: TransformRequestInput) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_id: v.dataset_id,
                dataset_alias: v.dataset_alias,
                query_alias: v.query_alias,
                vocab: dtos::dataset::DatasetVocabulary::try_from(v.vocab)?,
                offset_interval: v
                    .offset_interval
                    .map(|v| dtos::dataset::OffsetInterval::try_from(v))
                    .transpose()?,
                data_paths: v.data_paths,
                schema_file: v.schema_file,
                explicit_watermarks: v
                    .explicit_watermarks
                    .into_iter()
                    .map(|i| dtos::dataset::Watermark::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::engine::TransformRequestInput, TransformRequestInput);

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum TransformResponse {
        #[serde(alias = "progress")]
        Progress(engine::TransformResponseProgress),
        #[serde(alias = "success")]
        Success(engine::TransformResponseSuccess),
        #[serde(alias = "invalidQuery", alias = "invalidquery")]
        InvalidQuery(engine::TransformResponseInvalidQuery),
        #[serde(alias = "internalError", alias = "internalerror")]
        InternalError(engine::TransformResponseInternalError),
    }

    impl IntoDto for TransformResponse {
        type Dto = dtos::engine::TransformResponse;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::TransformResponse> for TransformResponse {
        fn from(v: dtos::engine::TransformResponse) -> Self {
            match v {
                dtos::engine::TransformResponse::Progress(v) => Self::Progress(v.into()),
                dtos::engine::TransformResponse::Success(v) => Self::Success(v.into()),
                dtos::engine::TransformResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
                dtos::engine::TransformResponse::InternalError(v) => Self::InternalError(v.into()),
            }
        }
    }

    impl TryFrom<TransformResponse> for dtos::engine::TransformResponse {
        type Error = ValidationError;
        fn try_from(v: TransformResponse) -> Result<Self, Self::Error> {
            match v {
                TransformResponse::Progress(v) => Ok(Self::Progress(v.try_into()?)),
                TransformResponse::Success(v) => Ok(Self::Success(v.try_into()?)),
                TransformResponse::InvalidQuery(v) => Ok(Self::InvalidQuery(v.try_into()?)),
                TransformResponse::InternalError(v) => Ok(Self::InternalError(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::engine::TransformResponse, TransformResponse);

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/InternalError
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformResponseInternalError {
        pub message: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub backtrace: Option<String>,
    }

    impl IntoDto for TransformResponseInternalError {
        type Dto = dtos::engine::TransformResponseInternalError;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::TransformResponseInternalError> for TransformResponseInternalError {
        fn from(v: dtos::engine::TransformResponseInternalError) -> Self {
            Self {
                message: v.message,
                backtrace: v.backtrace,
            }
        }
    }

    impl TryFrom<TransformResponseInternalError> for dtos::engine::TransformResponseInternalError {
        type Error = ValidationError;
        fn try_from(v: TransformResponseInternalError) -> Result<Self, ValidationError> {
            Ok(Self {
                message: v.message,
                backtrace: v.backtrace,
            })
        }
    }

    implement_serde_as!(
        dtos::engine::TransformResponseInternalError,
        TransformResponseInternalError
    );

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/InvalidQuery
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformResponseInvalidQuery {
        pub message: String,
    }

    impl IntoDto for TransformResponseInvalidQuery {
        type Dto = dtos::engine::TransformResponseInvalidQuery;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::TransformResponseInvalidQuery> for TransformResponseInvalidQuery {
        fn from(v: dtos::engine::TransformResponseInvalidQuery) -> Self {
            Self { message: v.message }
        }
    }

    impl TryFrom<TransformResponseInvalidQuery> for dtos::engine::TransformResponseInvalidQuery {
        type Error = ValidationError;
        fn try_from(v: TransformResponseInvalidQuery) -> Result<Self, ValidationError> {
            Ok(Self { message: v.message })
        }
    }

    implement_serde_as!(
        dtos::engine::TransformResponseInvalidQuery,
        TransformResponseInvalidQuery
    );

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/Progress
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformResponseProgress {}

    impl IntoDto for TransformResponseProgress {
        type Dto = dtos::engine::TransformResponseProgress;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::TransformResponseProgress> for TransformResponseProgress {
        fn from(v: dtos::engine::TransformResponseProgress) -> Self {
            Self {}
        }
    }

    impl TryFrom<TransformResponseProgress> for dtos::engine::TransformResponseProgress {
        type Error = ValidationError;
        fn try_from(v: TransformResponseProgress) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::engine::TransformResponseProgress,
        TransformResponseProgress
    );

    // Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/Success
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformResponseSuccess {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_offset_interval: Option<dataset::OffsetInterval>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub new_watermark: Option<DateTime<Utc>>,
    }

    impl IntoDto for TransformResponseSuccess {
        type Dto = dtos::engine::TransformResponseSuccess;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engine::TransformResponseSuccess> for TransformResponseSuccess {
        fn from(v: dtos::engine::TransformResponseSuccess) -> Self {
            Self {
                new_offset_interval: v.new_offset_interval.map(|v| v.into()),
                new_watermark: v.new_watermark,
            }
        }
    }

    impl TryFrom<TransformResponseSuccess> for dtos::engine::TransformResponseSuccess {
        type Error = ValidationError;
        fn try_from(v: TransformResponseSuccess) -> Result<Self, ValidationError> {
            Ok(Self {
                new_offset_interval: v
                    .new_offset_interval
                    .map(|v| dtos::dataset::OffsetInterval::try_from(v))
                    .transpose()?,
                new_watermark: v.new_watermark,
            })
        }
    }

    implement_serde_as!(
        dtos::engine::TransformResponseSuccess,
        TransformResponseSuccess
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// event
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod event {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/event/v1alpha1/EventFilter
    #[derive(Debug, Serialize, Deserialize)]
    pub struct EventFilter {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    impl IntoDto for EventFilter {
        type Dto = dtos::event::EventFilter;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::event::EventFilter> for EventFilter {
        fn from(v: dtos::event::EventFilter) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<EventFilter> for dtos::event::EventFilter {
        type Error = ValidationError;
        fn try_from(v: EventFilter) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::event::EventFilter, EventFilter);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// flow
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod flow {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowSpec {
        pub target: StructOrString<resource::ResourceSelector>,
        pub triggers: Vec<flow::FlowTrigger>,
        pub tasks: Vec<flow::TaskSpec>,
    }

    impl IntoDto for FlowSpec {
        type Dto = dtos::flow::FlowSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::FlowSpec> for FlowSpec {
        fn from(v: dtos::flow::FlowSpec) -> Self {
            Self {
                target: v.target.into(),
                triggers: v.triggers.into_iter().map(Into::into).collect(),
                tasks: v.tasks.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<FlowSpec> for dtos::flow::FlowSpec {
        type Error = ValidationError;
        fn try_from(v: FlowSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                target: dtos::resource::ResourceSelector::try_from(v.target)?,
                triggers: v
                    .triggers
                    .into_iter()
                    .map(|i| dtos::flow::FlowTrigger::try_from(i))
                    .collect::<Result<_, _>>()?,
                tasks: v
                    .tasks
                    .into_iter()
                    .map(|i| dtos::flow::TaskSpec::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::flow::FlowSpec, FlowSpec);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum FlowTrigger {
        #[serde(alias = "schedule")]
        Schedule(flow::FlowTriggerSchedule),
        #[serde(alias = "event")]
        Event(flow::FlowTriggerEvent),
        #[serde(alias = "source")]
        Source(flow::FlowTriggerSource),
        #[serde(alias = "dataset")]
        Dataset(flow::FlowTriggerDataset),
    }

    impl IntoDto for FlowTrigger {
        type Dto = dtos::flow::FlowTrigger;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::FlowTrigger> for FlowTrigger {
        fn from(v: dtos::flow::FlowTrigger) -> Self {
            match v {
                dtos::flow::FlowTrigger::Schedule(v) => Self::Schedule(v.into()),
                dtos::flow::FlowTrigger::Event(v) => Self::Event(v.into()),
                dtos::flow::FlowTrigger::Source(v) => Self::Source(v.into()),
                dtos::flow::FlowTrigger::Dataset(v) => Self::Dataset(v.into()),
            }
        }
    }

    impl TryFrom<FlowTrigger> for dtos::flow::FlowTrigger {
        type Error = ValidationError;
        fn try_from(v: FlowTrigger) -> Result<Self, Self::Error> {
            match v {
                FlowTrigger::Schedule(v) => Ok(Self::Schedule(v.try_into()?)),
                FlowTrigger::Event(v) => Ok(Self::Event(v.try_into()?)),
                FlowTrigger::Source(v) => Ok(Self::Source(v.try_into()?)),
                FlowTrigger::Dataset(v) => Ok(Self::Dataset(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::flow::FlowTrigger, FlowTrigger);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Dataset
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerDataset {
        pub dataset: StructOrString<dataset::DatasetSelector>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub events: Option<Vec<String>>,
    }

    impl IntoDto for FlowTriggerDataset {
        type Dto = dtos::flow::FlowTriggerDataset;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::FlowTriggerDataset> for FlowTriggerDataset {
        fn from(v: dtos::flow::FlowTriggerDataset) -> Self {
            Self {
                dataset: v.dataset.into(),
                events: v.events,
            }
        }
    }

    impl TryFrom<FlowTriggerDataset> for dtos::flow::FlowTriggerDataset {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerDataset) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset: dtos::dataset::DatasetSelector::try_from(v.dataset)?,
                events: v.events,
            })
        }
    }

    implement_serde_as!(dtos::flow::FlowTriggerDataset, FlowTriggerDataset);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Event
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerEvent {
        pub events: event::EventFilter,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cooldown: Option<DurationString>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cooldown_max_batch: Option<u64>,
    }

    impl IntoDto for FlowTriggerEvent {
        type Dto = dtos::flow::FlowTriggerEvent;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::FlowTriggerEvent> for FlowTriggerEvent {
        fn from(v: dtos::flow::FlowTriggerEvent) -> Self {
            Self {
                events: v.events.into(),
                cooldown: v.cooldown,
                cooldown_max_batch: v.cooldown_max_batch,
            }
        }
    }

    impl TryFrom<FlowTriggerEvent> for dtos::flow::FlowTriggerEvent {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerEvent) -> Result<Self, ValidationError> {
            Ok(Self {
                events: dtos::event::EventFilter::try_from(v.events)?,
                cooldown: v.cooldown,
                cooldown_max_batch: v.cooldown_max_batch,
            })
        }
    }

    implement_serde_as!(dtos::flow::FlowTriggerEvent, FlowTriggerEvent);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Schedule
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerSchedule {
        pub cron: String,
    }

    impl IntoDto for FlowTriggerSchedule {
        type Dto = dtos::flow::FlowTriggerSchedule;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::FlowTriggerSchedule> for FlowTriggerSchedule {
        fn from(v: dtos::flow::FlowTriggerSchedule) -> Self {
            Self { cron: v.cron }
        }
    }

    impl TryFrom<FlowTriggerSchedule> for dtos::flow::FlowTriggerSchedule {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerSchedule) -> Result<Self, ValidationError> {
            Ok(Self { cron: v.cron })
        }
    }

    implement_serde_as!(dtos::flow::FlowTriggerSchedule, FlowTriggerSchedule);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Source
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerSource {
        pub source: StructOrString<resource::ResourceRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub min_records_to_await: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_await_interval: Option<DurationString>,
    }

    impl IntoDto for FlowTriggerSource {
        type Dto = dtos::flow::FlowTriggerSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::FlowTriggerSource> for FlowTriggerSource {
        fn from(v: dtos::flow::FlowTriggerSource) -> Self {
            Self {
                source: v.source.into(),
                min_records_to_await: v.min_records_to_await,
                max_await_interval: v.max_await_interval,
            }
        }
    }

    impl TryFrom<FlowTriggerSource> for dtos::flow::FlowTriggerSource {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerSource) -> Result<Self, ValidationError> {
            Ok(Self {
                source: dtos::resource::ResourceRef::try_from(v.source)?,
                min_records_to_await: v.min_records_to_await,
                max_await_interval: v.max_await_interval,
            })
        }
    }

    implement_serde_as!(dtos::flow::FlowTriggerSource, FlowTriggerSource);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum TaskSpec {
        #[serde(alias = "ingest")]
        Ingest(flow::TaskSpecIngest),
        #[serde(alias = "compaction")]
        Compaction(flow::TaskSpecCompaction),
        #[serde(alias = "garbageCollection", alias = "garbagecollection")]
        GarbageCollection(flow::TaskSpecGarbageCollection),
        #[serde(alias = "webhookCall", alias = "webhookcall")]
        WebhookCall(flow::TaskSpecWebhookCall),
    }

    impl IntoDto for TaskSpec {
        type Dto = dtos::flow::TaskSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::TaskSpec> for TaskSpec {
        fn from(v: dtos::flow::TaskSpec) -> Self {
            match v {
                dtos::flow::TaskSpec::Ingest(v) => Self::Ingest(v.into()),
                dtos::flow::TaskSpec::Compaction(v) => Self::Compaction(v.into()),
                dtos::flow::TaskSpec::GarbageCollection(v) => Self::GarbageCollection(v.into()),
                dtos::flow::TaskSpec::WebhookCall(v) => Self::WebhookCall(v.into()),
            }
        }
    }

    impl TryFrom<TaskSpec> for dtos::flow::TaskSpec {
        type Error = ValidationError;
        fn try_from(v: TaskSpec) -> Result<Self, Self::Error> {
            match v {
                TaskSpec::Ingest(v) => Ok(Self::Ingest(v.try_into()?)),
                TaskSpec::Compaction(v) => Ok(Self::Compaction(v.try_into()?)),
                TaskSpec::GarbageCollection(v) => Ok(Self::GarbageCollection(v.try_into()?)),
                TaskSpec::WebhookCall(v) => Ok(Self::WebhookCall(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::flow::TaskSpec, TaskSpec);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/Compaction
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecCompaction {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub params: Option<dataset::CompactionParams>,
    }

    impl IntoDto for TaskSpecCompaction {
        type Dto = dtos::flow::TaskSpecCompaction;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::TaskSpecCompaction> for TaskSpecCompaction {
        fn from(v: dtos::flow::TaskSpecCompaction) -> Self {
            Self {
                params: v.params.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecCompaction> for dtos::flow::TaskSpecCompaction {
        type Error = ValidationError;
        fn try_from(v: TaskSpecCompaction) -> Result<Self, ValidationError> {
            Ok(Self {
                params: v
                    .params
                    .map(|v| dtos::dataset::CompactionParams::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::flow::TaskSpecCompaction, TaskSpecCompaction);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/GarbageCollection
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecGarbageCollection {}

    impl IntoDto for TaskSpecGarbageCollection {
        type Dto = dtos::flow::TaskSpecGarbageCollection;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::TaskSpecGarbageCollection> for TaskSpecGarbageCollection {
        fn from(v: dtos::flow::TaskSpecGarbageCollection) -> Self {
            Self {}
        }
    }

    impl TryFrom<TaskSpecGarbageCollection> for dtos::flow::TaskSpecGarbageCollection {
        type Error = ValidationError;
        fn try_from(v: TaskSpecGarbageCollection) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::flow::TaskSpecGarbageCollection,
        TaskSpecGarbageCollection
    );

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/Ingest
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecIngest {
        pub source: StructOrString<resource::ResourceRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub params: Option<source::IngestParams>,
    }

    impl IntoDto for TaskSpecIngest {
        type Dto = dtos::flow::TaskSpecIngest;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::TaskSpecIngest> for TaskSpecIngest {
        fn from(v: dtos::flow::TaskSpecIngest) -> Self {
            Self {
                source: v.source.into(),
                params: v.params.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecIngest> for dtos::flow::TaskSpecIngest {
        type Error = ValidationError;
        fn try_from(v: TaskSpecIngest) -> Result<Self, ValidationError> {
            Ok(Self {
                source: dtos::resource::ResourceRef::try_from(v.source)?,
                params: v
                    .params
                    .map(|v| dtos::source::IngestParams::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::flow::TaskSpecIngest, TaskSpecIngest);

    // Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/WebhookCall
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecWebhookCall {
        pub target: StructOrString<resource::ResourceRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub payload: Option<String>,
    }

    impl IntoDto for TaskSpecWebhookCall {
        type Dto = dtos::flow::TaskSpecWebhookCall;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flow::TaskSpecWebhookCall> for TaskSpecWebhookCall {
        fn from(v: dtos::flow::TaskSpecWebhookCall) -> Self {
            Self {
                target: v.target.into(),
                payload: v.payload,
            }
        }
    }

    impl TryFrom<TaskSpecWebhookCall> for dtos::flow::TaskSpecWebhookCall {
        type Error = ValidationError;
        fn try_from(v: TaskSpecWebhookCall) -> Result<Self, ValidationError> {
            Ok(Self {
                target: dtos::resource::ResourceRef::try_from(v.target)?,
                payload: v.payload,
            })
        }
    }

    implement_serde_as!(dtos::flow::TaskSpecWebhookCall, TaskSpecWebhookCall);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// legacy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod legacy {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/legacy/v0/AddPushSource
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AddPushSource {
        pub source_name: String,
        pub read: source::ReadStep,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub preprocess: Option<dataset::Transform>,
        pub merge: source::MergeStrategy,
    }

    impl IntoDto for AddPushSource {
        type Dto = dtos::legacy::AddPushSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::AddPushSource> for AddPushSource {
        fn from(v: dtos::legacy::AddPushSource) -> Self {
            Self {
                source_name: v.source_name,
                read: v.read.into(),
                preprocess: v.preprocess.map(|v| v.into()),
                merge: v.merge.into(),
            }
        }
    }

    impl TryFrom<AddPushSource> for dtos::legacy::AddPushSource {
        type Error = ValidationError;
        fn try_from(v: AddPushSource) -> Result<Self, ValidationError> {
            Ok(Self {
                source_name: v.source_name,
                read: dtos::source::ReadStep::try_from(v.read)?,
                preprocess: v
                    .preprocess
                    .map(|v| dtos::dataset::Transform::try_from(v))
                    .transpose()?,
                merge: dtos::source::MergeStrategy::try_from(v.merge)?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::AddPushSource, AddPushSource);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/DatasetSnapshot
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetSnapshot {
        pub name: DatasetAlias,
        pub kind: dataset::DatasetKind,
        pub metadata: Vec<dataset::MetadataEvent>,
    }

    impl IntoDto for DatasetSnapshot {
        type Dto = dtos::legacy::DatasetSnapshot;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::DatasetSnapshot> for DatasetSnapshot {
        fn from(v: dtos::legacy::DatasetSnapshot) -> Self {
            Self {
                name: v.name,
                kind: v.kind.into(),
                metadata: v.metadata.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<DatasetSnapshot> for dtos::legacy::DatasetSnapshot {
        type Error = ValidationError;
        fn try_from(v: DatasetSnapshot) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                kind: dtos::dataset::DatasetKind::try_from(v.kind)?,
                metadata: v
                    .metadata
                    .into_iter()
                    .map(|i| dtos::dataset::MetadataEvent::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::DatasetSnapshot, DatasetSnapshot);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePollingSource
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DisablePollingSource {}

    impl IntoDto for DisablePollingSource {
        type Dto = dtos::legacy::DisablePollingSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::DisablePollingSource> for DisablePollingSource {
        fn from(v: dtos::legacy::DisablePollingSource) -> Self {
            Self {}
        }
    }

    impl TryFrom<DisablePollingSource> for dtos::legacy::DisablePollingSource {
        type Error = ValidationError;
        fn try_from(v: DisablePollingSource) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::legacy::DisablePollingSource, DisablePollingSource);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePushSource
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DisablePushSource {
        pub source_name: String,
    }

    impl IntoDto for DisablePushSource {
        type Dto = dtos::legacy::DisablePushSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::DisablePushSource> for DisablePushSource {
        fn from(v: dtos::legacy::DisablePushSource) -> Self {
            Self {
                source_name: v.source_name,
            }
        }
    }

    impl TryFrom<DisablePushSource> for dtos::legacy::DisablePushSource {
        type Error = ValidationError;
        fn try_from(v: DisablePushSource) -> Result<Self, ValidationError> {
            Ok(Self {
                source_name: v.source_name,
            })
        }
    }

    implement_serde_as!(dtos::legacy::DisablePushSource, DisablePushSource);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum FetchStep {
        #[serde(alias = "url")]
        Url(legacy::FetchStepUrl),
        #[serde(alias = "filesGlob", alias = "filesglob")]
        FilesGlob(legacy::FetchStepFilesGlob),
        #[serde(alias = "container")]
        Container(legacy::FetchStepContainer),
        #[serde(alias = "mqtt")]
        Mqtt(legacy::FetchStepMqtt),
        #[serde(alias = "ethereumLogs", alias = "ethereumlogs")]
        EthereumLogs(legacy::FetchStepEthereumLogs),
    }

    impl IntoDto for FetchStep {
        type Dto = dtos::legacy::FetchStep;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::FetchStep> for FetchStep {
        fn from(v: dtos::legacy::FetchStep) -> Self {
            match v {
                dtos::legacy::FetchStep::Url(v) => Self::Url(v.into()),
                dtos::legacy::FetchStep::FilesGlob(v) => Self::FilesGlob(v.into()),
                dtos::legacy::FetchStep::Container(v) => Self::Container(v.into()),
                dtos::legacy::FetchStep::Mqtt(v) => Self::Mqtt(v.into()),
                dtos::legacy::FetchStep::EthereumLogs(v) => Self::EthereumLogs(v.into()),
            }
        }
    }

    impl TryFrom<FetchStep> for dtos::legacy::FetchStep {
        type Error = ValidationError;
        fn try_from(v: FetchStep) -> Result<Self, Self::Error> {
            match v {
                FetchStep::Url(v) => Ok(Self::Url(v.try_into()?)),
                FetchStep::FilesGlob(v) => Ok(Self::FilesGlob(v.try_into()?)),
                FetchStep::Container(v) => Ok(Self::Container(v.try_into()?)),
                FetchStep::Mqtt(v) => Ok(Self::Mqtt(v.try_into()?)),
                FetchStep::EthereumLogs(v) => Ok(Self::EthereumLogs(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::legacy::FetchStep, FetchStep);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Container
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FetchStepContainer {
        pub image: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub command: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub args: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub env: Option<Vec<source::EnvVar>>,
    }

    impl IntoDto for FetchStepContainer {
        type Dto = dtos::legacy::FetchStepContainer;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::FetchStepContainer> for FetchStepContainer {
        fn from(v: dtos::legacy::FetchStepContainer) -> Self {
            Self {
                image: v.image,
                command: v.command,
                args: v.args,
                env: v.env.map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<FetchStepContainer> for dtos::legacy::FetchStepContainer {
        type Error = ValidationError;
        fn try_from(v: FetchStepContainer) -> Result<Self, ValidationError> {
            Ok(Self {
                image: v.image,
                command: v.command,
                args: v.args,
                env: v
                    .env
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::source::EnvVar::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::FetchStepContainer, FetchStepContainer);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/EthereumLogs
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FetchStepEthereumLogs {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub chain_id: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub node_url: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub filter: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub signature: Option<String>,
    }

    impl IntoDto for FetchStepEthereumLogs {
        type Dto = dtos::legacy::FetchStepEthereumLogs;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::FetchStepEthereumLogs> for FetchStepEthereumLogs {
        fn from(v: dtos::legacy::FetchStepEthereumLogs) -> Self {
            Self {
                chain_id: v.chain_id,
                node_url: v.node_url,
                filter: v.filter,
                signature: v.signature,
            }
        }
    }

    impl TryFrom<FetchStepEthereumLogs> for dtos::legacy::FetchStepEthereumLogs {
        type Error = ValidationError;
        fn try_from(v: FetchStepEthereumLogs) -> Result<Self, ValidationError> {
            Ok(Self {
                chain_id: v.chain_id,
                node_url: v.node_url,
                filter: v.filter,
                signature: v.signature,
            })
        }
    }

    implement_serde_as!(dtos::legacy::FetchStepEthereumLogs, FetchStepEthereumLogs);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/FilesGlob
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FetchStepFilesGlob {
        pub path: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_time: Option<UnionOrString<source::EventTimeSource>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cache: Option<UnionOrString<source::SourceCaching>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub order: Option<source::SourceOrdering>,
    }

    impl IntoDto for FetchStepFilesGlob {
        type Dto = dtos::legacy::FetchStepFilesGlob;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::FetchStepFilesGlob> for FetchStepFilesGlob {
        fn from(v: dtos::legacy::FetchStepFilesGlob) -> Self {
            Self {
                path: v.path,
                event_time: v.event_time.map(|v| v.into()),
                cache: v.cache.map(|v| v.into()),
                order: v.order.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<FetchStepFilesGlob> for dtos::legacy::FetchStepFilesGlob {
        type Error = ValidationError;
        fn try_from(v: FetchStepFilesGlob) -> Result<Self, ValidationError> {
            Ok(Self {
                path: v.path,
                event_time: v
                    .event_time
                    .map(|v| dtos::source::EventTimeSource::try_from(v))
                    .transpose()?,
                cache: v
                    .cache
                    .map(|v| dtos::source::SourceCaching::try_from(v))
                    .transpose()?,
                order: v
                    .order
                    .map(|v| dtos::source::SourceOrdering::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::FetchStepFilesGlob, FetchStepFilesGlob);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Mqtt
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FetchStepMqtt {
        pub host: String,
        pub port: i32,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub username: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub password: Option<String>,
        pub topics: Vec<source::MqttTopicSubscription>,
    }

    impl IntoDto for FetchStepMqtt {
        type Dto = dtos::legacy::FetchStepMqtt;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::FetchStepMqtt> for FetchStepMqtt {
        fn from(v: dtos::legacy::FetchStepMqtt) -> Self {
            Self {
                host: v.host,
                port: v.port,
                username: v.username,
                password: v.password,
                topics: v.topics.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<FetchStepMqtt> for dtos::legacy::FetchStepMqtt {
        type Error = ValidationError;
        fn try_from(v: FetchStepMqtt) -> Result<Self, ValidationError> {
            Ok(Self {
                host: v.host,
                port: v.port,
                username: v.username,
                password: v.password,
                topics: v
                    .topics
                    .into_iter()
                    .map(|i| dtos::source::MqttTopicSubscription::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::FetchStepMqtt, FetchStepMqtt);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Url
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FetchStepUrl {
        pub url: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_time: Option<UnionOrString<source::EventTimeSource>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cache: Option<UnionOrString<source::SourceCaching>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub headers: Option<Vec<source::RequestHeader>>,
    }

    impl IntoDto for FetchStepUrl {
        type Dto = dtos::legacy::FetchStepUrl;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::FetchStepUrl> for FetchStepUrl {
        fn from(v: dtos::legacy::FetchStepUrl) -> Self {
            Self {
                url: v.url,
                event_time: v.event_time.map(|v| v.into()),
                cache: v.cache.map(|v| v.into()),
                headers: v.headers.map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<FetchStepUrl> for dtos::legacy::FetchStepUrl {
        type Error = ValidationError;
        fn try_from(v: FetchStepUrl) -> Result<Self, ValidationError> {
            Ok(Self {
                url: v.url,
                event_time: v
                    .event_time
                    .map(|v| dtos::source::EventTimeSource::try_from(v))
                    .transpose()?,
                cache: v
                    .cache
                    .map(|v| dtos::source::SourceCaching::try_from(v))
                    .transpose()?,
                headers: v
                    .headers
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::source::RequestHeader::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::FetchStepUrl, FetchStepUrl);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/Manifest
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Manifest<ContentT> {
        pub kind: String,
        pub version: i32,
        pub content: ContentT,
    }

    impl<ContentT> IntoDto for Manifest<ContentT>
    where
        ContentT: IntoDto,
        <ContentT as IntoDto>::Dto: TryFrom<ContentT>,
        ValidationError: From<<<ContentT as IntoDto>::Dto as TryFrom<ContentT>>::Error>,
    {
        type Dto = dtos::legacy::Manifest<ContentT::Dto>;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl<ContentTFrom, ContentTTo> From<dtos::legacy::Manifest<ContentTFrom>> for Manifest<ContentTTo>
    where
        ContentTTo: From<ContentTFrom>,
    {
        fn from(v: dtos::legacy::Manifest<ContentTFrom>) -> Self {
            Self {
                kind: v.kind,
                version: v.version,
                content: v.content.into(),
            }
        }
    }

    impl<ContentTFrom, ContentTTo> TryFrom<Manifest<ContentTFrom>>
        for dtos::legacy::Manifest<ContentTTo>
    where
        ContentTTo: TryFrom<ContentTFrom>,
        ValidationError: From<<ContentTTo as TryFrom<ContentTFrom>>::Error>,
    {
        type Error = ValidationError;
        fn try_from(v: Manifest<ContentTFrom>) -> Result<Self, ValidationError> {
            Ok(Self {
                kind: v.kind,
                version: v.version,
                content: ContentTTo::try_from(v.content)?,
            })
        }
    }

    // Schema: https://opendatafabric.org/schemas/legacy/v0/SetPollingSource
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetPollingSource {
        pub fetch: legacy::FetchStep,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prepare: Option<Vec<source::PrepStep>>,
        pub read: source::ReadStep,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub preprocess: Option<dataset::Transform>,
        pub merge: source::MergeStrategy,
    }

    impl IntoDto for SetPollingSource {
        type Dto = dtos::legacy::SetPollingSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::legacy::SetPollingSource> for SetPollingSource {
        fn from(v: dtos::legacy::SetPollingSource) -> Self {
            Self {
                fetch: v.fetch.into(),
                prepare: v.prepare.map(|v| v.into_iter().map(Into::into).collect()),
                read: v.read.into(),
                preprocess: v.preprocess.map(|v| v.into()),
                merge: v.merge.into(),
            }
        }
    }

    impl TryFrom<SetPollingSource> for dtos::legacy::SetPollingSource {
        type Error = ValidationError;
        fn try_from(v: SetPollingSource) -> Result<Self, ValidationError> {
            Ok(Self {
                fetch: dtos::legacy::FetchStep::try_from(v.fetch)?,
                prepare: v
                    .prepare
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::source::PrepStep::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                read: dtos::source::ReadStep::try_from(v.read)?,
                preprocess: v
                    .preprocess
                    .map(|v| dtos::dataset::Transform::try_from(v))
                    .transpose()?,
                merge: dtos::source::MergeStrategy::try_from(v.merge)?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::SetPollingSource, SetPollingSource);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// resource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod resource {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/LabelFilter
    #[derive(Debug, Serialize, Deserialize)]
    pub struct LabelFilter {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    impl IntoDto for LabelFilter {
        type Dto = dtos::resource::LabelFilter;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::LabelFilter> for LabelFilter {
        fn from(v: dtos::resource::LabelFilter) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<LabelFilter> for dtos::resource::LabelFilter {
        type Error = ValidationError;
        fn try_from(v: LabelFilter) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::resource::LabelFilter, LabelFilter);

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/Resource
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Resource<SpecT> {
        #[serde(rename = "$schema")]
        pub schema: TypeUri,
        pub headers: resource::ResourceHeaders,
        pub spec: SpecT,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub status: Option<resource::ResourceStatus>,
    }

    impl<SpecT> IntoDto for Resource<SpecT>
    where
        SpecT: IntoDto,
        <SpecT as IntoDto>::Dto: TryFrom<SpecT>,
        ValidationError: From<<<SpecT as IntoDto>::Dto as TryFrom<SpecT>>::Error>,
    {
        type Dto = dtos::resource::Resource<SpecT::Dto>;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl<SpecTFrom, SpecTTo> From<dtos::resource::Resource<SpecTFrom>> for Resource<SpecTTo>
    where
        SpecTTo: From<SpecTFrom>,
    {
        fn from(v: dtos::resource::Resource<SpecTFrom>) -> Self {
            Self {
                schema: v.schema,
                headers: v.headers.into(),
                spec: v.spec.into(),
                status: v.status.map(|v| v.into()),
            }
        }
    }

    impl<SpecTFrom, SpecTTo> TryFrom<Resource<SpecTFrom>> for dtos::resource::Resource<SpecTTo>
    where
        SpecTTo: TryFrom<SpecTFrom>,
        ValidationError: From<<SpecTTo as TryFrom<SpecTFrom>>::Error>,
    {
        type Error = ValidationError;
        fn try_from(v: Resource<SpecTFrom>) -> Result<Self, ValidationError> {
            Ok(Self {
                schema: v.schema,
                headers: dtos::resource::ResourceHeaders::try_from(v.headers)?,
                spec: SpecTTo::try_from(v.spec)?,
                status: v
                    .status
                    .map(|v| dtos::resource::ResourceStatus::try_from(v))
                    .transpose()?,
            })
        }
    }

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceAnnotations
    #[derive(Debug, Serialize, Deserialize)]
    pub struct ResourceAnnotations {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    impl IntoDto for ResourceAnnotations {
        type Dto = dtos::resource::ResourceAnnotations;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::ResourceAnnotations> for ResourceAnnotations {
        fn from(v: dtos::resource::ResourceAnnotations) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<ResourceAnnotations> for dtos::resource::ResourceAnnotations {
        type Error = ValidationError;
        fn try_from(v: ResourceAnnotations) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::resource::ResourceAnnotations, ResourceAnnotations);

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceConditions
    #[derive(Debug, Serialize, Deserialize)]
    pub struct ResourceConditions {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    impl IntoDto for ResourceConditions {
        type Dto = dtos::resource::ResourceConditions;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::ResourceConditions> for ResourceConditions {
        fn from(v: dtos::resource::ResourceConditions) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<ResourceConditions> for dtos::resource::ResourceConditions {
        type Error = ValidationError;
        fn try_from(v: ResourceConditions) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::resource::ResourceConditions, ResourceConditions);

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceHeaders
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceHeaders {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<ResourceID>,
        pub name: ResourceName,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub labels: Option<resource::ResourceLabels>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub annotations: Option<resource::ResourceAnnotations>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub generation: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub created_at: Option<DateTime<Utc>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub updated_at: Option<DateTime<Utc>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub deleted_at: Option<DateTime<Utc>>,
    }

    impl IntoDto for ResourceHeaders {
        type Dto = dtos::resource::ResourceHeaders;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::ResourceHeaders> for ResourceHeaders {
        fn from(v: dtos::resource::ResourceHeaders) -> Self {
            Self {
                id: v.id,
                name: v.name,
                account: v.account.map(|v| v.into()),
                labels: v.labels.map(|v| v.into()),
                annotations: v.annotations.map(|v| v.into()),
                generation: v.generation,
                created_at: v.created_at,
                updated_at: v.updated_at,
                deleted_at: v.deleted_at,
            }
        }
    }

    impl TryFrom<ResourceHeaders> for dtos::resource::ResourceHeaders {
        type Error = ValidationError;
        fn try_from(v: ResourceHeaders) -> Result<Self, ValidationError> {
            Ok(Self {
                id: v.id,
                name: v.name,
                account: v
                    .account
                    .map(|v| dtos::auth::AccountRef::try_from(v))
                    .transpose()?,
                labels: v
                    .labels
                    .map(|v| dtos::resource::ResourceLabels::try_from(v))
                    .transpose()?,
                annotations: v
                    .annotations
                    .map(|v| dtos::resource::ResourceAnnotations::try_from(v))
                    .transpose()?,
                generation: v.generation,
                created_at: v.created_at,
                updated_at: v.updated_at,
                deleted_at: v.deleted_at,
            })
        }
    }

    implement_serde_as!(dtos::resource::ResourceHeaders, ResourceHeaders);

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceLabels
    #[derive(Debug, Serialize, Deserialize)]
    pub struct ResourceLabels {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    impl IntoDto for ResourceLabels {
        type Dto = dtos::resource::ResourceLabels;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::ResourceLabels> for ResourceLabels {
        fn from(v: dtos::resource::ResourceLabels) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<ResourceLabels> for dtos::resource::ResourceLabels {
        type Error = ValidationError;
        fn try_from(v: ResourceLabels) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::resource::ResourceLabels, ResourceLabels);

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourcePhase
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum ResourcePhase {
        #[serde(alias = "pending")]
        Pending,
        #[serde(alias = "reconciling")]
        Reconciling,
        #[serde(alias = "ready")]
        Ready,
        #[serde(alias = "failed")]
        Failed,
    }

    impl IntoDto for ResourcePhase {
        type Dto = dtos::resource::ResourcePhase;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::ResourcePhase> for ResourcePhase {
        fn from(v: dtos::resource::ResourcePhase) -> Self {
            match v {
                dtos::resource::ResourcePhase::Pending => Self::Pending,
                dtos::resource::ResourcePhase::Reconciling => Self::Reconciling,
                dtos::resource::ResourcePhase::Ready => Self::Ready,
                dtos::resource::ResourcePhase::Failed => Self::Failed,
            }
        }
    }

    impl TryFrom<ResourcePhase> for dtos::resource::ResourcePhase {
        type Error = ValidationError;
        fn try_from(v: ResourcePhase) -> Result<Self, Self::Error> {
            match v {
                ResourcePhase::Pending => Ok(Self::Pending),
                ResourcePhase::Reconciling => Ok(Self::Reconciling),
                ResourcePhase::Ready => Ok(Self::Ready),
                ResourcePhase::Failed => Ok(Self::Failed),
            }
        }
    }

    implement_serde_as!(dtos::resource::ResourcePhase, ResourcePhase);

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceRef
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceRef {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        pub r#type: TypeRef,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<ResourceName>,
    }

    impl IntoDto for ResourceRef {
        type Dto = dtos::resource::ResourceRef;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::ResourceRef> for StructOrString<ResourceRef> {
        fn from(v: dtos::resource::ResourceRef) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<ResourceRef>> for dtos::resource::ResourceRef {
        type Error = ValidationError;
        fn try_from(v: StructOrString<ResourceRef>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    implement_serde_as!(dtos::resource::ResourceRef, ResourceRef);

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceSelector
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceSelector {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        pub r#type: TypeRef,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub labels: Option<resource::LabelFilter>,
    }

    impl IntoDto for ResourceSelector {
        type Dto = dtos::resource::ResourceSelector;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::ResourceSelector> for StructOrString<ResourceSelector> {
        fn from(v: dtos::resource::ResourceSelector) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<ResourceSelector>> for dtos::resource::ResourceSelector {
        type Error = ValidationError;
        fn try_from(v: StructOrString<ResourceSelector>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    implement_serde_as!(dtos::resource::ResourceSelector, ResourceSelector);

    // Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceStatus
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceStatus {
        pub phase: resource::ResourcePhase,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub observed_generation: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub reconciled_at: Option<DateTime<Utc>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub conditions: Option<resource::ResourceConditions>,
    }

    impl IntoDto for ResourceStatus {
        type Dto = dtos::resource::ResourceStatus;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resource::ResourceStatus> for ResourceStatus {
        fn from(v: dtos::resource::ResourceStatus) -> Self {
            Self {
                phase: v.phase.into(),
                observed_generation: v.observed_generation,
                reconciled_at: v.reconciled_at,
                conditions: v.conditions.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ResourceStatus> for dtos::resource::ResourceStatus {
        type Error = ValidationError;
        fn try_from(v: ResourceStatus) -> Result<Self, ValidationError> {
            Ok(Self {
                phase: dtos::resource::ResourcePhase::try_from(v.phase)?,
                observed_generation: v.observed_generation,
                reconciled_at: v.reconciled_at,
                conditions: v
                    .conditions
                    .map(|v| dtos::resource::ResourceConditions::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::resource::ResourceStatus, ResourceStatus);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// sink
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod sink {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct WebhookTargetSpec {
        pub url: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub secret: Option<StructOrString<config::Secret>>,
    }

    impl IntoDto for WebhookTargetSpec {
        type Dto = dtos::sink::WebhookTargetSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sink::WebhookTargetSpec> for WebhookTargetSpec {
        fn from(v: dtos::sink::WebhookTargetSpec) -> Self {
            Self {
                url: v.url,
                secret: v.secret.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<WebhookTargetSpec> for dtos::sink::WebhookTargetSpec {
        type Error = ValidationError;
        fn try_from(v: WebhookTargetSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                url: v.url,
                secret: v
                    .secret
                    .map(|v| dtos::config::Secret::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sink::WebhookTargetSpec, WebhookTargetSpec);

    // Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetStatus
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct WebhookTargetStatus {
        pub value: sink::WebhookTargetStatusValue,
    }

    impl IntoDto for WebhookTargetStatus {
        type Dto = dtos::sink::WebhookTargetStatus;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sink::WebhookTargetStatus> for WebhookTargetStatus {
        fn from(v: dtos::sink::WebhookTargetStatus) -> Self {
            Self {
                value: v.value.into(),
            }
        }
    }

    impl TryFrom<WebhookTargetStatus> for dtos::sink::WebhookTargetStatus {
        type Error = ValidationError;
        fn try_from(v: WebhookTargetStatus) -> Result<Self, ValidationError> {
            Ok(Self {
                value: dtos::sink::WebhookTargetStatusValue::try_from(v.value)?,
            })
        }
    }

    implement_serde_as!(dtos::sink::WebhookTargetStatus, WebhookTargetStatus);

    // Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetStatus#/$defs/Value
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum WebhookTargetStatusValue {
        #[serde(alias = "ready")]
        Ready,
        #[serde(alias = "failed")]
        Failed,
    }

    impl IntoDto for WebhookTargetStatusValue {
        type Dto = dtos::sink::WebhookTargetStatusValue;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sink::WebhookTargetStatusValue> for WebhookTargetStatusValue {
        fn from(v: dtos::sink::WebhookTargetStatusValue) -> Self {
            match v {
                dtos::sink::WebhookTargetStatusValue::Ready => Self::Ready,
                dtos::sink::WebhookTargetStatusValue::Failed => Self::Failed,
            }
        }
    }

    impl TryFrom<WebhookTargetStatusValue> for dtos::sink::WebhookTargetStatusValue {
        type Error = ValidationError;
        fn try_from(v: WebhookTargetStatusValue) -> Result<Self, Self::Error> {
            match v {
                WebhookTargetStatusValue::Ready => Ok(Self::Ready),
                WebhookTargetStatusValue::Failed => Ok(Self::Failed),
            }
        }
    }

    implement_serde_as!(
        dtos::sink::WebhookTargetStatusValue,
        WebhookTargetStatusValue
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// source
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod source {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/CompressionFormat
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum CompressionFormat {
        #[serde(alias = "gzip")]
        Gzip,
        #[serde(alias = "zip")]
        Zip,
    }

    impl IntoDto for CompressionFormat {
        type Dto = dtos::source::CompressionFormat;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::CompressionFormat> for CompressionFormat {
        fn from(v: dtos::source::CompressionFormat) -> Self {
            match v {
                dtos::source::CompressionFormat::Gzip => Self::Gzip,
                dtos::source::CompressionFormat::Zip => Self::Zip,
            }
        }
    }

    impl TryFrom<CompressionFormat> for dtos::source::CompressionFormat {
        type Error = ValidationError;
        fn try_from(v: CompressionFormat) -> Result<Self, Self::Error> {
            match v {
                CompressionFormat::Gzip => Ok(Self::Gzip),
                CompressionFormat::Zip => Ok(Self::Zip),
            }
        }
    }

    implement_serde_as!(dtos::source::CompressionFormat, CompressionFormat);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/EnvVar
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct EnvVar {
        pub name: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub value: Option<String>,
    }

    impl IntoDto for EnvVar {
        type Dto = dtos::source::EnvVar;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::EnvVar> for EnvVar {
        fn from(v: dtos::source::EnvVar) -> Self {
            Self {
                name: v.name,
                value: v.value,
            }
        }
    }

    impl TryFrom<EnvVar> for dtos::source::EnvVar {
        type Error = ValidationError;
        fn try_from(v: EnvVar) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                value: v.value,
            })
        }
    }

    implement_serde_as!(dtos::source::EnvVar, EnvVar);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum EventTimeSource {
        #[serde(alias = "fromMetadata", alias = "frommetadata")]
        FromMetadata(source::EventTimeSourceFromMetadata),
        #[serde(alias = "fromPath", alias = "frompath")]
        FromPath(source::EventTimeSourceFromPath),
        #[serde(alias = "fromSystemTime", alias = "fromsystemtime")]
        FromSystemTime(source::EventTimeSourceFromSystemTime),
    }

    impl From<dtos::source::EventTimeSource> for UnionOrString<EventTimeSource> {
        fn from(v: dtos::source::EventTimeSource) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<UnionOrString<EventTimeSource>> for dtos::source::EventTimeSource {
        type Error = ValidationError;
        fn try_from(v: UnionOrString<EventTimeSource>) -> Result<Self, Self::Error> {
            v.0.try_into()
        }
    }

    impl IntoDto for EventTimeSource {
        type Dto = dtos::source::EventTimeSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::EventTimeSource> for EventTimeSource {
        fn from(v: dtos::source::EventTimeSource) -> Self {
            match v {
                dtos::source::EventTimeSource::FromMetadata(v) => Self::FromMetadata(v.into()),
                dtos::source::EventTimeSource::FromPath(v) => Self::FromPath(v.into()),
                dtos::source::EventTimeSource::FromSystemTime(v) => Self::FromSystemTime(v.into()),
            }
        }
    }

    impl TryFrom<EventTimeSource> for dtos::source::EventTimeSource {
        type Error = ValidationError;
        fn try_from(v: EventTimeSource) -> Result<Self, Self::Error> {
            match v {
                EventTimeSource::FromMetadata(v) => Ok(Self::FromMetadata(v.try_into()?)),
                EventTimeSource::FromPath(v) => Ok(Self::FromPath(v.try_into()?)),
                EventTimeSource::FromSystemTime(v) => Ok(Self::FromSystemTime(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::source::EventTimeSource, EventTimeSource);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromMetadata
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct EventTimeSourceFromMetadata {}

    impl IntoDto for EventTimeSourceFromMetadata {
        type Dto = dtos::source::EventTimeSourceFromMetadata;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::EventTimeSourceFromMetadata> for EventTimeSourceFromMetadata {
        fn from(v: dtos::source::EventTimeSourceFromMetadata) -> Self {
            Self {}
        }
    }

    impl TryFrom<EventTimeSourceFromMetadata> for dtos::source::EventTimeSourceFromMetadata {
        type Error = ValidationError;
        fn try_from(v: EventTimeSourceFromMetadata) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::source::EventTimeSourceFromMetadata,
        EventTimeSourceFromMetadata
    );

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromPath
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct EventTimeSourceFromPath {
        pub pattern: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub timestamp_format: Option<String>,
    }

    impl IntoDto for EventTimeSourceFromPath {
        type Dto = dtos::source::EventTimeSourceFromPath;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::EventTimeSourceFromPath> for EventTimeSourceFromPath {
        fn from(v: dtos::source::EventTimeSourceFromPath) -> Self {
            Self {
                pattern: v.pattern,
                timestamp_format: v.timestamp_format,
            }
        }
    }

    impl TryFrom<EventTimeSourceFromPath> for dtos::source::EventTimeSourceFromPath {
        type Error = ValidationError;
        fn try_from(v: EventTimeSourceFromPath) -> Result<Self, ValidationError> {
            Ok(Self {
                pattern: v.pattern,
                timestamp_format: v.timestamp_format,
            })
        }
    }

    implement_serde_as!(
        dtos::source::EventTimeSourceFromPath,
        EventTimeSourceFromPath
    );

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromSystemTime
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct EventTimeSourceFromSystemTime {}

    impl IntoDto for EventTimeSourceFromSystemTime {
        type Dto = dtos::source::EventTimeSourceFromSystemTime;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::EventTimeSourceFromSystemTime> for EventTimeSourceFromSystemTime {
        fn from(v: dtos::source::EventTimeSourceFromSystemTime) -> Self {
            Self {}
        }
    }

    impl TryFrom<EventTimeSourceFromSystemTime> for dtos::source::EventTimeSourceFromSystemTime {
        type Error = ValidationError;
        fn try_from(v: EventTimeSourceFromSystemTime) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::source::EventTimeSourceFromSystemTime,
        EventTimeSourceFromSystemTime
    );

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngestParams
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngestParams {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub target_slice_records: Option<u64>,
    }

    impl IntoDto for IngestParams {
        type Dto = dtos::source::IngestParams;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngestParams> for IngestParams {
        fn from(v: dtos::source::IngestParams) -> Self {
            Self {
                target_slice_records: v.target_slice_records,
            }
        }
    }

    impl TryFrom<IngestParams> for dtos::source::IngestParams {
        type Error = ValidationError;
        fn try_from(v: IngestParams) -> Result<Self, ValidationError> {
            Ok(Self {
                target_slice_records: v.target_slice_records,
            })
        }
    }

    implement_serde_as!(dtos::source::IngestParams, IngestParams);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum Ingress {
        #[serde(alias = "url")]
        Url(source::IngressUrl),
        #[serde(alias = "filesGlob", alias = "filesglob")]
        FilesGlob(source::IngressFilesGlob),
        #[serde(alias = "container")]
        Container(source::IngressContainer),
        #[serde(alias = "mqtt")]
        Mqtt(source::IngressMqtt),
        #[serde(alias = "evmLogs", alias = "evmlogs")]
        EvmLogs(source::IngressEvmLogs),
        #[serde(alias = "restEndpoint", alias = "restendpoint")]
        RestEndpoint(source::IngressRestEndpoint),
    }

    impl IntoDto for Ingress {
        type Dto = dtos::source::Ingress;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::Ingress> for Ingress {
        fn from(v: dtos::source::Ingress) -> Self {
            match v {
                dtos::source::Ingress::Url(v) => Self::Url(v.into()),
                dtos::source::Ingress::FilesGlob(v) => Self::FilesGlob(v.into()),
                dtos::source::Ingress::Container(v) => Self::Container(v.into()),
                dtos::source::Ingress::Mqtt(v) => Self::Mqtt(v.into()),
                dtos::source::Ingress::EvmLogs(v) => Self::EvmLogs(v.into()),
                dtos::source::Ingress::RestEndpoint(v) => Self::RestEndpoint(v.into()),
            }
        }
    }

    impl TryFrom<Ingress> for dtos::source::Ingress {
        type Error = ValidationError;
        fn try_from(v: Ingress) -> Result<Self, Self::Error> {
            match v {
                Ingress::Url(v) => Ok(Self::Url(v.try_into()?)),
                Ingress::FilesGlob(v) => Ok(Self::FilesGlob(v.try_into()?)),
                Ingress::Container(v) => Ok(Self::Container(v.try_into()?)),
                Ingress::Mqtt(v) => Ok(Self::Mqtt(v.try_into()?)),
                Ingress::EvmLogs(v) => Ok(Self::EvmLogs(v.try_into()?)),
                Ingress::RestEndpoint(v) => Ok(Self::RestEndpoint(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::source::Ingress, Ingress);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngressBuffer
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum IngressBuffer {
        #[serde(alias = "memory")]
        Memory(source::IngressBufferMemory),
    }

    impl IntoDto for IngressBuffer {
        type Dto = dtos::source::IngressBuffer;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngressBuffer> for IngressBuffer {
        fn from(v: dtos::source::IngressBuffer) -> Self {
            match v {
                dtos::source::IngressBuffer::Memory(v) => Self::Memory(v.into()),
            }
        }
    }

    impl TryFrom<IngressBuffer> for dtos::source::IngressBuffer {
        type Error = ValidationError;
        fn try_from(v: IngressBuffer) -> Result<Self, Self::Error> {
            match v {
                IngressBuffer::Memory(v) => Ok(Self::Memory(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::source::IngressBuffer, IngressBuffer);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngressBuffer#/$defs/Memory
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressBufferMemory {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub buffer_size: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub overflow_policy: Option<String>,
    }

    impl IntoDto for IngressBufferMemory {
        type Dto = dtos::source::IngressBufferMemory;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngressBufferMemory> for IngressBufferMemory {
        fn from(v: dtos::source::IngressBufferMemory) -> Self {
            Self {
                buffer_size: v.buffer_size,
                overflow_policy: v.overflow_policy,
            }
        }
    }

    impl TryFrom<IngressBufferMemory> for dtos::source::IngressBufferMemory {
        type Error = ValidationError;
        fn try_from(v: IngressBufferMemory) -> Result<Self, ValidationError> {
            Ok(Self {
                buffer_size: v.buffer_size,
                overflow_policy: v.overflow_policy,
            })
        }
    }

    implement_serde_as!(dtos::source::IngressBufferMemory, IngressBufferMemory);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Container
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressContainer {
        pub image: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub command: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub args: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub env: Option<Vec<source::EnvVar>>,
    }

    impl IntoDto for IngressContainer {
        type Dto = dtos::source::IngressContainer;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngressContainer> for IngressContainer {
        fn from(v: dtos::source::IngressContainer) -> Self {
            Self {
                image: v.image,
                command: v.command,
                args: v.args,
                env: v.env.map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<IngressContainer> for dtos::source::IngressContainer {
        type Error = ValidationError;
        fn try_from(v: IngressContainer) -> Result<Self, ValidationError> {
            Ok(Self {
                image: v.image,
                command: v.command,
                args: v.args,
                env: v
                    .env
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::source::EnvVar::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::IngressContainer, IngressContainer);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/EvmLogs
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressEvmLogs {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub chain_id: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub node_url: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub filter: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub signature: Option<String>,
    }

    impl IntoDto for IngressEvmLogs {
        type Dto = dtos::source::IngressEvmLogs;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngressEvmLogs> for IngressEvmLogs {
        fn from(v: dtos::source::IngressEvmLogs) -> Self {
            Self {
                chain_id: v.chain_id,
                node_url: v.node_url,
                filter: v.filter,
                signature: v.signature,
            }
        }
    }

    impl TryFrom<IngressEvmLogs> for dtos::source::IngressEvmLogs {
        type Error = ValidationError;
        fn try_from(v: IngressEvmLogs) -> Result<Self, ValidationError> {
            Ok(Self {
                chain_id: v.chain_id,
                node_url: v.node_url,
                filter: v.filter,
                signature: v.signature,
            })
        }
    }

    implement_serde_as!(dtos::source::IngressEvmLogs, IngressEvmLogs);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/FilesGlob
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressFilesGlob {
        pub path: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_time: Option<UnionOrString<source::EventTimeSource>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cache: Option<UnionOrString<source::SourceCaching>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub order: Option<source::SourceOrdering>,
    }

    impl IntoDto for IngressFilesGlob {
        type Dto = dtos::source::IngressFilesGlob;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngressFilesGlob> for IngressFilesGlob {
        fn from(v: dtos::source::IngressFilesGlob) -> Self {
            Self {
                path: v.path,
                event_time: v.event_time.map(|v| v.into()),
                cache: v.cache.map(|v| v.into()),
                order: v.order.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<IngressFilesGlob> for dtos::source::IngressFilesGlob {
        type Error = ValidationError;
        fn try_from(v: IngressFilesGlob) -> Result<Self, ValidationError> {
            Ok(Self {
                path: v.path,
                event_time: v
                    .event_time
                    .map(|v| dtos::source::EventTimeSource::try_from(v))
                    .transpose()?,
                cache: v
                    .cache
                    .map(|v| dtos::source::SourceCaching::try_from(v))
                    .transpose()?,
                order: v
                    .order
                    .map(|v| dtos::source::SourceOrdering::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::IngressFilesGlob, IngressFilesGlob);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Mqtt
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressMqtt {
        pub host: String,
        pub port: i32,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub username: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub password: Option<String>,
        pub topics: Vec<source::MqttTopicSubscription>,
    }

    impl IntoDto for IngressMqtt {
        type Dto = dtos::source::IngressMqtt;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngressMqtt> for IngressMqtt {
        fn from(v: dtos::source::IngressMqtt) -> Self {
            Self {
                host: v.host,
                port: v.port,
                username: v.username,
                password: v.password,
                topics: v.topics.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<IngressMqtt> for dtos::source::IngressMqtt {
        type Error = ValidationError;
        fn try_from(v: IngressMqtt) -> Result<Self, ValidationError> {
            Ok(Self {
                host: v.host,
                port: v.port,
                username: v.username,
                password: v.password,
                topics: v
                    .topics
                    .into_iter()
                    .map(|i| dtos::source::MqttTopicSubscription::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::source::IngressMqtt, IngressMqtt);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/RestEndpoint
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressRestEndpoint {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub buffer: Option<source::IngressBuffer>,
    }

    impl IntoDto for IngressRestEndpoint {
        type Dto = dtos::source::IngressRestEndpoint;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngressRestEndpoint> for IngressRestEndpoint {
        fn from(v: dtos::source::IngressRestEndpoint) -> Self {
            Self {
                buffer: v.buffer.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<IngressRestEndpoint> for dtos::source::IngressRestEndpoint {
        type Error = ValidationError;
        fn try_from(v: IngressRestEndpoint) -> Result<Self, ValidationError> {
            Ok(Self {
                buffer: v
                    .buffer
                    .map(|v| dtos::source::IngressBuffer::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::IngressRestEndpoint, IngressRestEndpoint);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Url
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressUrl {
        pub url: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_time: Option<UnionOrString<source::EventTimeSource>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cache: Option<UnionOrString<source::SourceCaching>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub headers: Option<Vec<source::RequestHeader>>,
    }

    impl IntoDto for IngressUrl {
        type Dto = dtos::source::IngressUrl;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::IngressUrl> for IngressUrl {
        fn from(v: dtos::source::IngressUrl) -> Self {
            Self {
                url: v.url,
                event_time: v.event_time.map(|v| v.into()),
                cache: v.cache.map(|v| v.into()),
                headers: v.headers.map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<IngressUrl> for dtos::source::IngressUrl {
        type Error = ValidationError;
        fn try_from(v: IngressUrl) -> Result<Self, ValidationError> {
            Ok(Self {
                url: v.url,
                event_time: v
                    .event_time
                    .map(|v| dtos::source::EventTimeSource::try_from(v))
                    .transpose()?,
                cache: v
                    .cache
                    .map(|v| dtos::source::SourceCaching::try_from(v))
                    .transpose()?,
                headers: v
                    .headers
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::source::RequestHeader::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::IngressUrl, IngressUrl);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum MergeStrategy {
        #[serde(alias = "append")]
        Append(source::MergeStrategyAppend),
        #[serde(alias = "ledger")]
        Ledger(source::MergeStrategyLedger),
        #[serde(alias = "snapshot")]
        Snapshot(source::MergeStrategySnapshot),
        #[serde(alias = "changelogStream", alias = "changelogstream")]
        ChangelogStream(source::MergeStrategyChangelogStream),
        #[serde(alias = "upsertStream", alias = "upsertstream")]
        UpsertStream(source::MergeStrategyUpsertStream),
    }

    impl IntoDto for MergeStrategy {
        type Dto = dtos::source::MergeStrategy;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::MergeStrategy> for MergeStrategy {
        fn from(v: dtos::source::MergeStrategy) -> Self {
            match v {
                dtos::source::MergeStrategy::Append(v) => Self::Append(v.into()),
                dtos::source::MergeStrategy::Ledger(v) => Self::Ledger(v.into()),
                dtos::source::MergeStrategy::Snapshot(v) => Self::Snapshot(v.into()),
                dtos::source::MergeStrategy::ChangelogStream(v) => Self::ChangelogStream(v.into()),
                dtos::source::MergeStrategy::UpsertStream(v) => Self::UpsertStream(v.into()),
            }
        }
    }

    impl TryFrom<MergeStrategy> for dtos::source::MergeStrategy {
        type Error = ValidationError;
        fn try_from(v: MergeStrategy) -> Result<Self, Self::Error> {
            match v {
                MergeStrategy::Append(v) => Ok(Self::Append(v.try_into()?)),
                MergeStrategy::Ledger(v) => Ok(Self::Ledger(v.try_into()?)),
                MergeStrategy::Snapshot(v) => Ok(Self::Snapshot(v.try_into()?)),
                MergeStrategy::ChangelogStream(v) => Ok(Self::ChangelogStream(v.try_into()?)),
                MergeStrategy::UpsertStream(v) => Ok(Self::UpsertStream(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::source::MergeStrategy, MergeStrategy);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Append
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategyAppend {}

    impl IntoDto for MergeStrategyAppend {
        type Dto = dtos::source::MergeStrategyAppend;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::MergeStrategyAppend> for MergeStrategyAppend {
        fn from(v: dtos::source::MergeStrategyAppend) -> Self {
            Self {}
        }
    }

    impl TryFrom<MergeStrategyAppend> for dtos::source::MergeStrategyAppend {
        type Error = ValidationError;
        fn try_from(v: MergeStrategyAppend) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::source::MergeStrategyAppend, MergeStrategyAppend);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/ChangelogStream
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategyChangelogStream {
        pub primary_key: Vec<String>,
    }

    impl IntoDto for MergeStrategyChangelogStream {
        type Dto = dtos::source::MergeStrategyChangelogStream;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::MergeStrategyChangelogStream> for MergeStrategyChangelogStream {
        fn from(v: dtos::source::MergeStrategyChangelogStream) -> Self {
            Self {
                primary_key: v.primary_key,
            }
        }
    }

    impl TryFrom<MergeStrategyChangelogStream> for dtos::source::MergeStrategyChangelogStream {
        type Error = ValidationError;
        fn try_from(v: MergeStrategyChangelogStream) -> Result<Self, ValidationError> {
            Ok(Self {
                primary_key: v.primary_key,
            })
        }
    }

    implement_serde_as!(
        dtos::source::MergeStrategyChangelogStream,
        MergeStrategyChangelogStream
    );

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Ledger
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategyLedger {
        pub primary_key: Vec<String>,
    }

    impl IntoDto for MergeStrategyLedger {
        type Dto = dtos::source::MergeStrategyLedger;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::MergeStrategyLedger> for MergeStrategyLedger {
        fn from(v: dtos::source::MergeStrategyLedger) -> Self {
            Self {
                primary_key: v.primary_key,
            }
        }
    }

    impl TryFrom<MergeStrategyLedger> for dtos::source::MergeStrategyLedger {
        type Error = ValidationError;
        fn try_from(v: MergeStrategyLedger) -> Result<Self, ValidationError> {
            Ok(Self {
                primary_key: v.primary_key,
            })
        }
    }

    implement_serde_as!(dtos::source::MergeStrategyLedger, MergeStrategyLedger);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Snapshot
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategySnapshot {
        pub primary_key: Vec<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub compare_columns: Option<Vec<String>>,
    }

    impl IntoDto for MergeStrategySnapshot {
        type Dto = dtos::source::MergeStrategySnapshot;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::MergeStrategySnapshot> for MergeStrategySnapshot {
        fn from(v: dtos::source::MergeStrategySnapshot) -> Self {
            Self {
                primary_key: v.primary_key,
                compare_columns: v.compare_columns,
            }
        }
    }

    impl TryFrom<MergeStrategySnapshot> for dtos::source::MergeStrategySnapshot {
        type Error = ValidationError;
        fn try_from(v: MergeStrategySnapshot) -> Result<Self, ValidationError> {
            Ok(Self {
                primary_key: v.primary_key,
                compare_columns: v.compare_columns,
            })
        }
    }

    implement_serde_as!(dtos::source::MergeStrategySnapshot, MergeStrategySnapshot);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/UpsertStream
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategyUpsertStream {
        pub primary_key: Vec<String>,
    }

    impl IntoDto for MergeStrategyUpsertStream {
        type Dto = dtos::source::MergeStrategyUpsertStream;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::MergeStrategyUpsertStream> for MergeStrategyUpsertStream {
        fn from(v: dtos::source::MergeStrategyUpsertStream) -> Self {
            Self {
                primary_key: v.primary_key,
            }
        }
    }

    impl TryFrom<MergeStrategyUpsertStream> for dtos::source::MergeStrategyUpsertStream {
        type Error = ValidationError;
        fn try_from(v: MergeStrategyUpsertStream) -> Result<Self, ValidationError> {
            Ok(Self {
                primary_key: v.primary_key,
            })
        }
    }

    implement_serde_as!(
        dtos::source::MergeStrategyUpsertStream,
        MergeStrategyUpsertStream
    );

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/MqttQos
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum MqttQos {
        #[serde(alias = "atMostOnce", alias = "atmostonce")]
        AtMostOnce,
        #[serde(alias = "atLeastOnce", alias = "atleastonce")]
        AtLeastOnce,
        #[serde(alias = "exactlyOnce", alias = "exactlyonce")]
        ExactlyOnce,
    }

    impl IntoDto for MqttQos {
        type Dto = dtos::source::MqttQos;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::MqttQos> for MqttQos {
        fn from(v: dtos::source::MqttQos) -> Self {
            match v {
                dtos::source::MqttQos::AtMostOnce => Self::AtMostOnce,
                dtos::source::MqttQos::AtLeastOnce => Self::AtLeastOnce,
                dtos::source::MqttQos::ExactlyOnce => Self::ExactlyOnce,
            }
        }
    }

    impl TryFrom<MqttQos> for dtos::source::MqttQos {
        type Error = ValidationError;
        fn try_from(v: MqttQos) -> Result<Self, Self::Error> {
            match v {
                MqttQos::AtMostOnce => Ok(Self::AtMostOnce),
                MqttQos::AtLeastOnce => Ok(Self::AtLeastOnce),
                MqttQos::ExactlyOnce => Ok(Self::ExactlyOnce),
            }
        }
    }

    implement_serde_as!(dtos::source::MqttQos, MqttQos);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/MqttTopicSubscription
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MqttTopicSubscription {
        pub path: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub qos: Option<source::MqttQos>,
    }

    impl IntoDto for MqttTopicSubscription {
        type Dto = dtos::source::MqttTopicSubscription;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::MqttTopicSubscription> for MqttTopicSubscription {
        fn from(v: dtos::source::MqttTopicSubscription) -> Self {
            Self {
                path: v.path,
                qos: v.qos.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<MqttTopicSubscription> for dtos::source::MqttTopicSubscription {
        type Error = ValidationError;
        fn try_from(v: MqttTopicSubscription) -> Result<Self, ValidationError> {
            Ok(Self {
                path: v.path,
                qos: v
                    .qos
                    .map(|v| dtos::source::MqttQos::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::MqttTopicSubscription, MqttTopicSubscription);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum PrepStep {
        #[serde(alias = "decompress")]
        Decompress(source::PrepStepDecompress),
        #[serde(alias = "pipe")]
        Pipe(source::PrepStepPipe),
    }

    impl IntoDto for PrepStep {
        type Dto = dtos::source::PrepStep;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::PrepStep> for PrepStep {
        fn from(v: dtos::source::PrepStep) -> Self {
            match v {
                dtos::source::PrepStep::Decompress(v) => Self::Decompress(v.into()),
                dtos::source::PrepStep::Pipe(v) => Self::Pipe(v.into()),
            }
        }
    }

    impl TryFrom<PrepStep> for dtos::source::PrepStep {
        type Error = ValidationError;
        fn try_from(v: PrepStep) -> Result<Self, Self::Error> {
            match v {
                PrepStep::Decompress(v) => Ok(Self::Decompress(v.try_into()?)),
                PrepStep::Pipe(v) => Ok(Self::Pipe(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::source::PrepStep, PrepStep);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep#/$defs/Decompress
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct PrepStepDecompress {
        pub format: source::CompressionFormat,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub sub_path: Option<String>,
    }

    impl IntoDto for PrepStepDecompress {
        type Dto = dtos::source::PrepStepDecompress;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::PrepStepDecompress> for PrepStepDecompress {
        fn from(v: dtos::source::PrepStepDecompress) -> Self {
            Self {
                format: v.format.into(),
                sub_path: v.sub_path,
            }
        }
    }

    impl TryFrom<PrepStepDecompress> for dtos::source::PrepStepDecompress {
        type Error = ValidationError;
        fn try_from(v: PrepStepDecompress) -> Result<Self, ValidationError> {
            Ok(Self {
                format: dtos::source::CompressionFormat::try_from(v.format)?,
                sub_path: v.sub_path,
            })
        }
    }

    implement_serde_as!(dtos::source::PrepStepDecompress, PrepStepDecompress);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep#/$defs/Pipe
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct PrepStepPipe {
        pub command: Vec<String>,
    }

    impl IntoDto for PrepStepPipe {
        type Dto = dtos::source::PrepStepPipe;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::PrepStepPipe> for PrepStepPipe {
        fn from(v: dtos::source::PrepStepPipe) -> Self {
            Self { command: v.command }
        }
    }

    impl TryFrom<PrepStepPipe> for dtos::source::PrepStepPipe {
        type Error = ValidationError;
        fn try_from(v: PrepStepPipe) -> Result<Self, ValidationError> {
            Ok(Self { command: v.command })
        }
    }

    implement_serde_as!(dtos::source::PrepStepPipe, PrepStepPipe);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum ReadStep {
        #[serde(alias = "csv")]
        Csv(source::ReadStepCsv),
        #[serde(alias = "geoJson", alias = "geojson")]
        GeoJson(source::ReadStepGeoJson),
        #[serde(alias = "esriShapefile", alias = "esrishapefile")]
        EsriShapefile(source::ReadStepEsriShapefile),
        #[serde(alias = "parquet")]
        Parquet(source::ReadStepParquet),
        #[serde(alias = "json")]
        Json(source::ReadStepJson),
        #[serde(alias = "ndJson", alias = "ndjson")]
        NdJson(source::ReadStepNdJson),
        #[serde(alias = "ndGeoJson", alias = "ndgeojson")]
        NdGeoJson(source::ReadStepNdGeoJson),
    }

    impl IntoDto for ReadStep {
        type Dto = dtos::source::ReadStep;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::ReadStep> for ReadStep {
        fn from(v: dtos::source::ReadStep) -> Self {
            match v {
                dtos::source::ReadStep::Csv(v) => Self::Csv(v.into()),
                dtos::source::ReadStep::GeoJson(v) => Self::GeoJson(v.into()),
                dtos::source::ReadStep::EsriShapefile(v) => Self::EsriShapefile(v.into()),
                dtos::source::ReadStep::Parquet(v) => Self::Parquet(v.into()),
                dtos::source::ReadStep::Json(v) => Self::Json(v.into()),
                dtos::source::ReadStep::NdJson(v) => Self::NdJson(v.into()),
                dtos::source::ReadStep::NdGeoJson(v) => Self::NdGeoJson(v.into()),
            }
        }
    }

    impl TryFrom<ReadStep> for dtos::source::ReadStep {
        type Error = ValidationError;
        fn try_from(v: ReadStep) -> Result<Self, Self::Error> {
            match v {
                ReadStep::Csv(v) => Ok(Self::Csv(v.try_into()?)),
                ReadStep::GeoJson(v) => Ok(Self::GeoJson(v.try_into()?)),
                ReadStep::EsriShapefile(v) => Ok(Self::EsriShapefile(v.try_into()?)),
                ReadStep::Parquet(v) => Ok(Self::Parquet(v.try_into()?)),
                ReadStep::Json(v) => Ok(Self::Json(v.try_into()?)),
                ReadStep::NdJson(v) => Ok(Self::NdJson(v.try_into()?)),
                ReadStep::NdGeoJson(v) => Ok(Self::NdGeoJson(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::source::ReadStep, ReadStep);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Csv
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ReadStepCsv {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ddl_schema: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub separator: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub encoding: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub quote: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub escape: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub header: Option<bool>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub infer_schema: Option<bool>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub null_value: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub date_format: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub timestamp_format: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<data::DataSchema>,
    }

    impl IntoDto for ReadStepCsv {
        type Dto = dtos::source::ReadStepCsv;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::ReadStepCsv> for ReadStepCsv {
        fn from(v: dtos::source::ReadStepCsv) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                separator: v.separator,
                encoding: v.encoding,
                quote: v.quote,
                escape: v.escape,
                header: v.header,
                infer_schema: v.infer_schema,
                null_value: v.null_value,
                date_format: v.date_format,
                timestamp_format: v.timestamp_format,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepCsv> for dtos::source::ReadStepCsv {
        type Error = ValidationError;
        fn try_from(v: ReadStepCsv) -> Result<Self, ValidationError> {
            Ok(Self {
                ddl_schema: v.ddl_schema,
                separator: v.separator,
                encoding: v.encoding,
                quote: v.quote,
                escape: v.escape,
                header: v.header,
                infer_schema: v.infer_schema,
                null_value: v.null_value,
                date_format: v.date_format,
                timestamp_format: v.timestamp_format,
                schema: v
                    .schema
                    .map(|v| dtos::data::DataSchema::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::ReadStepCsv, ReadStepCsv);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/EsriShapefile
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ReadStepEsriShapefile {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ddl_schema: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub sub_path: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<data::DataSchema>,
    }

    impl IntoDto for ReadStepEsriShapefile {
        type Dto = dtos::source::ReadStepEsriShapefile;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::ReadStepEsriShapefile> for ReadStepEsriShapefile {
        fn from(v: dtos::source::ReadStepEsriShapefile) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                sub_path: v.sub_path,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepEsriShapefile> for dtos::source::ReadStepEsriShapefile {
        type Error = ValidationError;
        fn try_from(v: ReadStepEsriShapefile) -> Result<Self, ValidationError> {
            Ok(Self {
                ddl_schema: v.ddl_schema,
                sub_path: v.sub_path,
                schema: v
                    .schema
                    .map(|v| dtos::data::DataSchema::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::ReadStepEsriShapefile, ReadStepEsriShapefile);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/GeoJson
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ReadStepGeoJson {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ddl_schema: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<data::DataSchema>,
    }

    impl IntoDto for ReadStepGeoJson {
        type Dto = dtos::source::ReadStepGeoJson;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::ReadStepGeoJson> for ReadStepGeoJson {
        fn from(v: dtos::source::ReadStepGeoJson) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepGeoJson> for dtos::source::ReadStepGeoJson {
        type Error = ValidationError;
        fn try_from(v: ReadStepGeoJson) -> Result<Self, ValidationError> {
            Ok(Self {
                ddl_schema: v.ddl_schema,
                schema: v
                    .schema
                    .map(|v| dtos::data::DataSchema::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::ReadStepGeoJson, ReadStepGeoJson);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Json
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ReadStepJson {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub sub_path: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ddl_schema: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub date_format: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub encoding: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub timestamp_format: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<data::DataSchema>,
    }

    impl IntoDto for ReadStepJson {
        type Dto = dtos::source::ReadStepJson;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::ReadStepJson> for ReadStepJson {
        fn from(v: dtos::source::ReadStepJson) -> Self {
            Self {
                sub_path: v.sub_path,
                ddl_schema: v.ddl_schema,
                date_format: v.date_format,
                encoding: v.encoding,
                timestamp_format: v.timestamp_format,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepJson> for dtos::source::ReadStepJson {
        type Error = ValidationError;
        fn try_from(v: ReadStepJson) -> Result<Self, ValidationError> {
            Ok(Self {
                sub_path: v.sub_path,
                ddl_schema: v.ddl_schema,
                date_format: v.date_format,
                encoding: v.encoding,
                timestamp_format: v.timestamp_format,
                schema: v
                    .schema
                    .map(|v| dtos::data::DataSchema::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::ReadStepJson, ReadStepJson);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/NdGeoJson
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ReadStepNdGeoJson {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ddl_schema: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<data::DataSchema>,
    }

    impl IntoDto for ReadStepNdGeoJson {
        type Dto = dtos::source::ReadStepNdGeoJson;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::ReadStepNdGeoJson> for ReadStepNdGeoJson {
        fn from(v: dtos::source::ReadStepNdGeoJson) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepNdGeoJson> for dtos::source::ReadStepNdGeoJson {
        type Error = ValidationError;
        fn try_from(v: ReadStepNdGeoJson) -> Result<Self, ValidationError> {
            Ok(Self {
                ddl_schema: v.ddl_schema,
                schema: v
                    .schema
                    .map(|v| dtos::data::DataSchema::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::ReadStepNdGeoJson, ReadStepNdGeoJson);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/NdJson
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ReadStepNdJson {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ddl_schema: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub date_format: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub encoding: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub timestamp_format: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<data::DataSchema>,
    }

    impl IntoDto for ReadStepNdJson {
        type Dto = dtos::source::ReadStepNdJson;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::ReadStepNdJson> for ReadStepNdJson {
        fn from(v: dtos::source::ReadStepNdJson) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                date_format: v.date_format,
                encoding: v.encoding,
                timestamp_format: v.timestamp_format,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepNdJson> for dtos::source::ReadStepNdJson {
        type Error = ValidationError;
        fn try_from(v: ReadStepNdJson) -> Result<Self, ValidationError> {
            Ok(Self {
                ddl_schema: v.ddl_schema,
                date_format: v.date_format,
                encoding: v.encoding,
                timestamp_format: v.timestamp_format,
                schema: v
                    .schema
                    .map(|v| dtos::data::DataSchema::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::ReadStepNdJson, ReadStepNdJson);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Parquet
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ReadStepParquet {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ddl_schema: Option<Vec<String>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<data::DataSchema>,
    }

    impl IntoDto for ReadStepParquet {
        type Dto = dtos::source::ReadStepParquet;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::ReadStepParquet> for ReadStepParquet {
        fn from(v: dtos::source::ReadStepParquet) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepParquet> for dtos::source::ReadStepParquet {
        type Error = ValidationError;
        fn try_from(v: ReadStepParquet) -> Result<Self, ValidationError> {
            Ok(Self {
                ddl_schema: v.ddl_schema,
                schema: v
                    .schema
                    .map(|v| dtos::data::DataSchema::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::ReadStepParquet, ReadStepParquet);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/RequestHeader
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RequestHeader {
        pub name: String,
        pub value: String,
    }

    impl IntoDto for RequestHeader {
        type Dto = dtos::source::RequestHeader;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::RequestHeader> for RequestHeader {
        fn from(v: dtos::source::RequestHeader) -> Self {
            Self {
                name: v.name,
                value: v.value,
            }
        }
    }

    impl TryFrom<RequestHeader> for dtos::source::RequestHeader {
        type Error = ValidationError;
        fn try_from(v: RequestHeader) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                value: v.value,
            })
        }
    }

    implement_serde_as!(dtos::source::RequestHeader, RequestHeader);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceCaching
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum SourceCaching {
        #[serde(alias = "forever")]
        Forever(source::SourceCachingForever),
    }

    impl From<dtos::source::SourceCaching> for UnionOrString<SourceCaching> {
        fn from(v: dtos::source::SourceCaching) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<UnionOrString<SourceCaching>> for dtos::source::SourceCaching {
        type Error = ValidationError;
        fn try_from(v: UnionOrString<SourceCaching>) -> Result<Self, Self::Error> {
            v.0.try_into()
        }
    }

    impl IntoDto for SourceCaching {
        type Dto = dtos::source::SourceCaching;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::SourceCaching> for SourceCaching {
        fn from(v: dtos::source::SourceCaching) -> Self {
            match v {
                dtos::source::SourceCaching::Forever(v) => Self::Forever(v.into()),
            }
        }
    }

    impl TryFrom<SourceCaching> for dtos::source::SourceCaching {
        type Error = ValidationError;
        fn try_from(v: SourceCaching) -> Result<Self, Self::Error> {
            match v {
                SourceCaching::Forever(v) => Ok(Self::Forever(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::source::SourceCaching, SourceCaching);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceCaching#/$defs/Forever
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SourceCachingForever {}

    impl IntoDto for SourceCachingForever {
        type Dto = dtos::source::SourceCachingForever;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::SourceCachingForever> for SourceCachingForever {
        fn from(v: dtos::source::SourceCachingForever) -> Self {
            Self {}
        }
    }

    impl TryFrom<SourceCachingForever> for dtos::source::SourceCachingForever {
        type Error = ValidationError;
        fn try_from(v: SourceCachingForever) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::source::SourceCachingForever, SourceCachingForever);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceOrdering
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum SourceOrdering {
        #[serde(alias = "byEventTime", alias = "byeventtime")]
        ByEventTime,
        #[serde(alias = "byName", alias = "byname")]
        ByName,
    }

    impl IntoDto for SourceOrdering {
        type Dto = dtos::source::SourceOrdering;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::SourceOrdering> for SourceOrdering {
        fn from(v: dtos::source::SourceOrdering) -> Self {
            match v {
                dtos::source::SourceOrdering::ByEventTime => Self::ByEventTime,
                dtos::source::SourceOrdering::ByName => Self::ByName,
            }
        }
    }

    impl TryFrom<SourceOrdering> for dtos::source::SourceOrdering {
        type Error = ValidationError;
        fn try_from(v: SourceOrdering) -> Result<Self, Self::Error> {
            match v {
                SourceOrdering::ByEventTime => Ok(Self::ByEventTime),
                SourceOrdering::ByName => Ok(Self::ByName),
            }
        }
    }

    implement_serde_as!(dtos::source::SourceOrdering, SourceOrdering);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SourceSpec {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub config: Option<config::ValueRefs>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ingress: Option<source::Ingress>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prepare: Option<Vec<source::PrepStep>>,
        pub read: source::ReadStep,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub preprocess: Option<dataset::Transform>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub merge: Option<source::MergeStrategy>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vocab: Option<dataset::DatasetVocabulary>,
    }

    impl IntoDto for SourceSpec {
        type Dto = dtos::source::SourceSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::SourceSpec> for SourceSpec {
        fn from(v: dtos::source::SourceSpec) -> Self {
            Self {
                config: v.config.map(|v| v.into()),
                ingress: v.ingress.map(|v| v.into()),
                prepare: v.prepare.map(|v| v.into_iter().map(Into::into).collect()),
                read: v.read.into(),
                preprocess: v.preprocess.map(|v| v.into()),
                merge: v.merge.map(|v| v.into()),
                vocab: v.vocab.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<SourceSpec> for dtos::source::SourceSpec {
        type Error = ValidationError;
        fn try_from(v: SourceSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                config: v
                    .config
                    .map(|v| dtos::config::ValueRefs::try_from(v))
                    .transpose()?,
                ingress: v
                    .ingress
                    .map(|v| dtos::source::Ingress::try_from(v))
                    .transpose()?,
                prepare: v
                    .prepare
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::source::PrepStep::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                read: dtos::source::ReadStep::try_from(v.read)?,
                preprocess: v
                    .preprocess
                    .map(|v| dtos::dataset::Transform::try_from(v))
                    .transpose()?,
                merge: v
                    .merge
                    .map(|v| dtos::source::MergeStrategy::try_from(v))
                    .transpose()?,
                vocab: v
                    .vocab
                    .map(|v| dtos::dataset::DatasetVocabulary::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::source::SourceSpec, SourceSpec);

    // Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceState
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SourceState {
        pub source_name: String,
        pub kind: String,
        pub value: String,
    }

    impl IntoDto for SourceState {
        type Dto = dtos::source::SourceState;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::source::SourceState> for SourceState {
        fn from(v: dtos::source::SourceState) -> Self {
            Self {
                source_name: v.source_name,
                kind: v.kind,
                value: v.value,
            }
        }
    }

    impl TryFrom<SourceState> for dtos::source::SourceState {
        type Error = ValidationError;
        fn try_from(v: SourceState) -> Result<Self, ValidationError> {
            Ok(Self {
                source_name: v.source_name,
                kind: v.kind,
                value: v.value,
            })
        }
    }

    implement_serde_as!(dtos::source::SourceState, SourceState);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// storage
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod storage {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/storage/v1alpha1/AwsCredentials
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AwsCredentials {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub access_key: Option<StructOrString<config::ValueRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub secret_key: Option<StructOrString<config::ValueRef>>,
    }

    impl IntoDto for AwsCredentials {
        type Dto = dtos::storage::AwsCredentials;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::storage::AwsCredentials> for AwsCredentials {
        fn from(v: dtos::storage::AwsCredentials) -> Self {
            Self {
                access_key: v.access_key.map(|v| v.into()),
                secret_key: v.secret_key.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<AwsCredentials> for dtos::storage::AwsCredentials {
        type Error = ValidationError;
        fn try_from(v: AwsCredentials) -> Result<Self, ValidationError> {
            Ok(Self {
                access_key: v
                    .access_key
                    .map(|v| dtos::config::ValueRef::try_from(v))
                    .transpose()?,
                secret_key: v
                    .secret_key
                    .map(|v| dtos::config::ValueRef::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::storage::AwsCredentials, AwsCredentials);

    // Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeRef
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct PersistentVolumeRef {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<ResourceName>,
    }

    impl IntoDto for PersistentVolumeRef {
        type Dto = dtos::storage::PersistentVolumeRef;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::storage::PersistentVolumeRef> for StructOrString<PersistentVolumeRef> {
        fn from(v: dtos::storage::PersistentVolumeRef) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<PersistentVolumeRef>> for dtos::storage::PersistentVolumeRef {
        type Error = ValidationError;
        fn try_from(v: StructOrString<PersistentVolumeRef>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    implement_serde_as!(dtos::storage::PersistentVolumeRef, PersistentVolumeRef);

    // Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum PersistentVolumeSpec {
        #[serde(alias = "s3")]
        S3(storage::PersistentVolumeSpecS3),
    }

    impl IntoDto for PersistentVolumeSpec {
        type Dto = dtos::storage::PersistentVolumeSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::storage::PersistentVolumeSpec> for PersistentVolumeSpec {
        fn from(v: dtos::storage::PersistentVolumeSpec) -> Self {
            match v {
                dtos::storage::PersistentVolumeSpec::S3(v) => Self::S3(v.into()),
            }
        }
    }

    impl TryFrom<PersistentVolumeSpec> for dtos::storage::PersistentVolumeSpec {
        type Error = ValidationError;
        fn try_from(v: PersistentVolumeSpec) -> Result<Self, Self::Error> {
            match v {
                PersistentVolumeSpec::S3(v) => Ok(Self::S3(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::storage::PersistentVolumeSpec, PersistentVolumeSpec);

    // Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec#/$defs/S3
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct PersistentVolumeSpecS3 {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub endpoint: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub region: Option<String>,
        pub bucket: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prefix: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub capacity: Option<storage::VolumeCapacity>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub credentials: Option<storage::AwsCredentials>,
    }

    impl IntoDto for PersistentVolumeSpecS3 {
        type Dto = dtos::storage::PersistentVolumeSpecS3;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::storage::PersistentVolumeSpecS3> for PersistentVolumeSpecS3 {
        fn from(v: dtos::storage::PersistentVolumeSpecS3) -> Self {
            Self {
                endpoint: v.endpoint,
                region: v.region,
                bucket: v.bucket,
                prefix: v.prefix,
                capacity: v.capacity.map(|v| v.into()),
                credentials: v.credentials.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<PersistentVolumeSpecS3> for dtos::storage::PersistentVolumeSpecS3 {
        type Error = ValidationError;
        fn try_from(v: PersistentVolumeSpecS3) -> Result<Self, ValidationError> {
            Ok(Self {
                endpoint: v.endpoint,
                region: v.region,
                bucket: v.bucket,
                prefix: v.prefix,
                capacity: v
                    .capacity
                    .map(|v| dtos::storage::VolumeCapacity::try_from(v))
                    .transpose()?,
                credentials: v
                    .credentials
                    .map(|v| dtos::storage::AwsCredentials::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(
        dtos::storage::PersistentVolumeSpecS3,
        PersistentVolumeSpecS3
    );

    // Schema: https://opendatafabric.org/schemas/storage/v1alpha1/VolumeCapacity
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct VolumeCapacity {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub storage: Option<ByteSize>,
    }

    impl IntoDto for VolumeCapacity {
        type Dto = dtos::storage::VolumeCapacity;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::storage::VolumeCapacity> for VolumeCapacity {
        fn from(v: dtos::storage::VolumeCapacity) -> Self {
            Self { storage: v.storage }
        }
    }

    impl TryFrom<VolumeCapacity> for dtos::storage::VolumeCapacity {
        type Error = ValidationError;
        fn try_from(v: VolumeCapacity) -> Result<Self, ValidationError> {
            Ok(Self { storage: v.storage })
        }
    }

    implement_serde_as!(dtos::storage::VolumeCapacity, VolumeCapacity);
}
