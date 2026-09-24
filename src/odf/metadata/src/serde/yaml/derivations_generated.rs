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
use setty::types::{ByteSize, DurationString};

use super::formats::*;
use crate as odf;
use crate::dtos;
use crate::errors::*;

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

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountHandle
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AccountHandle {
        pub id: odf::resources::ResourceID,
        pub did: odf::auth::AccountID,
        pub name: odf::auth::AccountName,
    }

    impl IntoDto for AccountHandle {
        type Dto = dtos::auth::AccountHandle;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::AccountHandle> for AccountHandle {
        fn from(v: dtos::auth::AccountHandle) -> Self {
            Self {
                id: v.id,
                did: v.did,
                name: v.name,
            }
        }
    }

    impl TryFrom<AccountHandle> for dtos::auth::AccountHandle {
        type Error = ValidationError;
        fn try_from(v: AccountHandle) -> Result<Self, ValidationError> {
            Ok(Self {
                id: v.id,
                did: v.did,
                name: v.name,
            })
        }
    }

    implement_serde_as!(dtos::auth::AccountHandle, AccountHandle);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountRef
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AccountRef {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<odf::resources::ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<odf::auth::AccountID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<odf::auth::AccountName>,
    }

    impl IntoDto for AccountRef {
        type Dto = dtos::auth::AccountRef;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl std::str::FromStr for AccountRef {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::auth::AccountRef::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
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

    impl From<dtos::auth::AccountRef> for AccountRef {
        fn from(v: dtos::auth::AccountRef) -> Self {
            Self {
                id: v.id,
                did: v.did,
                name: v.name,
            }
        }
    }

    impl TryFrom<AccountRef> for dtos::auth::AccountRef {
        type Error = ValidationError;
        fn try_from(v: AccountRef) -> Result<Self, ValidationError> {
            Ok(Self {
                id: v.id,
                did: v.did,
                name: v.name,
            })
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
        pub did: Option<odf::auth::AccountID>,
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

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AccountSpecInput {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<odf::auth::AccountID>,
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

    impl IntoDto for AccountSpecInput {
        type Dto = dtos::auth::AccountSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::AccountSpecInput> for AccountSpecInput {
        fn from(v: dtos::auth::AccountSpecInput) -> Self {
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

    impl TryFrom<AccountSpecInput> for dtos::auth::AccountSpecInput {
        type Error = ValidationError;
        fn try_from(v: AccountSpecInput) -> Result<Self, ValidationError> {
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

    implement_serde_as!(dtos::auth::AccountSpecInput, AccountSpecInput);

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

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/GroupSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct GroupSpec {}

    impl IntoDto for GroupSpec {
        type Dto = dtos::auth::GroupSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::GroupSpec> for GroupSpec {
        fn from(v: dtos::auth::GroupSpec) -> Self {
            Self {}
        }
    }

    impl TryFrom<GroupSpec> for dtos::auth::GroupSpec {
        type Error = ValidationError;
        fn try_from(v: GroupSpec) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::auth::GroupSpec, GroupSpec);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/GroupSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct GroupSpecInput {}

    impl IntoDto for GroupSpecInput {
        type Dto = dtos::auth::GroupSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::GroupSpecInput> for GroupSpecInput {
        fn from(v: dtos::auth::GroupSpecInput) -> Self {
            Self {}
        }
    }

    impl TryFrom<GroupSpecInput> for dtos::auth::GroupSpecInput {
        type Error = ValidationError;
        fn try_from(v: GroupSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::auth::GroupSpecInput, GroupSpecInput);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Relation
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Relation {
        pub subject: resources::ResourceHandle,
        pub relation: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub value: Option<serde_json::Value>,
        pub object: resources::ResourceHandle,
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
                subject: dtos::resources::ResourceHandle::try_from(v.subject)?,
                relation: v.relation,
                value: v.value,
                object: dtos::resources::ResourceHandle::try_from(v.object)?,
            })
        }
    }

    implement_serde_as!(dtos::auth::Relation, Relation);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/RelationInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RelationInput {
        pub subject: StructOrString<resources::ResourceRef>,
        pub relation: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub value: Option<serde_json::Value>,
        pub object: StructOrString<resources::ResourceRef>,
    }

    impl IntoDto for RelationInput {
        type Dto = dtos::auth::RelationInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::RelationInput> for RelationInput {
        fn from(v: dtos::auth::RelationInput) -> Self {
            Self {
                subject: v.subject.into(),
                relation: v.relation,
                value: v.value,
                object: v.object.into(),
            }
        }
    }

    impl TryFrom<RelationInput> for dtos::auth::RelationInput {
        type Error = ValidationError;
        fn try_from(v: RelationInput) -> Result<Self, ValidationError> {
            Ok(Self {
                subject: dtos::resources::ResourceRef::try_from(v.subject)?,
                relation: v.relation,
                value: v.value,
                object: dtos::resources::ResourceRef::try_from(v.object)?,
            })
        }
    }

    implement_serde_as!(dtos::auth::RelationInput, RelationInput);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/RelationsSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RelationsSpec {
        pub relations: Vec<auth::Relation>,
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
                relations: v.relations.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<RelationsSpec> for dtos::auth::RelationsSpec {
        type Error = ValidationError;
        fn try_from(v: RelationsSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                relations: v
                    .relations
                    .into_iter()
                    .map(|i| dtos::auth::Relation::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::auth::RelationsSpec, RelationsSpec);

    // Schema: https://opendatafabric.org/schemas/auth/v1alpha1/RelationsSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RelationsSpecInput {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub relations: Option<Vec<auth::RelationInput>>,
    }

    impl IntoDto for RelationsSpecInput {
        type Dto = dtos::auth::RelationsSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::auth::RelationsSpecInput> for RelationsSpecInput {
        fn from(v: dtos::auth::RelationsSpecInput) -> Self {
            Self {
                relations: v.relations.map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<RelationsSpecInput> for dtos::auth::RelationsSpecInput {
        type Error = ValidationError;
        fn try_from(v: RelationsSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                relations: v
                    .relations
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::auth::RelationInput::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::auth::RelationsSpecInput, RelationsSpecInput);
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

    impl std::str::FromStr for Secret {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::config::Secret::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
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

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSetSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SecretSetSpecInput {
        pub secrets: config::Secrets,
    }

    impl IntoDto for SecretSetSpecInput {
        type Dto = dtos::config::SecretSetSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::SecretSetSpecInput> for SecretSetSpecInput {
        fn from(v: dtos::config::SecretSetSpecInput) -> Self {
            Self {
                secrets: v.secrets.into(),
            }
        }
    }

    impl TryFrom<SecretSetSpecInput> for dtos::config::SecretSetSpecInput {
        type Error = ValidationError;
        fn try_from(v: SecretSetSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                secrets: dtos::config::Secrets::try_from(v.secrets)?,
            })
        }
    }

    implement_serde_as!(dtos::config::SecretSetSpecInput, SecretSetSpecInput);

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

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueHandle
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ValueHandle {
        pub account: auth::AccountHandle,
        pub r#type: odf::resources::TypeUri,
        pub id: odf::resources::ResourceID,
        pub name: odf::resources::ResourceName,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub path: Option<String>,
    }

    impl IntoDto for ValueHandle {
        type Dto = dtos::config::ValueHandle;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::ValueHandle> for ValueHandle {
        fn from(v: dtos::config::ValueHandle) -> Self {
            Self {
                account: v.account.into(),
                r#type: v.r#type,
                id: v.id,
                name: v.name,
                path: v.path,
            }
        }
    }

    impl TryFrom<ValueHandle> for dtos::config::ValueHandle {
        type Error = ValidationError;
        fn try_from(v: ValueHandle) -> Result<Self, ValidationError> {
            Ok(Self {
                account: dtos::auth::AccountHandle::try_from(v.account)?,
                r#type: v.r#type,
                id: v.id,
                name: v.name,
                path: v.path,
            })
        }
    }

    implement_serde_as!(dtos::config::ValueHandle, ValueHandle);

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueRef
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ValueRef {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<odf::resources::ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub r#type: Option<odf::resources::TypeRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<odf::resources::ResourceName>,
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

    impl std::str::FromStr for ValueRef {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::config::ValueRef::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
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

    impl From<dtos::config::ValueRef> for ValueRef {
        fn from(v: dtos::config::ValueRef) -> Self {
            Self {
                account: v.account.map(|v| v.into()),
                id: v.id,
                r#type: v.r#type,
                name: v.name,
                path: v.path,
            }
        }
    }

    impl TryFrom<ValueRef> for dtos::config::ValueRef {
        type Error = ValidationError;
        fn try_from(v: ValueRef) -> Result<Self, ValidationError> {
            Ok(Self {
                account: v
                    .account
                    .map(|v| dtos::auth::AccountRef::try_from(v))
                    .transpose()?,
                id: v.id,
                r#type: v.r#type,
                name: v.name,
                path: v.path,
            })
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

    impl std::str::FromStr for Variable {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::config::Variable::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
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

    // Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSetSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct VariableSetSpecInput {
        pub variables: config::Variables,
    }

    impl IntoDto for VariableSetSpecInput {
        type Dto = dtos::config::VariableSetSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::config::VariableSetSpecInput> for VariableSetSpecInput {
        fn from(v: dtos::config::VariableSetSpecInput) -> Self {
            Self {
                variables: v.variables.into(),
            }
        }
    }

    impl TryFrom<VariableSetSpecInput> for dtos::config::VariableSetSpecInput {
        type Error = ValidationError;
        fn try_from(v: VariableSetSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                variables: dtos::config::Variables::try_from(v.variables)?,
            })
        }
    }

    implement_serde_as!(dtos::config::VariableSetSpecInput, VariableSetSpecInput);

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
// datasets
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod datasets {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/AddData
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AddData {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_checkpoint: Option<odf::Multihash>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_offset: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_data: Option<datasets::DataSlice>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_checkpoint: Option<datasets::Checkpoint>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub new_watermark: Option<DateTime<Utc>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_source_state: Option<sources::SourceState>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub extra: Option<data::ExtraAttributes>,
    }

    impl IntoDto for AddData {
        type Dto = dtos::datasets::AddData;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::AddData> for AddData {
        fn from(v: dtos::datasets::AddData) -> Self {
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

    impl TryFrom<AddData> for dtos::datasets::AddData {
        type Error = ValidationError;
        fn try_from(v: AddData) -> Result<Self, ValidationError> {
            Ok(Self {
                prev_checkpoint: v.prev_checkpoint,
                prev_offset: v.prev_offset,
                new_data: v
                    .new_data
                    .map(|v| dtos::datasets::DataSlice::try_from(v))
                    .transpose()?,
                new_checkpoint: v
                    .new_checkpoint
                    .map(|v| dtos::datasets::Checkpoint::try_from(v))
                    .transpose()?,
                new_watermark: v.new_watermark,
                new_source_state: v
                    .new_source_state
                    .map(|v| dtos::sources::SourceState::try_from(v))
                    .transpose()?,
                extra: v
                    .extra
                    .map(|v| dtos::data::ExtraAttributes::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::AddData, AddData);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/AttachmentEmbedded
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AttachmentEmbedded {
        pub path: String,
        pub content: String,
    }

    impl IntoDto for AttachmentEmbedded {
        type Dto = dtos::datasets::AttachmentEmbedded;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::AttachmentEmbedded> for AttachmentEmbedded {
        fn from(v: dtos::datasets::AttachmentEmbedded) -> Self {
            Self {
                path: v.path,
                content: v.content,
            }
        }
    }

    impl TryFrom<AttachmentEmbedded> for dtos::datasets::AttachmentEmbedded {
        type Error = ValidationError;
        fn try_from(v: AttachmentEmbedded) -> Result<Self, ValidationError> {
            Ok(Self {
                path: v.path,
                content: v.content,
            })
        }
    }

    implement_serde_as!(dtos::datasets::AttachmentEmbedded, AttachmentEmbedded);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Attachments
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum Attachments {
        #[serde(alias = "embedded")]
        Embedded(datasets::AttachmentsEmbedded),
    }

    impl IntoDto for Attachments {
        type Dto = dtos::datasets::Attachments;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::Attachments> for Attachments {
        fn from(v: dtos::datasets::Attachments) -> Self {
            match v {
                dtos::datasets::Attachments::Embedded(v) => Self::Embedded(v.into()),
            }
        }
    }

    impl TryFrom<Attachments> for dtos::datasets::Attachments {
        type Error = ValidationError;
        fn try_from(v: Attachments) -> Result<Self, Self::Error> {
            match v {
                Attachments::Embedded(v) => Ok(Self::Embedded(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::datasets::Attachments, Attachments);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Attachments#/$defs/Embedded
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AttachmentsEmbedded {
        pub items: Vec<datasets::AttachmentEmbedded>,
    }

    impl IntoDto for AttachmentsEmbedded {
        type Dto = dtos::datasets::AttachmentsEmbedded;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::AttachmentsEmbedded> for AttachmentsEmbedded {
        fn from(v: dtos::datasets::AttachmentsEmbedded) -> Self {
            Self {
                items: v.items.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<AttachmentsEmbedded> for dtos::datasets::AttachmentsEmbedded {
        type Error = ValidationError;
        fn try_from(v: AttachmentsEmbedded) -> Result<Self, ValidationError> {
            Ok(Self {
                items: v
                    .items
                    .into_iter()
                    .map(|i| dtos::datasets::AttachmentEmbedded::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::AttachmentsEmbedded, AttachmentsEmbedded);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Checkpoint
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Checkpoint {
        pub physical_hash: odf::Multihash,
        pub size: u64,
    }

    impl IntoDto for Checkpoint {
        type Dto = dtos::datasets::Checkpoint;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::Checkpoint> for Checkpoint {
        fn from(v: dtos::datasets::Checkpoint) -> Self {
            Self {
                physical_hash: v.physical_hash,
                size: v.size,
            }
        }
    }

    impl TryFrom<Checkpoint> for dtos::datasets::Checkpoint {
        type Error = ValidationError;
        fn try_from(v: Checkpoint) -> Result<Self, ValidationError> {
            Ok(Self {
                physical_hash: v.physical_hash,
                size: v.size,
            })
        }
    }

    implement_serde_as!(dtos::datasets::Checkpoint, Checkpoint);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/CompactionParams
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
        type Dto = dtos::datasets::CompactionParams;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::CompactionParams> for CompactionParams {
        fn from(v: dtos::datasets::CompactionParams) -> Self {
            Self {
                max_slice_size: v.max_slice_size,
                max_slice_records: v.max_slice_records,
            }
        }
    }

    impl TryFrom<CompactionParams> for dtos::datasets::CompactionParams {
        type Error = ValidationError;
        fn try_from(v: CompactionParams) -> Result<Self, ValidationError> {
            Ok(Self {
                max_slice_size: v.max_slice_size,
                max_slice_records: v.max_slice_records,
            })
        }
    }

    implement_serde_as!(dtos::datasets::CompactionParams, CompactionParams);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DataSlice
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DataSlice {
        pub logical_hash: odf::Multihash,
        pub physical_hash: odf::Multihash,
        pub offset_interval: datasets::OffsetInterval,
        pub size: u64,
    }

    impl IntoDto for DataSlice {
        type Dto = dtos::datasets::DataSlice;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::DataSlice> for DataSlice {
        fn from(v: dtos::datasets::DataSlice) -> Self {
            Self {
                logical_hash: v.logical_hash,
                physical_hash: v.physical_hash,
                offset_interval: v.offset_interval.into(),
                size: v.size,
            }
        }
    }

    impl TryFrom<DataSlice> for dtos::datasets::DataSlice {
        type Error = ValidationError;
        fn try_from(v: DataSlice) -> Result<Self, ValidationError> {
            Ok(Self {
                logical_hash: v.logical_hash,
                physical_hash: v.physical_hash,
                offset_interval: dtos::datasets::OffsetInterval::try_from(v.offset_interval)?,
                size: v.size,
            })
        }
    }

    implement_serde_as!(dtos::datasets::DataSlice, DataSlice);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetHandle
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetHandle {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<auth::AccountHandle>,
        pub id: odf::resources::ResourceID,
        pub did: odf::datasets::DatasetID,
        pub name: odf::resources::ResourceName,
    }

    impl IntoDto for DatasetHandle {
        type Dto = dtos::datasets::DatasetHandle;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::DatasetHandle> for DatasetHandle {
        fn from(v: dtos::datasets::DatasetHandle) -> Self {
            Self {
                account: v.account.map(|v| v.into()),
                id: v.id,
                did: v.did,
                name: v.name,
            }
        }
    }

    impl TryFrom<DatasetHandle> for dtos::datasets::DatasetHandle {
        type Error = ValidationError;
        fn try_from(v: DatasetHandle) -> Result<Self, ValidationError> {
            Ok(Self {
                account: v
                    .account
                    .map(|v| dtos::auth::AccountHandle::try_from(v))
                    .transpose()?,
                id: v.id,
                did: v.did,
                name: v.name,
            })
        }
    }

    implement_serde_as!(dtos::datasets::DatasetHandle, DatasetHandle);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetKind
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum DatasetKind {
        #[serde(alias = "root")]
        Root,
        #[serde(alias = "derivative")]
        Derivative,
    }

    impl IntoDto for DatasetKind {
        type Dto = dtos::datasets::DatasetKind;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::DatasetKind> for DatasetKind {
        fn from(v: dtos::datasets::DatasetKind) -> Self {
            match v {
                dtos::datasets::DatasetKind::Root => Self::Root,
                dtos::datasets::DatasetKind::Derivative => Self::Derivative,
            }
        }
    }

    impl TryFrom<DatasetKind> for dtos::datasets::DatasetKind {
        type Error = ValidationError;
        fn try_from(v: DatasetKind) -> Result<Self, Self::Error> {
            match v {
                DatasetKind::Root => Ok(Self::Root),
                DatasetKind::Derivative => Ok(Self::Derivative),
            }
        }
    }

    implement_serde_as!(dtos::datasets::DatasetKind, DatasetKind);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetRef
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetRef {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<odf::resources::ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<odf::datasets::DatasetID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<odf::resources::ResourceName>,
    }

    impl IntoDto for DatasetRef {
        type Dto = dtos::datasets::DatasetRef;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl std::str::FromStr for DatasetRef {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::datasets::DatasetRef::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
        }
    }

    impl From<dtos::datasets::DatasetRef> for StructOrString<DatasetRef> {
        fn from(v: dtos::datasets::DatasetRef) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<DatasetRef>> for dtos::datasets::DatasetRef {
        type Error = ValidationError;
        fn try_from(v: StructOrString<DatasetRef>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    impl From<dtos::datasets::DatasetRef> for DatasetRef {
        fn from(v: dtos::datasets::DatasetRef) -> Self {
            Self {
                account: v.account.map(|v| v.into()),
                id: v.id,
                did: v.did,
                name: v.name,
            }
        }
    }

    impl TryFrom<DatasetRef> for dtos::datasets::DatasetRef {
        type Error = ValidationError;
        fn try_from(v: DatasetRef) -> Result<Self, ValidationError> {
            Ok(Self {
                account: v
                    .account
                    .map(|v| dtos::auth::AccountRef::try_from(v))
                    .transpose()?,
                id: v.id,
                did: v.did,
                name: v.name,
            })
        }
    }

    implement_serde_as!(dtos::datasets::DatasetRef, DatasetRef);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetRole
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum DatasetRole {
        #[serde(alias = "reader")]
        Reader,
        #[serde(alias = "editor")]
        Editor,
        #[serde(alias = "maintainer")]
        Maintainer,
    }

    impl IntoDto for DatasetRole {
        type Dto = dtos::datasets::DatasetRole;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::DatasetRole> for DatasetRole {
        fn from(v: dtos::datasets::DatasetRole) -> Self {
            match v {
                dtos::datasets::DatasetRole::Reader => Self::Reader,
                dtos::datasets::DatasetRole::Editor => Self::Editor,
                dtos::datasets::DatasetRole::Maintainer => Self::Maintainer,
            }
        }
    }

    impl TryFrom<DatasetRole> for dtos::datasets::DatasetRole {
        type Error = ValidationError;
        fn try_from(v: DatasetRole) -> Result<Self, Self::Error> {
            match v {
                DatasetRole::Reader => Ok(Self::Reader),
                DatasetRole::Editor => Ok(Self::Editor),
                DatasetRole::Maintainer => Ok(Self::Maintainer),
            }
        }
    }

    implement_serde_as!(dtos::datasets::DatasetRole, DatasetRole);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetSelector
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetSelector {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<odf::resources::ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub labels: Option<resources::LabelFilter>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub kind: Option<datasets::DatasetKind>,
    }

    impl IntoDto for DatasetSelector {
        type Dto = dtos::datasets::DatasetSelector;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl std::str::FromStr for DatasetSelector {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::datasets::DatasetSelector::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
        }
    }

    impl From<dtos::datasets::DatasetSelector> for StructOrString<DatasetSelector> {
        fn from(v: dtos::datasets::DatasetSelector) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<DatasetSelector>> for dtos::datasets::DatasetSelector {
        type Error = ValidationError;
        fn try_from(v: StructOrString<DatasetSelector>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    implement_serde_as!(dtos::datasets::DatasetSelector, DatasetSelector);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetSpec {
        pub did: odf::datasets::DatasetID,
        pub kind: datasets::DatasetKind,
        pub metadata: Vec<datasets::MetadataEvent>,
        pub volume: resources::ResourceHandle,
    }

    impl IntoDto for DatasetSpec {
        type Dto = dtos::datasets::DatasetSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::DatasetSpec> for DatasetSpec {
        fn from(v: dtos::datasets::DatasetSpec) -> Self {
            Self {
                did: v.did,
                kind: v.kind.into(),
                metadata: v.metadata.into_iter().map(Into::into).collect(),
                volume: v.volume.into(),
            }
        }
    }

    impl TryFrom<DatasetSpec> for dtos::datasets::DatasetSpec {
        type Error = ValidationError;
        fn try_from(v: DatasetSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                did: v.did,
                kind: dtos::datasets::DatasetKind::try_from(v.kind)?,
                metadata: v
                    .metadata
                    .into_iter()
                    .map(|i| dtos::datasets::MetadataEvent::try_from(i))
                    .collect::<Result<_, _>>()?,
                volume: dtos::resources::ResourceHandle::try_from(v.volume)?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::DatasetSpec, DatasetSpec);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetSpecInput {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<odf::datasets::DatasetID>,
        pub kind: datasets::DatasetKind,
        pub metadata: Vec<datasets::MetadataEvent>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub volume: Option<StructOrString<storage::PersistentVolumeRef>>,
    }

    impl IntoDto for DatasetSpecInput {
        type Dto = dtos::datasets::DatasetSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::DatasetSpecInput> for DatasetSpecInput {
        fn from(v: dtos::datasets::DatasetSpecInput) -> Self {
            Self {
                did: v.did,
                kind: v.kind.into(),
                metadata: v.metadata.into_iter().map(Into::into).collect(),
                volume: v.volume.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<DatasetSpecInput> for dtos::datasets::DatasetSpecInput {
        type Error = ValidationError;
        fn try_from(v: DatasetSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                did: v.did,
                kind: dtos::datasets::DatasetKind::try_from(v.kind)?,
                metadata: v
                    .metadata
                    .into_iter()
                    .map(|i| dtos::datasets::MetadataEvent::try_from(i))
                    .collect::<Result<_, _>>()?,
                volume: v
                    .volume
                    .map(|v| dtos::storage::PersistentVolumeRef::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::DatasetSpecInput, DatasetSpecInput);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetVocabulary
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
        type Dto = dtos::datasets::DatasetVocabulary;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::DatasetVocabulary> for DatasetVocabulary {
        fn from(v: dtos::datasets::DatasetVocabulary) -> Self {
            Self {
                offset_column: v.offset_column,
                operation_type_column: v.operation_type_column,
                system_time_column: v.system_time_column,
                event_time_column: v.event_time_column,
            }
        }
    }

    impl TryFrom<DatasetVocabulary> for dtos::datasets::DatasetVocabulary {
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

    implement_serde_as!(dtos::datasets::DatasetVocabulary, DatasetVocabulary);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/ExecuteTransform
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ExecuteTransform {
        pub query_inputs: Vec<datasets::ExecuteTransformInput>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_checkpoint: Option<odf::Multihash>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_offset: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_data: Option<datasets::DataSlice>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_checkpoint: Option<datasets::Checkpoint>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub new_watermark: Option<DateTime<Utc>>,
    }

    impl IntoDto for ExecuteTransform {
        type Dto = dtos::datasets::ExecuteTransform;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::ExecuteTransform> for ExecuteTransform {
        fn from(v: dtos::datasets::ExecuteTransform) -> Self {
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

    impl TryFrom<ExecuteTransform> for dtos::datasets::ExecuteTransform {
        type Error = ValidationError;
        fn try_from(v: ExecuteTransform) -> Result<Self, ValidationError> {
            Ok(Self {
                query_inputs: v
                    .query_inputs
                    .into_iter()
                    .map(|i| dtos::datasets::ExecuteTransformInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                prev_checkpoint: v.prev_checkpoint,
                prev_offset: v.prev_offset,
                new_data: v
                    .new_data
                    .map(|v| dtos::datasets::DataSlice::try_from(v))
                    .transpose()?,
                new_checkpoint: v
                    .new_checkpoint
                    .map(|v| dtos::datasets::Checkpoint::try_from(v))
                    .transpose()?,
                new_watermark: v.new_watermark,
            })
        }
    }

    implement_serde_as!(dtos::datasets::ExecuteTransform, ExecuteTransform);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/ExecuteTransformInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ExecuteTransformInput {
        pub dataset_id: odf::datasets::DatasetID,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_block_hash: Option<odf::Multihash>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_block_hash: Option<odf::Multihash>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_offset: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_offset: Option<u64>,
    }

    impl IntoDto for ExecuteTransformInput {
        type Dto = dtos::datasets::ExecuteTransformInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::ExecuteTransformInput> for ExecuteTransformInput {
        fn from(v: dtos::datasets::ExecuteTransformInput) -> Self {
            Self {
                dataset_id: v.dataset_id,
                prev_block_hash: v.prev_block_hash,
                new_block_hash: v.new_block_hash,
                prev_offset: v.prev_offset,
                new_offset: v.new_offset,
            }
        }
    }

    impl TryFrom<ExecuteTransformInput> for dtos::datasets::ExecuteTransformInput {
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

    implement_serde_as!(dtos::datasets::ExecuteTransformInput, ExecuteTransformInput);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/MetadataBlock
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MetadataBlock {
        #[serde(with = "datetime_rfc3339")]
        pub system_time: DateTime<Utc>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_block_hash: Option<odf::Multihash>,
        pub sequence_number: u64,
        pub event: datasets::MetadataEvent,
    }

    impl IntoDto for MetadataBlock {
        type Dto = dtos::datasets::MetadataBlock;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::MetadataBlock> for MetadataBlock {
        fn from(v: dtos::datasets::MetadataBlock) -> Self {
            Self {
                system_time: v.system_time,
                prev_block_hash: v.prev_block_hash,
                sequence_number: v.sequence_number,
                event: v.event.into(),
            }
        }
    }

    impl TryFrom<MetadataBlock> for dtos::datasets::MetadataBlock {
        type Error = ValidationError;
        fn try_from(v: MetadataBlock) -> Result<Self, ValidationError> {
            Ok(Self {
                system_time: v.system_time,
                prev_block_hash: v.prev_block_hash,
                sequence_number: v.sequence_number,
                event: dtos::datasets::MetadataEvent::try_from(v.event)?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::MetadataBlock, MetadataBlock);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/MetadataEvent
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum MetadataEvent {
        #[serde(alias = "addData", alias = "adddata")]
        AddData(datasets::AddData),
        #[serde(alias = "executeTransform", alias = "executetransform")]
        ExecuteTransform(datasets::ExecuteTransform),
        #[serde(alias = "seed")]
        Seed(datasets::Seed),
        #[serde(alias = "setPollingSource", alias = "setpollingsource")]
        SetPollingSource(legacy::SetPollingSource),
        #[serde(alias = "setTransform", alias = "settransform")]
        SetTransform(datasets::SetTransform),
        #[serde(alias = "setVocab", alias = "setvocab")]
        SetVocab(datasets::SetVocab),
        #[serde(alias = "setAttachments", alias = "setattachments")]
        SetAttachments(datasets::SetAttachments),
        #[serde(alias = "setInfo", alias = "setinfo")]
        SetInfo(datasets::SetInfo),
        #[serde(alias = "setLicense", alias = "setlicense")]
        SetLicense(datasets::SetLicense),
        #[serde(alias = "setDataSchema", alias = "setdataschema")]
        SetDataSchema(datasets::SetDataSchema),
        #[serde(alias = "addPushSource", alias = "addpushsource")]
        AddPushSource(legacy::AddPushSource),
        #[serde(alias = "disablePushSource", alias = "disablepushsource")]
        DisablePushSource(legacy::DisablePushSource),
        #[serde(alias = "disablePollingSource", alias = "disablepollingsource")]
        DisablePollingSource(legacy::DisablePollingSource),
    }

    impl IntoDto for MetadataEvent {
        type Dto = dtos::datasets::MetadataEvent;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::MetadataEvent> for MetadataEvent {
        fn from(v: dtos::datasets::MetadataEvent) -> Self {
            match v {
                dtos::datasets::MetadataEvent::AddData(v) => Self::AddData(v.into()),
                dtos::datasets::MetadataEvent::ExecuteTransform(v) => {
                    Self::ExecuteTransform(v.into())
                }
                dtos::datasets::MetadataEvent::Seed(v) => Self::Seed(v.into()),
                dtos::datasets::MetadataEvent::SetPollingSource(v) => {
                    Self::SetPollingSource(v.into())
                }
                dtos::datasets::MetadataEvent::SetTransform(v) => Self::SetTransform(v.into()),
                dtos::datasets::MetadataEvent::SetVocab(v) => Self::SetVocab(v.into()),
                dtos::datasets::MetadataEvent::SetAttachments(v) => Self::SetAttachments(v.into()),
                dtos::datasets::MetadataEvent::SetInfo(v) => Self::SetInfo(v.into()),
                dtos::datasets::MetadataEvent::SetLicense(v) => Self::SetLicense(v.into()),
                dtos::datasets::MetadataEvent::SetDataSchema(v) => Self::SetDataSchema(v.into()),
                dtos::datasets::MetadataEvent::AddPushSource(v) => Self::AddPushSource(v.into()),
                dtos::datasets::MetadataEvent::DisablePushSource(v) => {
                    Self::DisablePushSource(v.into())
                }
                dtos::datasets::MetadataEvent::DisablePollingSource(v) => {
                    Self::DisablePollingSource(v.into())
                }
            }
        }
    }

    impl TryFrom<MetadataEvent> for dtos::datasets::MetadataEvent {
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

    implement_serde_as!(dtos::datasets::MetadataEvent, MetadataEvent);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/OffsetInterval
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct OffsetInterval {
        pub start: u64,
        pub end: u64,
    }

    impl IntoDto for OffsetInterval {
        type Dto = dtos::datasets::OffsetInterval;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::OffsetInterval> for OffsetInterval {
        fn from(v: dtos::datasets::OffsetInterval) -> Self {
            Self {
                start: v.start,
                end: v.end,
            }
        }
    }

    impl TryFrom<OffsetInterval> for dtos::datasets::OffsetInterval {
        type Error = ValidationError;
        fn try_from(v: OffsetInterval) -> Result<Self, ValidationError> {
            Ok(Self {
                start: v.start,
                end: v.end,
            })
        }
    }

    implement_serde_as!(dtos::datasets::OffsetInterval, OffsetInterval);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/ProjectionSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ProjectionSpec {
        pub inputs: Vec<datasets::TransformInput>,
        pub project: datasets::Transform,
    }

    impl IntoDto for ProjectionSpec {
        type Dto = dtos::datasets::ProjectionSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::ProjectionSpec> for ProjectionSpec {
        fn from(v: dtos::datasets::ProjectionSpec) -> Self {
            Self {
                inputs: v.inputs.into_iter().map(Into::into).collect(),
                project: v.project.into(),
            }
        }
    }

    impl TryFrom<ProjectionSpec> for dtos::datasets::ProjectionSpec {
        type Error = ValidationError;
        fn try_from(v: ProjectionSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                inputs: v
                    .inputs
                    .into_iter()
                    .map(|i| dtos::datasets::TransformInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                project: dtos::datasets::Transform::try_from(v.project)?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::ProjectionSpec, ProjectionSpec);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/ProjectionSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ProjectionSpecInput {
        pub inputs: Vec<datasets::TransformInput>,
        pub project: datasets::Transform,
    }

    impl IntoDto for ProjectionSpecInput {
        type Dto = dtos::datasets::ProjectionSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::ProjectionSpecInput> for ProjectionSpecInput {
        fn from(v: dtos::datasets::ProjectionSpecInput) -> Self {
            Self {
                inputs: v.inputs.into_iter().map(Into::into).collect(),
                project: v.project.into(),
            }
        }
    }

    impl TryFrom<ProjectionSpecInput> for dtos::datasets::ProjectionSpecInput {
        type Error = ValidationError;
        fn try_from(v: ProjectionSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                inputs: v
                    .inputs
                    .into_iter()
                    .map(|i| dtos::datasets::TransformInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                project: dtos::datasets::Transform::try_from(v.project)?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::ProjectionSpecInput, ProjectionSpecInput);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Seed
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Seed {
        pub dataset_id: odf::datasets::DatasetID,
        pub dataset_kind: datasets::DatasetKind,
    }

    impl IntoDto for Seed {
        type Dto = dtos::datasets::Seed;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::Seed> for Seed {
        fn from(v: dtos::datasets::Seed) -> Self {
            Self {
                dataset_id: v.dataset_id,
                dataset_kind: v.dataset_kind.into(),
            }
        }
    }

    impl TryFrom<Seed> for dtos::datasets::Seed {
        type Error = ValidationError;
        fn try_from(v: Seed) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_id: v.dataset_id,
                dataset_kind: dtos::datasets::DatasetKind::try_from(v.dataset_kind)?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::Seed, Seed);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetAttachments
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetAttachments {
        pub attachments: datasets::Attachments,
    }

    impl IntoDto for SetAttachments {
        type Dto = dtos::datasets::SetAttachments;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::SetAttachments> for SetAttachments {
        fn from(v: dtos::datasets::SetAttachments) -> Self {
            Self {
                attachments: v.attachments.into(),
            }
        }
    }

    impl TryFrom<SetAttachments> for dtos::datasets::SetAttachments {
        type Error = ValidationError;
        fn try_from(v: SetAttachments) -> Result<Self, ValidationError> {
            Ok(Self {
                attachments: dtos::datasets::Attachments::try_from(v.attachments)?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::SetAttachments, SetAttachments);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetDataSchema
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
        type Dto = dtos::datasets::SetDataSchema;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::SetDataSchema> for SetDataSchema {
        fn from(v: dtos::datasets::SetDataSchema) -> Self {
            Self {
                raw_arrow_schema: v.raw_arrow_schema,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<SetDataSchema> for dtos::datasets::SetDataSchema {
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

    implement_serde_as!(dtos::datasets::SetDataSchema, SetDataSchema);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetInfo
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
        type Dto = dtos::datasets::SetInfo;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::SetInfo> for SetInfo {
        fn from(v: dtos::datasets::SetInfo) -> Self {
            Self {
                description: v.description,
                keywords: v.keywords,
            }
        }
    }

    impl TryFrom<SetInfo> for dtos::datasets::SetInfo {
        type Error = ValidationError;
        fn try_from(v: SetInfo) -> Result<Self, ValidationError> {
            Ok(Self {
                description: v.description,
                keywords: v.keywords,
            })
        }
    }

    implement_serde_as!(dtos::datasets::SetInfo, SetInfo);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetLicense
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
        type Dto = dtos::datasets::SetLicense;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::SetLicense> for SetLicense {
        fn from(v: dtos::datasets::SetLicense) -> Self {
            Self {
                short_name: v.short_name,
                name: v.name,
                spdx_id: v.spdx_id,
                website_url: v.website_url,
            }
        }
    }

    impl TryFrom<SetLicense> for dtos::datasets::SetLicense {
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

    implement_serde_as!(dtos::datasets::SetLicense, SetLicense);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetTransform
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SetTransform {
        pub inputs: Vec<datasets::TransformInput>,
        pub transform: datasets::Transform,
    }

    impl IntoDto for SetTransform {
        type Dto = dtos::datasets::SetTransform;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::SetTransform> for SetTransform {
        fn from(v: dtos::datasets::SetTransform) -> Self {
            Self {
                inputs: v.inputs.into_iter().map(Into::into).collect(),
                transform: v.transform.into(),
            }
        }
    }

    impl TryFrom<SetTransform> for dtos::datasets::SetTransform {
        type Error = ValidationError;
        fn try_from(v: SetTransform) -> Result<Self, ValidationError> {
            Ok(Self {
                inputs: v
                    .inputs
                    .into_iter()
                    .map(|i| dtos::datasets::TransformInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                transform: dtos::datasets::Transform::try_from(v.transform)?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::SetTransform, SetTransform);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetVocab
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
        type Dto = dtos::datasets::SetVocab;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::SetVocab> for SetVocab {
        fn from(v: dtos::datasets::SetVocab) -> Self {
            Self {
                offset_column: v.offset_column,
                operation_type_column: v.operation_type_column,
                system_time_column: v.system_time_column,
                event_time_column: v.event_time_column,
            }
        }
    }

    impl TryFrom<SetVocab> for dtos::datasets::SetVocab {
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

    implement_serde_as!(dtos::datasets::SetVocab, SetVocab);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SqlQueryStep
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
        type Dto = dtos::datasets::SqlQueryStep;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::SqlQueryStep> for SqlQueryStep {
        fn from(v: dtos::datasets::SqlQueryStep) -> Self {
            Self {
                alias: v.alias,
                query: v.query,
            }
        }
    }

    impl TryFrom<SqlQueryStep> for dtos::datasets::SqlQueryStep {
        type Error = ValidationError;
        fn try_from(v: SqlQueryStep) -> Result<Self, ValidationError> {
            Ok(Self {
                alias: v.alias,
                query: v.query,
            })
        }
    }

    implement_serde_as!(dtos::datasets::SqlQueryStep, SqlQueryStep);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/TemporalTable
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TemporalTable {
        pub name: String,
        pub primary_key: Vec<String>,
    }

    impl IntoDto for TemporalTable {
        type Dto = dtos::datasets::TemporalTable;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::TemporalTable> for TemporalTable {
        fn from(v: dtos::datasets::TemporalTable) -> Self {
            Self {
                name: v.name,
                primary_key: v.primary_key,
            }
        }
    }

    impl TryFrom<TemporalTable> for dtos::datasets::TemporalTable {
        type Error = ValidationError;
        fn try_from(v: TemporalTable) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                primary_key: v.primary_key,
            })
        }
    }

    implement_serde_as!(dtos::datasets::TemporalTable, TemporalTable);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Transform
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum Transform {
        #[serde(alias = "sql")]
        Sql(datasets::TransformSql),
    }

    impl IntoDto for Transform {
        type Dto = dtos::datasets::Transform;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::Transform> for Transform {
        fn from(v: dtos::datasets::Transform) -> Self {
            match v {
                dtos::datasets::Transform::Sql(v) => Self::Sql(v.into()),
            }
        }
    }

    impl TryFrom<Transform> for dtos::datasets::Transform {
        type Error = ValidationError;
        fn try_from(v: Transform) -> Result<Self, Self::Error> {
            match v {
                Transform::Sql(v) => Ok(Self::Sql(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::datasets::Transform, Transform);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/TransformInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformInput {
        pub dataset_ref: odf::datasets::legacy::DatasetRef,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub alias: Option<String>,
    }

    impl IntoDto for TransformInput {
        type Dto = dtos::datasets::TransformInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::TransformInput> for TransformInput {
        fn from(v: dtos::datasets::TransformInput) -> Self {
            Self {
                dataset_ref: v.dataset_ref,
                alias: v.alias,
            }
        }
    }

    impl TryFrom<TransformInput> for dtos::datasets::TransformInput {
        type Error = ValidationError;
        fn try_from(v: TransformInput) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_ref: v.dataset_ref,
                alias: v.alias,
            })
        }
    }

    implement_serde_as!(dtos::datasets::TransformInput, TransformInput);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Transform#/$defs/Sql
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
        pub queries: Option<Vec<datasets::SqlQueryStep>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub temporal_tables: Option<Vec<datasets::TemporalTable>>,
    }

    impl IntoDto for TransformSql {
        type Dto = dtos::datasets::TransformSql;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::TransformSql> for TransformSql {
        fn from(v: dtos::datasets::TransformSql) -> Self {
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

    impl TryFrom<TransformSql> for dtos::datasets::TransformSql {
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
                            .map(|i| dtos::datasets::SqlQueryStep::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                temporal_tables: v
                    .temporal_tables
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::datasets::TemporalTable::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::datasets::TransformSql, TransformSql);

    // Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Watermark
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
        type Dto = dtos::datasets::Watermark;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::datasets::Watermark> for Watermark {
        fn from(v: dtos::datasets::Watermark) -> Self {
            Self {
                system_time: v.system_time,
                event_time: v.event_time,
            }
        }
    }

    impl TryFrom<Watermark> for dtos::datasets::Watermark {
        type Error = ValidationError;
        fn try_from(v: Watermark) -> Result<Self, ValidationError> {
            Ok(Self {
                system_time: v.system_time,
                event_time: v.event_time,
            })
        }
    }

    implement_serde_as!(dtos::datasets::Watermark, Watermark);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// engines
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod engines {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryRequest
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryRequest {
        pub input_data_paths: Vec<PathBuf>,
        pub transform: datasets::Transform,
        pub output_data_path: PathBuf,
    }

    impl IntoDto for RawQueryRequest {
        type Dto = dtos::engines::RawQueryRequest;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::RawQueryRequest> for RawQueryRequest {
        fn from(v: dtos::engines::RawQueryRequest) -> Self {
            Self {
                input_data_paths: v.input_data_paths,
                transform: v.transform.into(),
                output_data_path: v.output_data_path,
            }
        }
    }

    impl TryFrom<RawQueryRequest> for dtos::engines::RawQueryRequest {
        type Error = ValidationError;
        fn try_from(v: RawQueryRequest) -> Result<Self, ValidationError> {
            Ok(Self {
                input_data_paths: v.input_data_paths,
                transform: dtos::datasets::Transform::try_from(v.transform)?,
                output_data_path: v.output_data_path,
            })
        }
    }

    implement_serde_as!(dtos::engines::RawQueryRequest, RawQueryRequest);

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum RawQueryResponse {
        #[serde(alias = "progress")]
        Progress(engines::RawQueryResponseProgress),
        #[serde(alias = "success")]
        Success(engines::RawQueryResponseSuccess),
        #[serde(alias = "invalidQuery", alias = "invalidquery")]
        InvalidQuery(engines::RawQueryResponseInvalidQuery),
        #[serde(alias = "internalError", alias = "internalerror")]
        InternalError(engines::RawQueryResponseInternalError),
    }

    impl IntoDto for RawQueryResponse {
        type Dto = dtos::engines::RawQueryResponse;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::RawQueryResponse> for RawQueryResponse {
        fn from(v: dtos::engines::RawQueryResponse) -> Self {
            match v {
                dtos::engines::RawQueryResponse::Progress(v) => Self::Progress(v.into()),
                dtos::engines::RawQueryResponse::Success(v) => Self::Success(v.into()),
                dtos::engines::RawQueryResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
                dtos::engines::RawQueryResponse::InternalError(v) => Self::InternalError(v.into()),
            }
        }
    }

    impl TryFrom<RawQueryResponse> for dtos::engines::RawQueryResponse {
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

    implement_serde_as!(dtos::engines::RawQueryResponse, RawQueryResponse);

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse#/$defs/InternalError
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
        type Dto = dtos::engines::RawQueryResponseInternalError;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::RawQueryResponseInternalError> for RawQueryResponseInternalError {
        fn from(v: dtos::engines::RawQueryResponseInternalError) -> Self {
            Self {
                message: v.message,
                backtrace: v.backtrace,
            }
        }
    }

    impl TryFrom<RawQueryResponseInternalError> for dtos::engines::RawQueryResponseInternalError {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponseInternalError) -> Result<Self, ValidationError> {
            Ok(Self {
                message: v.message,
                backtrace: v.backtrace,
            })
        }
    }

    implement_serde_as!(
        dtos::engines::RawQueryResponseInternalError,
        RawQueryResponseInternalError
    );

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse#/$defs/InvalidQuery
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryResponseInvalidQuery {
        pub message: String,
    }

    impl IntoDto for RawQueryResponseInvalidQuery {
        type Dto = dtos::engines::RawQueryResponseInvalidQuery;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::RawQueryResponseInvalidQuery> for RawQueryResponseInvalidQuery {
        fn from(v: dtos::engines::RawQueryResponseInvalidQuery) -> Self {
            Self { message: v.message }
        }
    }

    impl TryFrom<RawQueryResponseInvalidQuery> for dtos::engines::RawQueryResponseInvalidQuery {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponseInvalidQuery) -> Result<Self, ValidationError> {
            Ok(Self { message: v.message })
        }
    }

    implement_serde_as!(
        dtos::engines::RawQueryResponseInvalidQuery,
        RawQueryResponseInvalidQuery
    );

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse#/$defs/Progress
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryResponseProgress {}

    impl IntoDto for RawQueryResponseProgress {
        type Dto = dtos::engines::RawQueryResponseProgress;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::RawQueryResponseProgress> for RawQueryResponseProgress {
        fn from(v: dtos::engines::RawQueryResponseProgress) -> Self {
            Self {}
        }
    }

    impl TryFrom<RawQueryResponseProgress> for dtos::engines::RawQueryResponseProgress {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponseProgress) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::engines::RawQueryResponseProgress,
        RawQueryResponseProgress
    );

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse#/$defs/Success
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RawQueryResponseSuccess {
        pub num_records: u64,
    }

    impl IntoDto for RawQueryResponseSuccess {
        type Dto = dtos::engines::RawQueryResponseSuccess;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::RawQueryResponseSuccess> for RawQueryResponseSuccess {
        fn from(v: dtos::engines::RawQueryResponseSuccess) -> Self {
            Self {
                num_records: v.num_records,
            }
        }
    }

    impl TryFrom<RawQueryResponseSuccess> for dtos::engines::RawQueryResponseSuccess {
        type Error = ValidationError;
        fn try_from(v: RawQueryResponseSuccess) -> Result<Self, ValidationError> {
            Ok(Self {
                num_records: v.num_records,
            })
        }
    }

    implement_serde_as!(
        dtos::engines::RawQueryResponseSuccess,
        RawQueryResponseSuccess
    );

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformRequest
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformRequest {
        pub dataset_id: odf::datasets::DatasetID,
        pub dataset_alias: odf::datasets::legacy::DatasetAlias,
        #[serde(with = "datetime_rfc3339")]
        pub system_time: DateTime<Utc>,
        pub vocab: datasets::DatasetVocabulary,
        pub transform: datasets::Transform,
        pub query_inputs: Vec<engines::TransformRequestInput>,
        pub next_offset: u64,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prev_checkpoint_path: Option<PathBuf>,
        pub new_checkpoint_path: PathBuf,
        pub new_data_path: PathBuf,
    }

    impl IntoDto for TransformRequest {
        type Dto = dtos::engines::TransformRequest;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::TransformRequest> for TransformRequest {
        fn from(v: dtos::engines::TransformRequest) -> Self {
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

    impl TryFrom<TransformRequest> for dtos::engines::TransformRequest {
        type Error = ValidationError;
        fn try_from(v: TransformRequest) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_id: v.dataset_id,
                dataset_alias: v.dataset_alias,
                system_time: v.system_time,
                vocab: dtos::datasets::DatasetVocabulary::try_from(v.vocab)?,
                transform: dtos::datasets::Transform::try_from(v.transform)?,
                query_inputs: v
                    .query_inputs
                    .into_iter()
                    .map(|i| dtos::engines::TransformRequestInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                next_offset: v.next_offset,
                prev_checkpoint_path: v.prev_checkpoint_path,
                new_checkpoint_path: v.new_checkpoint_path,
                new_data_path: v.new_data_path,
            })
        }
    }

    implement_serde_as!(dtos::engines::TransformRequest, TransformRequest);

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformRequestInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformRequestInput {
        pub dataset_id: odf::datasets::DatasetID,
        pub dataset_alias: odf::datasets::legacy::DatasetAlias,
        pub query_alias: String,
        pub vocab: datasets::DatasetVocabulary,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub offset_interval: Option<datasets::OffsetInterval>,
        pub data_paths: Vec<PathBuf>,
        pub schema_file: PathBuf,
        pub explicit_watermarks: Vec<datasets::Watermark>,
    }

    impl IntoDto for TransformRequestInput {
        type Dto = dtos::engines::TransformRequestInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::TransformRequestInput> for TransformRequestInput {
        fn from(v: dtos::engines::TransformRequestInput) -> Self {
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

    impl TryFrom<TransformRequestInput> for dtos::engines::TransformRequestInput {
        type Error = ValidationError;
        fn try_from(v: TransformRequestInput) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset_id: v.dataset_id,
                dataset_alias: v.dataset_alias,
                query_alias: v.query_alias,
                vocab: dtos::datasets::DatasetVocabulary::try_from(v.vocab)?,
                offset_interval: v
                    .offset_interval
                    .map(|v| dtos::datasets::OffsetInterval::try_from(v))
                    .transpose()?,
                data_paths: v.data_paths,
                schema_file: v.schema_file,
                explicit_watermarks: v
                    .explicit_watermarks
                    .into_iter()
                    .map(|i| dtos::datasets::Watermark::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::engines::TransformRequestInput, TransformRequestInput);

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum TransformResponse {
        #[serde(alias = "progress")]
        Progress(engines::TransformResponseProgress),
        #[serde(alias = "success")]
        Success(engines::TransformResponseSuccess),
        #[serde(alias = "invalidQuery", alias = "invalidquery")]
        InvalidQuery(engines::TransformResponseInvalidQuery),
        #[serde(alias = "internalError", alias = "internalerror")]
        InternalError(engines::TransformResponseInternalError),
    }

    impl IntoDto for TransformResponse {
        type Dto = dtos::engines::TransformResponse;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::TransformResponse> for TransformResponse {
        fn from(v: dtos::engines::TransformResponse) -> Self {
            match v {
                dtos::engines::TransformResponse::Progress(v) => Self::Progress(v.into()),
                dtos::engines::TransformResponse::Success(v) => Self::Success(v.into()),
                dtos::engines::TransformResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
                dtos::engines::TransformResponse::InternalError(v) => Self::InternalError(v.into()),
            }
        }
    }

    impl TryFrom<TransformResponse> for dtos::engines::TransformResponse {
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

    implement_serde_as!(dtos::engines::TransformResponse, TransformResponse);

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse#/$defs/InternalError
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
        type Dto = dtos::engines::TransformResponseInternalError;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::TransformResponseInternalError> for TransformResponseInternalError {
        fn from(v: dtos::engines::TransformResponseInternalError) -> Self {
            Self {
                message: v.message,
                backtrace: v.backtrace,
            }
        }
    }

    impl TryFrom<TransformResponseInternalError> for dtos::engines::TransformResponseInternalError {
        type Error = ValidationError;
        fn try_from(v: TransformResponseInternalError) -> Result<Self, ValidationError> {
            Ok(Self {
                message: v.message,
                backtrace: v.backtrace,
            })
        }
    }

    implement_serde_as!(
        dtos::engines::TransformResponseInternalError,
        TransformResponseInternalError
    );

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse#/$defs/InvalidQuery
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformResponseInvalidQuery {
        pub message: String,
    }

    impl IntoDto for TransformResponseInvalidQuery {
        type Dto = dtos::engines::TransformResponseInvalidQuery;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::TransformResponseInvalidQuery> for TransformResponseInvalidQuery {
        fn from(v: dtos::engines::TransformResponseInvalidQuery) -> Self {
            Self { message: v.message }
        }
    }

    impl TryFrom<TransformResponseInvalidQuery> for dtos::engines::TransformResponseInvalidQuery {
        type Error = ValidationError;
        fn try_from(v: TransformResponseInvalidQuery) -> Result<Self, ValidationError> {
            Ok(Self { message: v.message })
        }
    }

    implement_serde_as!(
        dtos::engines::TransformResponseInvalidQuery,
        TransformResponseInvalidQuery
    );

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse#/$defs/Progress
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformResponseProgress {}

    impl IntoDto for TransformResponseProgress {
        type Dto = dtos::engines::TransformResponseProgress;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::TransformResponseProgress> for TransformResponseProgress {
        fn from(v: dtos::engines::TransformResponseProgress) -> Self {
            Self {}
        }
    }

    impl TryFrom<TransformResponseProgress> for dtos::engines::TransformResponseProgress {
        type Error = ValidationError;
        fn try_from(v: TransformResponseProgress) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::engines::TransformResponseProgress,
        TransformResponseProgress
    );

    // Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse#/$defs/Success
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TransformResponseSuccess {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub new_offset_interval: Option<datasets::OffsetInterval>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub new_watermark: Option<DateTime<Utc>>,
    }

    impl IntoDto for TransformResponseSuccess {
        type Dto = dtos::engines::TransformResponseSuccess;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::engines::TransformResponseSuccess> for TransformResponseSuccess {
        fn from(v: dtos::engines::TransformResponseSuccess) -> Self {
            Self {
                new_offset_interval: v.new_offset_interval.map(|v| v.into()),
                new_watermark: v.new_watermark,
            }
        }
    }

    impl TryFrom<TransformResponseSuccess> for dtos::engines::TransformResponseSuccess {
        type Error = ValidationError;
        fn try_from(v: TransformResponseSuccess) -> Result<Self, ValidationError> {
            Ok(Self {
                new_offset_interval: v
                    .new_offset_interval
                    .map(|v| dtos::datasets::OffsetInterval::try_from(v))
                    .transpose()?,
                new_watermark: v.new_watermark,
            })
        }
    }

    implement_serde_as!(
        dtos::engines::TransformResponseSuccess,
        TransformResponseSuccess
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// events
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod events {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/events/v1alpha1/EventFilter
    #[derive(Debug, Serialize, Deserialize)]
    pub struct EventFilter {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    impl IntoDto for EventFilter {
        type Dto = dtos::events::EventFilter;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::events::EventFilter> for EventFilter {
        fn from(v: dtos::events::EventFilter) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<EventFilter> for dtos::events::EventFilter {
        type Error = ValidationError;
        fn try_from(v: EventFilter) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::events::EventFilter, EventFilter);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// flows
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod flows {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunActivationCause
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowRunActivationCause {
        #[serde(with = "datetime_rfc3339")]
        pub activation_time: DateTime<Utc>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub initiator: Option<auth::AccountHandle>,
        pub trigger: flows::FlowTrigger,
    }

    impl IntoDto for FlowRunActivationCause {
        type Dto = dtos::flows::FlowRunActivationCause;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowRunActivationCause> for FlowRunActivationCause {
        fn from(v: dtos::flows::FlowRunActivationCause) -> Self {
            Self {
                activation_time: v.activation_time,
                initiator: v.initiator.map(|v| v.into()),
                trigger: v.trigger.into(),
            }
        }
    }

    impl TryFrom<FlowRunActivationCause> for dtos::flows::FlowRunActivationCause {
        type Error = ValidationError;
        fn try_from(v: FlowRunActivationCause) -> Result<Self, ValidationError> {
            Ok(Self {
                activation_time: v.activation_time,
                initiator: v
                    .initiator
                    .map(|v| dtos::auth::AccountHandle::try_from(v))
                    .transpose()?,
                trigger: dtos::flows::FlowTrigger::try_from(v.trigger)?,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowRunActivationCause, FlowRunActivationCause);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunActivationCauses
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowRunActivationCauses {
        pub activation_causes: Vec<flows::FlowRunActivationCause>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub late_activation_causes: Option<Vec<flows::FlowRunActivationCause>>,
    }

    impl IntoDto for FlowRunActivationCauses {
        type Dto = dtos::flows::FlowRunActivationCauses;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowRunActivationCauses> for FlowRunActivationCauses {
        fn from(v: dtos::flows::FlowRunActivationCauses) -> Self {
            Self {
                activation_causes: v.activation_causes.into_iter().map(Into::into).collect(),
                late_activation_causes: v
                    .late_activation_causes
                    .map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<FlowRunActivationCauses> for dtos::flows::FlowRunActivationCauses {
        type Error = ValidationError;
        fn try_from(v: FlowRunActivationCauses) -> Result<Self, ValidationError> {
            Ok(Self {
                activation_causes: v
                    .activation_causes
                    .into_iter()
                    .map(|i| dtos::flows::FlowRunActivationCause::try_from(i))
                    .collect::<Result<_, _>>()?,
                late_activation_causes: v
                    .late_activation_causes
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::flows::FlowRunActivationCause::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(
        dtos::flows::FlowRunActivationCauses,
        FlowRunActivationCauses
    );

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunRetry
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowRunRetry {
        pub retry_of: resources::ResourceHandle,
    }

    impl IntoDto for FlowRunRetry {
        type Dto = dtos::flows::FlowRunRetry;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowRunRetry> for FlowRunRetry {
        fn from(v: dtos::flows::FlowRunRetry) -> Self {
            Self {
                retry_of: v.retry_of.into(),
            }
        }
    }

    impl TryFrom<FlowRunRetry> for dtos::flows::FlowRunRetry {
        type Error = ValidationError;
        fn try_from(v: FlowRunRetry) -> Result<Self, ValidationError> {
            Ok(Self {
                retry_of: dtos::resources::ResourceHandle::try_from(v.retry_of)?,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowRunRetry, FlowRunRetry);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowRunSpec {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub target: Option<resources::ResourceHandle>,
        pub tasks: Vec<tasks::TaskSpec>,
    }

    impl IntoDto for FlowRunSpec {
        type Dto = dtos::flows::FlowRunSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowRunSpec> for FlowRunSpec {
        fn from(v: dtos::flows::FlowRunSpec) -> Self {
            Self {
                target: v.target.map(|v| v.into()),
                tasks: v.tasks.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<FlowRunSpec> for dtos::flows::FlowRunSpec {
        type Error = ValidationError;
        fn try_from(v: FlowRunSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                target: v
                    .target
                    .map(|v| dtos::resources::ResourceHandle::try_from(v))
                    .transpose()?,
                tasks: v
                    .tasks
                    .into_iter()
                    .map(|i| dtos::tasks::TaskSpec::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowRunSpec, FlowRunSpec);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowRunSpecInput {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub target: Option<StructOrString<resources::ResourceRef>>,
        pub tasks: Vec<tasks::TaskSpecInput>,
    }

    impl IntoDto for FlowRunSpecInput {
        type Dto = dtos::flows::FlowRunSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowRunSpecInput> for FlowRunSpecInput {
        fn from(v: dtos::flows::FlowRunSpecInput) -> Self {
            Self {
                target: v.target.map(|v| v.into()),
                tasks: v.tasks.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<FlowRunSpecInput> for dtos::flows::FlowRunSpecInput {
        type Error = ValidationError;
        fn try_from(v: FlowRunSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                target: v
                    .target
                    .map(|v| dtos::resources::ResourceRef::try_from(v))
                    .transpose()?,
                tasks: v
                    .tasks
                    .into_iter()
                    .map(|i| dtos::tasks::TaskSpecInput::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowRunSpecInput, FlowRunSpecInput);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunStatus
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowRunStatus {
        pub status: flows::FlowRunStatusValue,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub tasks: Option<Vec<flows::FlowRunStatusTaskEntry>>,
    }

    impl IntoDto for FlowRunStatus {
        type Dto = dtos::flows::FlowRunStatus;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowRunStatus> for FlowRunStatus {
        fn from(v: dtos::flows::FlowRunStatus) -> Self {
            Self {
                status: v.status.into(),
                tasks: v.tasks.map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<FlowRunStatus> for dtos::flows::FlowRunStatus {
        type Error = ValidationError;
        fn try_from(v: FlowRunStatus) -> Result<Self, ValidationError> {
            Ok(Self {
                status: dtos::flows::FlowRunStatusValue::try_from(v.status)?,
                tasks: v
                    .tasks
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::flows::FlowRunStatusTaskEntry::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowRunStatus, FlowRunStatus);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunStatus#/$defs/TaskEntry
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowRunStatusTaskEntry {
        pub name: String,
        pub task: resources::ResourceHandle,
        pub status: tasks::TaskStatus,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub outcome: Option<tasks::TaskOutcome>,
        #[serde(with = "datetime_rfc3339")]
        pub last_updated_at: DateTime<Utc>,
    }

    impl IntoDto for FlowRunStatusTaskEntry {
        type Dto = dtos::flows::FlowRunStatusTaskEntry;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowRunStatusTaskEntry> for FlowRunStatusTaskEntry {
        fn from(v: dtos::flows::FlowRunStatusTaskEntry) -> Self {
            Self {
                name: v.name,
                task: v.task.into(),
                status: v.status.into(),
                outcome: v.outcome.map(|v| v.into()),
                last_updated_at: v.last_updated_at,
            }
        }
    }

    impl TryFrom<FlowRunStatusTaskEntry> for dtos::flows::FlowRunStatusTaskEntry {
        type Error = ValidationError;
        fn try_from(v: FlowRunStatusTaskEntry) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                task: dtos::resources::ResourceHandle::try_from(v.task)?,
                status: dtos::tasks::TaskStatus::try_from(v.status)?,
                outcome: v
                    .outcome
                    .map(|v| dtos::tasks::TaskOutcome::try_from(v))
                    .transpose()?,
                last_updated_at: v.last_updated_at,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowRunStatusTaskEntry, FlowRunStatusTaskEntry);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunStatus#/$defs/Value
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum FlowRunStatusValue {
        #[serde(alias = "waiting")]
        Waiting,
        #[serde(alias = "running")]
        Running,
        #[serde(alias = "finished")]
        Finished,
    }

    impl IntoDto for FlowRunStatusValue {
        type Dto = dtos::flows::FlowRunStatusValue;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowRunStatusValue> for FlowRunStatusValue {
        fn from(v: dtos::flows::FlowRunStatusValue) -> Self {
            match v {
                dtos::flows::FlowRunStatusValue::Waiting => Self::Waiting,
                dtos::flows::FlowRunStatusValue::Running => Self::Running,
                dtos::flows::FlowRunStatusValue::Finished => Self::Finished,
            }
        }
    }

    impl TryFrom<FlowRunStatusValue> for dtos::flows::FlowRunStatusValue {
        type Error = ValidationError;
        fn try_from(v: FlowRunStatusValue) -> Result<Self, Self::Error> {
            match v {
                FlowRunStatusValue::Waiting => Ok(Self::Waiting),
                FlowRunStatusValue::Running => Ok(Self::Running),
                FlowRunStatusValue::Finished => Ok(Self::Finished),
            }
        }
    }

    implement_serde_as!(dtos::flows::FlowRunStatusValue, FlowRunStatusValue);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowSpec {
        pub target: StructOrString<resources::ResourceSelector>,
        pub triggers: Vec<flows::FlowTrigger>,
        pub tasks: Vec<tasks::TaskSpec>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub retry_policy: Option<flows::RetryPolicy>,
    }

    impl IntoDto for FlowSpec {
        type Dto = dtos::flows::FlowSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowSpec> for FlowSpec {
        fn from(v: dtos::flows::FlowSpec) -> Self {
            Self {
                target: v.target.into(),
                triggers: v.triggers.into_iter().map(Into::into).collect(),
                tasks: v.tasks.into_iter().map(Into::into).collect(),
                retry_policy: v.retry_policy.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<FlowSpec> for dtos::flows::FlowSpec {
        type Error = ValidationError;
        fn try_from(v: FlowSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                target: dtos::resources::ResourceSelector::try_from(v.target)?,
                triggers: v
                    .triggers
                    .into_iter()
                    .map(|i| dtos::flows::FlowTrigger::try_from(i))
                    .collect::<Result<_, _>>()?,
                tasks: v
                    .tasks
                    .into_iter()
                    .map(|i| dtos::tasks::TaskSpec::try_from(i))
                    .collect::<Result<_, _>>()?,
                retry_policy: v
                    .retry_policy
                    .map(|v| dtos::flows::RetryPolicy::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowSpec, FlowSpec);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowSpecInput {
        pub target: StructOrString<resources::ResourceSelector>,
        pub triggers: Vec<flows::FlowTriggerInput>,
        pub tasks: Vec<tasks::TaskSpecInput>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub retry_policy: Option<flows::RetryPolicy>,
    }

    impl IntoDto for FlowSpecInput {
        type Dto = dtos::flows::FlowSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowSpecInput> for FlowSpecInput {
        fn from(v: dtos::flows::FlowSpecInput) -> Self {
            Self {
                target: v.target.into(),
                triggers: v.triggers.into_iter().map(Into::into).collect(),
                tasks: v.tasks.into_iter().map(Into::into).collect(),
                retry_policy: v.retry_policy.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<FlowSpecInput> for dtos::flows::FlowSpecInput {
        type Error = ValidationError;
        fn try_from(v: FlowSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                target: dtos::resources::ResourceSelector::try_from(v.target)?,
                triggers: v
                    .triggers
                    .into_iter()
                    .map(|i| dtos::flows::FlowTriggerInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                tasks: v
                    .tasks
                    .into_iter()
                    .map(|i| dtos::tasks::TaskSpecInput::try_from(i))
                    .collect::<Result<_, _>>()?,
                retry_policy: v
                    .retry_policy
                    .map(|v| dtos::flows::RetryPolicy::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowSpecInput, FlowSpecInput);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum FlowTrigger {
        #[serde(alias = "manual")]
        Manual(flows::FlowTriggerManual),
        #[serde(alias = "schedule")]
        Schedule(flows::FlowTriggerSchedule),
        #[serde(alias = "event")]
        Event(flows::FlowTriggerEvent),
        #[serde(alias = "source")]
        Source(flows::FlowTriggerSource),
        #[serde(alias = "dataset")]
        Dataset(flows::FlowTriggerDataset),
    }

    impl IntoDto for FlowTrigger {
        type Dto = dtos::flows::FlowTrigger;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTrigger> for FlowTrigger {
        fn from(v: dtos::flows::FlowTrigger) -> Self {
            match v {
                dtos::flows::FlowTrigger::Manual(v) => Self::Manual(v.into()),
                dtos::flows::FlowTrigger::Schedule(v) => Self::Schedule(v.into()),
                dtos::flows::FlowTrigger::Event(v) => Self::Event(v.into()),
                dtos::flows::FlowTrigger::Source(v) => Self::Source(v.into()),
                dtos::flows::FlowTrigger::Dataset(v) => Self::Dataset(v.into()),
            }
        }
    }

    impl TryFrom<FlowTrigger> for dtos::flows::FlowTrigger {
        type Error = ValidationError;
        fn try_from(v: FlowTrigger) -> Result<Self, Self::Error> {
            match v {
                FlowTrigger::Manual(v) => Ok(Self::Manual(v.try_into()?)),
                FlowTrigger::Schedule(v) => Ok(Self::Schedule(v.try_into()?)),
                FlowTrigger::Event(v) => Ok(Self::Event(v.try_into()?)),
                FlowTrigger::Source(v) => Ok(Self::Source(v.try_into()?)),
                FlowTrigger::Dataset(v) => Ok(Self::Dataset(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::flows::FlowTrigger, FlowTrigger);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Dataset
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerDataset {
        pub dataset: StructOrString<datasets::DatasetSelector>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub events: Option<Vec<String>>,
    }

    impl IntoDto for FlowTriggerDataset {
        type Dto = dtos::flows::FlowTriggerDataset;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerDataset> for FlowTriggerDataset {
        fn from(v: dtos::flows::FlowTriggerDataset) -> Self {
            Self {
                dataset: v.dataset.into(),
                events: v.events,
            }
        }
    }

    impl TryFrom<FlowTriggerDataset> for dtos::flows::FlowTriggerDataset {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerDataset) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset: dtos::datasets::DatasetSelector::try_from(v.dataset)?,
                events: v.events,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerDataset, FlowTriggerDataset);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Event
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerEvent {
        pub events: events::EventFilter,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cooldown: Option<DurationString>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cooldown_max_batch: Option<u64>,
    }

    impl IntoDto for FlowTriggerEvent {
        type Dto = dtos::flows::FlowTriggerEvent;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerEvent> for FlowTriggerEvent {
        fn from(v: dtos::flows::FlowTriggerEvent) -> Self {
            Self {
                events: v.events.into(),
                cooldown: v.cooldown,
                cooldown_max_batch: v.cooldown_max_batch,
            }
        }
    }

    impl TryFrom<FlowTriggerEvent> for dtos::flows::FlowTriggerEvent {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerEvent) -> Result<Self, ValidationError> {
            Ok(Self {
                events: dtos::events::EventFilter::try_from(v.events)?,
                cooldown: v.cooldown,
                cooldown_max_batch: v.cooldown_max_batch,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerEvent, FlowTriggerEvent);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum FlowTriggerInput {
        #[serde(alias = "manual")]
        Manual(flows::FlowTriggerInputManual),
        #[serde(alias = "schedule")]
        Schedule(flows::FlowTriggerInputSchedule),
        #[serde(alias = "event")]
        Event(flows::FlowTriggerInputEvent),
        #[serde(alias = "source")]
        Source(flows::FlowTriggerInputSource),
        #[serde(alias = "dataset")]
        Dataset(flows::FlowTriggerInputDataset),
    }

    impl IntoDto for FlowTriggerInput {
        type Dto = dtos::flows::FlowTriggerInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerInput> for FlowTriggerInput {
        fn from(v: dtos::flows::FlowTriggerInput) -> Self {
            match v {
                dtos::flows::FlowTriggerInput::Manual(v) => Self::Manual(v.into()),
                dtos::flows::FlowTriggerInput::Schedule(v) => Self::Schedule(v.into()),
                dtos::flows::FlowTriggerInput::Event(v) => Self::Event(v.into()),
                dtos::flows::FlowTriggerInput::Source(v) => Self::Source(v.into()),
                dtos::flows::FlowTriggerInput::Dataset(v) => Self::Dataset(v.into()),
            }
        }
    }

    impl TryFrom<FlowTriggerInput> for dtos::flows::FlowTriggerInput {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerInput) -> Result<Self, Self::Error> {
            match v {
                FlowTriggerInput::Manual(v) => Ok(Self::Manual(v.try_into()?)),
                FlowTriggerInput::Schedule(v) => Ok(Self::Schedule(v.try_into()?)),
                FlowTriggerInput::Event(v) => Ok(Self::Event(v.try_into()?)),
                FlowTriggerInput::Source(v) => Ok(Self::Source(v.try_into()?)),
                FlowTriggerInput::Dataset(v) => Ok(Self::Dataset(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerInput, FlowTriggerInput);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Dataset
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerInputDataset {
        pub dataset: StructOrString<datasets::DatasetSelector>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub events: Option<Vec<String>>,
    }

    impl IntoDto for FlowTriggerInputDataset {
        type Dto = dtos::flows::FlowTriggerInputDataset;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerInputDataset> for FlowTriggerInputDataset {
        fn from(v: dtos::flows::FlowTriggerInputDataset) -> Self {
            Self {
                dataset: v.dataset.into(),
                events: v.events,
            }
        }
    }

    impl TryFrom<FlowTriggerInputDataset> for dtos::flows::FlowTriggerInputDataset {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerInputDataset) -> Result<Self, ValidationError> {
            Ok(Self {
                dataset: dtos::datasets::DatasetSelector::try_from(v.dataset)?,
                events: v.events,
            })
        }
    }

    implement_serde_as!(
        dtos::flows::FlowTriggerInputDataset,
        FlowTriggerInputDataset
    );

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Event
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerInputEvent {
        pub events: events::EventFilter,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cooldown: Option<DurationString>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cooldown_max_batch: Option<u64>,
    }

    impl IntoDto for FlowTriggerInputEvent {
        type Dto = dtos::flows::FlowTriggerInputEvent;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerInputEvent> for FlowTriggerInputEvent {
        fn from(v: dtos::flows::FlowTriggerInputEvent) -> Self {
            Self {
                events: v.events.into(),
                cooldown: v.cooldown,
                cooldown_max_batch: v.cooldown_max_batch,
            }
        }
    }

    impl TryFrom<FlowTriggerInputEvent> for dtos::flows::FlowTriggerInputEvent {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerInputEvent) -> Result<Self, ValidationError> {
            Ok(Self {
                events: dtos::events::EventFilter::try_from(v.events)?,
                cooldown: v.cooldown,
                cooldown_max_batch: v.cooldown_max_batch,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerInputEvent, FlowTriggerInputEvent);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Manual
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerInputManual {}

    impl IntoDto for FlowTriggerInputManual {
        type Dto = dtos::flows::FlowTriggerInputManual;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerInputManual> for FlowTriggerInputManual {
        fn from(v: dtos::flows::FlowTriggerInputManual) -> Self {
            Self {}
        }
    }

    impl TryFrom<FlowTriggerInputManual> for dtos::flows::FlowTriggerInputManual {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerInputManual) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerInputManual, FlowTriggerInputManual);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Schedule
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerInputSchedule {
        pub cron: String,
    }

    impl IntoDto for FlowTriggerInputSchedule {
        type Dto = dtos::flows::FlowTriggerInputSchedule;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerInputSchedule> for FlowTriggerInputSchedule {
        fn from(v: dtos::flows::FlowTriggerInputSchedule) -> Self {
            Self { cron: v.cron }
        }
    }

    impl TryFrom<FlowTriggerInputSchedule> for dtos::flows::FlowTriggerInputSchedule {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerInputSchedule) -> Result<Self, ValidationError> {
            Ok(Self { cron: v.cron })
        }
    }

    implement_serde_as!(
        dtos::flows::FlowTriggerInputSchedule,
        FlowTriggerInputSchedule
    );

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Source
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerInputSource {
        pub source: StructOrString<resources::ResourceRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub min_records_to_await: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_await_interval: Option<DurationString>,
    }

    impl IntoDto for FlowTriggerInputSource {
        type Dto = dtos::flows::FlowTriggerInputSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerInputSource> for FlowTriggerInputSource {
        fn from(v: dtos::flows::FlowTriggerInputSource) -> Self {
            Self {
                source: v.source.into(),
                min_records_to_await: v.min_records_to_await,
                max_await_interval: v.max_await_interval,
            }
        }
    }

    impl TryFrom<FlowTriggerInputSource> for dtos::flows::FlowTriggerInputSource {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerInputSource) -> Result<Self, ValidationError> {
            Ok(Self {
                source: dtos::resources::ResourceRef::try_from(v.source)?,
                min_records_to_await: v.min_records_to_await,
                max_await_interval: v.max_await_interval,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerInputSource, FlowTriggerInputSource);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Manual
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerManual {}

    impl IntoDto for FlowTriggerManual {
        type Dto = dtos::flows::FlowTriggerManual;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerManual> for FlowTriggerManual {
        fn from(v: dtos::flows::FlowTriggerManual) -> Self {
            Self {}
        }
    }

    impl TryFrom<FlowTriggerManual> for dtos::flows::FlowTriggerManual {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerManual) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerManual, FlowTriggerManual);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Schedule
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerSchedule {
        pub cron: String,
    }

    impl IntoDto for FlowTriggerSchedule {
        type Dto = dtos::flows::FlowTriggerSchedule;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerSchedule> for FlowTriggerSchedule {
        fn from(v: dtos::flows::FlowTriggerSchedule) -> Self {
            Self { cron: v.cron }
        }
    }

    impl TryFrom<FlowTriggerSchedule> for dtos::flows::FlowTriggerSchedule {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerSchedule) -> Result<Self, ValidationError> {
            Ok(Self { cron: v.cron })
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerSchedule, FlowTriggerSchedule);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Source
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct FlowTriggerSource {
        pub source: resources::ResourceHandle,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub min_records_to_await: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_await_interval: Option<DurationString>,
    }

    impl IntoDto for FlowTriggerSource {
        type Dto = dtos::flows::FlowTriggerSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::FlowTriggerSource> for FlowTriggerSource {
        fn from(v: dtos::flows::FlowTriggerSource) -> Self {
            Self {
                source: v.source.into(),
                min_records_to_await: v.min_records_to_await,
                max_await_interval: v.max_await_interval,
            }
        }
    }

    impl TryFrom<FlowTriggerSource> for dtos::flows::FlowTriggerSource {
        type Error = ValidationError;
        fn try_from(v: FlowTriggerSource) -> Result<Self, ValidationError> {
            Ok(Self {
                source: dtos::resources::ResourceHandle::try_from(v.source)?,
                min_records_to_await: v.min_records_to_await,
                max_await_interval: v.max_await_interval,
            })
        }
    }

    implement_serde_as!(dtos::flows::FlowTriggerSource, FlowTriggerSource);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/RetryBackoff
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum RetryBackoff {
        #[serde(alias = "linear")]
        Linear,
        #[serde(alias = "exponential")]
        Exponential,
    }

    impl IntoDto for RetryBackoff {
        type Dto = dtos::flows::RetryBackoff;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::RetryBackoff> for RetryBackoff {
        fn from(v: dtos::flows::RetryBackoff) -> Self {
            match v {
                dtos::flows::RetryBackoff::Linear => Self::Linear,
                dtos::flows::RetryBackoff::Exponential => Self::Exponential,
            }
        }
    }

    impl TryFrom<RetryBackoff> for dtos::flows::RetryBackoff {
        type Error = ValidationError;
        fn try_from(v: RetryBackoff) -> Result<Self, Self::Error> {
            match v {
                RetryBackoff::Linear => Ok(Self::Linear),
                RetryBackoff::Exponential => Ok(Self::Exponential),
            }
        }
    }

    implement_serde_as!(dtos::flows::RetryBackoff, RetryBackoff);

    // Schema: https://opendatafabric.org/schemas/flows/v1alpha1/RetryPolicy
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RetryPolicy {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_attempts: Option<u32>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub min_delay: Option<DurationString>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub backoff: Option<flows::RetryBackoff>,
    }

    impl IntoDto for RetryPolicy {
        type Dto = dtos::flows::RetryPolicy;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::flows::RetryPolicy> for RetryPolicy {
        fn from(v: dtos::flows::RetryPolicy) -> Self {
            Self {
                max_attempts: v.max_attempts,
                min_delay: v.min_delay,
                backoff: v.backoff.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<RetryPolicy> for dtos::flows::RetryPolicy {
        type Error = ValidationError;
        fn try_from(v: RetryPolicy) -> Result<Self, ValidationError> {
            Ok(Self {
                max_attempts: v.max_attempts,
                min_delay: v.min_delay,
                backoff: v
                    .backoff
                    .map(|v| dtos::flows::RetryBackoff::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::flows::RetryPolicy, RetryPolicy);
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
        pub read: sources::ReadStep,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub preprocess: Option<datasets::Transform>,
        pub merge: sources::MergeStrategy,
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
                read: dtos::sources::ReadStep::try_from(v.read)?,
                preprocess: v
                    .preprocess
                    .map(|v| dtos::datasets::Transform::try_from(v))
                    .transpose()?,
                merge: dtos::sources::MergeStrategy::try_from(v.merge)?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::AddPushSource, AddPushSource);

    // Schema: https://opendatafabric.org/schemas/legacy/v0/DatasetSnapshot
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct DatasetSnapshot {
        pub name: odf::datasets::legacy::DatasetAlias,
        pub kind: datasets::DatasetKind,
        pub metadata: Vec<datasets::MetadataEvent>,
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
                kind: dtos::datasets::DatasetKind::try_from(v.kind)?,
                metadata: v
                    .metadata
                    .into_iter()
                    .map(|i| dtos::datasets::MetadataEvent::try_from(i))
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
        pub env: Option<Vec<sources::EnvVar>>,
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
                            .map(|i| dtos::sources::EnvVar::try_from(i))
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
        pub event_time: Option<UnionOrString<sources::EventTimeSource>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cache: Option<UnionOrString<sources::SourceCaching>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub order: Option<sources::SourceOrdering>,
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
                    .map(|v| dtos::sources::EventTimeSource::try_from(v))
                    .transpose()?,
                cache: v
                    .cache
                    .map(|v| dtos::sources::SourceCaching::try_from(v))
                    .transpose()?,
                order: v
                    .order
                    .map(|v| dtos::sources::SourceOrdering::try_from(v))
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
        pub topics: Vec<sources::MqttTopicSubscription>,
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
                    .map(|i| dtos::sources::MqttTopicSubscription::try_from(i))
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
        pub event_time: Option<UnionOrString<sources::EventTimeSource>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cache: Option<UnionOrString<sources::SourceCaching>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub headers: Option<Vec<sources::RequestHeader>>,
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
                    .map(|v| dtos::sources::EventTimeSource::try_from(v))
                    .transpose()?,
                cache: v
                    .cache
                    .map(|v| dtos::sources::SourceCaching::try_from(v))
                    .transpose()?,
                headers: v
                    .headers
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::sources::RequestHeader::try_from(i))
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
        pub prepare: Option<Vec<sources::PrepStep>>,
        pub read: sources::ReadStep,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub preprocess: Option<datasets::Transform>,
        pub merge: sources::MergeStrategy,
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
                            .map(|i| dtos::sources::PrepStep::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                read: dtos::sources::ReadStep::try_from(v.read)?,
                preprocess: v
                    .preprocess
                    .map(|v| dtos::datasets::Transform::try_from(v))
                    .transpose()?,
                merge: dtos::sources::MergeStrategy::try_from(v.merge)?,
            })
        }
    }

    implement_serde_as!(dtos::legacy::SetPollingSource, SetPollingSource);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// resources
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod resources {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/LabelFilter
    #[derive(Debug, Serialize, Deserialize)]
    pub struct LabelFilter {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    impl IntoDto for LabelFilter {
        type Dto = dtos::resources::LabelFilter;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::LabelFilter> for LabelFilter {
        fn from(v: dtos::resources::LabelFilter) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<LabelFilter> for dtos::resources::LabelFilter {
        type Error = ValidationError;
        fn try_from(v: LabelFilter) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::resources::LabelFilter, LabelFilter);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/Resource
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct Resource<SpecT> {
        #[serde(rename = "$schema")]
        pub schema: odf::resources::TypeUri,
        pub headers: resources::ResourceHeaders,
        pub spec: SpecT,
        pub status: resources::ResourceStatus,
    }

    impl<SpecT> IntoDto for Resource<SpecT>
    where
        SpecT: IntoDto,
        <SpecT as IntoDto>::Dto: TryFrom<SpecT>,
        ValidationError: From<<<SpecT as IntoDto>::Dto as TryFrom<SpecT>>::Error>,
    {
        type Dto = dtos::resources::Resource<SpecT::Dto>;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl<SpecTFrom, SpecTTo> From<dtos::resources::Resource<SpecTFrom>> for Resource<SpecTTo>
    where
        SpecTTo: From<SpecTFrom>,
    {
        fn from(v: dtos::resources::Resource<SpecTFrom>) -> Self {
            Self {
                schema: v.schema,
                headers: v.headers.into(),
                spec: v.spec.into(),
                status: v.status.into(),
            }
        }
    }

    impl<SpecTFrom, SpecTTo> TryFrom<Resource<SpecTFrom>> for dtos::resources::Resource<SpecTTo>
    where
        SpecTTo: TryFrom<SpecTFrom>,
        ValidationError: From<<SpecTTo as TryFrom<SpecTFrom>>::Error>,
    {
        type Error = ValidationError;
        fn try_from(v: Resource<SpecTFrom>) -> Result<Self, ValidationError> {
            Ok(Self {
                schema: v.schema,
                headers: dtos::resources::ResourceHeaders::try_from(v.headers)?,
                spec: SpecTTo::try_from(v.spec)?,
                status: dtos::resources::ResourceStatus::try_from(v.status)?,
            })
        }
    }

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceAnnotations
    #[derive(Debug, Serialize, Deserialize)]
    pub struct ResourceAnnotations {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<odf::resources::TypeRef, serde_json::Value>,
    }

    impl IntoDto for ResourceAnnotations {
        type Dto = dtos::resources::ResourceAnnotations;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::ResourceAnnotations> for ResourceAnnotations {
        fn from(v: dtos::resources::ResourceAnnotations) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<ResourceAnnotations> for dtos::resources::ResourceAnnotations {
        type Error = ValidationError;
        fn try_from(v: ResourceAnnotations) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::resources::ResourceAnnotations, ResourceAnnotations);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceConditions
    #[derive(Debug, Serialize, Deserialize)]
    pub struct ResourceConditions {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<odf::resources::TypeRef, serde_json::Value>,
    }

    impl IntoDto for ResourceConditions {
        type Dto = dtos::resources::ResourceConditions;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::ResourceConditions> for ResourceConditions {
        fn from(v: dtos::resources::ResourceConditions) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<ResourceConditions> for dtos::resources::ResourceConditions {
        type Error = ValidationError;
        fn try_from(v: ResourceConditions) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::resources::ResourceConditions, ResourceConditions);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceHandle
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceHandle {
        pub account: auth::AccountHandle,
        pub r#type: odf::resources::TypeUri,
        pub id: odf::resources::ResourceID,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<odf::Did>,
        pub name: odf::resources::ResourceName,
    }

    impl IntoDto for ResourceHandle {
        type Dto = dtos::resources::ResourceHandle;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::ResourceHandle> for ResourceHandle {
        fn from(v: dtos::resources::ResourceHandle) -> Self {
            Self {
                account: v.account.into(),
                r#type: v.r#type,
                id: v.id,
                did: v.did,
                name: v.name,
            }
        }
    }

    impl TryFrom<ResourceHandle> for dtos::resources::ResourceHandle {
        type Error = ValidationError;
        fn try_from(v: ResourceHandle) -> Result<Self, ValidationError> {
            Ok(Self {
                account: dtos::auth::AccountHandle::try_from(v.account)?,
                r#type: v.r#type,
                id: v.id,
                did: v.did,
                name: v.name,
            })
        }
    }

    implement_serde_as!(dtos::resources::ResourceHandle, ResourceHandle);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceHeaders
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceHeaders {
        pub id: odf::resources::ResourceID,
        pub name: odf::resources::ResourceName,
        pub account: auth::AccountHandle,
        pub labels: resources::ResourceLabels,
        pub annotations: resources::ResourceAnnotations,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub owner_references: Option<Vec<resources::ResourceHandle>>,
        pub generation: u64,
        #[serde(with = "datetime_rfc3339")]
        pub created_at: DateTime<Utc>,
        #[serde(with = "datetime_rfc3339")]
        pub updated_at: DateTime<Utc>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub deleted_at: Option<DateTime<Utc>>,
    }

    impl IntoDto for ResourceHeaders {
        type Dto = dtos::resources::ResourceHeaders;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::ResourceHeaders> for ResourceHeaders {
        fn from(v: dtos::resources::ResourceHeaders) -> Self {
            Self {
                id: v.id,
                name: v.name,
                account: v.account.into(),
                labels: v.labels.into(),
                annotations: v.annotations.into(),
                owner_references: v
                    .owner_references
                    .map(|v| v.into_iter().map(Into::into).collect()),
                generation: v.generation,
                created_at: v.created_at,
                updated_at: v.updated_at,
                deleted_at: v.deleted_at,
            }
        }
    }

    impl TryFrom<ResourceHeaders> for dtos::resources::ResourceHeaders {
        type Error = ValidationError;
        fn try_from(v: ResourceHeaders) -> Result<Self, ValidationError> {
            Ok(Self {
                id: v.id,
                name: v.name,
                account: dtos::auth::AccountHandle::try_from(v.account)?,
                labels: dtos::resources::ResourceLabels::try_from(v.labels)?,
                annotations: dtos::resources::ResourceAnnotations::try_from(v.annotations)?,
                owner_references: v
                    .owner_references
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::resources::ResourceHandle::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                generation: v.generation,
                created_at: v.created_at,
                updated_at: v.updated_at,
                deleted_at: v.deleted_at,
            })
        }
    }

    implement_serde_as!(dtos::resources::ResourceHeaders, ResourceHeaders);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceHeadersInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceHeadersInput {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<odf::resources::ResourceID>,
        pub name: odf::resources::ResourceName,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub labels: Option<resources::ResourceLabels>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub annotations: Option<resources::ResourceAnnotations>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub owner_references: Option<Vec<StructOrString<resources::ResourceRef>>>,
    }

    impl IntoDto for ResourceHeadersInput {
        type Dto = dtos::resources::ResourceHeadersInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::ResourceHeadersInput> for ResourceHeadersInput {
        fn from(v: dtos::resources::ResourceHeadersInput) -> Self {
            Self {
                id: v.id,
                name: v.name,
                account: v.account.map(|v| v.into()),
                labels: v.labels.map(|v| v.into()),
                annotations: v.annotations.map(|v| v.into()),
                owner_references: v
                    .owner_references
                    .map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<ResourceHeadersInput> for dtos::resources::ResourceHeadersInput {
        type Error = ValidationError;
        fn try_from(v: ResourceHeadersInput) -> Result<Self, ValidationError> {
            Ok(Self {
                id: v.id,
                name: v.name,
                account: v
                    .account
                    .map(|v| dtos::auth::AccountRef::try_from(v))
                    .transpose()?,
                labels: v
                    .labels
                    .map(|v| dtos::resources::ResourceLabels::try_from(v))
                    .transpose()?,
                annotations: v
                    .annotations
                    .map(|v| dtos::resources::ResourceAnnotations::try_from(v))
                    .transpose()?,
                owner_references: v
                    .owner_references
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::resources::ResourceRef::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::resources::ResourceHeadersInput, ResourceHeadersInput);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceInput<SpecT> {
        #[serde(rename = "$schema")]
        pub schema: odf::resources::TypeUri,
        pub headers: resources::ResourceHeadersInput,
        pub spec: SpecT,
    }

    impl<SpecT> IntoDto for ResourceInput<SpecT>
    where
        SpecT: IntoDto,
        <SpecT as IntoDto>::Dto: TryFrom<SpecT>,
        ValidationError: From<<<SpecT as IntoDto>::Dto as TryFrom<SpecT>>::Error>,
    {
        type Dto = dtos::resources::ResourceInput<SpecT::Dto>;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl<SpecTFrom, SpecTTo> From<dtos::resources::ResourceInput<SpecTFrom>> for ResourceInput<SpecTTo>
    where
        SpecTTo: From<SpecTFrom>,
    {
        fn from(v: dtos::resources::ResourceInput<SpecTFrom>) -> Self {
            Self {
                schema: v.schema,
                headers: v.headers.into(),
                spec: v.spec.into(),
            }
        }
    }

    impl<SpecTFrom, SpecTTo> TryFrom<ResourceInput<SpecTFrom>>
        for dtos::resources::ResourceInput<SpecTTo>
    where
        SpecTTo: TryFrom<SpecTFrom>,
        ValidationError: From<<SpecTTo as TryFrom<SpecTFrom>>::Error>,
    {
        type Error = ValidationError;
        fn try_from(v: ResourceInput<SpecTFrom>) -> Result<Self, ValidationError> {
            Ok(Self {
                schema: v.schema,
                headers: dtos::resources::ResourceHeadersInput::try_from(v.headers)?,
                spec: SpecTTo::try_from(v.spec)?,
            })
        }
    }

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceLabels
    #[derive(Debug, Serialize, Deserialize)]
    pub struct ResourceLabels {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<odf::resources::TypeRef, serde_json::Value>,
    }

    impl IntoDto for ResourceLabels {
        type Dto = dtos::resources::ResourceLabels;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::ResourceLabels> for ResourceLabels {
        fn from(v: dtos::resources::ResourceLabels) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<ResourceLabels> for dtos::resources::ResourceLabels {
        type Error = ValidationError;
        fn try_from(v: ResourceLabels) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::resources::ResourceLabels, ResourceLabels);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourcePhase
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum ResourcePhase {
        #[serde(alias = "pending")]
        Pending,
        #[serde(alias = "reconciling")]
        Reconciling,
        #[serde(alias = "ready")]
        Ready,
        #[serde(alias = "degraded")]
        Degraded,
        #[serde(alias = "failed")]
        Failed,
    }

    impl IntoDto for ResourcePhase {
        type Dto = dtos::resources::ResourcePhase;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::ResourcePhase> for ResourcePhase {
        fn from(v: dtos::resources::ResourcePhase) -> Self {
            match v {
                dtos::resources::ResourcePhase::Pending => Self::Pending,
                dtos::resources::ResourcePhase::Reconciling => Self::Reconciling,
                dtos::resources::ResourcePhase::Ready => Self::Ready,
                dtos::resources::ResourcePhase::Degraded => Self::Degraded,
                dtos::resources::ResourcePhase::Failed => Self::Failed,
            }
        }
    }

    impl TryFrom<ResourcePhase> for dtos::resources::ResourcePhase {
        type Error = ValidationError;
        fn try_from(v: ResourcePhase) -> Result<Self, Self::Error> {
            match v {
                ResourcePhase::Pending => Ok(Self::Pending),
                ResourcePhase::Reconciling => Ok(Self::Reconciling),
                ResourcePhase::Ready => Ok(Self::Ready),
                ResourcePhase::Degraded => Ok(Self::Degraded),
                ResourcePhase::Failed => Ok(Self::Failed),
            }
        }
    }

    implement_serde_as!(dtos::resources::ResourcePhase, ResourcePhase);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceRef
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceRef {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<odf::resources::ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<odf::Did>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub r#type: Option<odf::resources::TypeRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<odf::resources::ResourceName>,
    }

    impl IntoDto for ResourceRef {
        type Dto = dtos::resources::ResourceRef;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl std::str::FromStr for ResourceRef {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::resources::ResourceRef::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
        }
    }

    impl From<dtos::resources::ResourceRef> for StructOrString<ResourceRef> {
        fn from(v: dtos::resources::ResourceRef) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<ResourceRef>> for dtos::resources::ResourceRef {
        type Error = ValidationError;
        fn try_from(v: StructOrString<ResourceRef>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    impl From<dtos::resources::ResourceRef> for ResourceRef {
        fn from(v: dtos::resources::ResourceRef) -> Self {
            Self {
                account: v.account.map(|v| v.into()),
                id: v.id,
                did: v.did,
                r#type: v.r#type,
                name: v.name,
            }
        }
    }

    impl TryFrom<ResourceRef> for dtos::resources::ResourceRef {
        type Error = ValidationError;
        fn try_from(v: ResourceRef) -> Result<Self, ValidationError> {
            Ok(Self {
                account: v
                    .account
                    .map(|v| dtos::auth::AccountRef::try_from(v))
                    .transpose()?,
                id: v.id,
                did: v.did,
                r#type: v.r#type,
                name: v.name,
            })
        }
    }

    implement_serde_as!(dtos::resources::ResourceRef, ResourceRef);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceSelector
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceSelector {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub account: Option<StructOrString<auth::AccountRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<odf::resources::ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub did: Option<odf::Did>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub r#type: Option<odf::resources::TypeRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub labels: Option<resources::LabelFilter>,
    }

    impl IntoDto for ResourceSelector {
        type Dto = dtos::resources::ResourceSelector;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl std::str::FromStr for ResourceSelector {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::resources::ResourceSelector::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
        }
    }

    impl From<dtos::resources::ResourceSelector> for StructOrString<ResourceSelector> {
        fn from(v: dtos::resources::ResourceSelector) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<StructOrString<ResourceSelector>> for dtos::resources::ResourceSelector {
        type Error = ValidationError;
        fn try_from(v: StructOrString<ResourceSelector>) -> Result<Self, ValidationError> {
            v.0.try_into()
        }
    }

    implement_serde_as!(dtos::resources::ResourceSelector, ResourceSelector);

    // Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceStatus
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct ResourceStatus {
        pub phase: resources::ResourcePhase,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub observed_generation: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub observed_at: Option<DateTime<Utc>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub reconciled_generation: Option<u64>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "datetime_rfc3339_opt")]
        pub reconciled_at: Option<DateTime<Utc>>,
        pub conditions: resources::ResourceConditions,
    }

    impl IntoDto for ResourceStatus {
        type Dto = dtos::resources::ResourceStatus;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::resources::ResourceStatus> for ResourceStatus {
        fn from(v: dtos::resources::ResourceStatus) -> Self {
            Self {
                phase: v.phase.into(),
                observed_generation: v.observed_generation,
                observed_at: v.observed_at,
                reconciled_generation: v.reconciled_generation,
                reconciled_at: v.reconciled_at,
                conditions: v.conditions.into(),
            }
        }
    }

    impl TryFrom<ResourceStatus> for dtos::resources::ResourceStatus {
        type Error = ValidationError;
        fn try_from(v: ResourceStatus) -> Result<Self, ValidationError> {
            Ok(Self {
                phase: dtos::resources::ResourcePhase::try_from(v.phase)?,
                observed_generation: v.observed_generation,
                observed_at: v.observed_at,
                reconciled_generation: v.reconciled_generation,
                reconciled_at: v.reconciled_at,
                conditions: dtos::resources::ResourceConditions::try_from(v.conditions)?,
            })
        }
    }

    implement_serde_as!(dtos::resources::ResourceStatus, ResourceStatus);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// sinks
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod sinks {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/sinks/v1alpha1/WebhookTargetSpec
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
        type Dto = dtos::sinks::WebhookTargetSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sinks::WebhookTargetSpec> for WebhookTargetSpec {
        fn from(v: dtos::sinks::WebhookTargetSpec) -> Self {
            Self {
                url: v.url,
                secret: v.secret.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<WebhookTargetSpec> for dtos::sinks::WebhookTargetSpec {
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

    implement_serde_as!(dtos::sinks::WebhookTargetSpec, WebhookTargetSpec);

    // Schema: https://opendatafabric.org/schemas/sinks/v1alpha1/WebhookTargetSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct WebhookTargetSpecInput {
        pub url: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub secret: Option<StructOrString<config::Secret>>,
    }

    impl IntoDto for WebhookTargetSpecInput {
        type Dto = dtos::sinks::WebhookTargetSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sinks::WebhookTargetSpecInput> for WebhookTargetSpecInput {
        fn from(v: dtos::sinks::WebhookTargetSpecInput) -> Self {
            Self {
                url: v.url,
                secret: v.secret.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<WebhookTargetSpecInput> for dtos::sinks::WebhookTargetSpecInput {
        type Error = ValidationError;
        fn try_from(v: WebhookTargetSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                url: v.url,
                secret: v
                    .secret
                    .map(|v| dtos::config::Secret::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sinks::WebhookTargetSpecInput, WebhookTargetSpecInput);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// sources
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod sources {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/CompressionFormat
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum CompressionFormat {
        #[serde(alias = "gzip")]
        Gzip,
        #[serde(alias = "zip")]
        Zip,
    }

    impl IntoDto for CompressionFormat {
        type Dto = dtos::sources::CompressionFormat;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::CompressionFormat> for CompressionFormat {
        fn from(v: dtos::sources::CompressionFormat) -> Self {
            match v {
                dtos::sources::CompressionFormat::Gzip => Self::Gzip,
                dtos::sources::CompressionFormat::Zip => Self::Zip,
            }
        }
    }

    impl TryFrom<CompressionFormat> for dtos::sources::CompressionFormat {
        type Error = ValidationError;
        fn try_from(v: CompressionFormat) -> Result<Self, Self::Error> {
            match v {
                CompressionFormat::Gzip => Ok(Self::Gzip),
                CompressionFormat::Zip => Ok(Self::Zip),
            }
        }
    }

    implement_serde_as!(dtos::sources::CompressionFormat, CompressionFormat);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EnvVar
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
        type Dto = dtos::sources::EnvVar;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::EnvVar> for EnvVar {
        fn from(v: dtos::sources::EnvVar) -> Self {
            Self {
                name: v.name,
                value: v.value,
            }
        }
    }

    impl TryFrom<EnvVar> for dtos::sources::EnvVar {
        type Error = ValidationError;
        fn try_from(v: EnvVar) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                value: v.value,
            })
        }
    }

    implement_serde_as!(dtos::sources::EnvVar, EnvVar);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EventTimeSource
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum EventTimeSource {
        #[serde(alias = "fromMetadata", alias = "frommetadata")]
        FromMetadata(sources::EventTimeSourceFromMetadata),
        #[serde(alias = "fromPath", alias = "frompath")]
        FromPath(sources::EventTimeSourceFromPath),
        #[serde(alias = "fromSystemTime", alias = "fromsystemtime")]
        FromSystemTime(sources::EventTimeSourceFromSystemTime),
    }

    impl From<dtos::sources::EventTimeSource> for UnionOrString<EventTimeSource> {
        fn from(v: dtos::sources::EventTimeSource) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<UnionOrString<EventTimeSource>> for dtos::sources::EventTimeSource {
        type Error = ValidationError;
        fn try_from(v: UnionOrString<EventTimeSource>) -> Result<Self, Self::Error> {
            v.0.try_into()
        }
    }

    impl IntoDto for EventTimeSource {
        type Dto = dtos::sources::EventTimeSource;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::EventTimeSource> for EventTimeSource {
        fn from(v: dtos::sources::EventTimeSource) -> Self {
            match v {
                dtos::sources::EventTimeSource::FromMetadata(v) => Self::FromMetadata(v.into()),
                dtos::sources::EventTimeSource::FromPath(v) => Self::FromPath(v.into()),
                dtos::sources::EventTimeSource::FromSystemTime(v) => Self::FromSystemTime(v.into()),
            }
        }
    }

    impl TryFrom<EventTimeSource> for dtos::sources::EventTimeSource {
        type Error = ValidationError;
        fn try_from(v: EventTimeSource) -> Result<Self, Self::Error> {
            match v {
                EventTimeSource::FromMetadata(v) => Ok(Self::FromMetadata(v.try_into()?)),
                EventTimeSource::FromPath(v) => Ok(Self::FromPath(v.try_into()?)),
                EventTimeSource::FromSystemTime(v) => Ok(Self::FromSystemTime(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::sources::EventTimeSource, EventTimeSource);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EventTimeSource#/$defs/FromMetadata
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct EventTimeSourceFromMetadata {}

    impl IntoDto for EventTimeSourceFromMetadata {
        type Dto = dtos::sources::EventTimeSourceFromMetadata;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::EventTimeSourceFromMetadata> for EventTimeSourceFromMetadata {
        fn from(v: dtos::sources::EventTimeSourceFromMetadata) -> Self {
            Self {}
        }
    }

    impl TryFrom<EventTimeSourceFromMetadata> for dtos::sources::EventTimeSourceFromMetadata {
        type Error = ValidationError;
        fn try_from(v: EventTimeSourceFromMetadata) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::sources::EventTimeSourceFromMetadata,
        EventTimeSourceFromMetadata
    );

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EventTimeSource#/$defs/FromPath
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
        type Dto = dtos::sources::EventTimeSourceFromPath;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::EventTimeSourceFromPath> for EventTimeSourceFromPath {
        fn from(v: dtos::sources::EventTimeSourceFromPath) -> Self {
            Self {
                pattern: v.pattern,
                timestamp_format: v.timestamp_format,
            }
        }
    }

    impl TryFrom<EventTimeSourceFromPath> for dtos::sources::EventTimeSourceFromPath {
        type Error = ValidationError;
        fn try_from(v: EventTimeSourceFromPath) -> Result<Self, ValidationError> {
            Ok(Self {
                pattern: v.pattern,
                timestamp_format: v.timestamp_format,
            })
        }
    }

    implement_serde_as!(
        dtos::sources::EventTimeSourceFromPath,
        EventTimeSourceFromPath
    );

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EventTimeSource#/$defs/FromSystemTime
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct EventTimeSourceFromSystemTime {}

    impl IntoDto for EventTimeSourceFromSystemTime {
        type Dto = dtos::sources::EventTimeSourceFromSystemTime;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::EventTimeSourceFromSystemTime> for EventTimeSourceFromSystemTime {
        fn from(v: dtos::sources::EventTimeSourceFromSystemTime) -> Self {
            Self {}
        }
    }

    impl TryFrom<EventTimeSourceFromSystemTime> for dtos::sources::EventTimeSourceFromSystemTime {
        type Error = ValidationError;
        fn try_from(v: EventTimeSourceFromSystemTime) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(
        dtos::sources::EventTimeSourceFromSystemTime,
        EventTimeSourceFromSystemTime
    );

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/IngestParams
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngestParams {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub target_slice_records: Option<u64>,
    }

    impl IntoDto for IngestParams {
        type Dto = dtos::sources::IngestParams;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngestParams> for IngestParams {
        fn from(v: dtos::sources::IngestParams) -> Self {
            Self {
                target_slice_records: v.target_slice_records,
            }
        }
    }

    impl TryFrom<IngestParams> for dtos::sources::IngestParams {
        type Error = ValidationError;
        fn try_from(v: IngestParams) -> Result<Self, ValidationError> {
            Ok(Self {
                target_slice_records: v.target_slice_records,
            })
        }
    }

    implement_serde_as!(dtos::sources::IngestParams, IngestParams);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum Ingress {
        #[serde(alias = "url")]
        Url(sources::IngressUrl),
        #[serde(alias = "filesGlob", alias = "filesglob")]
        FilesGlob(sources::IngressFilesGlob),
        #[serde(alias = "container")]
        Container(sources::IngressContainer),
        #[serde(alias = "mqtt")]
        Mqtt(sources::IngressMqtt),
        #[serde(alias = "evmLogs", alias = "evmlogs")]
        EvmLogs(sources::IngressEvmLogs),
        #[serde(alias = "restEndpoint", alias = "restendpoint")]
        RestEndpoint(sources::IngressRestEndpoint),
    }

    impl IntoDto for Ingress {
        type Dto = dtos::sources::Ingress;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::Ingress> for Ingress {
        fn from(v: dtos::sources::Ingress) -> Self {
            match v {
                dtos::sources::Ingress::Url(v) => Self::Url(v.into()),
                dtos::sources::Ingress::FilesGlob(v) => Self::FilesGlob(v.into()),
                dtos::sources::Ingress::Container(v) => Self::Container(v.into()),
                dtos::sources::Ingress::Mqtt(v) => Self::Mqtt(v.into()),
                dtos::sources::Ingress::EvmLogs(v) => Self::EvmLogs(v.into()),
                dtos::sources::Ingress::RestEndpoint(v) => Self::RestEndpoint(v.into()),
            }
        }
    }

    impl TryFrom<Ingress> for dtos::sources::Ingress {
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

    implement_serde_as!(dtos::sources::Ingress, Ingress);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/IngressBuffer
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum IngressBuffer {
        #[serde(alias = "memory")]
        Memory(sources::IngressBufferMemory),
    }

    impl IntoDto for IngressBuffer {
        type Dto = dtos::sources::IngressBuffer;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngressBuffer> for IngressBuffer {
        fn from(v: dtos::sources::IngressBuffer) -> Self {
            match v {
                dtos::sources::IngressBuffer::Memory(v) => Self::Memory(v.into()),
            }
        }
    }

    impl TryFrom<IngressBuffer> for dtos::sources::IngressBuffer {
        type Error = ValidationError;
        fn try_from(v: IngressBuffer) -> Result<Self, Self::Error> {
            match v {
                IngressBuffer::Memory(v) => Ok(Self::Memory(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::sources::IngressBuffer, IngressBuffer);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/IngressBuffer#/$defs/Memory
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
        type Dto = dtos::sources::IngressBufferMemory;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngressBufferMemory> for IngressBufferMemory {
        fn from(v: dtos::sources::IngressBufferMemory) -> Self {
            Self {
                buffer_size: v.buffer_size,
                overflow_policy: v.overflow_policy,
            }
        }
    }

    impl TryFrom<IngressBufferMemory> for dtos::sources::IngressBufferMemory {
        type Error = ValidationError;
        fn try_from(v: IngressBufferMemory) -> Result<Self, ValidationError> {
            Ok(Self {
                buffer_size: v.buffer_size,
                overflow_policy: v.overflow_policy,
            })
        }
    }

    implement_serde_as!(dtos::sources::IngressBufferMemory, IngressBufferMemory);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/Container
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
        pub env: Option<Vec<sources::EnvVar>>,
    }

    impl IntoDto for IngressContainer {
        type Dto = dtos::sources::IngressContainer;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngressContainer> for IngressContainer {
        fn from(v: dtos::sources::IngressContainer) -> Self {
            Self {
                image: v.image,
                command: v.command,
                args: v.args,
                env: v.env.map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<IngressContainer> for dtos::sources::IngressContainer {
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
                            .map(|i| dtos::sources::EnvVar::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sources::IngressContainer, IngressContainer);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/EvmLogs
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
        type Dto = dtos::sources::IngressEvmLogs;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngressEvmLogs> for IngressEvmLogs {
        fn from(v: dtos::sources::IngressEvmLogs) -> Self {
            Self {
                chain_id: v.chain_id,
                node_url: v.node_url,
                filter: v.filter,
                signature: v.signature,
            }
        }
    }

    impl TryFrom<IngressEvmLogs> for dtos::sources::IngressEvmLogs {
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

    implement_serde_as!(dtos::sources::IngressEvmLogs, IngressEvmLogs);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/FilesGlob
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressFilesGlob {
        pub path: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_time: Option<UnionOrString<sources::EventTimeSource>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cache: Option<UnionOrString<sources::SourceCaching>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub order: Option<sources::SourceOrdering>,
    }

    impl IntoDto for IngressFilesGlob {
        type Dto = dtos::sources::IngressFilesGlob;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngressFilesGlob> for IngressFilesGlob {
        fn from(v: dtos::sources::IngressFilesGlob) -> Self {
            Self {
                path: v.path,
                event_time: v.event_time.map(|v| v.into()),
                cache: v.cache.map(|v| v.into()),
                order: v.order.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<IngressFilesGlob> for dtos::sources::IngressFilesGlob {
        type Error = ValidationError;
        fn try_from(v: IngressFilesGlob) -> Result<Self, ValidationError> {
            Ok(Self {
                path: v.path,
                event_time: v
                    .event_time
                    .map(|v| dtos::sources::EventTimeSource::try_from(v))
                    .transpose()?,
                cache: v
                    .cache
                    .map(|v| dtos::sources::SourceCaching::try_from(v))
                    .transpose()?,
                order: v
                    .order
                    .map(|v| dtos::sources::SourceOrdering::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sources::IngressFilesGlob, IngressFilesGlob);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/Mqtt
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
        pub topics: Vec<sources::MqttTopicSubscription>,
    }

    impl IntoDto for IngressMqtt {
        type Dto = dtos::sources::IngressMqtt;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngressMqtt> for IngressMqtt {
        fn from(v: dtos::sources::IngressMqtt) -> Self {
            Self {
                host: v.host,
                port: v.port,
                username: v.username,
                password: v.password,
                topics: v.topics.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<IngressMqtt> for dtos::sources::IngressMqtt {
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
                    .map(|i| dtos::sources::MqttTopicSubscription::try_from(i))
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    implement_serde_as!(dtos::sources::IngressMqtt, IngressMqtt);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/RestEndpoint
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressRestEndpoint {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub buffer: Option<sources::IngressBuffer>,
    }

    impl IntoDto for IngressRestEndpoint {
        type Dto = dtos::sources::IngressRestEndpoint;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngressRestEndpoint> for IngressRestEndpoint {
        fn from(v: dtos::sources::IngressRestEndpoint) -> Self {
            Self {
                buffer: v.buffer.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<IngressRestEndpoint> for dtos::sources::IngressRestEndpoint {
        type Error = ValidationError;
        fn try_from(v: IngressRestEndpoint) -> Result<Self, ValidationError> {
            Ok(Self {
                buffer: v
                    .buffer
                    .map(|v| dtos::sources::IngressBuffer::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sources::IngressRestEndpoint, IngressRestEndpoint);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/Url
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct IngressUrl {
        pub url: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_time: Option<UnionOrString<sources::EventTimeSource>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cache: Option<UnionOrString<sources::SourceCaching>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub headers: Option<Vec<sources::RequestHeader>>,
    }

    impl IntoDto for IngressUrl {
        type Dto = dtos::sources::IngressUrl;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::IngressUrl> for IngressUrl {
        fn from(v: dtos::sources::IngressUrl) -> Self {
            Self {
                url: v.url,
                event_time: v.event_time.map(|v| v.into()),
                cache: v.cache.map(|v| v.into()),
                headers: v.headers.map(|v| v.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl TryFrom<IngressUrl> for dtos::sources::IngressUrl {
        type Error = ValidationError;
        fn try_from(v: IngressUrl) -> Result<Self, ValidationError> {
            Ok(Self {
                url: v.url,
                event_time: v
                    .event_time
                    .map(|v| dtos::sources::EventTimeSource::try_from(v))
                    .transpose()?,
                cache: v
                    .cache
                    .map(|v| dtos::sources::SourceCaching::try_from(v))
                    .transpose()?,
                headers: v
                    .headers
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::sources::RequestHeader::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sources::IngressUrl, IngressUrl);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum MergeStrategy {
        #[serde(alias = "append")]
        Append(sources::MergeStrategyAppend),
        #[serde(alias = "ledger")]
        Ledger(sources::MergeStrategyLedger),
        #[serde(alias = "snapshot")]
        Snapshot(sources::MergeStrategySnapshot),
        #[serde(alias = "changelogStream", alias = "changelogstream")]
        ChangelogStream(sources::MergeStrategyChangelogStream),
        #[serde(alias = "upsertStream", alias = "upsertstream")]
        UpsertStream(sources::MergeStrategyUpsertStream),
    }

    impl IntoDto for MergeStrategy {
        type Dto = dtos::sources::MergeStrategy;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::MergeStrategy> for MergeStrategy {
        fn from(v: dtos::sources::MergeStrategy) -> Self {
            match v {
                dtos::sources::MergeStrategy::Append(v) => Self::Append(v.into()),
                dtos::sources::MergeStrategy::Ledger(v) => Self::Ledger(v.into()),
                dtos::sources::MergeStrategy::Snapshot(v) => Self::Snapshot(v.into()),
                dtos::sources::MergeStrategy::ChangelogStream(v) => Self::ChangelogStream(v.into()),
                dtos::sources::MergeStrategy::UpsertStream(v) => Self::UpsertStream(v.into()),
            }
        }
    }

    impl TryFrom<MergeStrategy> for dtos::sources::MergeStrategy {
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

    implement_serde_as!(dtos::sources::MergeStrategy, MergeStrategy);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/Append
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategyAppend {}

    impl IntoDto for MergeStrategyAppend {
        type Dto = dtos::sources::MergeStrategyAppend;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::MergeStrategyAppend> for MergeStrategyAppend {
        fn from(v: dtos::sources::MergeStrategyAppend) -> Self {
            Self {}
        }
    }

    impl TryFrom<MergeStrategyAppend> for dtos::sources::MergeStrategyAppend {
        type Error = ValidationError;
        fn try_from(v: MergeStrategyAppend) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::sources::MergeStrategyAppend, MergeStrategyAppend);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/ChangelogStream
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategyChangelogStream {
        pub primary_key: Vec<String>,
    }

    impl IntoDto for MergeStrategyChangelogStream {
        type Dto = dtos::sources::MergeStrategyChangelogStream;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::MergeStrategyChangelogStream> for MergeStrategyChangelogStream {
        fn from(v: dtos::sources::MergeStrategyChangelogStream) -> Self {
            Self {
                primary_key: v.primary_key,
            }
        }
    }

    impl TryFrom<MergeStrategyChangelogStream> for dtos::sources::MergeStrategyChangelogStream {
        type Error = ValidationError;
        fn try_from(v: MergeStrategyChangelogStream) -> Result<Self, ValidationError> {
            Ok(Self {
                primary_key: v.primary_key,
            })
        }
    }

    implement_serde_as!(
        dtos::sources::MergeStrategyChangelogStream,
        MergeStrategyChangelogStream
    );

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/Ledger
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategyLedger {
        pub primary_key: Vec<String>,
    }

    impl IntoDto for MergeStrategyLedger {
        type Dto = dtos::sources::MergeStrategyLedger;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::MergeStrategyLedger> for MergeStrategyLedger {
        fn from(v: dtos::sources::MergeStrategyLedger) -> Self {
            Self {
                primary_key: v.primary_key,
            }
        }
    }

    impl TryFrom<MergeStrategyLedger> for dtos::sources::MergeStrategyLedger {
        type Error = ValidationError;
        fn try_from(v: MergeStrategyLedger) -> Result<Self, ValidationError> {
            Ok(Self {
                primary_key: v.primary_key,
            })
        }
    }

    implement_serde_as!(dtos::sources::MergeStrategyLedger, MergeStrategyLedger);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/Snapshot
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
        type Dto = dtos::sources::MergeStrategySnapshot;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::MergeStrategySnapshot> for MergeStrategySnapshot {
        fn from(v: dtos::sources::MergeStrategySnapshot) -> Self {
            Self {
                primary_key: v.primary_key,
                compare_columns: v.compare_columns,
            }
        }
    }

    impl TryFrom<MergeStrategySnapshot> for dtos::sources::MergeStrategySnapshot {
        type Error = ValidationError;
        fn try_from(v: MergeStrategySnapshot) -> Result<Self, ValidationError> {
            Ok(Self {
                primary_key: v.primary_key,
                compare_columns: v.compare_columns,
            })
        }
    }

    implement_serde_as!(dtos::sources::MergeStrategySnapshot, MergeStrategySnapshot);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/UpsertStream
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MergeStrategyUpsertStream {
        pub primary_key: Vec<String>,
    }

    impl IntoDto for MergeStrategyUpsertStream {
        type Dto = dtos::sources::MergeStrategyUpsertStream;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::MergeStrategyUpsertStream> for MergeStrategyUpsertStream {
        fn from(v: dtos::sources::MergeStrategyUpsertStream) -> Self {
            Self {
                primary_key: v.primary_key,
            }
        }
    }

    impl TryFrom<MergeStrategyUpsertStream> for dtos::sources::MergeStrategyUpsertStream {
        type Error = ValidationError;
        fn try_from(v: MergeStrategyUpsertStream) -> Result<Self, ValidationError> {
            Ok(Self {
                primary_key: v.primary_key,
            })
        }
    }

    implement_serde_as!(
        dtos::sources::MergeStrategyUpsertStream,
        MergeStrategyUpsertStream
    );

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MqttQos
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
        type Dto = dtos::sources::MqttQos;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::MqttQos> for MqttQos {
        fn from(v: dtos::sources::MqttQos) -> Self {
            match v {
                dtos::sources::MqttQos::AtMostOnce => Self::AtMostOnce,
                dtos::sources::MqttQos::AtLeastOnce => Self::AtLeastOnce,
                dtos::sources::MqttQos::ExactlyOnce => Self::ExactlyOnce,
            }
        }
    }

    impl TryFrom<MqttQos> for dtos::sources::MqttQos {
        type Error = ValidationError;
        fn try_from(v: MqttQos) -> Result<Self, Self::Error> {
            match v {
                MqttQos::AtMostOnce => Ok(Self::AtMostOnce),
                MqttQos::AtLeastOnce => Ok(Self::AtLeastOnce),
                MqttQos::ExactlyOnce => Ok(Self::ExactlyOnce),
            }
        }
    }

    implement_serde_as!(dtos::sources::MqttQos, MqttQos);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MqttTopicSubscription
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct MqttTopicSubscription {
        pub path: String,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub qos: Option<sources::MqttQos>,
    }

    impl IntoDto for MqttTopicSubscription {
        type Dto = dtos::sources::MqttTopicSubscription;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::MqttTopicSubscription> for MqttTopicSubscription {
        fn from(v: dtos::sources::MqttTopicSubscription) -> Self {
            Self {
                path: v.path,
                qos: v.qos.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<MqttTopicSubscription> for dtos::sources::MqttTopicSubscription {
        type Error = ValidationError;
        fn try_from(v: MqttTopicSubscription) -> Result<Self, ValidationError> {
            Ok(Self {
                path: v.path,
                qos: v
                    .qos
                    .map(|v| dtos::sources::MqttQos::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sources::MqttTopicSubscription, MqttTopicSubscription);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/PrepStep
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum PrepStep {
        #[serde(alias = "decompress")]
        Decompress(sources::PrepStepDecompress),
        #[serde(alias = "pipe")]
        Pipe(sources::PrepStepPipe),
    }

    impl IntoDto for PrepStep {
        type Dto = dtos::sources::PrepStep;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::PrepStep> for PrepStep {
        fn from(v: dtos::sources::PrepStep) -> Self {
            match v {
                dtos::sources::PrepStep::Decompress(v) => Self::Decompress(v.into()),
                dtos::sources::PrepStep::Pipe(v) => Self::Pipe(v.into()),
            }
        }
    }

    impl TryFrom<PrepStep> for dtos::sources::PrepStep {
        type Error = ValidationError;
        fn try_from(v: PrepStep) -> Result<Self, Self::Error> {
            match v {
                PrepStep::Decompress(v) => Ok(Self::Decompress(v.try_into()?)),
                PrepStep::Pipe(v) => Ok(Self::Pipe(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::sources::PrepStep, PrepStep);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/PrepStep#/$defs/Decompress
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct PrepStepDecompress {
        pub format: sources::CompressionFormat,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub sub_path: Option<String>,
    }

    impl IntoDto for PrepStepDecompress {
        type Dto = dtos::sources::PrepStepDecompress;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::PrepStepDecompress> for PrepStepDecompress {
        fn from(v: dtos::sources::PrepStepDecompress) -> Self {
            Self {
                format: v.format.into(),
                sub_path: v.sub_path,
            }
        }
    }

    impl TryFrom<PrepStepDecompress> for dtos::sources::PrepStepDecompress {
        type Error = ValidationError;
        fn try_from(v: PrepStepDecompress) -> Result<Self, ValidationError> {
            Ok(Self {
                format: dtos::sources::CompressionFormat::try_from(v.format)?,
                sub_path: v.sub_path,
            })
        }
    }

    implement_serde_as!(dtos::sources::PrepStepDecompress, PrepStepDecompress);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/PrepStep#/$defs/Pipe
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct PrepStepPipe {
        pub command: Vec<String>,
    }

    impl IntoDto for PrepStepPipe {
        type Dto = dtos::sources::PrepStepPipe;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::PrepStepPipe> for PrepStepPipe {
        fn from(v: dtos::sources::PrepStepPipe) -> Self {
            Self { command: v.command }
        }
    }

    impl TryFrom<PrepStepPipe> for dtos::sources::PrepStepPipe {
        type Error = ValidationError;
        fn try_from(v: PrepStepPipe) -> Result<Self, ValidationError> {
            Ok(Self { command: v.command })
        }
    }

    implement_serde_as!(dtos::sources::PrepStepPipe, PrepStepPipe);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum ReadStep {
        #[serde(alias = "csv")]
        Csv(sources::ReadStepCsv),
        #[serde(alias = "geoJson", alias = "geojson")]
        GeoJson(sources::ReadStepGeoJson),
        #[serde(alias = "esriShapefile", alias = "esrishapefile")]
        EsriShapefile(sources::ReadStepEsriShapefile),
        #[serde(alias = "parquet")]
        Parquet(sources::ReadStepParquet),
        #[serde(alias = "json")]
        Json(sources::ReadStepJson),
        #[serde(alias = "ndJson", alias = "ndjson")]
        NdJson(sources::ReadStepNdJson),
        #[serde(alias = "ndGeoJson", alias = "ndgeojson")]
        NdGeoJson(sources::ReadStepNdGeoJson),
    }

    impl IntoDto for ReadStep {
        type Dto = dtos::sources::ReadStep;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::ReadStep> for ReadStep {
        fn from(v: dtos::sources::ReadStep) -> Self {
            match v {
                dtos::sources::ReadStep::Csv(v) => Self::Csv(v.into()),
                dtos::sources::ReadStep::GeoJson(v) => Self::GeoJson(v.into()),
                dtos::sources::ReadStep::EsriShapefile(v) => Self::EsriShapefile(v.into()),
                dtos::sources::ReadStep::Parquet(v) => Self::Parquet(v.into()),
                dtos::sources::ReadStep::Json(v) => Self::Json(v.into()),
                dtos::sources::ReadStep::NdJson(v) => Self::NdJson(v.into()),
                dtos::sources::ReadStep::NdGeoJson(v) => Self::NdGeoJson(v.into()),
            }
        }
    }

    impl TryFrom<ReadStep> for dtos::sources::ReadStep {
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

    implement_serde_as!(dtos::sources::ReadStep, ReadStep);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/Csv
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
        type Dto = dtos::sources::ReadStepCsv;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::ReadStepCsv> for ReadStepCsv {
        fn from(v: dtos::sources::ReadStepCsv) -> Self {
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

    impl TryFrom<ReadStepCsv> for dtos::sources::ReadStepCsv {
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

    implement_serde_as!(dtos::sources::ReadStepCsv, ReadStepCsv);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/EsriShapefile
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
        type Dto = dtos::sources::ReadStepEsriShapefile;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::ReadStepEsriShapefile> for ReadStepEsriShapefile {
        fn from(v: dtos::sources::ReadStepEsriShapefile) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                sub_path: v.sub_path,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepEsriShapefile> for dtos::sources::ReadStepEsriShapefile {
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

    implement_serde_as!(dtos::sources::ReadStepEsriShapefile, ReadStepEsriShapefile);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/GeoJson
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
        type Dto = dtos::sources::ReadStepGeoJson;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::ReadStepGeoJson> for ReadStepGeoJson {
        fn from(v: dtos::sources::ReadStepGeoJson) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepGeoJson> for dtos::sources::ReadStepGeoJson {
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

    implement_serde_as!(dtos::sources::ReadStepGeoJson, ReadStepGeoJson);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/Json
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
        type Dto = dtos::sources::ReadStepJson;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::ReadStepJson> for ReadStepJson {
        fn from(v: dtos::sources::ReadStepJson) -> Self {
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

    impl TryFrom<ReadStepJson> for dtos::sources::ReadStepJson {
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

    implement_serde_as!(dtos::sources::ReadStepJson, ReadStepJson);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/NdGeoJson
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
        type Dto = dtos::sources::ReadStepNdGeoJson;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::ReadStepNdGeoJson> for ReadStepNdGeoJson {
        fn from(v: dtos::sources::ReadStepNdGeoJson) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepNdGeoJson> for dtos::sources::ReadStepNdGeoJson {
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

    implement_serde_as!(dtos::sources::ReadStepNdGeoJson, ReadStepNdGeoJson);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/NdJson
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
        type Dto = dtos::sources::ReadStepNdJson;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::ReadStepNdJson> for ReadStepNdJson {
        fn from(v: dtos::sources::ReadStepNdJson) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                date_format: v.date_format,
                encoding: v.encoding,
                timestamp_format: v.timestamp_format,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepNdJson> for dtos::sources::ReadStepNdJson {
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

    implement_serde_as!(dtos::sources::ReadStepNdJson, ReadStepNdJson);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/Parquet
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
        type Dto = dtos::sources::ReadStepParquet;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::ReadStepParquet> for ReadStepParquet {
        fn from(v: dtos::sources::ReadStepParquet) -> Self {
            Self {
                ddl_schema: v.ddl_schema,
                schema: v.schema.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<ReadStepParquet> for dtos::sources::ReadStepParquet {
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

    implement_serde_as!(dtos::sources::ReadStepParquet, ReadStepParquet);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/RequestHeader
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct RequestHeader {
        pub name: String,
        pub value: String,
    }

    impl IntoDto for RequestHeader {
        type Dto = dtos::sources::RequestHeader;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::RequestHeader> for RequestHeader {
        fn from(v: dtos::sources::RequestHeader) -> Self {
            Self {
                name: v.name,
                value: v.value,
            }
        }
    }

    impl TryFrom<RequestHeader> for dtos::sources::RequestHeader {
        type Error = ValidationError;
        fn try_from(v: RequestHeader) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                value: v.value,
            })
        }
    }

    implement_serde_as!(dtos::sources::RequestHeader, RequestHeader);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceCaching
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum SourceCaching {
        #[serde(alias = "forever")]
        Forever(sources::SourceCachingForever),
    }

    impl From<dtos::sources::SourceCaching> for UnionOrString<SourceCaching> {
        fn from(v: dtos::sources::SourceCaching) -> Self {
            Self(v.into())
        }
    }
    impl TryFrom<UnionOrString<SourceCaching>> for dtos::sources::SourceCaching {
        type Error = ValidationError;
        fn try_from(v: UnionOrString<SourceCaching>) -> Result<Self, Self::Error> {
            v.0.try_into()
        }
    }

    impl IntoDto for SourceCaching {
        type Dto = dtos::sources::SourceCaching;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::SourceCaching> for SourceCaching {
        fn from(v: dtos::sources::SourceCaching) -> Self {
            match v {
                dtos::sources::SourceCaching::Forever(v) => Self::Forever(v.into()),
            }
        }
    }

    impl TryFrom<SourceCaching> for dtos::sources::SourceCaching {
        type Error = ValidationError;
        fn try_from(v: SourceCaching) -> Result<Self, Self::Error> {
            match v {
                SourceCaching::Forever(v) => Ok(Self::Forever(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::sources::SourceCaching, SourceCaching);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceCaching#/$defs/Forever
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SourceCachingForever {}

    impl IntoDto for SourceCachingForever {
        type Dto = dtos::sources::SourceCachingForever;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::SourceCachingForever> for SourceCachingForever {
        fn from(v: dtos::sources::SourceCachingForever) -> Self {
            Self {}
        }
    }

    impl TryFrom<SourceCachingForever> for dtos::sources::SourceCachingForever {
        type Error = ValidationError;
        fn try_from(v: SourceCachingForever) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::sources::SourceCachingForever, SourceCachingForever);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceOrdering
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum SourceOrdering {
        #[serde(alias = "byEventTime", alias = "byeventtime")]
        ByEventTime,
        #[serde(alias = "byName", alias = "byname")]
        ByName,
    }

    impl IntoDto for SourceOrdering {
        type Dto = dtos::sources::SourceOrdering;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::SourceOrdering> for SourceOrdering {
        fn from(v: dtos::sources::SourceOrdering) -> Self {
            match v {
                dtos::sources::SourceOrdering::ByEventTime => Self::ByEventTime,
                dtos::sources::SourceOrdering::ByName => Self::ByName,
            }
        }
    }

    impl TryFrom<SourceOrdering> for dtos::sources::SourceOrdering {
        type Error = ValidationError;
        fn try_from(v: SourceOrdering) -> Result<Self, Self::Error> {
            match v {
                SourceOrdering::ByEventTime => Ok(Self::ByEventTime),
                SourceOrdering::ByName => Ok(Self::ByName),
            }
        }
    }

    implement_serde_as!(dtos::sources::SourceOrdering, SourceOrdering);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SourceSpec {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub config: Option<config::ValueRefs>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ingress: Option<sources::Ingress>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prepare: Option<Vec<sources::PrepStep>>,
        pub read: sources::ReadStep,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub preprocess: Option<datasets::Transform>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub merge: Option<sources::MergeStrategy>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vocab: Option<datasets::DatasetVocabulary>,
    }

    impl IntoDto for SourceSpec {
        type Dto = dtos::sources::SourceSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::SourceSpec> for SourceSpec {
        fn from(v: dtos::sources::SourceSpec) -> Self {
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

    impl TryFrom<SourceSpec> for dtos::sources::SourceSpec {
        type Error = ValidationError;
        fn try_from(v: SourceSpec) -> Result<Self, ValidationError> {
            Ok(Self {
                config: v
                    .config
                    .map(|v| dtos::config::ValueRefs::try_from(v))
                    .transpose()?,
                ingress: v
                    .ingress
                    .map(|v| dtos::sources::Ingress::try_from(v))
                    .transpose()?,
                prepare: v
                    .prepare
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::sources::PrepStep::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                read: dtos::sources::ReadStep::try_from(v.read)?,
                preprocess: v
                    .preprocess
                    .map(|v| dtos::datasets::Transform::try_from(v))
                    .transpose()?,
                merge: v
                    .merge
                    .map(|v| dtos::sources::MergeStrategy::try_from(v))
                    .transpose()?,
                vocab: v
                    .vocab
                    .map(|v| dtos::datasets::DatasetVocabulary::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sources::SourceSpec, SourceSpec);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SourceSpecInput {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub config: Option<config::ValueRefs>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ingress: Option<sources::Ingress>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub prepare: Option<Vec<sources::PrepStep>>,
        pub read: sources::ReadStep,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub preprocess: Option<datasets::Transform>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub merge: Option<sources::MergeStrategy>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vocab: Option<datasets::DatasetVocabulary>,
    }

    impl IntoDto for SourceSpecInput {
        type Dto = dtos::sources::SourceSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::SourceSpecInput> for SourceSpecInput {
        fn from(v: dtos::sources::SourceSpecInput) -> Self {
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

    impl TryFrom<SourceSpecInput> for dtos::sources::SourceSpecInput {
        type Error = ValidationError;
        fn try_from(v: SourceSpecInput) -> Result<Self, ValidationError> {
            Ok(Self {
                config: v
                    .config
                    .map(|v| dtos::config::ValueRefs::try_from(v))
                    .transpose()?,
                ingress: v
                    .ingress
                    .map(|v| dtos::sources::Ingress::try_from(v))
                    .transpose()?,
                prepare: v
                    .prepare
                    .map(|v| {
                        v.into_iter()
                            .map(|i| dtos::sources::PrepStep::try_from(i))
                            .collect::<Result<_, _>>()
                    })
                    .transpose()?,
                read: dtos::sources::ReadStep::try_from(v.read)?,
                preprocess: v
                    .preprocess
                    .map(|v| dtos::datasets::Transform::try_from(v))
                    .transpose()?,
                merge: v
                    .merge
                    .map(|v| dtos::sources::MergeStrategy::try_from(v))
                    .transpose()?,
                vocab: v
                    .vocab
                    .map(|v| dtos::datasets::DatasetVocabulary::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::sources::SourceSpecInput, SourceSpecInput);

    // Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceState
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct SourceState {
        pub source_name: String,
        pub kind: String,
        pub value: String,
    }

    impl IntoDto for SourceState {
        type Dto = dtos::sources::SourceState;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::sources::SourceState> for SourceState {
        fn from(v: dtos::sources::SourceState) -> Self {
            Self {
                source_name: v.source_name,
                kind: v.kind,
                value: v.value,
            }
        }
    }

    impl TryFrom<SourceState> for dtos::sources::SourceState {
        type Error = ValidationError;
        fn try_from(v: SourceState) -> Result<Self, ValidationError> {
            Ok(Self {
                source_name: v.source_name,
                kind: v.kind,
                value: v.value,
            })
        }
    }

    implement_serde_as!(dtos::sources::SourceState, SourceState);
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
        pub access_key: Option<config::ValueHandle>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub secret_key: Option<config::ValueHandle>,
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
                    .map(|v| dtos::config::ValueHandle::try_from(v))
                    .transpose()?,
                secret_key: v
                    .secret_key
                    .map(|v| dtos::config::ValueHandle::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::storage::AwsCredentials, AwsCredentials);

    // Schema: https://opendatafabric.org/schemas/storage/v1alpha1/AwsCredentialsInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct AwsCredentialsInput {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub access_key: Option<StructOrString<config::ValueRef>>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub secret_key: Option<StructOrString<config::ValueRef>>,
    }

    impl IntoDto for AwsCredentialsInput {
        type Dto = dtos::storage::AwsCredentialsInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::storage::AwsCredentialsInput> for AwsCredentialsInput {
        fn from(v: dtos::storage::AwsCredentialsInput) -> Self {
            Self {
                access_key: v.access_key.map(|v| v.into()),
                secret_key: v.secret_key.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<AwsCredentialsInput> for dtos::storage::AwsCredentialsInput {
        type Error = ValidationError;
        fn try_from(v: AwsCredentialsInput) -> Result<Self, ValidationError> {
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

    implement_serde_as!(dtos::storage::AwsCredentialsInput, AwsCredentialsInput);

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
        pub id: Option<odf::resources::ResourceID>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<odf::resources::ResourceName>,
    }

    impl IntoDto for PersistentVolumeRef {
        type Dto = dtos::storage::PersistentVolumeRef;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl std::str::FromStr for PersistentVolumeRef {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let v = dtos::storage::PersistentVolumeRef::try_from(s).map_err(|e| e.to_string())?;
            Ok(v.into())
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

    impl From<dtos::storage::PersistentVolumeRef> for PersistentVolumeRef {
        fn from(v: dtos::storage::PersistentVolumeRef) -> Self {
            Self {
                account: v.account.map(|v| v.into()),
                id: v.id,
                name: v.name,
            }
        }
    }

    impl TryFrom<PersistentVolumeRef> for dtos::storage::PersistentVolumeRef {
        type Error = ValidationError;
        fn try_from(v: PersistentVolumeRef) -> Result<Self, ValidationError> {
            Ok(Self {
                account: v
                    .account
                    .map(|v| dtos::auth::AccountRef::try_from(v))
                    .transpose()?,
                id: v.id,
                name: v.name,
            })
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

    // Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum PersistentVolumeSpecInput {
        #[serde(alias = "s3")]
        S3(storage::PersistentVolumeSpecInputS3),
    }

    impl IntoDto for PersistentVolumeSpecInput {
        type Dto = dtos::storage::PersistentVolumeSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::storage::PersistentVolumeSpecInput> for PersistentVolumeSpecInput {
        fn from(v: dtos::storage::PersistentVolumeSpecInput) -> Self {
            match v {
                dtos::storage::PersistentVolumeSpecInput::S3(v) => Self::S3(v.into()),
            }
        }
    }

    impl TryFrom<PersistentVolumeSpecInput> for dtos::storage::PersistentVolumeSpecInput {
        type Error = ValidationError;
        fn try_from(v: PersistentVolumeSpecInput) -> Result<Self, Self::Error> {
            match v {
                PersistentVolumeSpecInput::S3(v) => Ok(Self::S3(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(
        dtos::storage::PersistentVolumeSpecInput,
        PersistentVolumeSpecInput
    );

    // Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpecInput#/$defs/S3
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct PersistentVolumeSpecInputS3 {
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
        pub credentials: Option<storage::AwsCredentialsInput>,
    }

    impl IntoDto for PersistentVolumeSpecInputS3 {
        type Dto = dtos::storage::PersistentVolumeSpecInputS3;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::storage::PersistentVolumeSpecInputS3> for PersistentVolumeSpecInputS3 {
        fn from(v: dtos::storage::PersistentVolumeSpecInputS3) -> Self {
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

    impl TryFrom<PersistentVolumeSpecInputS3> for dtos::storage::PersistentVolumeSpecInputS3 {
        type Error = ValidationError;
        fn try_from(v: PersistentVolumeSpecInputS3) -> Result<Self, ValidationError> {
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
                    .map(|v| dtos::storage::AwsCredentialsInput::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(
        dtos::storage::PersistentVolumeSpecInputS3,
        PersistentVolumeSpecInputS3
    );

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// tasks
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod tasks {
    #[allow(unused_imports)]
    use super::*;

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum TaskOutcome {
        #[serde(alias = "success")]
        Success(tasks::TaskOutcomeSuccess),
        #[serde(alias = "failed")]
        Failed(tasks::TaskOutcomeFailed),
        #[serde(alias = "noOp", alias = "noop")]
        NoOp(tasks::TaskOutcomeNoOp),
        #[serde(alias = "cancelled")]
        Cancelled(tasks::TaskOutcomeCancelled),
    }

    impl IntoDto for TaskOutcome {
        type Dto = dtos::tasks::TaskOutcome;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskOutcome> for TaskOutcome {
        fn from(v: dtos::tasks::TaskOutcome) -> Self {
            match v {
                dtos::tasks::TaskOutcome::Success(v) => Self::Success(v.into()),
                dtos::tasks::TaskOutcome::Failed(v) => Self::Failed(v.into()),
                dtos::tasks::TaskOutcome::NoOp(v) => Self::NoOp(v.into()),
                dtos::tasks::TaskOutcome::Cancelled(v) => Self::Cancelled(v.into()),
            }
        }
    }

    impl TryFrom<TaskOutcome> for dtos::tasks::TaskOutcome {
        type Error = ValidationError;
        fn try_from(v: TaskOutcome) -> Result<Self, Self::Error> {
            match v {
                TaskOutcome::Success(v) => Ok(Self::Success(v.try_into()?)),
                TaskOutcome::Failed(v) => Ok(Self::Failed(v.try_into()?)),
                TaskOutcome::NoOp(v) => Ok(Self::NoOp(v.try_into()?)),
                TaskOutcome::Cancelled(v) => Ok(Self::Cancelled(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::tasks::TaskOutcome, TaskOutcome);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome#/$defs/Cancelled
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskOutcomeCancelled {}

    impl IntoDto for TaskOutcomeCancelled {
        type Dto = dtos::tasks::TaskOutcomeCancelled;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskOutcomeCancelled> for TaskOutcomeCancelled {
        fn from(v: dtos::tasks::TaskOutcomeCancelled) -> Self {
            Self {}
        }
    }

    impl TryFrom<TaskOutcomeCancelled> for dtos::tasks::TaskOutcomeCancelled {
        type Error = ValidationError;
        fn try_from(v: TaskOutcomeCancelled) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::tasks::TaskOutcomeCancelled, TaskOutcomeCancelled);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome#/$defs/Failed
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskOutcomeFailed {
        pub message: String,
    }

    impl IntoDto for TaskOutcomeFailed {
        type Dto = dtos::tasks::TaskOutcomeFailed;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskOutcomeFailed> for TaskOutcomeFailed {
        fn from(v: dtos::tasks::TaskOutcomeFailed) -> Self {
            Self { message: v.message }
        }
    }

    impl TryFrom<TaskOutcomeFailed> for dtos::tasks::TaskOutcomeFailed {
        type Error = ValidationError;
        fn try_from(v: TaskOutcomeFailed) -> Result<Self, ValidationError> {
            Ok(Self { message: v.message })
        }
    }

    implement_serde_as!(dtos::tasks::TaskOutcomeFailed, TaskOutcomeFailed);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome#/$defs/NoOp
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskOutcomeNoOp {}

    impl IntoDto for TaskOutcomeNoOp {
        type Dto = dtos::tasks::TaskOutcomeNoOp;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskOutcomeNoOp> for TaskOutcomeNoOp {
        fn from(v: dtos::tasks::TaskOutcomeNoOp) -> Self {
            Self {}
        }
    }

    impl TryFrom<TaskOutcomeNoOp> for dtos::tasks::TaskOutcomeNoOp {
        type Error = ValidationError;
        fn try_from(v: TaskOutcomeNoOp) -> Result<Self, ValidationError> {
            Ok(Self {})
        }
    }

    implement_serde_as!(dtos::tasks::TaskOutcomeNoOp, TaskOutcomeNoOp);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome#/$defs/Success
    #[derive(Debug, Serialize, Deserialize)]
    pub struct TaskOutcomeSuccess {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    impl IntoDto for TaskOutcomeSuccess {
        type Dto = dtos::tasks::TaskOutcomeSuccess;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskOutcomeSuccess> for TaskOutcomeSuccess {
        fn from(v: dtos::tasks::TaskOutcomeSuccess) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<TaskOutcomeSuccess> for dtos::tasks::TaskOutcomeSuccess {
        type Error = ValidationError;
        fn try_from(v: TaskOutcomeSuccess) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::tasks::TaskOutcomeSuccess, TaskOutcomeSuccess);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskPlan
    #[derive(Debug, Serialize, Deserialize)]
    pub struct TaskPlan {
        #[serde(flatten)]
        #[serde(with = "map_value_limited_precision")]
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    impl IntoDto for TaskPlan {
        type Dto = dtos::tasks::TaskPlan;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskPlan> for TaskPlan {
        fn from(v: dtos::tasks::TaskPlan) -> Self {
            Self { entries: v.entries }
        }
    }

    impl TryFrom<TaskPlan> for dtos::tasks::TaskPlan {
        type Error = ValidationError;
        fn try_from(v: TaskPlan) -> Result<Self, Self::Error> {
            Ok(Self { entries: v.entries })
        }
    }

    implement_serde_as!(dtos::tasks::TaskPlan, TaskPlan);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum TaskSpec {
        #[serde(alias = "ingest")]
        Ingest(tasks::TaskSpecIngest),
        #[serde(alias = "transform")]
        Transform(tasks::TaskSpecTransform),
        #[serde(alias = "compaction")]
        Compaction(tasks::TaskSpecCompaction),
        #[serde(alias = "garbageCollection", alias = "garbagecollection")]
        GarbageCollection(tasks::TaskSpecGarbageCollection),
        #[serde(alias = "webhookCall", alias = "webhookcall")]
        WebhookCall(tasks::TaskSpecWebhookCall),
    }

    impl IntoDto for TaskSpec {
        type Dto = dtos::tasks::TaskSpec;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpec> for TaskSpec {
        fn from(v: dtos::tasks::TaskSpec) -> Self {
            match v {
                dtos::tasks::TaskSpec::Ingest(v) => Self::Ingest(v.into()),
                dtos::tasks::TaskSpec::Transform(v) => Self::Transform(v.into()),
                dtos::tasks::TaskSpec::Compaction(v) => Self::Compaction(v.into()),
                dtos::tasks::TaskSpec::GarbageCollection(v) => Self::GarbageCollection(v.into()),
                dtos::tasks::TaskSpec::WebhookCall(v) => Self::WebhookCall(v.into()),
            }
        }
    }

    impl TryFrom<TaskSpec> for dtos::tasks::TaskSpec {
        type Error = ValidationError;
        fn try_from(v: TaskSpec) -> Result<Self, Self::Error> {
            match v {
                TaskSpec::Ingest(v) => Ok(Self::Ingest(v.try_into()?)),
                TaskSpec::Transform(v) => Ok(Self::Transform(v.try_into()?)),
                TaskSpec::Compaction(v) => Ok(Self::Compaction(v.try_into()?)),
                TaskSpec::GarbageCollection(v) => Ok(Self::GarbageCollection(v.try_into()?)),
                TaskSpec::WebhookCall(v) => Ok(Self::WebhookCall(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::tasks::TaskSpec, TaskSpec);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/Compaction
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecCompaction {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub params: Option<datasets::CompactionParams>,
    }

    impl IntoDto for TaskSpecCompaction {
        type Dto = dtos::tasks::TaskSpecCompaction;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecCompaction> for TaskSpecCompaction {
        fn from(v: dtos::tasks::TaskSpecCompaction) -> Self {
            Self {
                name: v.name,
                params: v.params.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecCompaction> for dtos::tasks::TaskSpecCompaction {
        type Error = ValidationError;
        fn try_from(v: TaskSpecCompaction) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                params: v
                    .params
                    .map(|v| dtos::datasets::CompactionParams::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::tasks::TaskSpecCompaction, TaskSpecCompaction);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/GarbageCollection
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecGarbageCollection {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
    }

    impl IntoDto for TaskSpecGarbageCollection {
        type Dto = dtos::tasks::TaskSpecGarbageCollection;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecGarbageCollection> for TaskSpecGarbageCollection {
        fn from(v: dtos::tasks::TaskSpecGarbageCollection) -> Self {
            Self { name: v.name }
        }
    }

    impl TryFrom<TaskSpecGarbageCollection> for dtos::tasks::TaskSpecGarbageCollection {
        type Error = ValidationError;
        fn try_from(v: TaskSpecGarbageCollection) -> Result<Self, ValidationError> {
            Ok(Self { name: v.name })
        }
    }

    implement_serde_as!(
        dtos::tasks::TaskSpecGarbageCollection,
        TaskSpecGarbageCollection
    );

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/Ingest
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecIngest {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        pub source: resources::ResourceHandle,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub params: Option<sources::IngestParams>,
    }

    impl IntoDto for TaskSpecIngest {
        type Dto = dtos::tasks::TaskSpecIngest;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecIngest> for TaskSpecIngest {
        fn from(v: dtos::tasks::TaskSpecIngest) -> Self {
            Self {
                name: v.name,
                source: v.source.into(),
                params: v.params.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecIngest> for dtos::tasks::TaskSpecIngest {
        type Error = ValidationError;
        fn try_from(v: TaskSpecIngest) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                source: dtos::resources::ResourceHandle::try_from(v.source)?,
                params: v
                    .params
                    .map(|v| dtos::sources::IngestParams::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::tasks::TaskSpecIngest, TaskSpecIngest);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(tag = "kind")]
    pub enum TaskSpecInput {
        #[serde(alias = "ingest")]
        Ingest(tasks::TaskSpecInputIngest),
        #[serde(alias = "transform")]
        Transform(tasks::TaskSpecInputTransform),
        #[serde(alias = "compaction")]
        Compaction(tasks::TaskSpecInputCompaction),
        #[serde(alias = "garbageCollection", alias = "garbagecollection")]
        GarbageCollection(tasks::TaskSpecInputGarbageCollection),
        #[serde(alias = "webhookCall", alias = "webhookcall")]
        WebhookCall(tasks::TaskSpecInputWebhookCall),
    }

    impl IntoDto for TaskSpecInput {
        type Dto = dtos::tasks::TaskSpecInput;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecInput> for TaskSpecInput {
        fn from(v: dtos::tasks::TaskSpecInput) -> Self {
            match v {
                dtos::tasks::TaskSpecInput::Ingest(v) => Self::Ingest(v.into()),
                dtos::tasks::TaskSpecInput::Transform(v) => Self::Transform(v.into()),
                dtos::tasks::TaskSpecInput::Compaction(v) => Self::Compaction(v.into()),
                dtos::tasks::TaskSpecInput::GarbageCollection(v) => {
                    Self::GarbageCollection(v.into())
                }
                dtos::tasks::TaskSpecInput::WebhookCall(v) => Self::WebhookCall(v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecInput> for dtos::tasks::TaskSpecInput {
        type Error = ValidationError;
        fn try_from(v: TaskSpecInput) -> Result<Self, Self::Error> {
            match v {
                TaskSpecInput::Ingest(v) => Ok(Self::Ingest(v.try_into()?)),
                TaskSpecInput::Transform(v) => Ok(Self::Transform(v.try_into()?)),
                TaskSpecInput::Compaction(v) => Ok(Self::Compaction(v.try_into()?)),
                TaskSpecInput::GarbageCollection(v) => Ok(Self::GarbageCollection(v.try_into()?)),
                TaskSpecInput::WebhookCall(v) => Ok(Self::WebhookCall(v.try_into()?)),
            }
        }
    }

    implement_serde_as!(dtos::tasks::TaskSpecInput, TaskSpecInput);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/Compaction
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecInputCompaction {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub params: Option<datasets::CompactionParams>,
    }

    impl IntoDto for TaskSpecInputCompaction {
        type Dto = dtos::tasks::TaskSpecInputCompaction;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecInputCompaction> for TaskSpecInputCompaction {
        fn from(v: dtos::tasks::TaskSpecInputCompaction) -> Self {
            Self {
                name: v.name,
                params: v.params.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecInputCompaction> for dtos::tasks::TaskSpecInputCompaction {
        type Error = ValidationError;
        fn try_from(v: TaskSpecInputCompaction) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                params: v
                    .params
                    .map(|v| dtos::datasets::CompactionParams::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(
        dtos::tasks::TaskSpecInputCompaction,
        TaskSpecInputCompaction
    );

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/GarbageCollection
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecInputGarbageCollection {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
    }

    impl IntoDto for TaskSpecInputGarbageCollection {
        type Dto = dtos::tasks::TaskSpecInputGarbageCollection;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecInputGarbageCollection> for TaskSpecInputGarbageCollection {
        fn from(v: dtos::tasks::TaskSpecInputGarbageCollection) -> Self {
            Self { name: v.name }
        }
    }

    impl TryFrom<TaskSpecInputGarbageCollection> for dtos::tasks::TaskSpecInputGarbageCollection {
        type Error = ValidationError;
        fn try_from(v: TaskSpecInputGarbageCollection) -> Result<Self, ValidationError> {
            Ok(Self { name: v.name })
        }
    }

    implement_serde_as!(
        dtos::tasks::TaskSpecInputGarbageCollection,
        TaskSpecInputGarbageCollection
    );

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/Ingest
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecInputIngest {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        pub source: StructOrString<resources::ResourceRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub params: Option<sources::IngestParams>,
    }

    impl IntoDto for TaskSpecInputIngest {
        type Dto = dtos::tasks::TaskSpecInputIngest;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecInputIngest> for TaskSpecInputIngest {
        fn from(v: dtos::tasks::TaskSpecInputIngest) -> Self {
            Self {
                name: v.name,
                source: v.source.into(),
                params: v.params.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecInputIngest> for dtos::tasks::TaskSpecInputIngest {
        type Error = ValidationError;
        fn try_from(v: TaskSpecInputIngest) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                source: dtos::resources::ResourceRef::try_from(v.source)?,
                params: v
                    .params
                    .map(|v| dtos::sources::IngestParams::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::tasks::TaskSpecInputIngest, TaskSpecInputIngest);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/Transform
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecInputTransform {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub target: Option<StructOrString<datasets::DatasetRef>>,
    }

    impl IntoDto for TaskSpecInputTransform {
        type Dto = dtos::tasks::TaskSpecInputTransform;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecInputTransform> for TaskSpecInputTransform {
        fn from(v: dtos::tasks::TaskSpecInputTransform) -> Self {
            Self {
                name: v.name,
                target: v.target.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecInputTransform> for dtos::tasks::TaskSpecInputTransform {
        type Error = ValidationError;
        fn try_from(v: TaskSpecInputTransform) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                target: v
                    .target
                    .map(|v| dtos::datasets::DatasetRef::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::tasks::TaskSpecInputTransform, TaskSpecInputTransform);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/WebhookCall
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecInputWebhookCall {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        pub target: StructOrString<resources::ResourceRef>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub payload: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub retry_policy: Option<flows::RetryPolicy>,
    }

    impl IntoDto for TaskSpecInputWebhookCall {
        type Dto = dtos::tasks::TaskSpecInputWebhookCall;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecInputWebhookCall> for TaskSpecInputWebhookCall {
        fn from(v: dtos::tasks::TaskSpecInputWebhookCall) -> Self {
            Self {
                name: v.name,
                target: v.target.into(),
                payload: v.payload,
                retry_policy: v.retry_policy.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecInputWebhookCall> for dtos::tasks::TaskSpecInputWebhookCall {
        type Error = ValidationError;
        fn try_from(v: TaskSpecInputWebhookCall) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                target: dtos::resources::ResourceRef::try_from(v.target)?,
                payload: v.payload,
                retry_policy: v
                    .retry_policy
                    .map(|v| dtos::flows::RetryPolicy::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(
        dtos::tasks::TaskSpecInputWebhookCall,
        TaskSpecInputWebhookCall
    );

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/Transform
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecTransform {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub target: Option<datasets::DatasetHandle>,
    }

    impl IntoDto for TaskSpecTransform {
        type Dto = dtos::tasks::TaskSpecTransform;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecTransform> for TaskSpecTransform {
        fn from(v: dtos::tasks::TaskSpecTransform) -> Self {
            Self {
                name: v.name,
                target: v.target.map(|v| v.into()),
            }
        }
    }

    impl TryFrom<TaskSpecTransform> for dtos::tasks::TaskSpecTransform {
        type Error = ValidationError;
        fn try_from(v: TaskSpecTransform) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                target: v
                    .target
                    .map(|v| dtos::datasets::DatasetHandle::try_from(v))
                    .transpose()?,
            })
        }
    }

    implement_serde_as!(dtos::tasks::TaskSpecTransform, TaskSpecTransform);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/WebhookCall
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    pub struct TaskSpecWebhookCall {
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        pub target: resources::ResourceHandle,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub payload: Option<String>,
    }

    impl IntoDto for TaskSpecWebhookCall {
        type Dto = dtos::tasks::TaskSpecWebhookCall;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskSpecWebhookCall> for TaskSpecWebhookCall {
        fn from(v: dtos::tasks::TaskSpecWebhookCall) -> Self {
            Self {
                name: v.name,
                target: v.target.into(),
                payload: v.payload,
            }
        }
    }

    impl TryFrom<TaskSpecWebhookCall> for dtos::tasks::TaskSpecWebhookCall {
        type Error = ValidationError;
        fn try_from(v: TaskSpecWebhookCall) -> Result<Self, ValidationError> {
            Ok(Self {
                name: v.name,
                target: dtos::resources::ResourceHandle::try_from(v.target)?,
                payload: v.payload,
            })
        }
    }

    implement_serde_as!(dtos::tasks::TaskSpecWebhookCall, TaskSpecWebhookCall);

    // Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskStatus
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum TaskStatus {
        #[serde(alias = "pending")]
        Pending,
        #[serde(alias = "planning")]
        Planning,
        #[serde(alias = "ready")]
        Ready,
        #[serde(alias = "running")]
        Running,
        #[serde(alias = "committing")]
        Committing,
        #[serde(alias = "finished")]
        Finished,
    }

    impl IntoDto for TaskStatus {
        type Dto = dtos::tasks::TaskStatus;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            self.try_into()
        }
    }

    impl From<dtos::tasks::TaskStatus> for TaskStatus {
        fn from(v: dtos::tasks::TaskStatus) -> Self {
            match v {
                dtos::tasks::TaskStatus::Pending => Self::Pending,
                dtos::tasks::TaskStatus::Planning => Self::Planning,
                dtos::tasks::TaskStatus::Ready => Self::Ready,
                dtos::tasks::TaskStatus::Running => Self::Running,
                dtos::tasks::TaskStatus::Committing => Self::Committing,
                dtos::tasks::TaskStatus::Finished => Self::Finished,
            }
        }
    }

    impl TryFrom<TaskStatus> for dtos::tasks::TaskStatus {
        type Error = ValidationError;
        fn try_from(v: TaskStatus) -> Result<Self, Self::Error> {
            match v {
                TaskStatus::Pending => Ok(Self::Pending),
                TaskStatus::Planning => Ok(Self::Planning),
                TaskStatus::Ready => Ok(Self::Ready),
                TaskStatus::Running => Ok(Self::Running),
                TaskStatus::Committing => Ok(Self::Committing),
                TaskStatus::Finished => Ok(Self::Finished),
            }
        }
    }

    implement_serde_as!(dtos::tasks::TaskStatus, TaskStatus);
}
