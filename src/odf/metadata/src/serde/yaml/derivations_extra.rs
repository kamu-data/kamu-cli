// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::dtos;
use crate::errors::ValidationError;
use crate::serde::yaml as proxies;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for proxies::auth::AccountRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            id: None,
            name: Some(crate::auth::AccountName::try_from(s).map_err(|e| e.to_string())?),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for proxies::config::Secret {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            value: s.into(),
            content_encoding: None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for proxies::config::Variable {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { value: s.into() })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<dtos::auth::AccountRef> for proxies::auth::AccountRef {
    fn from(v: dtos::auth::AccountRef) -> Self {
        match v {
            dtos::auth::AccountRef::Id(id) => Self {
                id: Some(id),
                name: None,
            },
            dtos::auth::AccountRef::Name(name) => Self {
                id: None,
                name: Some(name),
            },
            dtos::auth::AccountRef::Both { id, name } => Self {
                id: Some(id),
                name: Some(name),
            },
        }
    }
}

impl TryFrom<proxies::auth::AccountRef> for dtos::auth::AccountRef {
    type Error = ValidationError;
    fn try_from(v: proxies::auth::AccountRef) -> Result<Self, ValidationError> {
        match v {
            proxies::auth::AccountRef {
                id: None,
                name: None,
            } => Err(ValidationError::new(
                "AccountRef must specify id or name or both",
            )),
            proxies::auth::AccountRef {
                id: Some(id),
                name: None,
            } => Ok(Self::Id(id)),
            proxies::auth::AccountRef {
                id: None,
                name: Some(name),
            } => Ok(Self::Name(name)),
            proxies::auth::AccountRef {
                id: Some(id),
                name: Some(name),
            } => Ok(Self::Both { id, name }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for proxies::resource::ResourceRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = dtos::resource::ResourceRef::try_from(s).map_err(|e| e.to_string())?;
        Ok(v.into())
    }
}

impl From<dtos::resource::ResourceRef> for proxies::resource::ResourceRef {
    fn from(v: dtos::resource::ResourceRef) -> Self {
        match v {
            dtos::resource::ResourceRef::Id { account, typ, id } => Self {
                account: account.map(Into::into),
                r#type: typ,
                id: Some(id),
                name: None,
            },
            dtos::resource::ResourceRef::Name { account, typ, name } => Self {
                account: account.map(Into::into),
                r#type: typ,
                id: None,
                name: Some(name),
            },
            dtos::resource::ResourceRef::Both {
                account,
                typ,
                id,
                name,
            } => Self {
                account: account.map(Into::into),
                r#type: typ,
                id: Some(id),
                name: Some(name),
            },
        }
    }
}

impl TryFrom<proxies::resource::ResourceRef> for dtos::resource::ResourceRef {
    type Error = ValidationError;
    fn try_from(v: proxies::resource::ResourceRef) -> Result<Self, Self::Error> {
        match v {
            proxies::resource::ResourceRef {
                account: _,
                r#type: _,
                id: None,
                name: None,
            } => Err(ValidationError::new(
                "ResourceRef must specify id or name or both",
            )),
            proxies::resource::ResourceRef {
                account,
                r#type,
                id: Some(id),
                name: None,
            } => Ok(Self::Id {
                account: account.map(TryInto::try_into).transpose()?,
                typ: r#type,
                id,
            }),
            proxies::resource::ResourceRef {
                account,
                r#type,
                id: None,
                name: Some(name),
            } => Ok(Self::Name {
                account: account.map(TryInto::try_into).transpose()?,
                typ: r#type,
                name,
            }),
            proxies::resource::ResourceRef {
                account,
                r#type,
                id: Some(id),
                name: Some(name),
            } => Ok(Self::Both {
                account: account.map(TryInto::try_into).transpose()?,
                typ: r#type,
                id,
                name,
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for proxies::config::ValueRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = dtos::config::ValueRef::try_from(s).map_err(|e| e.to_string())?;
        Ok(v.into())
    }
}

impl From<dtos::config::ValueRef> for proxies::config::ValueRef {
    fn from(v: dtos::config::ValueRef) -> Self {
        let r: proxies::resource::ResourceRef = v.resource.into();
        let proxies::resource::ResourceRef {
            account,
            r#type,
            id,
            name,
        } = r;
        Self {
            account,
            r#type,
            id,
            name,
            path: v.path,
        }
    }
}

impl TryFrom<proxies::config::ValueRef> for dtos::config::ValueRef {
    type Error = ValidationError;
    fn try_from(v: proxies::config::ValueRef) -> Result<Self, Self::Error> {
        let proxies::config::ValueRef {
            account,
            r#type,
            id,
            name,
            path,
        } = v;

        let r = proxies::resource::ResourceRef {
            account,
            r#type,
            id,
            name,
        };

        Ok(Self::new(r.try_into()?, path))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for proxies::storage::PersistentVolumeRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = dtos::storage::PersistentVolumeRef::try_from(s).map_err(|e| e.to_string())?;
        Ok(v.into())
    }
}

impl From<dtos::storage::PersistentVolumeRef> for proxies::storage::PersistentVolumeRef {
    fn from(v: dtos::storage::PersistentVolumeRef) -> Self {
        let r: proxies::resource::ResourceRef = v.into_inner().into();
        let proxies::resource::ResourceRef {
            account,
            r#type: _,
            id,
            name,
        } = r;
        proxies::storage::PersistentVolumeRef { account, id, name }
    }
}

impl TryFrom<proxies::storage::PersistentVolumeRef> for dtos::storage::PersistentVolumeRef {
    type Error = ValidationError;
    fn try_from(v: proxies::storage::PersistentVolumeRef) -> Result<Self, Self::Error> {
        let proxies::storage::PersistentVolumeRef { account, id, name } = v;
        let r = proxies::resource::ResourceRef {
            account,
            r#type: dtos::storage::PersistentVolume::schema().clone().into(),
            id,
            name,
        };
        Ok(Self::new_unchecked(r.try_into()?))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for proxies::resource::ResourceSelector {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = dtos::resource::ResourceSelector::try_from(s).map_err(|e| e.to_string())?;
        Ok(v.into())
    }
}

impl From<dtos::resource::ResourceSelector> for proxies::resource::ResourceSelector {
    fn from(_v: dtos::resource::ResourceSelector) -> Self {
        todo!()
    }
}

impl TryFrom<proxies::resource::ResourceSelector> for dtos::resource::ResourceSelector {
    type Error = ValidationError;
    fn try_from(v: proxies::resource::ResourceSelector) -> Result<Self, Self::Error> {
        let proxies::resource::ResourceSelector {
            account,
            r#type: _,
            id: _,
            name: _,
            labels: _,
        } = v;
        Ok(Self {
            account: account.map(TryInto::try_into).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for proxies::dataset::DatasetSelector {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = dtos::dataset::DatasetSelector::try_from(s).map_err(|e| e.to_string())?;
        Ok(v.into())
    }
}

impl From<dtos::dataset::DatasetSelector> for proxies::dataset::DatasetSelector {
    fn from(_v: dtos::dataset::DatasetSelector) -> Self {
        todo!()
    }
}

impl TryFrom<proxies::dataset::DatasetSelector> for dtos::dataset::DatasetSelector {
    type Error = ValidationError;
    fn try_from(_v: proxies::dataset::DatasetSelector) -> Result<Self, Self::Error> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
