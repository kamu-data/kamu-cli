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

impl From<dtos::auth::SubjectRef> for proxies::auth::SubjectRef {
    fn from(v: dtos::auth::SubjectRef) -> Self {
        match v {
            dtos::auth::SubjectRef::Principal(dtos::auth::PrincipalRef {
                r#type,
                account,
                id,
                did,
                name,
            }) => Self::Principal(proxies::auth::SubjectRefPrincipal {
                r#type,
                account: account.map(Into::into),
                id,
                did,
                name,
            }),
            dtos::auth::SubjectRef::Group(dtos::resources::ResourceRef {
                account,
                id,
                did: _,
                r#type: _,
                name,
            }) => Self::Group(proxies::auth::SubjectRefGroup {
                account: account.map(Into::into),
                id,
                name,
            }),
            dtos::auth::SubjectRef::Authenticated => {
                Self::Authenticated(proxies::auth::SubjectRefAuthenticated {})
            }
            dtos::auth::SubjectRef::Anonymous => {
                Self::Anonymous(proxies::auth::SubjectRefAnonymous {})
            }
            dtos::auth::SubjectRef::Everyone => {
                Self::Everyone(proxies::auth::SubjectRefEveryone {})
            }
        }
    }
}

impl TryFrom<proxies::auth::SubjectRef> for dtos::auth::SubjectRef {
    type Error = ValidationError;
    fn try_from(v: proxies::auth::SubjectRef) -> Result<Self, Self::Error> {
        match v {
            proxies::auth::SubjectRef::Principal(proxies::auth::SubjectRefPrincipal {
                r#type,
                account,
                id,
                did,
                name,
            }) => Ok(Self::Principal(dtos::auth::PrincipalRef {
                r#type,
                account: account.map(TryInto::try_into).transpose()?,
                id,
                did,
                name,
            })),
            proxies::auth::SubjectRef::Group(proxies::auth::SubjectRefGroup {
                account,
                id,
                name,
            }) => Ok(Self::Group(dtos::resources::ResourceRef {
                account: account.map(TryInto::try_into).transpose()?,
                id,
                did: None,
                r#type: Some("Group".parse().unwrap()),
                name,
            })),
            proxies::auth::SubjectRef::Authenticated(proxies::auth::SubjectRefAuthenticated {}) => {
                Ok(Self::Authenticated)
            }
            proxies::auth::SubjectRef::Anonymous(proxies::auth::SubjectRefAnonymous {}) => {
                Ok(Self::Anonymous)
            }
            proxies::auth::SubjectRef::Everyone(proxies::auth::SubjectRefEveryone {}) => {
                Ok(Self::Everyone)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<dtos::auth::SubjectHandle> for proxies::auth::SubjectHandle {
    fn from(_v: dtos::auth::SubjectHandle) -> Self {
        todo!()
    }
}

impl TryFrom<proxies::auth::SubjectHandle> for dtos::auth::SubjectHandle {
    type Error = ValidationError;
    fn try_from(_v: proxies::auth::SubjectHandle) -> Result<Self, Self::Error> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<dtos::resources::ResourceSelector> for proxies::resources::ResourceSelector {
    fn from(v: dtos::resources::ResourceSelector) -> Self {
        let dtos::resources::ResourceSelector {
            account,
            id,
            did,
            r#type,
            name,
            labels,
        } = v;
        Self {
            account: account.map(Into::into),
            id,
            did,
            r#type,
            name,
            labels: labels.map(Into::into),
        }
    }
}

impl TryFrom<proxies::resources::ResourceSelector> for dtos::resources::ResourceSelector {
    type Error = ValidationError;
    fn try_from(v: proxies::resources::ResourceSelector) -> Result<Self, Self::Error> {
        let proxies::resources::ResourceSelector {
            account,
            id,
            did,
            r#type,
            name,
            labels,
        } = v;
        Ok(Self {
            account: account.map(TryInto::try_into).transpose()?,
            id,
            did,
            r#type,
            name,
            labels: labels.map(TryInto::try_into).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<dtos::datasets::DatasetSelector> for proxies::datasets::DatasetSelector {
    fn from(_v: dtos::datasets::DatasetSelector) -> Self {
        todo!()
    }
}

impl TryFrom<proxies::datasets::DatasetSelector> for dtos::datasets::DatasetSelector {
    type Error = ValidationError;
    fn try_from(_v: proxies::datasets::DatasetSelector) -> Result<Self, Self::Error> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
