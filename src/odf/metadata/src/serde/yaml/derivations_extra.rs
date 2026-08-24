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

impl From<dtos::resource::ResourceSelector> for proxies::resource::ResourceSelector {
    fn from(v: dtos::resource::ResourceSelector) -> Self {
        let dtos::resource::ResourceSelector {
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

impl TryFrom<proxies::resource::ResourceSelector> for dtos::resource::ResourceSelector {
    type Error = ValidationError;
    fn try_from(v: proxies::resource::ResourceSelector) -> Result<Self, Self::Error> {
        let proxies::resource::ResourceSelector {
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
