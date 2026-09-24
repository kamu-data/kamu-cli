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
