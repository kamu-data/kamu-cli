// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TenancyConfig {
    SingleTenant,
    MultiTenant,
}

impl TenancyConfig {
    pub fn make_alias(
        &self,
        owner_name: odf::AccountName,
        dataset_name: odf::DatasetName,
    ) -> odf::DatasetAlias {
        match *self {
            TenancyConfig::MultiTenant => odf::DatasetAlias::new(Some(owner_name), dataset_name),
            TenancyConfig::SingleTenant => odf::DatasetAlias::new(None, dataset_name),
        }
    }

    pub fn default_dataset_visibility(&self) -> odf::DatasetVisibility {
        match *self {
            TenancyConfig::MultiTenant => odf::DatasetVisibility::Private,
            TenancyConfig::SingleTenant => odf::DatasetVisibility::Public,
        }
    }
}

impl Default for TenancyConfig {
    fn default() -> Self {
        Self::SingleTenant
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
