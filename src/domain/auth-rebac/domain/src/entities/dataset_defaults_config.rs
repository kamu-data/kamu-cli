// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::DatasetProperties;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Access properties of every dataset whose owner has not set them.
///
/// Only properties that differ from these are stored, so changing a default
/// applies to all datasets that do not override it, existing ones included.
#[derive(setty::Config, setty::Default)]
pub struct DatasetDefaultsConfig {
    /// Whether anonymous callers can read a public dataset
    #[config(default = true)]
    pub allows_anonymous_read: bool,
}

impl DatasetDefaultsConfig {
    /// Datasets are private unless created or made public
    pub fn dataset_properties(&self) -> DefaultDatasetProperties {
        DatasetProperties {
            allows_public_read: false,
            allows_anonymous_read: self.allows_anonymous_read,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Properties of a dataset that has not set its own, registered from
/// [`DatasetDefaultsConfig`]
pub type DefaultDatasetProperties = DatasetProperties;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
