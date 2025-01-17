// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
pub trait DidGenerator: Send + Sync {
    fn generate_dataset_id(&self) -> odf::DatasetID;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DidGenerator)]
pub struct DidGeneratorDefault;

impl DidGenerator for DidGeneratorDefault {
    fn generate_dataset_id(&self) -> odf::DatasetID {
        let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
        dataset_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "testing", test))]
impl MockDidGenerator {
    pub fn predefined_dataset_ids(dataset_ids: Vec<odf::DatasetID>) -> Self {
        let mut mock = Self::default();

        let dataset_ids_count = dataset_ids.len();
        let mut dataset_ids_it = dataset_ids.into_iter();

        mock.expect_generate_dataset_id()
            .times(dataset_ids_count)
            .returning(move || dataset_ids_it.next().unwrap());

        mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
