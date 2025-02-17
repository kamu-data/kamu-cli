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
    fn generate_dataset_id(&self) -> (odf::DatasetID, odf::metadata::PrivateKey);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DidGenerator)]
pub struct DidGeneratorDefault;

impl DidGenerator for DidGeneratorDefault {
    fn generate_dataset_id(&self) -> (odf::DatasetID, odf::metadata::PrivateKey) {
        let (key, dataset_id) = odf::DatasetID::new_generated_ed25519();
        (dataset_id, key.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "testing", test))]
impl MockDidGenerator {
    pub fn predefined_dataset_ids(dataset_ids: Vec<odf::DatasetID>) -> Self {
        // Note: we generate completely unrelated pk assuming tests will not care
        let random_key: odf::metadata::PrivateKey =
            odf::DatasetID::new_generated_ed25519().0.into();

        let mut mock = Self::default();

        let mut dataset_ids_it = dataset_ids.into_iter();

        mock.expect_generate_dataset_id()
            .returning(move || (dataset_ids_it.next().unwrap(), random_key.clone()));

        mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
