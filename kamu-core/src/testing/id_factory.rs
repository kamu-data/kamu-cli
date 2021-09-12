// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use opendatafabric::DatasetIDBuf;
use rand::Rng;

pub struct IDFactory;

/// Generates randomized unique identities for different resources
impl IDFactory {
    pub fn dataset_id() -> DatasetIDBuf {
        // TODO: create more readable IDs like docker does
        let mut id = String::with_capacity(20);
        id.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(20)
                .map(char::from),
        );
        DatasetIDBuf::try_from(id).unwrap()
    }
}
