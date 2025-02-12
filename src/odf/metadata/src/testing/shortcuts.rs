// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{AccountName, DatasetAlias, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn alias(account: &str, dataset_name: &str) -> DatasetAlias {
    DatasetAlias::new(
        Some(AccountName::new_unchecked(account)),
        DatasetName::new_unchecked(dataset_name),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
