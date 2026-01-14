// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};

pub fn extend_graphql_request(
    request: async_graphql::Request,
    catalog: &Catalog,
) -> async_graphql::Request {
    request
        .data(account_entity_data_loader(catalog))
        .data(dataset_handle_data_loader(catalog))
}
