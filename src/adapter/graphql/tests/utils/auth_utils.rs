// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_graphql::ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE;
use kamu_core::{AnonymousAccountReason, CurrentAccountSubject};

/////////////////////////////////////////////////////////////////////////////////////////

pub fn authentication_catalogs(base_catalog: &dill::Catalog) -> (dill::Catalog, dill::Catalog) {
    let catalog_anonymous = dill::CatalogBuilder::new_chained(&base_catalog)
        .add_value(CurrentAccountSubject::anonymous(
            AnonymousAccountReason::NoAuthenticationProvided,
        ))
        .build();

    let catalog_authorized = dill::CatalogBuilder::new_chained(&base_catalog)
        .add_value(CurrentAccountSubject::new_test())
        .build();

    (catalog_anonymous, catalog_authorized)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub fn expect_anonymous_access_error(response: async_graphql::Response) {
    assert!(response.is_err());
    assert_eq!(
        response
            .errors
            .into_iter()
            .map(|e| e.message)
            .collect::<Vec<_>>(),
        vec![ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE.to_string()]
    );
}

/////////////////////////////////////////////////////////////////////////////////////////
