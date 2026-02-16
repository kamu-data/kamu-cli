// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ensure_model(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    let model_key = EmbeddingModelKey {
        provider: "test-provider",
        name: "test-model".to_string(),
        revision: Some("v1.0"),
    };

    let result = repo.ensure_model(&model_key, 384).await;

    assert!(
        result.is_ok(),
        "ensure_model should succeed, but got error: {result:?}"
    );

    let model_row = result.unwrap();
    assert_eq!(model_row.key, model_key);
    assert_eq!(model_row.dims, 384);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
