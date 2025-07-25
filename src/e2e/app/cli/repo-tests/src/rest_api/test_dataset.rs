// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_adapter_http::general::{DatasetInfoResponse, DatasetOwnerInfo};
use kamu_cli_e2e_common::{
    CreateDatasetResponse,
    DatasetByIdError,
    KamuApiServerClient,
    KamuApiServerClientExt,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_datasets_by_id(mut kamu_api_server_client: KamuApiServerClient) {
    let (_, nonexistent_dataset_id) = odf::DatasetID::new_generated_ed25519();

    assert_matches!(
        kamu_api_server_client
            .dataset()
            .by_id(&nonexistent_dataset_id)
            .await,
        Err(DatasetByIdError::NotFound)
    );

    kamu_api_server_client.auth().login_as_kamu().await;

    assert_matches!(
        kamu_api_server_client
            .dataset()
            .by_id(&nonexistent_dataset_id)
            .await,
        Err(DatasetByIdError::NotFound)
    );

    let CreateDatasetResponse { dataset_id, .. } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    let expected_owner = DatasetOwnerInfo {
        account_name: odf::AccountName::new_unchecked("kamu"),
        account_id: Some(odf::AccountID::new_seeded_ed25519(b"kamu")),
    };

    assert_matches!(
        kamu_api_server_client.dataset().by_id(&dataset_id).await,
        Ok(DatasetInfoResponse {
            id,
            owner,
            dataset_name
        })
            if id == dataset_id
                && owner == Some(expected_owner)
                && dataset_name == odf::DatasetName::new_unchecked("player-scores")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_datasets_endpoint_restricted_for_anonymous(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    let (_, nonexistent_dataset_id) = odf::DatasetID::new_generated_ed25519();

    assert_matches!(
        kamu_api_server_client
            .dataset()
            .by_id(&nonexistent_dataset_id)
            .await,
        Err(DatasetByIdError::Unauthorized)
    );

    kamu_api_server_client.auth().login_as_kamu().await;

    assert_matches!(
        kamu_api_server_client
            .dataset()
            .by_id(&nonexistent_dataset_id)
            .await,
        Err(DatasetByIdError::NotFound)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
