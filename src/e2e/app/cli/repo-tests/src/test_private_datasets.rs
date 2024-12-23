// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_cli_e2e_common::{
    CreateDatasetResponse,
    GetDatasetVisibilityError,
    KamuApiServerClient,
    KamuApiServerClientExt,
    SetDatasetVisibilityError,
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_only_dataset_owner_can_change_dataset_visibility(
    kamu_api_server_client: KamuApiServerClient,
) {
    let mut owner_client = kamu_api_server_client.clone();
    owner_client.auth().login_as_kamu().await;
    let CreateDatasetResponse { dataset_id, .. } = owner_client
        .dataset()
        .create_dataset_with_visibility(
            &DATASET_ROOT_PLAYER_SCORES_SNAPSHOT,
            odf::DatasetVisibility::Public,
        )
        .await;

    let mut not_owner_client = kamu_api_server_client;
    not_owner_client.auth().login_as_e2e_user().await;

    assert_matches!(
        not_owner_client
            .dataset()
            .set_visibility(&dataset_id, odf::DatasetVisibility::Private)
            .await,
        Err(SetDatasetVisibilityError::Forbidden)
    );

    assert_matches!(
        owner_client.dataset().get_visibility(&dataset_id).await,
        Ok(odf::DatasetVisibility::Public)
    );
    assert_matches!(
        not_owner_client.dataset().get_visibility(&dataset_id).await,
        Ok(odf::DatasetVisibility::Public)
    );

    assert_matches!(
        owner_client
            .dataset()
            .set_visibility(&dataset_id, odf::DatasetVisibility::Private)
            .await,
        Ok(_)
    );

    assert_matches!(
        owner_client.dataset().get_visibility(&dataset_id).await,
        Ok(odf::DatasetVisibility::Private)
    );
    assert_matches!(
        not_owner_client.dataset().get_visibility(&dataset_id).await,
        Err(GetDatasetVisibilityError::NotFound)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_admin_can_change_visibility_of_any_dataset(
    kamu_api_server_client: KamuApiServerClient,
) {
    let mut admin_client = kamu_api_server_client.clone();
    admin_client.auth().login_as_kamu().await;

    let mut owner_client = kamu_api_server_client;
    owner_client.auth().login_as_e2e_user().await;
    let CreateDatasetResponse {
        dataset_id: private_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_with_visibility(
            &DATASET_ROOT_PLAYER_SCORES_SNAPSHOT,
            odf::DatasetVisibility::Private,
        )
        .await;
    let CreateDatasetResponse {
        dataset_id: public_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_with_visibility(
            &DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT,
            odf::DatasetVisibility::Public,
        )
        .await;

    assert_matches!(
        admin_client
            .dataset()
            .get_visibility(&private_dataset_id)
            .await,
        Ok(odf::DatasetVisibility::Private)
    );
    assert_matches!(
        owner_client
            .dataset()
            .get_visibility(&private_dataset_id)
            .await,
        Ok(odf::DatasetVisibility::Private)
    );
    assert_matches!(
        admin_client
            .dataset()
            .get_visibility(&public_dataset_id)
            .await,
        Ok(odf::DatasetVisibility::Public)
    );
    assert_matches!(
        owner_client
            .dataset()
            .get_visibility(&public_dataset_id)
            .await,
        Ok(odf::DatasetVisibility::Public)
    );

    assert_matches!(
        admin_client
            .dataset()
            .set_visibility(&private_dataset_id, odf::DatasetVisibility::Public)
            .await,
        Ok(_)
    );
    let before_private_now_public_dataset = private_dataset_id;

    assert_matches!(
        admin_client
            .dataset()
            .set_visibility(&public_dataset_id, odf::DatasetVisibility::Private)
            .await,
        Ok(_)
    );
    let before_public_now_private_dataset = public_dataset_id;

    assert_matches!(
        admin_client
            .dataset()
            .get_visibility(&before_private_now_public_dataset)
            .await,
        Ok(odf::DatasetVisibility::Public)
    );
    assert_matches!(
        owner_client
            .dataset()
            .get_visibility(&before_private_now_public_dataset)
            .await,
        Ok(odf::DatasetVisibility::Public)
    );
    assert_matches!(
        admin_client
            .dataset()
            .get_visibility(&before_public_now_private_dataset)
            .await,
        Ok(odf::DatasetVisibility::Private)
    );
    assert_matches!(
        owner_client
            .dataset()
            .get_visibility(&before_public_now_private_dataset)
            .await,
        Ok(odf::DatasetVisibility::Private)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
