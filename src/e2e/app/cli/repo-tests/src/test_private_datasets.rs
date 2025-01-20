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
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_only_the_dataset_owner_or_admin_can_change_its_visibility(
    kamu_api_server_client: KamuApiServerClient,
) {
    let mut owner_client = kamu_api_server_client.clone();
    owner_client
        .auth()
        .login_with_password("alice", "alice")
        .await;
    let CreateDatasetResponse { dataset_id, .. } = owner_client
        .dataset()
        .create_dataset_with_visibility(
            &DATASET_ROOT_PLAYER_SCORES_SNAPSHOT,
            odf::DatasetVisibility::Public,
        )
        .await;

    let mut not_owner_client = kamu_api_server_client.clone();
    not_owner_client
        .auth()
        .login_with_password("bob", "bob")
        .await;

    let mut admin_client = kamu_api_server_client;
    admin_client
        .auth()
        .login_with_password("admin", "admin")
        .await;

    {
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
            admin_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
    }
    {
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
        assert_matches!(
            admin_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Private)
        );
    }
    {
        assert_matches!(
            admin_client
                .dataset()
                .set_visibility(&dataset_id, odf::DatasetVisibility::Public)
                .await,
            Ok(_)
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
            admin_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
