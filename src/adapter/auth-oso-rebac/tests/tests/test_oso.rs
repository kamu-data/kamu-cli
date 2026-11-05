// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_auth_oso_rebac::{DatasetResource, KamuAuthOso, UserActor};
use kamu_auth_rebac::{AccountToDatasetRelation as Role, DatasetProperties};
use kamu_datasets::DatasetAction;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! assert_allowed {
    ($check_result: expr) => {
        assert!($check_result.is_ok() && $check_result.unwrap());
    };
}

macro_rules! assert_forbidden {
    ($check_result: expr) => {
        assert!($check_result.is_ok() && !$check_result.unwrap());
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write() {
    let owner_account_id = random_account_id();
    let is_admin = false;
    let can_provision_accounts = false;
    let owner_user_actor = UserActor::logged(&owner_account_id, is_admin, can_provision_accounts);

    let allows_public_read = false;
    let owned_dataset_resource = DatasetResource::new(
        &owner_account_id,
        &dataset_properties(allows_public_read, allows_public_read),
    );

    let oso = KamuAuthOso::new();

    let write_result = oso.is_allowed(
        owner_user_actor.clone(),
        DatasetAction::Write,
        owned_dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        owner_user_actor.clone(),
        DatasetAction::Read,
        owned_dataset_resource.clone(),
    );

    assert_allowed!(write_result);
    assert_allowed!(read_result);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unrelated_can_read_public() {
    let is_admin = false;
    let can_provision_accounts = false;
    let unrelated_user_actor =
        UserActor::logged(&random_account_id(), is_admin, can_provision_accounts);

    let allows_public_read = true;
    let public_dataset_resource = DatasetResource::new(
        &random_account_id(),
        &dataset_properties(allows_public_read, allows_public_read),
    );

    let oso = KamuAuthOso::new();

    let write_result = oso.is_allowed(
        unrelated_user_actor.clone(),
        DatasetAction::Write,
        public_dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        unrelated_user_actor.clone(),
        DatasetAction::Read,
        public_dataset_resource.clone(),
    );

    assert_forbidden!(write_result);
    assert_allowed!(read_result);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unrelated_cannot_read_private() {
    let is_admin = false;
    let can_provision_accounts = false;
    let unrelated_user_actor =
        UserActor::logged(&random_account_id(), is_admin, can_provision_accounts);

    let allows_public_read = false;
    let private_dataset_resource = DatasetResource::new(
        &random_account_id(),
        &dataset_properties(allows_public_read, allows_public_read),
    );

    let oso = KamuAuthOso::new();

    let write_result = oso.is_allowed(
        unrelated_user_actor.clone(),
        DatasetAction::Write,
        private_dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        unrelated_user_actor.clone(),
        DatasetAction::Read,
        private_dataset_resource.clone(),
    );

    assert_forbidden!(write_result);
    assert_forbidden!(read_result);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_having_explicit_read_permission_in_private_dataset() {
    let reader_account_id = random_account_id();
    let is_admin = false;
    let can_provision_accounts = false;
    let reader_user_actor = UserActor::logged(&reader_account_id, is_admin, can_provision_accounts);

    let allows_public_read = false;
    let mut private_dataset_resource = DatasetResource::new(
        &random_account_id(),
        &dataset_properties(allows_public_read, allows_public_read),
    );
    private_dataset_resource.authorize_account(&reader_account_id, Role::Reader);

    let oso = KamuAuthOso::new();

    let write_result = oso.is_allowed(
        reader_user_actor.clone(),
        DatasetAction::Write,
        private_dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        reader_user_actor.clone(),
        DatasetAction::Read,
        private_dataset_resource.clone(),
    );

    assert_forbidden!(write_result);
    assert_allowed!(read_result);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_having_explicit_write_permission_in_private_dataset() {
    let editor_account_id = random_account_id();
    let is_admin = false;
    let can_provision_accounts = false;
    let editor_user_actor = UserActor::logged(&editor_account_id, is_admin, can_provision_accounts);

    let allows_public_read = false;
    let mut private_dataset_resource = DatasetResource::new(
        &random_account_id(),
        &dataset_properties(allows_public_read, allows_public_read),
    );
    private_dataset_resource.authorize_account(&editor_account_id, Role::Editor);

    let oso = KamuAuthOso::new();

    let write_result = oso.is_allowed(
        editor_user_actor.clone(),
        DatasetAction::Write,
        private_dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        editor_user_actor.clone(),
        DatasetAction::Read,
        private_dataset_resource.clone(),
    );

    assert_allowed!(write_result);
    assert_allowed!(read_result);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_another_private_dataset() {
    let is_admin = true;
    let can_provision_accounts = false;
    let admin_user_actor =
        UserActor::logged(&random_account_id(), is_admin, can_provision_accounts);

    let allows_public_read = false;
    let dataset_resource = DatasetResource::new(
        &random_account_id(),
        &dataset_properties(allows_public_read, allows_public_read),
    );

    let oso = KamuAuthOso::new();

    let write_result = oso.is_allowed(
        admin_user_actor.clone(),
        DatasetAction::Write,
        dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        admin_user_actor.clone(),
        DatasetAction::Read,
        dataset_resource.clone(),
    );

    assert_allowed!(write_result);
    assert_allowed!(read_result);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_read_of_public_dataset_follows_anonymous_flag() {
    let oso = KamuAuthOso::new();
    let is_admin = false;
    let can_provision_accounts = false;
    let logged_user_actor =
        UserActor::logged(&random_account_id(), is_admin, can_provision_accounts);

    let open_to_anonymous =
        DatasetResource::new(&random_account_id(), &dataset_properties(true, true));
    let closed_to_anonymous =
        DatasetResource::new(&random_account_id(), &dataset_properties(true, false));

    let anonymous_read_open = oso.is_allowed(
        UserActor::anonymous(),
        DatasetAction::Read,
        open_to_anonymous.clone(),
    );
    let anonymous_read_closed = oso.is_allowed(
        UserActor::anonymous(),
        DatasetAction::Read,
        closed_to_anonymous.clone(),
    );
    let logged_read_closed = oso.is_allowed(
        logged_user_actor,
        DatasetAction::Read,
        closed_to_anonymous.clone(),
    );

    assert_allowed!(anonymous_read_open);
    assert_forbidden!(anonymous_read_closed);
    assert_allowed!(logged_read_closed);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn dataset_properties(allows_public_read: bool, allows_anonymous_read: bool) -> DatasetProperties {
    DatasetProperties {
        allows_anonymous_read,
        allows_public_read,
    }
}

fn random_account_id() -> odf::AccountID {
    odf::AccountID::new_generated_ed25519().1
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
