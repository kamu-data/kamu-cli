// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_auth_oso::dataset_resource::DatasetResource;
use kamu_adapter_auth_oso::load_oso;
use kamu_adapter_auth_oso::user_actor::UserActor;
use kamu_core::auth::DatasetAction;

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write() {
    let user_actor = UserActor::new("foo");
    let dataset_resource = DatasetResource::new("foo", false);

    let oso = load_oso().unwrap();

    let write_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Write),
        dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Read),
        dataset_resource.clone(),
    );

    assert_allowed!(write_result);
    assert_allowed!(read_result);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unrelated_can_read_public() {
    let user_actor = UserActor::new("foo");
    let dataset_resource = DatasetResource::new("bar", true);

    let oso = load_oso().unwrap();

    let write_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Write),
        dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Read),
        dataset_resource.clone(),
    );

    assert_forbidden!(write_result);
    assert_allowed!(read_result);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unrelated_cannot_read_private() {
    let user_actor = UserActor::new("foo");
    let dataset_resource = DatasetResource::new("bar", false);

    let oso = load_oso().unwrap();

    let write_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Write),
        dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Read),
        dataset_resource.clone(),
    );

    assert_forbidden!(write_result);
    assert_forbidden!(read_result);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_having_explicit_read_permission_in_private_dataset() {
    let user_actor = UserActor::new("foo");
    let mut dataset_resource = DatasetResource::new("bar", false);
    dataset_resource.authorize_reader("foo");

    let oso = load_oso().unwrap();

    let write_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Write),
        dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Read),
        dataset_resource.clone(),
    );

    assert_forbidden!(write_result);
    assert_allowed!(read_result);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_having_explicit_write_permission_in_private_dataset() {
    let user_actor = UserActor::new("foo");
    let mut dataset_resource = DatasetResource::new("bar", false);
    dataset_resource.authorize_editor("foo");

    let oso = load_oso().unwrap();

    let write_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Write),
        dataset_resource.clone(),
    );
    let read_result = oso.is_allowed(
        user_actor.clone(),
        format!("{}", DatasetAction::Read),
        dataset_resource.clone(),
    );

    assert_allowed!(write_result);
    assert_forbidden!(read_result);
}

/////////////////////////////////////////////////////////////////////////////////////////
