// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn make_logged_client(
    anonymous: &KamuApiServerClient,
    account_name: &str,
) -> KamuApiServerClient {
    let mut new_client = anonymous.clone();
    let password_same_as_account_name = account_name;

    new_client
        .auth()
        .login_with_password(account_name, password_same_as_account_name)
        .await;

    new_client
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn make_logged_clients<const N: usize>(
    anonymous: &KamuApiServerClient,
    account_names: [&str; N],
) -> [KamuApiServerClient; N] {
    use std::mem::MaybeUninit;

    let mut logged_clients: [MaybeUninit<KamuApiServerClient>; N] =
        [const { MaybeUninit::uninit() }; N];

    for (i, account_name) in account_names.iter().enumerate() {
        let logged_client = make_logged_client(anonymous, account_name).await;
        logged_clients[i].write(logged_client);
    }

    unsafe { logged_clients.map(|item| item.assume_init()) }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
