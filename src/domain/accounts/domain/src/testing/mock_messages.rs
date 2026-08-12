// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;

use crate::{AccountDisplayName, AccountLifecycleMessage, MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[bon::builder]
pub fn expect_outbox_account_created(
    mock_outbox: &mut messaging_outbox::MockOutbox,
    //
    expected_account_id: Option<odf::AccountID>,
    expected_account_name: Option<odf::AccountName>,
    expected_display_name: Option<AccountDisplayName>,
    expected_email: Option<Email>,
    //
    expected_times: Option<usize>,
) {
    use mockall::predicate::{always, eq, function};

    mock_outbox
        .expect_post_message_as_json()
        .with(
            eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
            function(move |message_as_json: &serde_json::Value| {
                matches!(
                    serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone()),
                    Ok(AccountLifecycleMessage::Created(m))
                        if
                            expected_account_id.clone().is_none_or(|id| m.account_id == id) &&
                            expected_account_name.clone().is_none_or(|n| {
                                m.account_name == n
                            }) &&
                            expected_display_name.clone().is_none_or(|n| m.display_name == n) &&
                            expected_email.clone().is_none_or(|e| {
                                m.email == e
                            })
                )
            }),
            always(),
        )
        .times(expected_times.unwrap_or(1))
        .returning(|_, _, _| Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[bon::builder]
pub fn expect_outbox_account_updated(
    mock_outbox: &mut messaging_outbox::MockOutbox,
    //
    expected_account_id: Option<odf::AccountID>,
    expected_old_email: Option<Email>,
    expected_new_email: Option<Email>,
    expected_old_account_name: Option<odf::AccountName>,
    expected_new_account_name: Option<odf::AccountName>,
    expected_old_display_name: Option<AccountDisplayName>,
    expected_new_display_name: Option<AccountDisplayName>,
    //
    expected_times: Option<usize>,
) {
    use mockall::predicate::{always, eq, function};

    mock_outbox
        .expect_post_message_as_json()
        .with(
            eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
            function(move |message_as_json: &serde_json::Value| {
                matches!(
                    serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone()),
                    Ok(AccountLifecycleMessage::Updated(m))
                        if
                            expected_account_id.clone().is_none_or(|id| m.account_id == id) &&
                            expected_old_email.clone().is_none_or(|oe| {
                                m.old_email == oe
                            }) &&
                            expected_new_email.clone().is_none_or(|ne| {
                                m.new_email == ne
                            }) &&
                            expected_old_account_name.clone().is_none_or(|oan| {
                                m.old_account_name == oan
                            }) &&
                            expected_new_account_name.clone().is_none_or(|nan| {
                                m.new_account_name == nan
                            }) &&
                            expected_old_display_name.clone().is_none_or(|odn| {
                                m.old_display_name == odn
                            }) &&
                            expected_new_display_name.clone().is_none_or(|ndn| {
                                m.new_display_name == ndn
                            })
                )
            }),
            always(),
        )
        .times(expected_times.unwrap_or(1))
        .returning(|_, _, _| Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
