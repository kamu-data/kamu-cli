// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::Utc;
use dill::Catalog;
use event_sourcing::EventID;
use kamu_accounts::{
    AccountQuotaAdded,
    AccountQuotaEvent,
    AccountQuotaEventStore,
    AccountQuotaModified,
    AccountQuotaQuery,
    AccountQuotaRemoved,
    QuotaType,
    QuotaUnit,
};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_quota_added_and_fetched(catalog: &Catalog) {
    let store = catalog.get_one::<dyn AccountQuotaEventStore>().unwrap();

    let account_id = odf::AccountID::new_seeded_ed25519(b"quota-user-1");
    let quota_id = Uuid::new_v4();
    let event_time = Utc::now();

    let added = AccountQuotaEvent::AccountQuotaAdded(AccountQuotaAdded {
        event_time,
        quota_id,
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
        quota_payload: super::quota_payload(10),
    });

    let query = AccountQuotaQuery {
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
    };

    assert_matches!(
        store.save_quota_events(&query, None, vec![added]).await,
        Ok(_)
    );

    let quota = store
        .get_quota_by_account_id(&account_id, QuotaType::Space)
        .await
        .unwrap();

    assert_eq!(quota.id, quota_id);
    assert_eq!(quota.quota_payload.value, 10);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_quota_modified(catalog: &Catalog) {
    let store = catalog.get_one::<dyn AccountQuotaEventStore>().unwrap();

    let account_id = odf::AccountID::new_seeded_ed25519(b"quota-user-2");
    let quota_id = Uuid::new_v4();
    let event_time = Utc::now();

    let query = AccountQuotaQuery {
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
    };

    let added = AccountQuotaEvent::AccountQuotaAdded(AccountQuotaAdded {
        event_time,
        quota_id,
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
        quota_payload: super::quota_payload(5),
    });

    let event_id = store
        .save_quota_events(&query, None, vec![added])
        .await
        .unwrap();

    let modified = AccountQuotaEvent::AccountQuotaModified(AccountQuotaModified {
        event_time: event_time + chrono::Duration::seconds(1),
        quota_id,
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
        quota_payload: super::quota_payload(20),
    });

    store
        .save_quota_events(&query, Some(event_id), vec![modified])
        .await
        .unwrap();

    let quota = store
        .get_quota_by_account_id(&account_id, QuotaType::Space)
        .await
        .unwrap();

    assert_eq!(quota.quota_payload.value, 20);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_quota_removed(catalog: &Catalog) {
    let store = catalog.get_one::<dyn AccountQuotaEventStore>().unwrap();

    let account_id = odf::AccountID::new_seeded_ed25519(b"quota-user-3");
    let quota_id = Uuid::new_v4();
    let event_time = Utc::now();

    let query = AccountQuotaQuery {
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
    };

    let added = AccountQuotaEvent::AccountQuotaAdded(AccountQuotaAdded {
        event_time,
        quota_id,
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
        quota_payload: super::quota_payload(15),
    });

    let event_id = store
        .save_quota_events(&query, None, vec![added])
        .await
        .unwrap();

    let removed = AccountQuotaEvent::AccountQuotaRemoved(AccountQuotaRemoved {
        event_time: event_time + chrono::Duration::seconds(1),
        quota_id,
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
    });

    store
        .save_quota_events(&query, Some(event_id), vec![removed])
        .await
        .unwrap();

    assert_matches!(
        store
            .get_quota_by_account_id(&account_id, QuotaType::Space)
            .await,
        Err(kamu_accounts::GetAccountQuotaError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_concurrent_modification_rejected(catalog: &Catalog) {
    let store = catalog.get_one::<dyn AccountQuotaEventStore>().unwrap();

    let account_id = odf::AccountID::new_seeded_ed25519(b"quota-user-4");
    let quota_id = Uuid::new_v4();
    let event_time = Utc::now();

    let query = AccountQuotaQuery {
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
    };

    let added = AccountQuotaEvent::AccountQuotaAdded(AccountQuotaAdded {
        event_time,
        quota_id,
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
        quota_payload: super::quota_payload(30),
    });

    let event_id = store
        .save_quota_events(&query, None, vec![added])
        .await
        .unwrap();

    let modified = AccountQuotaEvent::AccountQuotaModified(AccountQuotaModified {
        event_time: event_time + chrono::Duration::seconds(1),
        quota_id,
        account_id: account_id.clone(),
        quota_type: QuotaType::Space,
        quota_payload: super::quota_payload(35),
    });

    assert_matches!(
        store
            .save_quota_events(
                &query,
                Some(EventID::new(event_id.into_inner() - 1)),
                vec![modified.clone()],
            )
            .await,
        Err(kamu_accounts::SaveAccountQuotaError::Concurrent(_))
    );

    // Correct CAS should succeed
    assert_matches!(
        store
            .save_quota_events(&query, Some(event_id), vec![modified])
            .await,
        Ok(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn quota_payload(value: u64) -> kamu_accounts::AccountQuotaPayload {
    kamu_accounts::AccountQuotaPayload {
        units: QuotaUnit::Bytes,
        value,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
