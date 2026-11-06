// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use kamu_datasets_services::MetadataChainRefRepositoryDatabaseBackedImpl;
use kamu_datasets_services::testing::FakeDatasetReferenceService;
use odf::dataset::MetadataChainReferenceRepository;
use odf::storage::ReferenceRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_get_ref_reads_the_reference_service_once() {
    let harness = DbBackedRefRepoHarness::new().await;
    harness.move_head_in_service(&harness.hash_a);

    pretty_assertions::assert_eq!(harness.head().await, harness.hash_a);

    // Moved behind the repository's back: the cached head is still returned
    harness.move_head_in_service(&harness.hash_b);
    pretty_assertions::assert_eq!(harness.head().await, harness.hash_a);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_get_ref_reports_a_missing_reference() {
    let harness = DbBackedRefRepoHarness::new().await;

    assert_matches!(
        harness.ref_repo.get_ref(&odf::BlockRef::Head).await,
        Err(odf::storage::GetRefError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_ref_cas_failure_reports_expected_and_actual_heads() {
    let harness = DbBackedRefRepoHarness::new().await;
    harness.move_head_in_service(&harness.hash_a);

    assert_matches!(
        harness
            .set_head_expecting(Some(&harness.hash_b), &harness.hash_c)
            .await,
        Err(odf::dataset::SetChainRefError::CASFailed(odf::dataset::RefCASError {
            reference: odf::BlockRef::Head,
            expected,
            actual,
        })) if expected == Some(harness.hash_b.clone()) && actual == Some(harness.hash_a.clone())
    );
    pretty_assertions::assert_eq!(harness.service_head(), Some(harness.hash_a.clone()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_ref_clears_the_cached_head() {
    let harness = DbBackedRefRepoHarness::new().await;
    harness.move_head_in_service(&harness.hash_a);
    pretty_assertions::assert_eq!(harness.head().await, harness.hash_a);

    harness
        .set_head_expecting(Some(&harness.hash_a), &harness.hash_b)
        .await
        .unwrap();

    pretty_assertions::assert_eq!(harness.head().await, harness.hash_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_ref_without_check_moves_from_the_current_head() {
    let harness = DbBackedRefRepoHarness::new().await;
    harness.move_head_in_service(&harness.hash_a);

    harness.set_head_unchecked(&harness.hash_b).await.unwrap();

    pretty_assertions::assert_eq!(harness.service_head(), Some(harness.hash_b.clone()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A write without a check still expects the head the repository last saw, so
/// it never overwrites a head moved since then
#[tokio::test]
async fn test_set_ref_without_check_fails_on_a_head_moved_since_it_was_read() {
    let harness = DbBackedRefRepoHarness::new().await;
    harness.move_head_in_service(&harness.hash_a);
    pretty_assertions::assert_eq!(harness.head().await, harness.hash_a);

    harness.move_head_in_service(&harness.hash_b);

    assert_matches!(
        harness.set_head_unchecked(&harness.hash_c).await,
        Err(odf::dataset::SetChainRefError::CASFailed(odf::dataset::RefCASError {
            expected,
            actual,
            ..
        })) if expected == Some(harness.hash_a.clone()) && actual == Some(harness.hash_b.clone())
    );
    pretty_assertions::assert_eq!(harness.service_head(), Some(harness.hash_b.clone()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_get_ref_detached_from_transaction_reads_storage() {
    let storage_head = odf::Multihash::from_digest_sha3_256(b"storage");
    let harness = DbBackedRefRepoHarness::with_storage_head(&storage_head).await;
    harness.move_head_in_service(&harness.hash_a);

    harness.ref_repo.detach_from_transaction();

    pretty_assertions::assert_eq!(harness.head().await, storage_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The `HEAD` of one dataset, kept by a fake reference service, with an
/// in-memory storage copy as the fallback
struct DbBackedRefRepoHarness {
    reference_service: Arc<FakeDatasetReferenceService>,
    ref_repo: MetadataChainRefRepositoryDatabaseBackedImpl<
        odf::storage::ReferenceRepositoryImpl<odf::storage::inmem::NamedObjectRepositoryInMemory>,
    >,
    dataset_id: odf::DatasetID,
    hash_a: odf::Multihash,
    hash_b: odf::Multihash,
    hash_c: odf::Multihash,
}

impl DbBackedRefRepoHarness {
    async fn new() -> Self {
        Self::build(None).await
    }

    /// The storage copy of `HEAD` points at `storage_head`
    async fn with_storage_head(storage_head: &odf::Multihash) -> Self {
        Self::build(Some(storage_head)).await
    }

    async fn build(maybe_storage_head: Option<&odf::Multihash>) -> Self {
        let reference_service = Arc::new(FakeDatasetReferenceService::new());
        let storage_ref_repo = odf::storage::ReferenceRepositoryImpl::new(
            odf::storage::inmem::NamedObjectRepositoryInMemory::new(),
        );
        if let Some(storage_head) = maybe_storage_head {
            storage_ref_repo
                .set(odf::BlockRef::Head.as_str(), storage_head)
                .await
                .unwrap();
        }
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

        Self {
            ref_repo: MetadataChainRefRepositoryDatabaseBackedImpl::new(
                reference_service.clone(),
                storage_ref_repo,
                dataset_id.clone(),
            ),
            reference_service,
            dataset_id,
            hash_a: odf::Multihash::from_digest_sha3_256(b"a"),
            hash_b: odf::Multihash::from_digest_sha3_256(b"b"),
            hash_c: odf::Multihash::from_digest_sha3_256(b"c"),
        }
    }

    fn move_head_in_service(&self, head: &odf::Multihash) {
        self.reference_service.set_head(&self.dataset_id, head);
    }

    fn service_head(&self) -> Option<odf::Multihash> {
        self.reference_service.head(&self.dataset_id)
    }

    async fn head(&self) -> odf::Multihash {
        self.ref_repo.get_ref(&odf::BlockRef::Head).await.unwrap()
    }

    /// Moves `HEAD` only if it points at `expected`, or is absent for `None`
    async fn set_head_expecting(
        &self,
        expected: Option<&odf::Multihash>,
        head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        self.ref_repo
            .set_ref(&odf::BlockRef::Head, head, Some(expected))
            .await
    }

    /// Moves `HEAD` without naming the expected head
    async fn set_head_unchecked(
        &self,
        head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        self.ref_repo
            .set_ref(&odf::BlockRef::Head, head, None)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
