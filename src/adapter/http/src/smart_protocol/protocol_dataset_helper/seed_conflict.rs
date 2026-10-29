// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How an incoming `Seed` block conflicts with an existing dataset
#[derive(Debug)]
pub(crate) enum SeedConflict {
    DatasetId {
        incoming: odf::DatasetID,
        existing: odf::DatasetID,
    },
    Kind,
}

impl SeedConflict {
    /// Finds the conflict between the first incoming block, if it is a `Seed`,
    /// and the existing dataset. A batch that does not start with a `Seed`
    /// extends the dataset's chain and cannot conflict with it.
    pub(crate) fn find(
        first_incoming_block_maybe: Option<&odf::dataset::HashedMetadataBlockBytesDecoded>,
        existing_dataset_handle: &odf::DatasetHandle,
    ) -> Option<Self> {
        let (_, first_incoming_block, _) = first_incoming_block_maybe?;
        let odf::MetadataEvent::Seed(seed_event) = &first_incoming_block.event else {
            return None;
        };

        if seed_event.dataset_id != existing_dataset_handle.id {
            Some(Self::DatasetId {
                incoming: seed_event.dataset_id.clone(),
                existing: existing_dataset_handle.id.clone(),
            })
        } else if seed_event.dataset_kind != existing_dataset_handle.kind {
            Some(Self::Kind)
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::SeedConflict;

    #[test]
    fn test_seed_of_another_dataset_is_a_dataset_id_conflict() {
        let fixtures = SeedFixtures;

        assert_matches!(
            SeedConflict::find(
                Some(&fixtures.block(fixtures.seed(b"other", odf::DatasetKind::Root))),
                &fixtures.existing_root(),
            ),
            Some(SeedConflict::DatasetId { incoming, existing })
                if incoming == fixtures.id(b"other") && existing == fixtures.id(b"existing")
        );
    }

    #[test]
    fn test_seed_of_another_kind_is_a_kind_conflict() {
        let fixtures = SeedFixtures;

        assert_matches!(
            SeedConflict::find(
                Some(&fixtures.block(fixtures.seed(b"existing", odf::DatasetKind::Derivative))),
                &fixtures.existing_root(),
            ),
            Some(SeedConflict::Kind)
        );
    }

    #[test]
    fn test_own_seed_or_a_continuation_does_not_conflict() {
        let fixtures = SeedFixtures;
        let existing = fixtures.existing_root();

        assert_matches!(
            SeedConflict::find(
                Some(&fixtures.block(fixtures.seed(b"existing", odf::DatasetKind::Root))),
                &existing,
            ),
            None
        );
        assert_matches!(
            SeedConflict::find(
                Some(
                    &fixtures.block(odf::MetadataEvent::SetInfo(odf::metadata::SetInfo {
                        description: None,
                        keywords: None,
                    }))
                ),
                &existing,
            ),
            None
        );
        assert_matches!(SeedConflict::find(None, &existing), None);
    }

    struct SeedFixtures;

    impl SeedFixtures {
        fn id(&self, seed: &[u8]) -> odf::DatasetID {
            odf::DatasetID::new_seeded_ed25519(seed)
        }

        fn existing_root(&self) -> odf::DatasetHandle {
            odf::DatasetHandle::new(
                self.id(b"existing"),
                odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo")),
                odf::DatasetKind::Root,
            )
        }

        fn seed(&self, id_seed: &[u8], dataset_kind: odf::DatasetKind) -> odf::MetadataEvent {
            odf::MetadataEvent::Seed(odf::metadata::Seed {
                dataset_id: self.id(id_seed),
                dataset_kind,
            })
        }

        fn block(
            &self,
            event: odf::MetadataEvent,
        ) -> odf::dataset::HashedMetadataBlockBytesDecoded {
            (
                odf::Multihash::from_digest_sha3_256(b"block"),
                odf::MetadataBlock {
                    system_time: chrono::Utc::now(),
                    prev_block_hash: None,
                    sequence_number: 0,
                    event,
                },
                odf::MetadataBlockBytes(bytes::Bytes::new()),
            )
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
