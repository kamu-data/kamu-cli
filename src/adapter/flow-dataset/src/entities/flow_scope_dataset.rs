// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowScopeDataset<'a>(&'a fs::FlowScope);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_SCOPE_TYPE_DATASET: &str = "Dataset";

pub const FLOW_SCOPE_ATTRIBUTE_DATASET_ID: &str = "dataset_id";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> FlowScopeDataset<'a> {
    pub fn new(scope: &'a fs::FlowScope) -> Self {
        assert_eq!(scope.scope_type(), FLOW_SCOPE_TYPE_DATASET);
        FlowScopeDataset(scope)
    }

    pub fn make_scope(dataset_id: &odf::DatasetID) -> fs::FlowScope {
        let payload = serde_json::json!({
            fs::FLOW_SCOPE_ATTRIBUTE_TYPE: FLOW_SCOPE_TYPE_DATASET,
            FLOW_SCOPE_ATTRIBUTE_DATASET_ID: dataset_id.to_string(),
        });
        fs::FlowScope::new(payload)
    }

    pub fn query_for_single_dataset(dataset_id: &odf::DatasetID) -> fs::FlowScopeQuery {
        fs::FlowScopeQuery {
            attributes: vec![(
                FLOW_SCOPE_ATTRIBUTE_DATASET_ID,
                vec![dataset_id.to_string()],
            )],
        }
    }

    pub fn query_for_multiple_datasets(dataset_ids: &[&odf::DatasetID]) -> fs::FlowScopeQuery {
        fs::FlowScopeQuery {
            attributes: vec![(
                FLOW_SCOPE_ATTRIBUTE_DATASET_ID,
                dataset_ids.iter().map(ToString::to_string).collect(),
            )],
        }
    }

    pub fn dataset_id(&self) -> odf::DatasetID {
        Self::maybe_dataset_id_in_scope(self.0).unwrap_or_else(|| {
            panic!("FlowScopeDataset must have a '{FLOW_SCOPE_ATTRIBUTE_DATASET_ID}' attribute")
        })
    }

    pub fn maybe_dataset_id_in_scope(scope: &fs::FlowScope) -> Option<odf::DatasetID> {
        scope
            .get_attribute(FLOW_SCOPE_ATTRIBUTE_DATASET_ID)
            .and_then(|value| value.as_str())
            .and_then(|id_str| odf::DatasetID::from_did_str(id_str).ok())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use kamu_flow_system::FlowScope;

    use super::*;

    #[test]
    fn test_scope_pack_unpack() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"test_dataset");

        let scope = FlowScopeDataset::make_scope(&dataset_id);
        assert_eq!(scope.scope_type(), FLOW_SCOPE_TYPE_DATASET);

        let unpacked = FlowScopeDataset::new(&scope);
        assert_eq!(unpacked.dataset_id(), dataset_id);
    }

    #[test]
    fn test_matches_single_dataset_query() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"test_dataset");
        let scope = FlowScopeDataset::make_scope(&dataset_id);

        let query = FlowScopeDataset::query_for_single_dataset(&dataset_id);
        assert!(scope.matches_query(&query));

        let wrong_query = FlowScopeDataset::query_for_single_dataset(
            &odf::DatasetID::new_seeded_ed25519(b"wrong_dataset"),
        );
        assert!(!scope.matches_query(&wrong_query));
    }

    #[test]
    fn test_matches_multiple_datasets_query() {
        let dataset_id1 = odf::DatasetID::new_seeded_ed25519(b"test_dataset1");
        let dataset_id2 = odf::DatasetID::new_seeded_ed25519(b"test_dataset2");
        let dataset_id3 = odf::DatasetID::new_seeded_ed25519(b"test_dataset3");

        let scope1 = FlowScopeDataset::make_scope(&dataset_id1);
        let scope2 = FlowScopeDataset::make_scope(&dataset_id2);

        let query = FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id1, &dataset_id2]);
        assert!(scope1.matches_query(&query));
        assert!(scope2.matches_query(&query));

        let partially_wrong_query =
            FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id1, &dataset_id3]);
        assert!(scope1.matches_query(&partially_wrong_query));
        assert!(!scope2.matches_query(&partially_wrong_query));
    }

    #[test]
    fn test_maybe_dataset_id_in_scope() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"test_dataset");

        let scope1 = FlowScopeDataset::make_scope(&dataset_id);
        assert_eq!(
            FlowScopeDataset::maybe_dataset_id_in_scope(&scope1),
            Some(dataset_id)
        );

        let scope2 = FlowScope::make_system_scope();
        assert_eq!(FlowScopeDataset::maybe_dataset_id_in_scope(&scope2), None);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
