// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_search::{
    SearchEntitySchema,
    SearchEntitySchemaFlags,
    SearchEntitySchemaName,
    SearchEntitySchemaUpgradeMode,
    SearchIndexUpdateOperation,
    SearchSchemaField,
    SearchSchemaFieldRole,
};
use kamu_search_elasticsearch::ElasticsearchRepositoryConfig;
use kamu_search_elasticsearch::testing::{
    ElasticsearchBaseHarness,
    ElasticsearchIndexMappings,
    ElasticsearchTestContext,
    ElasticsearchVersionedEntityIndex,
    EntityIndexEnsureOutcome,
    IndexVersionMetadata,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_SCHEMA_NAME: SearchEntitySchemaName = "entity";

const V1_FIELDS: &[SearchSchemaField] = &[
    SearchSchemaField {
        path: "name",
        role: SearchSchemaFieldRole::Name,
    },
    SearchSchemaField {
        path: "kind",
        role: SearchSchemaFieldRole::Keyword,
    },
];

const V1_DRIFT_FIELDS: &[SearchSchemaField] = &[
    SearchSchemaField {
        path: "name",
        role: SearchSchemaFieldRole::Name,
    },
    SearchSchemaField {
        path: "kind",
        role: SearchSchemaFieldRole::Keyword,
    },
    SearchSchemaField {
        path: "extra",
        role: SearchSchemaFieldRole::Integer,
    },
];

const V2_FIELDS: &[SearchSchemaField] = &[
    SearchSchemaField {
        path: "name",
        role: SearchSchemaFieldRole::Name,
    },
    SearchSchemaField {
        path: "kind",
        role: SearchSchemaFieldRole::Keyword,
    },
    SearchSchemaField {
        path: "updated_at",
        role: SearchSchemaFieldRole::DateTime,
    },
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_ensure_version_existence_create_and_up_to_date(ctx: Arc<ElasticsearchTestContext>) {
    let harness = VersionedEntityIndexHarness::new(ctx);

    let schema =
        VersionedEntityIndexHarness::schema(1, SearchEntitySchemaUpgradeMode::Reindex, V1_FIELDS);
    let mappings = harness.mappings(&schema);
    let index = harness.index(schema.version);

    let create_outcome = index
        .ensure_version_existence(
            ElasticsearchIndexMappings {
                mappings_json: mappings.mappings_json.clone(),
                mappings_hash: mappings.mappings_hash.clone(),
            },
            &schema,
        )
        .await
        .unwrap();

    assert!(matches!(
        create_outcome,
        EntityIndexEnsureOutcome::CreatedNew { .. }
    ));

    let expected_alias = index.alias_name();
    let expected_index = index.index_name();

    let alias_indices = harness
        .es_ctx()
        .client()
        .resolve_alias_indices(&expected_alias)
        .await
        .unwrap();
    assert_eq!(alias_indices, vec![expected_index.clone()]);

    let index_meta = harness
        .es_ctx()
        .client()
        .read_index_meta(&expected_index)
        .await
        .unwrap();
    let parsed_meta: IndexVersionMetadata<'static> = serde_json::from_value(index_meta).unwrap();
    assert_eq!(parsed_meta.schema_version, schema.version);
    assert_eq!(parsed_meta.mapping_hash, mappings.mappings_hash);

    let up_to_date_outcome = index
        .ensure_version_existence(mappings, &schema)
        .await
        .unwrap();
    assert!(matches!(
        up_to_date_outcome,
        EntityIndexEnsureOutcome::UpToDate { .. }
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_ensure_version_existence_detects_drift_for_same_version(
    ctx: Arc<ElasticsearchTestContext>,
) {
    let harness = VersionedEntityIndexHarness::new(ctx);

    let schema_v1 =
        VersionedEntityIndexHarness::schema(1, SearchEntitySchemaUpgradeMode::Reindex, V1_FIELDS);
    let mappings_v1 = harness.mappings(&schema_v1);
    let index_v1 = harness.index(schema_v1.version);

    index_v1
        .ensure_version_existence(mappings_v1, &schema_v1)
        .await
        .unwrap();

    let drift_schema = VersionedEntityIndexHarness::schema(
        1,
        SearchEntitySchemaUpgradeMode::Reindex,
        V1_DRIFT_FIELDS,
    );
    let drift_mappings = harness.mappings(&drift_schema);

    let drift_outcome = index_v1
        .ensure_version_existence(drift_mappings, &drift_schema)
        .await
        .unwrap();

    assert!(matches!(
        drift_outcome,
        EntityIndexEnsureOutcome::DriftDetected {
            existing_version: 1,
            ..
        }
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_ensure_version_existence_upgrade_reindex_moves_data(
    ctx: Arc<ElasticsearchTestContext>,
) {
    let harness = VersionedEntityIndexHarness::new(ctx);

    let schema_v1 =
        VersionedEntityIndexHarness::schema(1, SearchEntitySchemaUpgradeMode::Reindex, V1_FIELDS);
    let mappings_v1 = harness.mappings(&schema_v1);
    let index_v1 = harness.index(schema_v1.version);

    index_v1
        .ensure_version_existence(mappings_v1, &schema_v1)
        .await
        .unwrap();

    harness
        .es_ctx()
        .client()
        .bulk_update(
            &index_v1.index_name(),
            vec![SearchIndexUpdateOperation::Index {
                id: "doc-1".to_string(),
                doc: serde_json::json!({
                    "name": "alpha",
                    "kind": "source"
                }),
            }],
        )
        .await
        .unwrap();
    harness.es_ctx().refresh_indices().await;

    let schema_v2 =
        VersionedEntityIndexHarness::schema(2, SearchEntitySchemaUpgradeMode::Reindex, V2_FIELDS);
    let mappings_v2 = harness.mappings(&schema_v2);
    let index_v2 = harness.index(schema_v2.version);

    let upgrade_outcome = index_v2
        .ensure_version_existence(mappings_v2, &schema_v2)
        .await
        .unwrap();

    assert!(matches!(
        upgrade_outcome,
        EntityIndexEnsureOutcome::UpgradePerformed {
            existing_version: 1,
            new_version: 2,
            upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
            ..
        }
    ));

    let alias_indices = harness
        .es_ctx()
        .client()
        .resolve_alias_indices(&index_v2.alias_name())
        .await
        .unwrap();
    assert_eq!(alias_indices, vec![index_v2.index_name()]);

    assert_eq!(
        harness
            .es_ctx()
            .client()
            .documents_in_index(&index_v1.index_name())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        harness
            .es_ctx()
            .client()
            .documents_in_index(&index_v2.index_name())
            .await
            .unwrap(),
        1
    );
    assert!(
        harness
            .es_ctx()
            .client()
            .find_document_by_id(&index_v2.index_name(), "doc-1")
            .await
            .unwrap()
            .is_some()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_ensure_version_existence_upgrade_breaking_recreate_keeps_new_index_empty(
    ctx: Arc<ElasticsearchTestContext>,
) {
    let harness = VersionedEntityIndexHarness::new(ctx);

    let schema_v1 =
        VersionedEntityIndexHarness::schema(1, SearchEntitySchemaUpgradeMode::Reindex, V1_FIELDS);
    let mappings_v1 = harness.mappings(&schema_v1);
    let index_v1 = harness.index(schema_v1.version);

    index_v1
        .ensure_version_existence(mappings_v1, &schema_v1)
        .await
        .unwrap();

    harness
        .es_ctx()
        .client()
        .bulk_update(
            &index_v1.index_name(),
            vec![SearchIndexUpdateOperation::Index {
                id: "doc-1".to_string(),
                doc: serde_json::json!({
                    "name": "alpha",
                    "kind": "source"
                }),
            }],
        )
        .await
        .unwrap();
    harness.es_ctx().refresh_indices().await;

    let schema_v2 = VersionedEntityIndexHarness::schema(
        2,
        SearchEntitySchemaUpgradeMode::BreakingRecreate,
        V2_FIELDS,
    );
    let mappings_v2 = harness.mappings(&schema_v2);
    let index_v2 = harness.index(schema_v2.version);

    let upgrade_outcome = index_v2
        .ensure_version_existence(mappings_v2, &schema_v2)
        .await
        .unwrap();

    assert!(matches!(
        upgrade_outcome,
        EntityIndexEnsureOutcome::UpgradePerformed {
            existing_version: 1,
            new_version: 2,
            upgrade_mode: SearchEntitySchemaUpgradeMode::BreakingRecreate,
            ..
        }
    ));

    let alias_indices = harness
        .es_ctx()
        .client()
        .resolve_alias_indices(&index_v2.alias_name())
        .await
        .unwrap();
    assert_eq!(alias_indices, vec![index_v2.index_name()]);

    assert_eq!(
        harness
            .es_ctx()
            .client()
            .documents_in_index(&index_v1.index_name())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        harness
            .es_ctx()
            .client()
            .documents_in_index(&index_v2.index_name())
            .await
            .unwrap(),
        0
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_ensure_version_existence_detects_downgrade_attempt(
    ctx: Arc<ElasticsearchTestContext>,
) {
    let harness = VersionedEntityIndexHarness::new(ctx);

    let schema_v2 =
        VersionedEntityIndexHarness::schema(2, SearchEntitySchemaUpgradeMode::Reindex, V2_FIELDS);
    let mappings_v2 = harness.mappings(&schema_v2);
    let index_v2 = harness.index(schema_v2.version);

    index_v2
        .ensure_version_existence(mappings_v2, &schema_v2)
        .await
        .unwrap();

    let schema_v1 =
        VersionedEntityIndexHarness::schema(1, SearchEntitySchemaUpgradeMode::Reindex, V1_FIELDS);
    let mappings_v1 = harness.mappings(&schema_v1);
    let index_v1 = harness.index(schema_v1.version);

    let outcome = index_v1
        .ensure_version_existence(mappings_v1, &schema_v1)
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        EntityIndexEnsureOutcome::DowngradeAttempted {
            existing_version: 2,
            attempted_version: 1,
            ..
        }
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct VersionedEntityIndexHarness {
    es_base_harness: ElasticsearchBaseHarness,
    repo_config: ElasticsearchRepositoryConfig,
}

impl VersionedEntityIndexHarness {
    pub fn new(ctx: Arc<ElasticsearchTestContext>) -> Self {
        let es_base_harness =
            ElasticsearchBaseHarness::new(ctx, time_source::SystemTimeSourceProvider::Default);

        let repo_config = ElasticsearchRepositoryConfig {
            index_prefix: es_base_harness.es_ctx().index_prefix().to_string(),
            embedding_dimensions: 1536,
        };

        Self {
            es_base_harness,
            repo_config,
        }
    }

    fn es_ctx(&self) -> &ElasticsearchTestContext {
        self.es_base_harness.es_ctx()
    }

    fn schema(
        version: u32,
        upgrade_mode: SearchEntitySchemaUpgradeMode,
        fields: &'static [SearchSchemaField],
    ) -> SearchEntitySchema {
        SearchEntitySchema {
            schema_name: TEST_SCHEMA_NAME,
            version,
            upgrade_mode,
            fields,
            title_field: "name",
            flags: SearchEntitySchemaFlags {
                enable_banning: false,
                enable_security: false,
                enable_embeddings: false,
            },
        }
    }

    fn mappings(&self, schema: &SearchEntitySchema) -> ElasticsearchIndexMappings {
        ElasticsearchIndexMappings::from_entity_schema(
            schema,
            self.repo_config.embedding_dimensions,
        )
    }

    fn index(&self, version: u32) -> ElasticsearchVersionedEntityIndex<'_> {
        ElasticsearchVersionedEntityIndex::new(
            self.es_ctx().client(),
            &self.repo_config,
            TEST_SCHEMA_NAME,
            version,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
