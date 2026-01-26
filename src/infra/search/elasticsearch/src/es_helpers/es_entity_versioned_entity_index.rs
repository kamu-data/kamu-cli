// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_search::{SearchEntitySchema, SearchEntitySchemaName, SearchEntitySchemaUpgradeMode};

use super::ElasticsearchIndexMappings;
use crate::ElasticsearchRepositoryConfig;
use crate::es_client::ElasticsearchClient;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticsearchVersionedEntityIndex<'a> {
    client: &'a ElasticsearchClient,
    repo_config: &'a ElasticsearchRepositoryConfig,
    schema_name: SearchEntitySchemaName,
    version: u32,
}

#[common_macros::method_names_consts]
impl<'a> ElasticsearchVersionedEntityIndex<'a> {
    pub fn new(
        client: &'a ElasticsearchClient,
        repo_config: &'a ElasticsearchRepositoryConfig,
        schema_name: SearchEntitySchemaName,
        version: u32,
    ) -> Self {
        Self {
            client,
            repo_config,
            schema_name,
            version,
        }
    }

    #[tracing::instrument(
        level = "info",
        name = ElasticsearchVersionedEntityIndex_ensure_version_existence,
        skip_all,
        fields(schema_name = self.schema_name, version = self.version)
    )]
    pub async fn ensure_version_existence(
        &self,
        mappings: ElasticsearchIndexMappings,
        schema: &SearchEntitySchema,
    ) -> Result<EntityIndexEnsureOutcome, InternalError> {
        let alias_name = self.alias_name();
        let index_name = self.index_name();

        let version_metadata = IndexVersionMetadata {
            schema_version: self.version,
            mapping_hash: Cow::Borrowed(&mappings.mappings_hash),
        };

        // Try to resolve existing index by alias
        let maybe_existing_index = self.resolve_index_alias().await?;
        if let Some(existing_index_name) = maybe_existing_index {
            tracing::debug!(
                index_name = existing_index_name,
                alias_name,
                "Found existing index for alias"
            );

            // Read existing index version metadata
            let existing_version_metadata = serde_json::from_value::<IndexVersionMetadata>(
                self.client
                    .read_index_meta(&existing_index_name)
                    .await
                    .int_err()?,
            )
            .int_err()?;

            // compare expected version from schema metadata and existing index version
            match existing_version_metadata.schema_version.cmp(&self.version) {
                std::cmp::Ordering::Equal => {
                    tracing::debug!(
                        index_name = existing_index_name,
                        alias_name,
                        version = existing_version_metadata.schema_version,
                        "Existing index version matches expected version"
                    );

                    // same version, check hash
                    if existing_version_metadata.mapping_hash.as_ref() == mappings.mappings_hash {
                        tracing::debug!(
                            index_name = existing_index_name,
                            alias_name,
                            "Existing index mapping hash matches expected hash"
                        );
                        Ok(EntityIndexEnsureOutcome::UpToDate {
                            alias: alias_name,
                            index: existing_index_name,
                        })
                    } else {
                        tracing::error!(
                            index_name = existing_index_name,
                            alias_name,
                            expected_hash = mappings.mappings_hash,
                            actual_hash = existing_version_metadata.mapping_hash.as_ref(),
                            "Index schema drift detected: existing index mapping hash does not \
                             match expected value"
                        );
                        Ok(EntityIndexEnsureOutcome::DriftDetected {
                            alias: alias_name,
                            index: existing_index_name,
                            existing_version: existing_version_metadata.schema_version,
                            expected_hash: mappings.mappings_hash,
                            actual_hash: existing_version_metadata.mapping_hash.into_owned(),
                        })
                    }
                }
                std::cmp::Ordering::Greater => {
                    tracing::error!(
                        index_name = existing_index_name,
                        alias_name,
                        existing_version = existing_version_metadata.schema_version,
                        attempted_version = self.version,
                        "Index downgrade attempt: existing index version is greater than expected"
                    );

                    // Downgrade attempt
                    Ok(EntityIndexEnsureOutcome::DowngradeAttempted {
                        alias: alias_name,
                        index: existing_index_name,
                        existing_version: existing_version_metadata.schema_version,
                        attempted_version: self.version,
                    })
                }
                std::cmp::Ordering::Less => {
                    // Upgrade scenario

                    tracing::warn!(
                        index_name = existing_index_name,
                        alias_name,
                        existing_version = existing_version_metadata.schema_version,
                        new_version = self.version,
                        upgrade_mode = ?schema.upgrade_mode,
                        enable_banning = schema.enable_banning,
                        "Upgrading index to new version",
                    );

                    // Create new index version
                    self.create_new_index(&index_name, &mappings, version_metadata)
                        .await?;

                    match schema.upgrade_mode {
                        SearchEntitySchemaUpgradeMode::Reindex => {
                            // Try reindexing data from old index to new index
                            // Keep old data for retention, new index will have updated schema and
                            // will contain all previous data records
                            self.client
                                .reindex_data(&existing_index_name, &index_name)
                                .await
                                .int_err()?;
                        }
                        SearchEntitySchemaUpgradeMode::BreakingRecreate => {
                            // Keep old data for retention, but alias will point
                            // to new empty index,
                            // so reindexing from source is
                            // expected
                        }
                    }

                    // Assign alias to new index version
                    self.client
                        .assign_alias(
                            &alias_name,
                            &index_name,
                            self.build_alias_filter_json(schema),
                        )
                        .await
                        .int_err()?;

                    Ok(EntityIndexEnsureOutcome::UpgradePerformed {
                        alias: alias_name,
                        from_index: existing_index_name,
                        to_index: index_name,
                        existing_version: existing_version_metadata.schema_version,
                        new_version: self.version,
                        upgrade_mode: schema.upgrade_mode,
                    })
                }
            }
        } else {
            tracing::debug!(alias_name, "No existing index found for alias");

            // Creating from scratch
            self.create_new_index(&index_name, &mappings, version_metadata)
                .await
                .int_err()?;

            // Assign alias to new index
            self.client
                .assign_alias(
                    &alias_name,
                    &index_name,
                    self.build_alias_filter_json(schema),
                )
                .await
                .int_err()?;

            Ok(EntityIndexEnsureOutcome::CreatedNew {
                alias: alias_name,
                index: index_name,
            })
        }
    }

    fn build_alias_filter_json(&self, schema: &SearchEntitySchema) -> Option<serde_json::Value> {
        // Apply automatic banning filter in the alias if enabled in schema.
        // This will make banned documents invisible in search results.
        // Note that the documents will still exist in the index,
        // just won't be returned in search queries.
        if schema.enable_banning {
            Some(serde_json::json!({
                "bool": {
                    "must_not": [
                        {
                            "term": {
                                kamu_search::fields::IS_BANNED: true
                            }
                        }
                    ]
                }
            }))
        } else {
            None
        }
    }

    pub fn alias_name(&self) -> String {
        if self.repo_config.index_prefix.is_empty() {
            self.schema_name.to_string()
        } else {
            format!(
                "{prefix}-{schema_name}",
                prefix = self.repo_config.index_prefix,
                schema_name = self.schema_name
            )
        }
    }

    pub fn index_name(&self) -> String {
        if self.repo_config.index_prefix.is_empty() {
            format!(
                "{schema_name}-v{ver}",
                schema_name = self.schema_name,
                ver = self.version
            )
        } else {
            format!(
                "{prefix}-{schema_name}-{ver}",
                prefix = self.repo_config.index_prefix,
                schema_name = self.schema_name,
                ver = self.version
            )
        }
    }

    async fn resolve_index_alias(&self) -> Result<Option<String>, InternalError> {
        let alias_name = self.alias_name();
        let mut indices = self
            .client
            .resolve_alias_indices(&alias_name)
            .await
            .int_err()?;

        if indices.is_empty() {
            Ok(None)
        } else if indices.len() == 1 {
            Ok(Some(indices.remove(0)))
        } else {
            Err(InternalError::new(format!(
                "alias {alias_name} points to multiple indices: {indices:?}",
            )))
        }
    }

    async fn create_new_index(
        &self,
        index_name: &str,
        mappings: &ElasticsearchIndexMappings,
        version_metadata: IndexVersionMetadata<'_>,
    ) -> Result<(), InternalError> {
        let mut mappings_json = mappings.mappings_json.clone();
        if let serde_json::Value::Object(ref mut obj) = mappings_json {
            obj.insert(
                "_meta".to_string(),
                serde_json::to_value(version_metadata).unwrap(),
            );
        }

        let body = serde_json::json!({
            "settings": {
                "analysis": ElasticsearchIndexMappings::build_analysis_settings_json(),
                "index": {
                    "max_ngram_diff": 3
                },
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
            "mappings": mappings_json,
        });

        self.client.create_index(index_name, body).await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexVersionMetadata<'a> {
    pub schema_version: u32,
    pub mapping_hash: Cow<'a, str>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
#[derive(Debug)]
pub enum EntityIndexEnsureOutcome {
    CreatedNew {
        alias: String,
        index: String,
    },
    UpToDate {
        alias: String,
        index: String,
    },
    DriftDetected {
        alias: String,
        index: String,
        existing_version: u32,
        expected_hash: String,
        actual_hash: String,
    },
    DowngradeAttempted {
        alias: String,
        index: String,
        existing_version: u32,
        attempted_version: u32,
    },
    UpgradePerformed {
        alias: String,
        from_index: String,
        existing_version: u32,
        to_index: String,
        new_version: u32,
        upgrade_mode: SearchEntitySchemaUpgradeMode,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
