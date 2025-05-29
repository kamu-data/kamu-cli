// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetActionAuthorizer: Sync + Send {
    async fn check_action_allowed(
        &self,
        dataset_id: &odf::DatasetID,
        action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError>;

    async fn get_allowed_actions(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetAction>, InternalError>;

    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        action: DatasetAction,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError>;

    async fn classify_dataset_handles_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError>;

    async fn classify_dataset_ids_by_allowance(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceIdsResponse, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, strum::EnumString, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum DatasetAction {
    Read,
    Write,
    Maintain,
    Own,
}

impl DatasetAction {
    pub fn resolve_access(set: &HashSet<Self>, action: Self) -> DatasetActionAccess {
        if set.contains(&action) {
            DatasetActionAccess::Full
        } else if set.contains(&Self::Read) {
            DatasetActionAccess::Limited
        } else {
            DatasetActionAccess::Forbidden
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DatasetActionAccess {
    Forbidden,
    Limited,
    Full,
}

#[test]
fn test_dataset_action_resolve_access() {
    use {DatasetAction as Action, DatasetActionAccess as Access};

    struct FixtureSubTest {
        current_action: Action,
        expected_access: Access,
    }

    struct Fixture<'a> {
        allowed_actions: &'a HashSet<Action>,
        sub_tests: &'a [FixtureSubTest],
    }

    for (
        fixture_index,
        Fixture {
            allowed_actions,
            sub_tests,
        },
    ) in [
        Fixture {
            allowed_actions: &HashSet::from([]),
            sub_tests: &[
                FixtureSubTest {
                    current_action: DatasetAction::Read,
                    expected_access: DatasetActionAccess::Forbidden,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Write,
                    expected_access: DatasetActionAccess::Forbidden,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Maintain,
                    expected_access: DatasetActionAccess::Forbidden,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Own,
                    expected_access: DatasetActionAccess::Forbidden,
                },
            ],
        },
        Fixture {
            allowed_actions: &HashSet::from([Action::Read]),
            sub_tests: &[
                FixtureSubTest {
                    current_action: DatasetAction::Read,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Write,
                    expected_access: DatasetActionAccess::Limited,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Maintain,
                    expected_access: DatasetActionAccess::Limited,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Own,
                    expected_access: DatasetActionAccess::Limited,
                },
            ],
        },
        Fixture {
            allowed_actions: &HashSet::from([Action::Read, Action::Write]),
            sub_tests: &[
                FixtureSubTest {
                    current_action: DatasetAction::Read,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Write,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Maintain,
                    expected_access: DatasetActionAccess::Limited,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Own,
                    expected_access: DatasetActionAccess::Limited,
                },
            ],
        },
        Fixture {
            allowed_actions: &HashSet::from([Action::Read, Action::Write, Action::Maintain]),
            sub_tests: &[
                FixtureSubTest {
                    current_action: DatasetAction::Read,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Write,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Maintain,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Own,
                    expected_access: DatasetActionAccess::Limited,
                },
            ],
        },
        Fixture {
            allowed_actions: &HashSet::from([
                Action::Read,
                Action::Write,
                Action::Maintain,
                Action::Own,
            ]),
            sub_tests: &[
                FixtureSubTest {
                    current_action: DatasetAction::Read,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Write,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Maintain,
                    expected_access: DatasetActionAccess::Full,
                },
                FixtureSubTest {
                    current_action: DatasetAction::Own,
                    expected_access: DatasetActionAccess::Full,
                },
            ],
        },
    ]
    .into_iter()
    .enumerate()
    {
        for (sub_test_index, sub_test) in sub_tests.iter().enumerate() {
            assert_eq!(
                sub_test.expected_access,
                DatasetAction::resolve_access(allowed_actions, sub_test.current_action),
                "Fixture index: {fixture_index}; Sub-test index: {sub_test_index}",
            );
        }
    }
}

#[cfg(feature = "oso")]
impl oso::FromPolar for DatasetAction {
    fn from_polar(polar_value: oso::PolarValue) -> oso::Result<Self> {
        use std::str::FromStr;

        use oso::PolarValue;
        use oso::errors::{OsoError, TypeError};

        let PolarValue::String(raw_dataset_action) = polar_value else {
            return Err(TypeError::expected("String").user());
        };

        Self::from_str(&raw_dataset_action).map_err(|e| OsoError::Custom {
            message: e.to_string(),
        })
    }
}

#[cfg(feature = "oso")]
impl oso::ToPolar for DatasetAction {
    fn to_polar(self) -> oso::PolarValue {
        self.to_string().to_polar()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DatasetActionUnauthorizedError {
    #[error(transparent)]
    Access(odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl DatasetActionUnauthorizedError {
    pub fn not_enough_permissions(dataset_ref: odf::DatasetRef, action: DatasetAction) -> Self {
        Self::Access(odf::AccessError::Unauthorized(
            DatasetActionNotEnoughPermissionsError {
                action,
                dataset_ref,
            }
            .into(),
        ))
    }
}

#[derive(Debug, Error)]
#[error("User has no '{action}' permission in dataset '{dataset_ref}'")]
pub struct DatasetActionNotEnoughPermissionsError {
    pub action: DatasetAction,
    pub dataset_ref: odf::DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ClassifyByAllowanceResponse {
    pub authorized_handles: Vec<odf::DatasetHandle>,
    pub unauthorized_handles_with_errors: Vec<(odf::DatasetHandle, DatasetActionUnauthorizedError)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetActionAuthorizerExt: DatasetActionAuthorizer {
    async fn is_action_allowed(
        &self,
        dataset_id: &odf::DatasetID,
        action: DatasetAction,
    ) -> Result<bool, InternalError>;

    fn filtered_datasets_stream<'a>(
        &'a self,
        dataset_handles_stream: odf::dataset::DatasetHandleStream<'a>,
        action: DatasetAction,
    ) -> odf::dataset::DatasetHandleStream<'a>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl<T> DatasetActionAuthorizerExt for T
where
    T: DatasetActionAuthorizer,
    T: ?Sized,
{
    async fn is_action_allowed(
        &self,
        dataset_id: &odf::DatasetID,
        action: DatasetAction,
    ) -> Result<bool, InternalError> {
        match self.check_action_allowed(dataset_id, action).await {
            Ok(()) => Ok(true),
            Err(DatasetActionUnauthorizedError::Access(_)) => Ok(false),
            Err(DatasetActionUnauthorizedError::Internal(err)) => Err(err),
        }
    }

    fn filtered_datasets_stream<'a>(
        &'a self,
        dataset_handles_stream: odf::dataset::DatasetHandleStream<'a>,
        action: DatasetAction,
    ) -> odf::dataset::DatasetHandleStream<'a> {
        const STREAM_CHUNK_LEN: usize = 100;

        Box::pin(async_stream::stream! {
            use futures::TryStreamExt;

            // Page by page check...
            let mut chunked_dataset_handles = dataset_handles_stream
                .try_chunks(STREAM_CHUNK_LEN);

            while let Some(datataset_handles_chunk) =
                chunked_dataset_handles.try_next().await.int_err()?
            {
                // ... the datasets that are accessed.
                let hdls = self
                    .filter_datasets_allowing(datataset_handles_chunk, action)
                    .await?;

                for hdl in hdls {
                    yield Ok(hdl);
                }
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: use classify_datasets_by_allowance() name
//       after migration
#[derive(Debug)]
pub struct ClassifyByAllowanceIdsResponse {
    pub authorized_ids: Vec<odf::DatasetID>,
    pub unauthorized_ids_with_errors: Vec<(odf::DatasetID, DatasetActionUnauthorizedError)>,
}

#[cfg(any(feature = "testing", test))]
impl From<ClassifyByAllowanceResponse> for ClassifyByAllowanceIdsResponse {
    fn from(v: ClassifyByAllowanceResponse) -> Self {
        Self {
            authorized_ids: v.authorized_handles.into_iter().map(|h| h.id).collect(),
            unauthorized_ids_with_errors: v
                .unauthorized_handles_with_errors
                .into_iter()
                .map(|(h, e)| (h.id, e))
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetActionAuthorizer)]
pub struct AlwaysHappyDatasetActionAuthorizer {}

#[async_trait::async_trait]
impl DatasetActionAuthorizer for AlwaysHappyDatasetActionAuthorizer {
    async fn check_action_allowed(
        &self,
        _dataset_id: &odf::DatasetID,
        _action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        // Ignore rules
        Ok(())
    }

    async fn get_allowed_actions(
        &self,
        _dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetAction>, InternalError> {
        let all_actions = [
            DatasetAction::Read,
            DatasetAction::Write,
            DatasetAction::Maintain,
            DatasetAction::Own,
        ];

        Ok(all_actions.into())
    }

    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        _action: DatasetAction,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        Ok(dataset_handles)
    }

    async fn classify_dataset_handles_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        _action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        Ok(ClassifyByAllowanceResponse {
            authorized_handles: dataset_handles,
            unauthorized_handles_with_errors: vec![],
        })
    }

    async fn classify_dataset_ids_by_allowance(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
        _action: DatasetAction,
    ) -> Result<ClassifyByAllowanceIdsResponse, InternalError> {
        Ok(ClassifyByAllowanceIdsResponse {
            authorized_ids: dataset_ids,
            unauthorized_ids_with_errors: vec![],
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
