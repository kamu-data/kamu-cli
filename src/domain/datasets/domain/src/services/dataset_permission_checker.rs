// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashSet;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetPermissionChecker: Sync + Send {
    async fn check_permission(
        &self,
        dataset_id: &odf::DatasetID,
        permission: DatasetPermission,
    ) -> Result<(), DatasetPermissionError>;

    async fn get_granted_permissions(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetPermission>, GetGrantedPermissionsError>;

    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        permission: DatasetPermission,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError>;

    async fn classify_dataset_handles_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceResponse, InternalError>;

    /// Same as [`Self::classify_dataset_handles_by_allowance`], for the given
    /// principal instead of the current account
    async fn classify_dataset_handles_by_allowance_for(
        &self,
        principal: &CurrentAccountSubject,
        dataset_handles: Vec<odf::DatasetHandle>,
        permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceResponse, InternalError>;

    async fn classify_dataset_ids_by_allowance<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
        permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceIdsResponse, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, strum::EnumString, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum DatasetPermission {
    Read,
    Write,
    Maintain,
    Own,
}

impl DatasetPermission {
    pub fn resolve_access(set: &HashSet<Self>, permission: Self) -> DatasetPermissionAccess {
        if set.contains(&permission) {
            DatasetPermissionAccess::Allowed
        } else if set.contains(&Self::Read) {
            DatasetPermissionAccess::Insufficient
        } else {
            DatasetPermissionAccess::Forbidden
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DatasetPermissionAccess {
    /// Subject has no access to the object.
    Forbidden,
    /// Subject has access to an object's presence but lacks the required
    /// permission.
    Insufficient,
    /// Subject has the permission.
    Allowed,
}

#[test]
fn test_dataset_permission_resolve_access() {
    use DatasetPermission as Permission;
    use DatasetPermissionAccess as Access;

    struct FixtureSubTest {
        current_permission: Permission,
        expected_access: Access,
    }

    struct Fixture<'a> {
        granted_permissions: &'a HashSet<Permission>,
        sub_tests: &'a [FixtureSubTest],
    }

    for (
        fixture_index,
        Fixture {
            granted_permissions,
            sub_tests,
        },
    ) in [
        Fixture {
            granted_permissions: &HashSet::from([]),
            sub_tests: &[
                FixtureSubTest {
                    current_permission: DatasetPermission::Read,
                    expected_access: DatasetPermissionAccess::Forbidden,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Write,
                    expected_access: DatasetPermissionAccess::Forbidden,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Maintain,
                    expected_access: DatasetPermissionAccess::Forbidden,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Own,
                    expected_access: DatasetPermissionAccess::Forbidden,
                },
            ],
        },
        Fixture {
            granted_permissions: &HashSet::from([Permission::Read]),
            sub_tests: &[
                FixtureSubTest {
                    current_permission: DatasetPermission::Read,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Write,
                    expected_access: DatasetPermissionAccess::Insufficient,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Maintain,
                    expected_access: DatasetPermissionAccess::Insufficient,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Own,
                    expected_access: DatasetPermissionAccess::Insufficient,
                },
            ],
        },
        Fixture {
            granted_permissions: &HashSet::from([Permission::Read, Permission::Write]),
            sub_tests: &[
                FixtureSubTest {
                    current_permission: DatasetPermission::Read,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Write,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Maintain,
                    expected_access: DatasetPermissionAccess::Insufficient,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Own,
                    expected_access: DatasetPermissionAccess::Insufficient,
                },
            ],
        },
        Fixture {
            granted_permissions: &HashSet::from([
                Permission::Read,
                Permission::Write,
                Permission::Maintain,
            ]),
            sub_tests: &[
                FixtureSubTest {
                    current_permission: DatasetPermission::Read,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Write,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Maintain,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Own,
                    expected_access: DatasetPermissionAccess::Insufficient,
                },
            ],
        },
        Fixture {
            granted_permissions: &HashSet::from([
                Permission::Read,
                Permission::Write,
                Permission::Maintain,
                Permission::Own,
            ]),
            sub_tests: &[
                FixtureSubTest {
                    current_permission: DatasetPermission::Read,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Write,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Maintain,
                    expected_access: DatasetPermissionAccess::Allowed,
                },
                FixtureSubTest {
                    current_permission: DatasetPermission::Own,
                    expected_access: DatasetPermissionAccess::Allowed,
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
                DatasetPermission::resolve_access(granted_permissions, sub_test.current_permission),
                "Fixture index: {fixture_index}; Sub-test index: {sub_test_index}",
            );
        }
    }
}

#[cfg(feature = "oso")]
impl oso::FromPolar for DatasetPermission {
    fn from_polar(polar_value: oso::PolarValue) -> oso::Result<Self> {
        use std::str::FromStr;

        use oso::PolarValue;
        use oso::errors::{OsoError, TypeError};

        let PolarValue::String(raw_dataset_permission) = polar_value else {
            return Err(TypeError::expected("String").user());
        };

        Self::from_str(&raw_dataset_permission).map_err(|e| OsoError::Custom {
            message: e.to_string(),
        })
    }
}

#[cfg(feature = "oso")]
impl oso::ToPolar for DatasetPermission {
    fn to_polar(self) -> oso::PolarValue {
        self.to_string().to_polar()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum DatasetPermissionError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl DatasetPermissionError {
    pub fn not_enough_permissions(
        dataset_ref: odf::DatasetRef,
        permission: DatasetPermission,
    ) -> Self {
        Self::Access(odf::AccessError::Unauthorized(
            DatasetPermissionMissingError {
                permission,
                dataset_ref,
            }
            .into(),
        ))
    }
}

#[derive(thiserror::Error, Debug)]
#[error("User has no '{permission}' permission in dataset '{dataset_ref}'")]
pub struct DatasetPermissionMissingError {
    pub permission: DatasetPermission,
    pub dataset_ref: odf::DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum GetGrantedPermissionsError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ClassifyByAllowanceDatasetPermissionError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl ClassifyByAllowanceDatasetPermissionError {
    pub fn not_enough_permissions(
        dataset_ref: odf::DatasetRef,
        permission: DatasetPermission,
    ) -> Self {
        Self::Access(odf::AccessError::Unauthorized(
            DatasetPermissionMissingError {
                permission,
                dataset_ref,
            }
            .into(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ClassifyByAllowanceResponse {
    pub authorized_handles: Vec<odf::DatasetHandle>,
    pub unauthorized_handles_with_errors: Vec<(
        odf::DatasetHandle,
        ClassifyByAllowanceDatasetPermissionError,
    )>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetPermissionCheckerExt: DatasetPermissionChecker {
    async fn is_permission_granted(
        &self,
        dataset_id: &odf::DatasetID,
        permission: DatasetPermission,
    ) -> Result<bool, InternalError>;

    async fn get_granted_permissions_for_existing_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetPermission>, InternalError>;

    fn filtered_datasets_stream<'a>(
        &'a self,
        dataset_handles_stream: odf::dataset::DatasetHandleStream<'a>,
        permission: DatasetPermission,
    ) -> odf::dataset::DatasetHandleStream<'a>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl<T> DatasetPermissionCheckerExt for T
where
    T: DatasetPermissionChecker,
    T: ?Sized,
{
    async fn is_permission_granted(
        &self,
        dataset_id: &odf::DatasetID,
        permission: DatasetPermission,
    ) -> Result<bool, InternalError> {
        use DatasetPermissionError as E;

        match self.check_permission(dataset_id, permission).await {
            Ok(()) => Ok(true),
            Err(E::NotFound(_) | E::Access(_)) => Ok(false),
            Err(e @ E::Internal(_)) => Err(e.int_err()),
        }
    }

    async fn get_granted_permissions_for_existing_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetPermission>, InternalError> {
        self.get_granted_permissions(dataset_id).await.map_err(|e| {
            use GetGrantedPermissionsError as E;

            // NOTE: We checked for the presence of datasets earlier,
            //       so if any are missing now, that would be unexpected.
            match e {
                e @ (E::NotFound(_) | E::Internal(_)) => e.int_err(),
            }
        })
    }

    fn filtered_datasets_stream<'a>(
        &'a self,
        dataset_handles_stream: odf::dataset::DatasetHandleStream<'a>,
        permission: DatasetPermission,
    ) -> odf::dataset::DatasetHandleStream<'a> {
        const STREAM_CHUNK_LEN: usize = 100;

        Box::pin(async_stream::stream! {
            use futures::TryStreamExt;

            // Page by page check...
            let mut chunked_dataset_handles = dataset_handles_stream
                .try_chunks(STREAM_CHUNK_LEN);

            while let Some(dataset_handles_chunk) =
                chunked_dataset_handles.try_next().await.int_err()?
            {
                // ... the datasets that are accessed.
                let hdls = self
                    .filter_datasets_allowing(dataset_handles_chunk, permission)
                    .await?;

                for hdl in hdls {
                    yield Ok(hdl);
                }
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ClassifyByAllowanceIdsResponse {
    pub authorized_ids: Vec<odf::DatasetID>,
    pub unauthorized_ids_with_errors:
        Vec<(odf::DatasetID, ClassifyByAllowanceDatasetPermissionError)>,
}

#[cfg(feature = "testing")]
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

#[dill::component]
#[dill::interface(dyn DatasetPermissionChecker)]
pub struct AlwaysHappyDatasetPermissionChecker {}

#[async_trait::async_trait]
impl DatasetPermissionChecker for AlwaysHappyDatasetPermissionChecker {
    async fn check_permission(
        &self,
        _dataset_id: &odf::DatasetID,
        _permission: DatasetPermission,
    ) -> Result<(), DatasetPermissionError> {
        // Ignore rules
        Ok(())
    }

    async fn get_granted_permissions(
        &self,
        _dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetPermission>, GetGrantedPermissionsError> {
        let all_permissions = [
            DatasetPermission::Read,
            DatasetPermission::Write,
            DatasetPermission::Maintain,
            DatasetPermission::Own,
        ];

        Ok(all_permissions.into())
    }

    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        _permission: DatasetPermission,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        Ok(dataset_handles)
    }

    async fn classify_dataset_handles_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        _permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        Ok(ClassifyByAllowanceResponse {
            authorized_handles: dataset_handles,
            unauthorized_handles_with_errors: vec![],
        })
    }

    async fn classify_dataset_handles_by_allowance_for(
        &self,
        _principal: &CurrentAccountSubject,
        dataset_handles: Vec<odf::DatasetHandle>,
        permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        self.classify_dataset_handles_by_allowance(dataset_handles, permission)
            .await
    }

    async fn classify_dataset_ids_by_allowance<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
        _permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceIdsResponse, InternalError> {
        Ok(ClassifyByAllowanceIdsResponse {
            authorized_ids: dataset_ids.iter().map(|d| d.as_ref().clone()).collect(),
            unauthorized_ids_with_errors: vec![],
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
