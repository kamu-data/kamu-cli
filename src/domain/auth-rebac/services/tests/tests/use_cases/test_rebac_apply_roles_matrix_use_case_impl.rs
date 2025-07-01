// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use dill::CatalogBuilder;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_auth_rebac::{
    AccountToDatasetRelation as Role,
    ApplyRelationMatrixError,
    RebacApplyRolesMatrixUseCase,
    RebacService,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacApplyRolesMatrixUseCaseImpl,
    RebacServiceImpl,
};
use kamu_core::auth::DatasetActionAuthorizer;
use pretty_assertions::{assert_eq, assert_matches};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NO_ROLE: Option<Role> = None;
const READER: Option<Role> = Some(Role::Reader);
const MAINTAINER: Option<Role> = Some(Role::Maintainer);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Harness = RebacApplyRolesMatrixUseCaseImplHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_roles_matrix_success() {
    let mut harness = Harness::new(MockDatasetActionAuthorizer::allowing());

    let account_id_1 = odf::AccountID::new_generated_ed25519().1;
    let account_id_2 = odf::AccountID::new_generated_ed25519().1;
    let account_id_3 = odf::AccountID::new_generated_ed25519().1;

    let dataset_id_1 = odf::DatasetID::new_generated_ed25519().1;
    let dataset_id_2 = odf::DatasetID::new_generated_ed25519().1;
    let dataset_id_3 = odf::DatasetID::new_generated_ed25519().1;

    harness.register_id_pseudonyms(
        &[&account_id_1, &account_id_2, &account_id_3],
        &[&dataset_id_1, &dataset_id_2, &dataset_id_3],
    );

    // Round 1: Setting Maintainer role for Account1
    assert_matches!(
        harness
            .use_case
            .execute(
                &[&account_id_1],
                &[
                    (dataset_id_1.clone(), MAINTAINER),
                    (dataset_id_2.clone(), MAINTAINER),
                    (dataset_id_3.clone(), MAINTAINER),
                ],
            )
            .await,
        Ok(_)
    );
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset1:
            - Account1: maintainer

            Dataset2:
            - Account1: maintainer

            Dataset3:
            - Account1: maintainer

            "#
        ),
        harness
            .rebac_report(&[&dataset_id_1, &dataset_id_2, &dataset_id_3])
            .await
    );

    // Round 2: Setting READER role for Account2, Account3
    assert_matches!(
        harness
            .use_case
            .execute(
                &[&account_id_2, &account_id_3],
                &[
                    (dataset_id_1.clone(), READER),
                    (dataset_id_2.clone(), READER),
                    (dataset_id_3.clone(), READER),
                ],
            )
            .await,
        Ok(_)
    );
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset1:
            - Account1: maintainer
            - Account2: reader
            - Account3: reader

            Dataset2:
            - Account1: maintainer
            - Account2: reader
            - Account3: reader

            Dataset3:
            - Account1: maintainer
            - Account2: reader
            - Account3: reader

            "#
        ),
        harness
            .rebac_report(&[&dataset_id_1, &dataset_id_2, &dataset_id_3])
            .await
    );

    // Round 3: Setting READER role for Account2, Account3 again (idempotence)
    assert_matches!(
        harness
            .use_case
            .execute(
                &[&account_id_2, &account_id_3],
                &[
                    (dataset_id_1.clone(), READER),
                    (dataset_id_2.clone(), READER),
                    (dataset_id_3.clone(), READER),
                ],
            )
            .await,
        Ok(_)
    );
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset1:
            - Account1: maintainer
            - Account2: reader
            - Account3: reader

            Dataset2:
            - Account1: maintainer
            - Account2: reader
            - Account3: reader

            Dataset3:
            - Account1: maintainer
            - Account2: reader
            - Account3: reader

            "#
        ),
        harness
            .rebac_report(&[&dataset_id_1, &dataset_id_2, &dataset_id_3])
            .await
    );

    // Round 4: Removing all granted roles for Dataset1
    assert_matches!(
        harness
            .use_case
            .execute(
                &[&account_id_1, &account_id_2, &account_id_3],
                &[(dataset_id_1.clone(), NO_ROLE)],
            )
            .await,
        Ok(_)
    );
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset1:

            Dataset2:
            - Account1: maintainer
            - Account2: reader
            - Account3: reader

            Dataset3:
            - Account1: maintainer
            - Account2: reader
            - Account3: reader

            "#
        ),
        harness
            .rebac_report(&[&dataset_id_1, &dataset_id_2, &dataset_id_3])
            .await
    );

    // Round 5: Removing all granted roles for Dataset2, Dataset3
    assert_matches!(
        harness
            .use_case
            .execute(
                &[&account_id_1, &account_id_2, &account_id_3],
                &[
                    (dataset_id_2.clone(), NO_ROLE),
                    (dataset_id_3.clone(), NO_ROLE),
                ],
            )
            .await,
        Ok(_)
    );
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset1:

            Dataset2:

            Dataset3:

            "#
        ),
        harness
            .rebac_report(&[&dataset_id_1, &dataset_id_2, &dataset_id_3])
            .await
    );

    // Round 6: Removing all granted roles (idempotence)
    assert_matches!(
        harness
            .use_case
            .execute(
                &[&account_id_1, &account_id_2, &account_id_3],
                &[
                    (dataset_id_1.clone(), NO_ROLE),
                    (dataset_id_2.clone(), NO_ROLE),
                    (dataset_id_3.clone(), NO_ROLE),
                ],
            )
            .await,
        Ok(_)
    );
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset1:

            Dataset2:

            Dataset3:

            "#
        ),
        harness
            .rebac_report(&[&dataset_id_1, &dataset_id_2, &dataset_id_3])
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_roles_matrix_not_authorized() {
    let mut harness = Harness::new(MockDatasetActionAuthorizer::denying());

    let account_id_1 = odf::AccountID::new_generated_ed25519().1;
    let account_id_2 = odf::AccountID::new_generated_ed25519().1;
    let account_id_3 = odf::AccountID::new_generated_ed25519().1;

    let dataset_id_1 = odf::DatasetID::new_generated_ed25519().1;
    let dataset_id_2 = odf::DatasetID::new_generated_ed25519().1;
    let dataset_id_3 = odf::DatasetID::new_generated_ed25519().1;

    harness.register_id_pseudonyms(
        &[&account_id_1, &account_id_2, &account_id_3],
        &[&dataset_id_1, &dataset_id_2, &dataset_id_3],
    );

    assert_matches!(
        harness
            .use_case
            .execute(
                &[&account_id_1, &account_id_2, &account_id_3],
                &[
                    (dataset_id_1.clone(), MAINTAINER),
                    (dataset_id_2.clone(), MAINTAINER),
                    (dataset_id_3.clone(), MAINTAINER),
                ],
            )
            .await,
        Err(ApplyRelationMatrixError::Access(odf::AccessError::Unauthorized(e))) if {
            let error_message = harness.replace_ids_with_pseudonyms(e.to_string());
            error_message == "User has no 'maintain' permission in datasets: 'Dataset1, Dataset2, Dataset3'"
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RebacApplyRolesMatrixUseCaseImplHarness {
    use_case: Arc<dyn RebacApplyRolesMatrixUseCase>,
    service: Arc<dyn RebacService>,
    account_name_pseudonyms: HashMap<odf::AccountID, String>,
    dataset_name_pseudonyms: HashMap<odf::DatasetID, String>,
}

impl RebacApplyRolesMatrixUseCaseImplHarness {
    pub fn new(mock_dataset_action_authorizer: MockDatasetActionAuthorizer) -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add::<RebacApplyRolesMatrixUseCaseImpl>();
            b.add::<RebacServiceImpl>();
            b.add_value(DefaultAccountProperties::default());
            b.add_value(DefaultDatasetProperties::default());
            b.add::<InMemoryRebacRepository>();
            b.add_value(mock_dataset_action_authorizer)
                .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>();

            b.build()
        };

        Self {
            use_case: catalog.get_one().unwrap(),
            service: catalog.get_one().unwrap(),
            account_name_pseudonyms: Default::default(),
            dataset_name_pseudonyms: Default::default(),
        }
    }

    pub fn register_id_pseudonyms(
        &mut self,
        account_ids: &[&odf::AccountID],
        dataset_ids: &[&odf::DatasetID],
    ) {
        for (index, account_id) in account_ids.iter().enumerate() {
            self.account_name_pseudonyms
                .insert((*account_id).clone(), format!("Account{}", index + 1));
        }
        for (index, dataset_id) in dataset_ids.iter().enumerate() {
            self.dataset_name_pseudonyms
                .insert((*dataset_id).clone(), format!("Dataset{}", index + 1));
        }
    }

    pub async fn rebac_report(&self, dataset_ids: &[&odf::DatasetID]) -> String {
        use std::fmt::Write as _;

        let mut report = String::new();

        for dataset_id in dataset_ids {
            let dataset_pseudonym = &self.dataset_name_pseudonyms[dataset_id];

            writeln!(report, "{dataset_pseudonym}:").unwrap();

            let mut authorized_accounts = self
                .service
                .get_authorized_accounts(dataset_id)
                .await
                .unwrap();

            authorized_accounts.sort_by(|a, b| {
                let a_pseudonym = &self.account_name_pseudonyms[&a.account_id];
                let b_pseudonym = &self.account_name_pseudonyms[&b.account_id];
                a_pseudonym.cmp(b_pseudonym)
            });

            for authorized_account in authorized_accounts {
                let account_pseudonym =
                    &self.account_name_pseudonyms[&authorized_account.account_id];

                writeln!(report, "- {account_pseudonym}: {}", authorized_account.role).unwrap();
            }

            report += "\n";
        }

        report
    }

    pub fn replace_ids_with_pseudonyms(&self, error_message: String) -> String {
        let mut res = error_message;
        for (dataset_id, pseudonym) in &self.dataset_name_pseudonyms {
            let dataset_id_stack = dataset_id.as_did_str().to_stack_string();
            res = res.replace(dataset_id_stack.as_str(), pseudonym);
        }
        res
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
