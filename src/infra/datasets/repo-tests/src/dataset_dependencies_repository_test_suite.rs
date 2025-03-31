// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use dill::Catalog;
use itertools::{assert_equal, sorted};
use kamu_accounts::AccountRepository;
use kamu_datasets::*;

use crate::helpers::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_crud_single_dependency(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;
    let entry_foo = new_dataset_entry_with(&account, "foo", odf::DatasetKind::Root);
    let entry_bar = new_dataset_entry_with(&account, "bar", odf::DatasetKind::Derivative);

    for entry in [&entry_foo, &entry_bar] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    assert_matches!(
        dataset_dependency_repo.stores_any_dependencies().await,
        Ok(false)
    );

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_bar.id, &[&entry_foo.id])
        .await;
    assert_matches!(res, Ok(()));

    assert_matches!(
        dataset_dependency_repo.stores_any_dependencies().await,
        Ok(true)
    );

    use futures::TryStreamExt;
    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        dependencies,
        vec![DatasetDependencies {
            downstream_dataset_id: entry_bar.id,
            upstream_dataset_ids: vec![entry_foo.id]
        }]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_several_unrelated_dependencies(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;
    let entry_alpha = new_dataset_entry_with(&account, "alpha", odf::DatasetKind::Root);
    let entry_beta = new_dataset_entry_with(&account, "beta", odf::DatasetKind::Derivative);

    let entry_phi = new_dataset_entry_with(&account, "phi", odf::DatasetKind::Root);
    let entry_ksi = new_dataset_entry_with(&account, "ksi", odf::DatasetKind::Derivative);

    for entry in [&entry_alpha, &entry_beta, &entry_phi, &entry_ksi] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_beta.id, &[&entry_alpha.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_ksi.id, &[&entry_phi.id])
        .await;
    assert_matches!(res, Ok(()));

    use futures::TryStreamExt;
    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_equal(
        sorted(dependencies),
        sorted(vec![
            DatasetDependencies {
                downstream_dataset_id: entry_beta.id,
                upstream_dataset_ids: vec![entry_alpha.id],
            },
            DatasetDependencies {
                downstream_dataset_id: entry_ksi.id,
                upstream_dataset_ids: vec![entry_phi.id],
            },
        ]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dependency_chain(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;
    let entry_foo = new_dataset_entry_with(&account, "foo", odf::DatasetKind::Root);
    let entry_bar = new_dataset_entry_with(&account, "bar", odf::DatasetKind::Derivative);
    let entry_baz = new_dataset_entry_with(&account, "baz", odf::DatasetKind::Derivative);

    for entry in [&entry_foo, &entry_bar, &entry_baz] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_bar.id, &[&entry_foo.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_baz.id, &[&entry_bar.id])
        .await;
    assert_matches!(res, Ok(()));

    use futures::TryStreamExt;
    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_equal(
        sorted(dependencies),
        sorted(vec![
            DatasetDependencies {
                downstream_dataset_id: entry_bar.id.clone(),
                upstream_dataset_ids: vec![entry_foo.id],
            },
            DatasetDependencies {
                downstream_dataset_id: entry_baz.id,
                upstream_dataset_ids: vec![entry_bar.id],
            },
        ]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dependency_fanins(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;

    let entry_a = new_dataset_entry_with(&account, "a", odf::DatasetKind::Root);
    let entry_b = new_dataset_entry_with(&account, "b", odf::DatasetKind::Root);
    let entry_c = new_dataset_entry_with(&account, "c", odf::DatasetKind::Root);
    let entry_d = new_dataset_entry_with(&account, "d", odf::DatasetKind::Root);
    let entry_e = new_dataset_entry_with(&account, "e", odf::DatasetKind::Root);
    let entry_abc = new_dataset_entry_with(&account, "abc", odf::DatasetKind::Derivative);
    let entry_de = new_dataset_entry_with(&account, "de", odf::DatasetKind::Derivative);
    let entry_abc_de = new_dataset_entry_with(&account, "abc-de", odf::DatasetKind::Derivative);

    for entry in [
        &entry_a,
        &entry_b,
        &entry_c,
        &entry_d,
        &entry_e,
        &entry_abc,
        &entry_de,
        &entry_abc_de,
    ] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_abc.id, &[&entry_a.id, &entry_b.id, &entry_c.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_de.id, &[&entry_d.id, &entry_e.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_abc_de.id, &[&entry_abc.id, &entry_de.id])
        .await;
    assert_matches!(res, Ok(()));

    use futures::TryStreamExt;
    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_equal(
        sorted(dependencies),
        sorted(vec![
            DatasetDependencies {
                downstream_dataset_id: entry_abc.id.clone(),
                upstream_dataset_ids: sorted(vec![entry_a.id, entry_b.id, entry_c.id]).collect(),
            },
            DatasetDependencies {
                downstream_dataset_id: entry_de.id.clone(),
                upstream_dataset_ids: sorted(vec![entry_d.id, entry_e.id]).collect(),
            },
            DatasetDependencies {
                downstream_dataset_id: entry_abc_de.id,
                upstream_dataset_ids: sorted(vec![entry_abc.id, entry_de.id]).collect(),
            },
        ]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dependency_fanouts(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;

    let entry_a = new_dataset_entry_with(&account, "a", odf::DatasetKind::Root);
    let entry_a1 = new_dataset_entry_with(&account, "a1", odf::DatasetKind::Derivative);
    let entry_a2 = new_dataset_entry_with(&account, "a2", odf::DatasetKind::Derivative);
    let entry_a1_1 = new_dataset_entry_with(&account, "a1_1", odf::DatasetKind::Derivative);
    let entry_a1_2 = new_dataset_entry_with(&account, "a1_2", odf::DatasetKind::Derivative);
    let entry_a2_1 = new_dataset_entry_with(&account, "a2_1", odf::DatasetKind::Derivative);
    let entry_a2_2 = new_dataset_entry_with(&account, "a2_2", odf::DatasetKind::Derivative);

    for entry in [
        &entry_a,
        &entry_a1,
        &entry_a2,
        &entry_a1_1,
        &entry_a1_2,
        &entry_a2_1,
        &entry_a2_2,
    ] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_a1.id, &[&entry_a.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_a2.id, &[&entry_a.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_a1_1.id, &[&entry_a1.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_a1_2.id, &[&entry_a1.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_a2_1.id, &[&entry_a2.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_a2_2.id, &[&entry_a2.id])
        .await;
    assert_matches!(res, Ok(()));

    use futures::TryStreamExt;
    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_equal(
        sorted(dependencies),
        sorted(vec![
            DatasetDependencies {
                downstream_dataset_id: entry_a1.id.clone(),
                upstream_dataset_ids: vec![entry_a.id.clone()],
            },
            DatasetDependencies {
                downstream_dataset_id: entry_a2.id.clone(),
                upstream_dataset_ids: vec![entry_a.id],
            },
            DatasetDependencies {
                downstream_dataset_id: entry_a1_1.id,
                upstream_dataset_ids: vec![entry_a1.id.clone()],
            },
            DatasetDependencies {
                downstream_dataset_id: entry_a1_2.id,
                upstream_dataset_ids: vec![entry_a1.id],
            },
            DatasetDependencies {
                downstream_dataset_id: entry_a2_1.id,
                upstream_dataset_ids: vec![entry_a2.id.clone()],
            },
            DatasetDependencies {
                downstream_dataset_id: entry_a2_2.id,
                upstream_dataset_ids: vec![entry_a2.id],
            },
        ]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_add_duplicate_dependency(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;

    let entry_foo = new_dataset_entry_with(&account, "foo", odf::DatasetKind::Root);
    let entry_bar = new_dataset_entry_with(&account, "bar", odf::DatasetKind::Derivative);

    for entry in [&entry_foo, &entry_bar] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_bar.id, &[&entry_foo.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_bar.id, &[&entry_foo.id])
        .await;
    assert_matches!(
        res,
        Err(AddDependenciesError::Duplicate(
            AddDependencyDuplicateError {
                downstream_dataset_id
            }
        )) if downstream_dataset_id == entry_bar.id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_dependency(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;

    let entry_foo = new_dataset_entry_with(&account, "foo", odf::DatasetKind::Root);
    let entry_bar = new_dataset_entry_with(&account, "bar", odf::DatasetKind::Root);
    let entry_baz = new_dataset_entry_with(&account, "baz", odf::DatasetKind::Derivative);

    for entry in [&entry_foo, &entry_bar, &entry_baz] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_baz.id, &[&entry_foo.id, &entry_bar.id])
        .await;
    assert_matches!(res, Ok(()));

    use futures::TryStreamExt;
    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_equal(
        dependencies,
        vec![DatasetDependencies {
            downstream_dataset_id: entry_baz.id.clone(),
            upstream_dataset_ids: sorted(vec![entry_foo.id.clone(), entry_bar.id.clone()])
                .collect(),
        }],
    );

    let res = dataset_dependency_repo
        .remove_upstream_dependencies(&entry_baz.id, &[&entry_foo.id])
        .await;
    assert_matches!(res, Ok(()));

    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_equal(
        dependencies,
        vec![DatasetDependencies {
            downstream_dataset_id: entry_baz.id.clone(),
            upstream_dataset_ids: vec![entry_bar.id.clone()],
        }],
    );

    let res = dataset_dependency_repo
        .remove_upstream_dependencies(&entry_baz.id, &[&entry_bar.id])
        .await;
    assert_matches!(res, Ok(()));

    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert!(dependencies.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_missing_dependency(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;

    let entry_foo = new_dataset_entry_with(&account, "foo", odf::DatasetKind::Root);
    let entry_bar = new_dataset_entry_with(&account, "bar", odf::DatasetKind::Derivative);
    let entry_baz = new_dataset_entry_with(&account, "baz", odf::DatasetKind::Derivative);

    for entry in [&entry_foo, &entry_bar, &entry_baz] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_bar.id, &[&entry_foo.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .remove_upstream_dependencies(&entry_baz.id, &[&entry_foo.id])
        .await;
    assert_matches!(res, Err(RemoveDependenciesError::NotFound(RemoveDependencyMissingError {
        downstream_dataset_id
    })) if downstream_dataset_id == entry_baz.id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_all_dataset_dependencies(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;

    let entry_foo = new_dataset_entry_with(&account, "foo", odf::DatasetKind::Root);
    let entry_bar = new_dataset_entry_with(&account, "bar", odf::DatasetKind::Derivative);
    let entry_baz = new_dataset_entry_with(&account, "baz", odf::DatasetKind::Derivative);

    for entry in [&entry_foo, &entry_bar, &entry_baz] {
        dataset_entry_repo.save_dataset_entry(entry).await.unwrap();
    }

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_bar.id, &[&entry_foo.id])
        .await;
    assert_matches!(res, Ok(()));

    let res = dataset_dependency_repo
        .add_upstream_dependencies(&entry_baz.id, &[&entry_foo.id, &entry_bar.id])
        .await;
    assert_matches!(res, Ok(()));

    use futures::TryStreamExt;
    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_equal(
        sorted(dependencies),
        sorted(vec![
            DatasetDependencies {
                downstream_dataset_id: entry_bar.id.clone(),
                upstream_dataset_ids: vec![entry_foo.id.clone()],
            },
            DatasetDependencies {
                downstream_dataset_id: entry_baz.id.clone(),
                upstream_dataset_ids: sorted(vec![entry_foo.id.clone(), entry_bar.id.clone()])
                    .collect(),
            },
        ]),
    );

    /////

    let res = dataset_entry_repo.delete_dataset_entry(&entry_foo.id).await;
    assert_matches!(res, Ok(()));

    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert_equal(
        dependencies,
        vec![DatasetDependencies {
            downstream_dataset_id: entry_baz.id.clone(),
            upstream_dataset_ids: vec![entry_bar.id.clone()],
        }],
    );

    /////

    let res = dataset_entry_repo.delete_dataset_entry(&entry_baz.id).await;
    assert_matches!(res, Ok(()));

    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert!(dependencies.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_orphan_dependencies(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let dataset_dependency_repo = catalog
        .get_one::<dyn DatasetDependencyRepository>()
        .unwrap();

    let account = new_account(&account_repo).await;

    let entry_foo = new_dataset_entry_with(&account, "foo", odf::DatasetKind::Root);
    dataset_entry_repo
        .save_dataset_entry(&entry_foo)
        .await
        .unwrap();

    let res = dataset_entry_repo.delete_dataset_entry(&entry_foo.id).await;
    assert_matches!(res, Ok(()));

    use futures::TryStreamExt;
    let dependencies: Vec<_> = dataset_dependency_repo
        .list_all_dependencies()
        .try_collect()
        .await
        .unwrap();

    assert!(dependencies.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
