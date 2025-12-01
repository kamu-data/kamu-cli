// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(b: &mut CatalogBuilder) {
    b.add::<MoleculeDatasetServiceImpl>();
    // Note: don't register MoleculeDataRoomDirectCollectionAdapter,
    // it is registered explicitly by application depending on the needs
    // (local direct access vs remote access within GQL federation)

    b.add::<MoleculeCreateProjectUseCaseImpl>();
    b.add::<MoleculeFindProjectUseCaseImpl>();
    b.add::<MoleculeViewProjectsUseCaseImpl>();

    b.add::<MoleculeFindProjectDataRoomEntryUseCaseImpl>();
    b.add::<MoleculeViewProjectDataRoomEntriesUseCaseImpl>();

    b.add::<MoleculeAppendGlobalDataRoomActivityUseCaseImpl>();
    b.add::<MoleculeViewGlobalDataRoomActivitiesUseCaseImpl>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
