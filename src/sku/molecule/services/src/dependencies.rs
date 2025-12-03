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
    b.add::<MoleculeDataRoomCollectionServiceImpl>();

    b.add::<MoleculeEnableProjectUseCaseImpl>();
    b.add::<MoleculeCreateProjectUseCaseImpl>();
    b.add::<MoleculeDisableProjectUseCaseImpl>();
    b.add::<MoleculeFindProjectUseCaseImpl>();
    b.add::<MoleculeViewProjectsUseCaseImpl>();

    b.add::<MoleculeFindDataRoomEntryUseCaseImpl>();
    b.add::<MoleculeMoveDataRoomEntryUseCaseImpl>();
    b.add::<MoleculeRemoveDataRoomEntryUseCaseImpl>();
    b.add::<MoleculeUpsertDataRoomEntryUseCaseImpl>();
    b.add::<MoleculeViewDataRoomEntriesUseCaseImpl>();

    b.add::<MoleculeAppendGlobalDataRoomActivityUseCaseImpl>();
    b.add::<MoleculeViewGlobalDataRoomActivitiesUseCaseImpl>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
