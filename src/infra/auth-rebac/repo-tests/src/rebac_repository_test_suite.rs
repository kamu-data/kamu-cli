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
use kamu_auth_rebac::DeleteEntityPropertyError::{EntityNotFound, PropertyNotFound};
use kamu_auth_rebac::{
    Entity,
    EntityType,
    GetEntityPropertiesError,
    Property,
    PropertyName,
    RebacRepository,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_get_properties_from_nonexistent_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let nonexistent_entity = Entity::new_dataset("foo");
    let res = rebac_repo.get_entity_properties(&nonexistent_entity).await;

    assert_matches!(
        res,
        Err(GetEntityPropertiesError::NotFound(e))
            if e.entity_type == EntityType::Dataset
                && e.entity_id == "foo".to_string()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_property(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = Property::new(PropertyName::DatasetAllowsAnonymousRead, "true");
    let set_res = rebac_repo
        .set_entity_property(&entity, &anon_read_property)
        .await;

    assert_matches!(set_res, Ok(_));

    let get_res = rebac_repo.get_entity_properties(&entity).await;

    assert_matches!(
        get_res,
        Ok(properites)
            if properites == vec![anon_read_property]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_delete_property_from_nonexistent_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let nonexistent_entity = Entity::new_dataset("foo");
    let delete_res = rebac_repo
        .delete_entity_property(
            &nonexistent_entity,
            PropertyName::DatasetAllowsAnonymousRead,
        )
        .await;

    assert_matches!(
        delete_res,
        Err(EntityNotFound(e))
            if e.entity_type == EntityType::Dataset
                && e.entity_id == "foo".to_string()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_delete_nonexistent_property_from_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = Property::new(PropertyName::DatasetAllowsAnonymousRead, "true");
    let set_res = rebac_repo
        .set_entity_property(&entity, &anon_read_property)
        .await;

    assert_matches!(set_res, Ok(_));

    let delete_res = rebac_repo
        .delete_entity_property(&entity, PropertyName::DatasetAllowsPublicRead)
        .await;

    assert_matches!(
        delete_res,
        Err(PropertyNotFound(e))
            if e.entity_type == EntityType::Dataset
                && e.entity_id == "foo".to_string()
                && e.property_name == PropertyName::DatasetAllowsPublicRead
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_property_from_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = Property::new(PropertyName::DatasetAllowsAnonymousRead, "true");
    {
        let set_res = rebac_repo
            .set_entity_property(&entity, &anon_read_property)
            .await;

        assert_matches!(set_res, Ok(_));
    }

    let public_read_property = Property::new(PropertyName::DatasetAllowsAnonymousRead, "true");
    {
        let set_res = rebac_repo
            .set_entity_property(&entity, &public_read_property)
            .await;

        assert_matches!(set_res, Ok(_));
    }

    {
        let get_res = rebac_repo.get_entity_properties(&entity).await;

        assert_matches!(
            get_res,
            Ok(properites)
                if properites == vec![anon_read_property, public_read_property.clone()]
        );
    }

    {
        let delete_res = rebac_repo
            .delete_entity_property(&entity, PropertyName::DatasetAllowsAnonymousRead)
            .await;

        assert_matches!(delete_res, Ok(_));
    }

    {
        let delete_res = rebac_repo
            .delete_entity_property(&entity, PropertyName::DatasetAllowsAnonymousRead)
            .await;

        assert_matches!(
            delete_res,
            Err(PropertyNotFound(e))
                if e.entity_type == EntityType::Dataset
                    && e.entity_id == "bar".to_string()
                    && e.property_name == PropertyName::DatasetAllowsAnonymousRead
        );
    }

    {
        let get_res = rebac_repo.get_entity_properties(&entity).await;

        assert_matches!(
            get_res,
            Ok(properites)
                if properites == vec![public_read_property]
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
