// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_task_system::TaskDefinition;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_definition_struct! {
    struct TaskDefinitionFoo {
        value: u32,
    }
    => "test.foo"
}

kamu_task_system::task_definition_struct! {
    struct TaskDefinitionBar {
        name: String,
    }
    => "test.bar"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_downcast_to_own_type() {
    let foo = TaskDefinition::new(TaskDefinitionFoo { value: 42 })
        .downcast::<TaskDefinitionFoo>()
        .unwrap();

    assert_eq!(foo.value, 42);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_downcast_to_other_type_returns_definition_unchanged() {
    let definition = TaskDefinition::new(TaskDefinitionFoo { value: 42 })
        .downcast::<TaskDefinitionBar>()
        .unwrap_err();

    assert_eq!(definition.task_type(), TaskDefinitionFoo::TASK_TYPE);
    assert_eq!(
        definition.downcast::<TaskDefinitionFoo>().unwrap().value,
        42
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
