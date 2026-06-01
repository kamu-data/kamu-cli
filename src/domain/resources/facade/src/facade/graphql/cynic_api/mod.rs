// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cynic::schema("kamu")]
pub(crate) mod schema {}

pub(crate) mod conversions;
pub(crate) mod fragments;
pub(crate) mod inputs;
pub(crate) mod operations;
pub(crate) mod scalars;
pub(crate) mod variables;
