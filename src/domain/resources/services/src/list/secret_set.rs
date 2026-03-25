// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::shared::declare_list_resources_by_kind_use_case;
use crate::domain::SecretSetResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

declare_list_resources_by_kind_use_case!(
    use_case = SecretSetListResourcesByKindUseCaseImpl,
    resource = SecretSetResource
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
