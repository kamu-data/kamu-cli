// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub trait Hasher {
    fn hash(&self, value: &[u8]) -> String;
    fn verify(&self, value: &[u8], hashed_value: &str) -> bool;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
