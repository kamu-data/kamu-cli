// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait IntoOwned<T> {
    fn into_owned(self) -> T;
}

impl<T: Clone> IntoOwned<T> for &T {
    fn into_owned(self) -> T {
        self.clone()
    }
}

impl<T> IntoOwned<T> for T {
    fn into_owned(self) -> T {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
