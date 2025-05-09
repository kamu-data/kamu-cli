// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use nutype::nutype;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype(sanitize(trim), validate(len_char_min = 8), derive(Debug))]
pub struct Web3AuthenticationNonce(String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
