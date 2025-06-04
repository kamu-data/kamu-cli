// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(not(feature = "did-pkh"))]
compile_error!("Feature `did-pkh` must be enabled for using Web3WalletAuthenticationProvider");

mod dependencies;
mod web3_wallet_authentication_provider;

pub use dependencies::*;
pub use web3_wallet_authentication_provider::*;
