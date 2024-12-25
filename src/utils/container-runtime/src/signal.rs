// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use tokio::signal::ctrl_c;

#[cfg(unix)]
pub async fn terminate() -> std::io::Result<()> {
    let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    sig.recv().await;
    Ok(())
}

#[cfg(not(unix))]
pub fn terminate() -> impl std::future::Future<Output = std::io::Result<()>> {
    std::future::pending()
}

pub async fn graceful_stop() -> std::io::Result<()> {
    tokio::select! {
        res = tokio::signal::ctrl_c() => res,
        res = terminate() => res,
    }
}
