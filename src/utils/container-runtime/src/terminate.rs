// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

/// An extension trait to [`tokio::process::Child`] that provides a graceful
/// termination option
#[async_trait::async_trait]
pub trait Terminate {
    const KILL_TIMEOUT: Duration = Duration::from_millis(500);
    const BLOCKING_TICK_TIMEOUT: Duration = Duration::from_millis(50);

    /// Attempts to gracefully terminate the child process.
    /// On Unix platforms will be sent SIGTERM signal and try waiting for
    /// a process to exit before sending SIGKILL.
    async fn terminate(&mut self, timeout: Duration) -> std::io::Result<TerminateStatus>;

    /// A blocking version of [`Terminate::terminate()`]
    fn terminate_blocking(&mut self, timeout: Duration) -> std::io::Result<TerminateStatus>;
}

#[async_trait::async_trait]
impl Terminate for tokio::process::Child {
    async fn terminate(&mut self, timeout: Duration) -> std::io::Result<TerminateStatus> {
        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                if let Some(id) = self.id() {
                    #[allow(clippy::cast_possible_wrap)]
                    unsafe { libc::kill(id as libc::pid_t, libc::SIGTERM); }

                    match tokio::time::timeout(timeout, self.wait()).await {
                        Ok(res) => return Ok(TerminateStatus::Exited(res?)),
                        Err(_) => (/* timed out */),
                    }
                }
            }
        }

        self.kill().await?;
        Ok(TerminateStatus::Killed)
    }

    fn terminate_blocking(&mut self, timeout: Duration) -> std::io::Result<TerminateStatus> {
        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                if let Some(id) = self.id() {
                    #[allow(clippy::cast_possible_wrap)]
                    unsafe { libc::kill(id as libc::pid_t, libc::SIGTERM); }

                    let start = Instant::now();
                    while start.elapsed() < timeout {
                        match self.try_wait()? {
                            None => std::thread::sleep(Self::BLOCKING_TICK_TIMEOUT),
                            Some(status) => return Ok(TerminateStatus::Exited(status)),
                        }
                    }
                }
            }
        }

        // TODO: Ideally we would reach to inner handle and block on it, but tokio makes
        // it hard, so we do a busy loop.
        self.start_kill()?;

        let start = Instant::now();
        while start.elapsed() < Self::KILL_TIMEOUT {
            match self.try_wait()? {
                None => std::thread::sleep(Self::BLOCKING_TICK_TIMEOUT),
                Some(_) => break,
            }
        }

        Ok(TerminateStatus::Killed)
    }
}

#[derive(Debug)]
pub enum TerminateStatus {
    /// Process has exited gracefully after receiving the signal
    Exited(std::process::ExitStatus),
    /// Process had to be killed by force
    Killed,
}
