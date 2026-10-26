// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use libunftp::ServerBuilder;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use unftp_sbe_fs::Filesystem;

/// In-process FTP server that serves files from a local directory.
///
/// Accepts any user name and password. The control port is ephemeral, and
/// passive data connections use the library's default unprivileged port range.
pub struct FtpServer {
    pub host_port: u16,
    accept_loop: JoinHandle<()>,
}

impl FtpServer {
    pub async fn new(server_dir: &Path) -> Self {
        if !server_dir.exists() {
            std::fs::create_dir(server_dir).unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host_port = listener.local_addr().unwrap().port();

        let server_dir = server_dir.to_path_buf();
        let accept_loop = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let server_dir = server_dir.clone();
                let server = ServerBuilder::new(Box::new(move || {
                    Filesystem::new(server_dir.clone()).unwrap()
                }))
                .build()
                .unwrap();
                tokio::spawn(server.service(stream));
            }
        });

        Self {
            host_port,
            accept_loop,
        }
    }
}

impl Drop for FtpServer {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}
