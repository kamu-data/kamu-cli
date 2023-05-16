// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::File;
use std::io::prelude::*;
use std::io::Error as IOError;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

use opendatafabric::*;
use thiserror::Error;

use crate::domain::*;

const BUFFER_SIZE: usize = 8096;

pub struct PrepService {}

impl PrepService {
    pub fn new() -> Self {
        Self {}
    }

    pub fn prepare(
        &self,
        prep_steps: &Vec<PrepStep>,
        src_path: &Path,
        target_path: &Path,
    ) -> Result<(), IngestError> {
        let mut stream: Box<dyn Stream> = Box::new(File::open(src_path).int_err()?);

        for step in prep_steps.iter() {
            stream = match step {
                PrepStep::Pipe(ref p) => Box::new(
                    PipeStream::new(&p.command, stream)
                        .map_err(|e| IngestError::pipe(p.command.clone(), e))?,
                ),
                PrepStep::Decompress(ref dc) => match dc.format {
                    CompressionFormat::Zip => Box::new(DecompressZipStream::new(stream).int_err()?),
                    CompressionFormat::Gzip => Box::new(DecompressGzipStream::new(stream)),
                },
            };
        }

        let sink = Box::new(FileSink::new(File::create(target_path).int_err()?, stream));

        sink.join()?;

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////
// Ghetto Streams
///////////////////////////////////////////////////////////////////////////////

// TODO: Error handling (?)

trait Stream: std::io::Read + Send {
    fn as_read(&mut self) -> &mut dyn std::io::Read;

    fn as_seekable_read(&mut self) -> Option<&mut dyn ReadAndSeek>;

    fn join(self: Box<Self>) -> Result<(), IngestError>;
}

impl Stream for std::fs::File {
    fn as_read(&mut self) -> &mut dyn std::io::Read {
        self
    }

    fn as_seekable_read(&mut self) -> Option<&mut dyn ReadAndSeek> {
        Some(self)
    }

    fn join(self: Box<Self>) -> Result<(), IngestError> {
        Ok(())
    }
}

impl ReadAndSeek for std::fs::File {}

trait ReadAndSeek: std::io::Read + std::io::Seek {}

///////////////////////////////////////////////////////////////////////////////
// Pipe Streams
///////////////////////////////////////////////////////////////////////////////

struct PipeStream {
    ingress: std::thread::JoinHandle<Result<(), IngestError>>,
    stdout: std::process::ChildStdout,
}

impl PipeStream {
    fn new(cmd: &Vec<String>, mut input: Box<dyn Stream>) -> Result<Self, IOError> {
        let mut process = Command::new(cmd.get(0).unwrap())
            .args(&cmd[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        let stdout = process.stdout.take().unwrap();
        let mut stdin = process.stdin.take().unwrap();
        let cmd = cmd.clone();
        let ingress = std::thread::Builder::new()
            .name("pipe_stream".to_owned())
            .spawn(move || {
                let mut buf = [0; BUFFER_SIZE];

                loop {
                    let read = input.read(&mut buf).unwrap();
                    if read == 0 {
                        break;
                    }
                    stdin.write_all(&buf[..read]).unwrap();
                }

                drop(stdin);
                input.join()?;

                let status = process.wait().unwrap();

                if !status.success() {
                    Err(IngestError::pipe(
                        cmd,
                        BadStatusCode {
                            code: status.code().unwrap(),
                        },
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap();

        Ok(Self { ingress, stdout })
    }
}

impl Read for PipeStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        self.stdout.read(buf)
    }
}

impl Stream for PipeStream {
    fn as_read(&mut self) -> &mut dyn std::io::Read {
        self
    }

    fn as_seekable_read(&mut self) -> Option<&mut dyn ReadAndSeek> {
        None
    }

    fn join(self: Box<Self>) -> Result<(), IngestError> {
        self.ingress.join().unwrap()
    }
}

#[derive(Debug, Error)]
#[error("Command exited with code {code}")]
struct BadStatusCode {
    code: i32,
}

///////////////////////////////////////////////////////////////////////////////
// DecompressZipStream
///////////////////////////////////////////////////////////////////////////////

struct ReaderHelper<'a>(&'a mut dyn Stream);

impl<'a> Read for ReaderHelper<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        self.0.read(buf)
    }
}

struct DecompressZipStream {
    ingress: std::thread::JoinHandle<Result<(), IngestError>>,
    consumer: ringbuf::Consumer<u8, Arc<ringbuf::HeapRb<u8>>>,
    done_recvr: std::sync::mpsc::Receiver<usize>,
}

impl DecompressZipStream {
    fn new(mut input: Box<dyn Stream>) -> Result<Self, IOError> {
        let (mut producer, consumer) = ringbuf::HeapRb::<u8>::new(BUFFER_SIZE).split();

        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        // TODO: Threading is a complete overkill here
        // Only reason for this is the ownership/lifetime issues when creating archive
        // from references See: https://github.com/mvdnes/zip-rs/issues/111
        let ingress = std::thread::Builder::new()
            .name("decompress_zip_stream".to_owned())
            .spawn(move || {
                if let Some(seekable) = input.as_seekable_read() {
                    let mut archive = zip::read::ZipArchive::new(seekable).int_err()?;
                    let mut file = archive.by_index(0).int_err()?;

                    loop {
                        let read = producer.read_from(&mut file, None).unwrap();
                        tx.send(read).unwrap();
                        if read == 0 {
                            break;
                        }
                    }
                } else {
                    let mut read_helper = ReaderHelper(input.as_mut());
                    let mut file = zip::read::read_zipfile_from_stream(&mut read_helper)
                        .unwrap()
                        .unwrap();

                    loop {
                        let read = producer.read_from(&mut file, None).unwrap();
                        tx.send(read).unwrap();
                        if read == 0 {
                            break;
                        }
                    }
                }

                input.join()
            })
            .unwrap();

        Ok(Self {
            ingress,
            consumer,
            done_recvr: rx,
        })
    }
}

impl Read for DecompressZipStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        while self.consumer.is_empty() {
            match self.done_recvr.recv() {
                Ok(0) => break,
                Ok(_) => (),
                Err(_) => break,
            }
        }

        if !self.consumer.is_empty() {
            self.consumer.read(buf)
        } else {
            Ok(0)
        }
    }
}

impl Stream for DecompressZipStream {
    fn as_read(&mut self) -> &mut dyn std::io::Read {
        self
    }

    fn as_seekable_read(&mut self) -> Option<&mut dyn ReadAndSeek> {
        None
    }

    fn join(self: Box<Self>) -> Result<(), IngestError> {
        self.ingress.join().unwrap()
    }
}

///////////////////////////////////////////////////////////////////////////////
// DecompressGzipStream
///////////////////////////////////////////////////////////////////////////////

struct DecompressGzipStream {
    decoder: flate2::read::GzDecoder<Box<dyn Stream>>,
}

impl DecompressGzipStream {
    fn new(input: Box<dyn Stream>) -> Self {
        let decoder = flate2::read::GzDecoder::new(input);
        Self { decoder }
    }
}

impl Read for DecompressGzipStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        self.decoder.read(buf)
    }
}

impl Stream for DecompressGzipStream {
    fn as_read(&mut self) -> &mut dyn std::io::Read {
        self
    }

    fn as_seekable_read(&mut self) -> Option<&mut dyn ReadAndSeek> {
        None
    }

    fn join(self: Box<Self>) -> Result<(), IngestError> {
        self.decoder.into_inner().join()
    }
}

///////////////////////////////////////////////////////////////////////////////
// Sink
///////////////////////////////////////////////////////////////////////////////

struct FileSink {
    ingress: std::thread::JoinHandle<Result<(), IngestError>>,
}

impl FileSink {
    fn new(mut file: std::fs::File, mut input: Box<dyn Stream>) -> Self {
        let ingress = std::thread::Builder::new()
            .name("file_sink".to_owned())
            .spawn(move || {
                let mut buf = [0; BUFFER_SIZE];

                loop {
                    let read = input.read(&mut buf).unwrap();
                    if read == 0 {
                        break;
                    }
                    file.write_all(&buf[..read]).unwrap();
                }

                input.join()
            })
            .unwrap();

        Self { ingress }
    }

    fn join(self: Box<Self>) -> Result<(), IngestError> {
        self.ingress.join().unwrap()
    }
}
