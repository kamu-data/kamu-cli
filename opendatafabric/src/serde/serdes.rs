use crate::{DatasetSnapshot, MetadataBlock, Sha3_256};
use std::backtrace::Backtrace;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////

pub trait MetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<(Sha3_256, Buffer<u8>), Error>;

    fn write_manifest_unchecked(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error>;
}

///////////////////////////////////////////////////////////////////////////////

pub trait MetadataBlockDeserializer {
    fn validate_manifest(&self, data: &[u8]) -> Result<(), Error>;

    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error>;

    fn read_manifest_unchecked(&self, data: &[u8]) -> Result<MetadataBlock, Error>;
}

///////////////////////////////////////////////////////////////////////////////

pub trait DatasetSnapshotSerializer {
    fn write_manifest(&self, snapshot: &DatasetSnapshot) -> Result<Buffer<u8>, Error>;
}

///////////////////////////////////////////////////////////////////////////////

pub trait DatasetSnapshotDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<DatasetSnapshot, Error>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct Buffer<T> {
    buf: Vec<T>,
    head: usize,
    tail: usize,
}

impl<T> Buffer<T> {
    pub fn new(head: usize, tail: usize, buf: Vec<T>) -> Self {
        Self {
            buf: buf,
            head: head,
            tail: tail,
        }
    }

    pub fn inner(&self) -> &[T] {
        &self.buf
    }

    pub fn inner_mut(&mut self) -> &mut [T] {
        &mut self.buf
    }

    pub fn head(&self) -> usize {
        self.head
    }

    pub fn set_head(&mut self, head: usize) {
        self.head = head;
    }

    pub fn tail(&self) -> usize {
        self.tail
    }

    pub fn set_tail(&mut self, tail: usize) {
        self.tail = tail;
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub fn ensure_capacity(&mut self, space_left: usize, space_right: usize)
    where
        T: Default + Copy,
    {
        if self.head < space_left || self.buf.len() - self.tail < space_right {
            let nlen = self.buf.len() + space_left + space_right;
            let ntail = nlen - space_right;
            let mut nbuf = vec![T::default(); nlen];
            nbuf[space_left..nlen - space_right].copy_from_slice(&self.buf[self.head..self.tail]);

            self.head = space_left;
            self.tail = ntail;
            self.buf = nbuf;
        }
    }

    pub fn collapse(self) -> (Vec<T>, usize, usize) {
        (self.buf, self.head, self.tail)
    }
}

impl<T> std::ops::Deref for Buffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.buf[self.head..self.tail]
    }
}

impl<T> std::ops::DerefMut for Buffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf[self.head..self.tail]
    }
}

///////////////////////////////////////////////////////////////////////////////

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid hash {actual} expected {expected}")]
    InvalidHash {
        actual: Sha3_256,
        expected: Sha3_256,
    },
    #[error("IO error: {source}")]
    IoError {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[error("Serde error: {source}")]
    SerdeError {
        source: BoxedError,
        backtrace: Backtrace,
    },
}

impl Error {
    pub fn invalid_hash(actual: Sha3_256, expected: Sha3_256) -> Self {
        Self::InvalidHash {
            actual: actual,
            expected: expected,
        }
    }

    pub fn io_error(e: std::io::Error) -> Self {
        Self::IoError {
            source: e,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn serde(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::SerdeError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
