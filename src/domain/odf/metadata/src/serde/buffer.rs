// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Consider using `bytes` crate
#[derive(Clone, Debug)]
pub struct Buffer<T> {
    buf: Vec<T>,
    head: usize,
    tail: usize,
}

impl<T> Buffer<T> {
    pub fn new(head: usize, tail: usize, buf: Vec<T>) -> Self {
        Self { buf, head, tail }
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
            let nlen = self.tail - self.head + space_left + space_right;
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

    pub fn collapse_vec(self) -> Vec<T> {
        let mut buf = self.buf;
        buf.drain(0..self.head);
        buf.truncate(self.tail - self.head);
        buf
    }
}

impl<T> std::ops::Deref for Buffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.buf[self.head..self.tail]
    }
}

impl<T> std::convert::AsRef<[T]> for Buffer<T> {
    fn as_ref(&self) -> &[T] {
        &self.buf[self.head..self.tail]
    }
}

impl<T> std::ops::DerefMut for Buffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf[self.head..self.tail]
    }
}
