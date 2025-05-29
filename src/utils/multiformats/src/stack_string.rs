// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A stack-allocated string to avoid allocations
pub struct StackString<const S: usize> {
    buf: [u8; S],
    len: usize,
}

impl<const S: usize> StackString<S> {
    pub fn new(buf: [u8; S], len: usize) -> Self {
        assert!(len <= S);
        Self { buf, len }
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len]).unwrap()
    }
}

impl<const S: usize> std::ops::Deref for StackString<S> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<const S: usize> AsRef<str> for StackString<S> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<const S: usize> AsRef<std::path::Path> for StackString<S> {
    fn as_ref(&self) -> &std::path::Path {
        std::path::Path::new(self.as_str())
    }
}

impl<const S: usize> std::fmt::Display for StackString<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", std::str::from_utf8(&self.buf[..self.len]).unwrap())
    }
}

impl<const S: usize> std::fmt::Debug for StackString<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<const S: usize> std::cmp::PartialEq<&str> for StackString<S> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<const S: usize> std::cmp::PartialEq<String> for StackString<S> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == *other
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait AsStackString<const S: usize>
where
    Self: std::fmt::Display + Sized,
{
    fn as_stack_string(&self) -> StackString<S> {
        use std::io::Write;

        let mut buf = [0u8; S];

        let len = {
            let mut c = std::io::Cursor::new(&mut buf[..]);
            write!(c, "{self}").unwrap();
            usize::try_from(c.position()).unwrap()
        };

        StackString::new(buf, len)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
