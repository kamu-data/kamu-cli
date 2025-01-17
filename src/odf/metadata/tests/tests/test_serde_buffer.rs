// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric_metadata::serde::Buffer;

#[test]
fn ensure_capacity_realloc() {
    let mut buf = Buffer::new(1, 4, vec![0, 3, 4, 5, 0]);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.inner(), [0, 3, 4, 5, 0]);
    assert_eq!(&buf as &[i32], [3, 4, 5]);

    buf.ensure_capacity(2, 2);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.inner(), [0, 0, 3, 4, 5, 0, 0]);

    buf.set_head(0);
    buf.set_tail(7);
    assert_eq!(buf.len(), 7);
    buf[0] = 1;
    buf[1] = 2;
    buf[5] = 6;
    buf[6] = 7;
    assert_eq!(buf.inner(), [1, 2, 3, 4, 5, 6, 7]);
}
