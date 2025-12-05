// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::CollectionPathV2;
use kamu_datasets_services::utils::DatasetNameGenerator;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[rstest::rstest]
#[case("/path/to/file", "00000000-1111-2222-3333-444444444444-path-to-file")]
#[case("/path/to//file", "00000000-1111-2222-3333-444444444444-path-to-file")]
#[case(
    "/path/to/file.txt",
    "00000000-1111-2222-3333-444444444444-path-to-file-txt"
)]
#[case(
    "path/to/file.txt",
    "00000000-1111-2222-3333-444444444444-path-to-file-txt"
)]
#[case("file.txt", "00000000-1111-2222-3333-444444444444-file-txt")]
#[case("file.tar.gz", "00000000-1111-2222-3333-444444444444-file-tar-gz")]
#[case(
    "file with spaces",
    "00000000-1111-2222-3333-444444444444-file-with-spaces"
)]
#[case(
    "-name--with---dashes----",
    "00000000-1111-2222-3333-444444444444-name-with-dashes"
)]
// Just in case (URL encoded)
#[case(
    "/file%20with%20spaces.txt",
    "00000000-1111-2222-3333-444444444444-file-20with-20spaces-txt"
)]
fn test_dataset_name_generator(#[case] input: &str, #[case] expected: &str) {
    let static_uuid_v4 = uuid::Uuid::parse_str("00000000-1111-2222-3333-444444444444").unwrap();

    let path = CollectionPathV2::try_new(input).unwrap();
    let name = DatasetNameGenerator::based_on_collection_path_with_uuid(&path, static_uuid_v4);

    assert_eq!(expected, name.as_str());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
