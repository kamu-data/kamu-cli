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
#[case("/file.txt", "00000000-1111-2222-3333-444444444444-file-txt")]
#[case("file.txt", "00000000-1111-2222-3333-444444444444-file-txt")]
#[case("/path/to/file", "00000000-1111-2222-3333-444444444444-file")]
#[case("/path/to/file.txt", "00000000-1111-2222-3333-444444444444-file-txt")]
#[case("path/to/file.txt", "00000000-1111-2222-3333-444444444444-file-txt")]
#[case("/path/to/%20/file", "00000000-1111-2222-3333-444444444444-file")]
#[case(
    "/-name--with---dashes----",
    "00000000-1111-2222-3333-444444444444-name-with-dashes"
)]
#[case(
    "/name%20with%20spaces",
    "00000000-1111-2222-3333-444444444444-name-with-spaces"
)]
#[case(
    "/name%20with%20mixed%20%D1%8E%D0%BD%D0%B8%D0%BA%D0%BE%D0%B4.txt",
    "00000000-1111-2222-3333-444444444444-name-with-mixed-txt"
)]
#[case(
    "/%20file%20with%20encoded%20spaces.txt",
    "00000000-1111-2222-3333-444444444444-file-with-encoded-spaces-txt"
)]
#[case(
    "/%2Ffile%2Fwith%2Fencoded%2Fslashes.txt",
    "00000000-1111-2222-3333-444444444444-file-with-encoded-slashes-txt"
)]
#[case(
    "/a/very/very/very/very/very/very/very/very/very/very/very/very/very/very/very/very/very/long/\
     path/to/a/file.txt",
    "00000000-1111-2222-3333-444444444444-file-txt"
)]
// Should be truncated to `DatasetName::MAX_LEN` characters
#[case(
    "/a-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-long-\
     file-name.txt",
    "00000000-1111-2222-3333-444444444444-a-very-very-very-very-very-very-very-very-very-very-very-very-v"
)]
// "/юникод.txt"
#[case(
    "/%D1%8E%D0%BD%D0%B8%D0%BA%D0%BE%D0%B4.txt",
    "00000000-1111-2222-3333-444444444444-txt"
)]
fn test_dataset_name_generator(#[case] input: &str, #[case] expected: &str) {
    let static_uuid_v4 = uuid::Uuid::parse_str("00000000-1111-2222-3333-444444444444").unwrap();

    let path = CollectionPathV2::try_new(input).unwrap();
    let name = DatasetNameGenerator::based_on_collection_path_with_uuid(&path, static_uuid_v4);

    assert_eq!(expected, name.as_str());

    // TODO: This likely needs to be ensured by `DatasetName` type itself
    assert!(name.len() <= odf::DatasetName::MAX_LEN);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
