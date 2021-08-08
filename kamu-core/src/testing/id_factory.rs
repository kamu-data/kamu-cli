use std::convert::TryFrom;

use opendatafabric::DatasetIDBuf;
use rand::Rng;

pub struct IDFactory;

/// Generates randomized unique identities for different resources
impl IDFactory {
    pub fn dataset_id() -> DatasetIDBuf {
        // TODO: create more readable IDs like docker does
        let mut id = String::with_capacity(20);
        id.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(20)
                .map(char::from),
        );
        DatasetIDBuf::try_from(id).unwrap()
    }
}
