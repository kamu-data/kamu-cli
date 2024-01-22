use kamu_core::DatasetHandleStream;
use opendatafabric::DatasetRefPattern;

pub fn filter_dataset_stream(
    dhs: DatasetHandleStream,
    dataset_ref_pattern: DatasetRefPattern,
) -> DatasetHandleStream<'_> {
    use futures::{future, StreamExt, TryStreamExt};
    dhs.try_filter(move |dsh| {
        future::ready(
            dataset_ref_pattern
                .match_pattern(dsh.to_string().as_str())
                .unwrap_or(false),
        )
    })
    .boxed()
}
