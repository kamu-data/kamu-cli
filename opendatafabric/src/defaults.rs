use crate::ReadStepCsv;

use super::DatasetVocabulary;

impl Default for DatasetVocabulary {
    fn default() -> Self {
        DatasetVocabulary {
            system_time_column: None,
            event_time_column: None,
        }
    }
}

impl Default for ReadStepCsv {
    fn default() -> Self {
        Self {
            schema: None,
            separator: None,
            encoding: None,
            quote: None,
            escape: None,
            comment: None,
            header: None,
            enforce_schema: None,
            infer_schema: None,
            ignore_leading_white_space: None,
            ignore_trailing_white_space: None,
            null_value: None,
            empty_value: None,
            nan_value: None,
            positive_inf: None,
            negative_inf: None,
            date_format: None,
            timestamp_format: None,
            multi_line: None,
        }
    }
}
