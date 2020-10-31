use super::DatasetVocabulary;

impl Default for DatasetVocabulary {
    fn default() -> Self {
        DatasetVocabulary {
            system_time_column: None,
            event_time_column: None,
        }
    }
}
