// TODO: Yep... all this to serde an Option<DateTime> in a slightly different
// format. See: https://github.com/serde-rs/serde/issues/723

pub mod datetime_rfc3339 {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        date: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let s = date.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.into())
            .map_err(serde::de::Error::custom)
    }
}

pub mod datetime_rfc3339_opt {
    use chrono::{DateTime, Utc};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        option: &Option<DateTime<Utc>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match option {
            None => serializer.serialize_none(),
            Some(date) => super::datetime_rfc3339::serialize(&date, serializer),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<DateTime<Utc>>, D::Error> {
        super::datetime_rfc3339::deserialize(deserializer).map(Some)
    }
}
