pub use super::Deserializer;
pub use super::Result;
pub use super::SerdeError;
pub use super::Serializer;

use std::convert;

pub struct YamlSerializer;
pub struct YamlDeserializer;

impl<T> Serializer<T> for YamlSerializer
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn serialize(&self, obj: &T, writer: &mut dyn std::io::Write) -> Result<()> {
        serde_yaml::to_writer(writer, obj)?;
        Ok(())
    }
}

impl<T> Deserializer<T> for YamlDeserializer
where
    T: serde::de::DeserializeOwned,
{
    fn deserialize(&self, buf: &[u8]) -> Result<T> {
        let obj = serde_yaml::from_slice::<T>(buf)?;
        Ok(obj)
    }
}

impl convert::From<serde_yaml::Error> for SerdeError {
    fn from(err: serde_yaml::Error) -> SerdeError {
        SerdeError(Box::new(err))
    }
}
