use super::Result;

pub struct Serializer {
    output: Vec<u8>,
}

impl Serializer {
    fn to_bytes<T>(value: &T) -> Result<Vec<u8>> {
        let mut serializer = Serializer { output: Vec::new() };
        // value.serialize(&mut serializer)?;
        Ok(serializer.output)
    }

    fn to_string<T>(value: &T) -> Result<String> {
        let bytes = Self::to_bytes(value)?;
        Ok(String::from_utf8(bytes)?)
    }

    fn to_writer<T, W>(value: &T, mut writer: W) -> std::io::Result<()>
        where
            W: std::io::Write,
    {
        let bytes = Self::to_bytes(value)?;
        writer.write_all(&bytes)
    }
}
//
// impl<'a> ser::Serializer for &'a mut Serializer {
//     type Ok = ();
//     type Error = Error;
//
//
//
// }