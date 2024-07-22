use num_traits::Num;
use serde::de;
use serde::de::Visitor;

use crate::parser::protocol::{ParsedValues, string};
use crate::parser::protocol::double::Double;
use crate::parser::protocol::integer::Integer;

use super::{Error, Result};
use super::super::protocol::boolean::Boolean;
use super::super::protocol::TryParse;

pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    pub fn from_slice(input: &'de [u8]) -> Self {
        Deserializer { input }
    }

    pub fn from_str(input: &'de str) -> Self {
        Deserializer { input: input.as_bytes() }
    }

    pub fn from_reader(input: &'de mut dyn std::io::Read) -> std::io::Result<Self> {
        let mut buf = Vec::new();
        input.read_to_end(&mut buf)?;
        Ok(Deserializer { input: &buf })
    }

    fn deserialize_int<T>(&mut self) -> Result<T> where T: Num {
        let (i, int) = Integer::try_parse(&self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > T::MAX as i64 || int < T::MIN as i64 {
            Err(Error::IntegerOutOfRange(int, T::MAX as i64))
        } else {
            Ok(int as T)
        }
    }

    fn deserialize_float<T>(&mut self) -> Result<T> where T: Num {
        let int = self.deserialize_int();

        if int.is_ok() {
            int
        } else {
            let (i, double) = Double::try_parse(&self.input)?;
            self.input = i;
            Ok(double.into())
        }
    }
}


pub fn from_slice<'a, T>(s: &'a [u8]) -> Result<T>
    where
        T: serde::de::Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_slice(s);
    let t = T::deserialize(&mut deserializer)?;

    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::TrailingCharacters)
    }
}

pub fn from_str<'a, T>(s: &'a str) -> Result<T>
    where
        T: serde::de::Deserialize<'a>,
{
    from_slice(s.as_bytes())
}

pub fn from_reader<'a, T, R>(reader: &'a mut R) -> std::io::Result<T>
    where
        T: serde::de::Deserialize<'a>,
        R: std::io::Read,
{
    let mut deserializer = Deserializer::from_reader(reader)?;
    let t = T::deserialize(&mut deserializer)?;

    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::TrailingCharacters.into())
    }
}


impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, v) = ParsedValues::try_parse(&self.input)?;
        self.input = i;

        match v {
            ParsedValues::Integer(i) => visitor.visit_i64(i.into()),
            ParsedValues::Double(d) => visitor.visit_f64(d.into()),
            ParsedValues::String(s) => visitor.visit_borrowed_str(&s),
            ParsedValues::Boolean(b) => visitor.visit_bool(b.into()),
            ParsedValues::Null => visitor.visit_unit(),
            ParsedValues::Array(a) => {
                let mut deserializer = Deserializer::from_slice(a);
                visitor.visit_seq(&mut deserializer)
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, boolean) = Boolean::try_parse(&self.input)?;
        self.input = i;
        visitor.visit_bool(boolean.into())
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_i8(self.deserialize_int()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_i16(self.deserialize_int()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_i32(self.deserialize_int()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_i64(self.deserialize_int()?)
    }

    fn deserialize_i128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_i128(self.deserialize_int()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_u8(self.deserialize_int()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_u16(self.deserialize_int()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_u32(self.deserialize_int()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_u64(self.deserialize_int()?)
    }

    fn deserialize_u128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_u128(self.deserialize_int()?)
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        // f32s can be deserialized for integers or doubles

        visitor.visit_f32(self.deserialize_float()?)
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        // f64s can be deserialized for integers or doubles

        visitor.visit_f64(self.deserialize_float()?)
    }

    fn deserialize_char<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        Err(Error::CharNotSupported)
    }

    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, s) = string(&self.input)?;
        self.input = i;
        let str = std::str::from_utf8(s)?;
        visitor.visit_borrowed_str(str)
    }

    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        Err(Error::BytesNotSupported)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        Err(Error::BytesNotSupported)
    }

    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_struct<V>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }
}