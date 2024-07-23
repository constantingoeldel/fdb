use nom::{AsBytes, Finish};
use nom::branch::alt;
use serde::{de, Deserialize};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::parser::protocol::array::array;
use crate::parser::protocol::double::Double;
use crate::parser::protocol::integer::Integer;
use crate::parser::protocol::push::push;
use crate::parser::protocol::set::set;
use crate::parser::protocol::string;

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

    // pub fn from_reader(input: &'de mut dyn std::io::Read) -> std::io::Result<Self> {
    //     let mut buf = Vec::new();
    //     input.read_to_end(&mut buf)?;
    //     Ok(Deserializer { input: buf.as_bytes() })
    // }

    // fn deserialize_float<T>(mut self) -> Result<T> where T: From<i64> + From<f64> {
    //     let int = self.deserialize_int();
    //
    //     if int.is_ok() {
    //         int
    //     } else {
    //         let (i, double) = Double::try_parse(&self.input)?;
    //         self.input = i;
    //         let float: f64 = double.into();
    //         Ok(float.into())
    //     }
    // }
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

// pub fn from_reader<'a, T, R>(reader: &'a mut R) -> std::io::Result<T>
//     where
//         T: serde::de::Deserialize<'a>,
//         R: std::io::Read,
// {
//     let mut deserializer = Deserializer::from_reader(reader)?;
//     let t = T::deserialize(&mut deserializer)?;
//
//     if deserializer.input.is_empty() {
//         Ok(t)
//     } else {
//         Err(Error::TrailingCharacters.into())
//     }
// }


impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!();

        // let (i, v) = ParsedValues::try_parse(&self.input)?;
        // self.input = i;
        //
        // match v {
        //     ParsedValues::Integer(i) => visitor.visit_i64(i.into()),
        //     ParsedValues::Double(d) => visitor.visit_f64(d.into()),
        //     ParsedValues::Boolean(b) => visitor.visit_bool(b.into()),
        //     ParsedValues::Null(_) => visitor.visit_unit(),
        //     ParsedValues::NullBulkString(_) => visitor.visit_unit(),
        //     ParsedValues::Array(a) => {
        //         visitor.visit_seq(a)
        //     }
        //
        //     ParsedValues::String(s) => visitor.visit_string(s),
        //     ParsedValues::VerbatimString(s) => visitor.visit_string(s.into()),
        //     ParsedValues::SimpleString(s) => visitor.visit_string(s.into()),
        //     ParsedValues::BulkString(s) => visitor.visit_string(s.into()),
        //
        //     ParsedValues::SimpleError(e) => visitor.visit_string(e.into()),
        //     ParsedValues::BulkError(e) => visitor.visit_string(e.into()),
        //     ParsedValues::Terminator(_) => unreachable!(),
        //     ParsedValues::Map(m) => visitor.visit_map(m),
        //     ParsedValues::Push(p) => visitor.visit_seq(p),
        //     ParsedValues::Set(s) => visitor.visit_seq(s),
        //
        //     ParsedValues::BigNumber(b) => {
        //         let b: BigInt = b.into();
        //         visitor.visit_string(b.to_string())
        //     },
        //     ParsedValues::NullArray(_) => visitor.visit_unit()
        // }
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, boolean) = Boolean::try_parse(&self.input)?;
        self.input = i;
        visitor.visit_bool(boolean.into())
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > i8::MAX as i64 || int < i8::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, i8::MAX as i128));
        }

        let int = int as i8;

        visitor.visit_i8(int)
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > i16::MAX as i64 || int < i16::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, i16::MAX as i128));
        }

        let int = int as i16;


        visitor.visit_i16(int)
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > i32::MAX as i64 || int < i32::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, i32::MAX as i128));
        }

        let int = int as i32;


        visitor.visit_i32(int)
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > i64::MAX || int < i64::MIN {
            return Err(Error::IntegerOutOfRange(int as i128, i64::MAX as i128));
        }

        let int = int;


        visitor.visit_i64(int)
    }

    fn deserialize_i128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > i128::MAX as i64 || int < i128::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, i128::MAX as i128));
        }

        let int = int as i128;


        visitor.visit_i128(int)
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > u8::MAX as i64 || int < u8::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, u8::MAX as i128));
        }

        let int = int as u8;


        visitor.visit_u8(int)
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > u16::MAX as i64 || int < u16::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, u16::MAX as i128));
        }

        let int = int as u16;


        visitor.visit_u16(int)
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > u32::MAX as i64 || int < u32::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, u32::MAX as i128));
        }

        let int = int as u32;


        visitor.visit_u32(int)
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > u64::MAX as i64 || int < u64::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, u64::MAX as i128));
        }

        let int = int as u64;


        visitor.visit_u64(int)
    }

    fn deserialize_u128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(&self.input.as_ref())?;
        self.input = i;

        let int: i64 = int.into();

        if int > u128::MAX as i64 || int < u128::MIN as i64 {
            return Err(Error::IntegerOutOfRange(int as i128, u128::MAX as i128));
        }

        let int = int as u128;


        visitor.visit_u128(int)
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        // f32s can be deserialized for integers or doubles
        let int = Integer::try_parse(&self.input);

        if let Ok((i, int)) = int {
            self.input = i;
            let int: i64 = int.into();
            visitor.visit_f32(int as f32)
        } else {
            let (i, double) = Double::try_parse(&self.input)?;
            self.input = i;
            let float: f64 = double.into();
            visitor.visit_f32(float as f32)
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        // f64s can be deserialized for integers or doubles

        let int = Integer::try_parse(&self.input);

        if let Ok((i, int)) = int {
            self.input = i;
            let int: i64 = int.into();
            visitor.visit_f64(int as f64)
        } else {
            let (i, double) = Double::try_parse(&self.input)?;
            self.input = i;
            let float: f64 = double.into();
            visitor.visit_f64(float)
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        Err(Error::CharNotSupported)
    }

    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, s) = string(&self.input).finish()?;
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
        let (i, slice) = alt((array, set, push))(&self.input).finish()?;
        self.input = i;

        visitor.visit_seq(Slice(slice))
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
        // use normal any
        unimplemented!()
    }
}

struct Slice<'a>(&'a [u8]);

impl<'de> SeqAccess<'de> for Slice<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> std::result::Result<Option<T::Value>, Self::Error> where T: DeserializeSeed<'de> {
        if self.0.is_empty() {
            Ok(None)
        } else {
            seed.deserialize(&mut Deserializer::from_slice(self.0)).map(Some)
            // seed.deserialize(value.into_deserializer()).map(Some)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }
}
//
// impl<'de, 'a> SeqAccess<'de> for Push {
//     type Error = Error;
//
//     fn next_element_seed<T>(&mut self, seed: T) -> std::result::Result<Option<T::Value>, Self::Error> where T: DeserializeSeed<'de> {
//         if self.0.is_empty() {
//             Ok(None)
//         } else {
//             let value = self.0.remove(0);
//             seed.deserialize(value).map(Some)
//         }
//     }
//
//     fn size_hint(&self) -> Option<usize> {
//         Some(self.0.len())
//     }
// }
//
//
// impl<'de, 'a> SeqAccess<'de> for Set {
//     type Error = Error;
//
//     fn next_element_seed<T>(&mut self, seed: T) -> std::result::Result<Option<T::Value>, Self::Error> where T: DeserializeSeed<'de> {
//         let value = self.iter().next();
//         if let Some(value) = value {
//             Ok(Some(seed.deserialize(value).map(Some)))
//         } else {
//             Ok(None)
//         }
//     }
//
//     fn size_hint(&self) -> Option<usize> {
//         Some(self.0.len())
//     }
// }
//
//
// impl<'de, 'a> MapAccess<'de> for Map {
//     type Error = Error;
//
//     fn size_hint(&self) -> Option<usize> {
//         Some(self.0.len())
//     }
//
//
//     fn next_entry_seed<K, V>(&mut self, kseed: K, vseed: V) -> std::result::Result<Option<(K::Value, V::Value)>, Self::Error> where K: DeserializeSeed<'de>, V: DeserializeSeed<'de> {
//         let next_entry = self.iter().next();
//
//         if let Some((key, value)) = next_entry {
//             // let key = key.into_deserializer();
//             // let value = value.into_deserializer();
//
//             let key = kseed.deserialize(key)?;
//             let value = vseed.deserialize(value)?;
//
//             Ok(Some((key, value)))
//         } else {
//             Ok(None)
//         }
//     }
//
//     fn next_key_seed<K>(&mut self, seed: K) -> std::result::Result<Option<K::Value>, Self::Error> where K: DeserializeSeed<'de> {
//         todo!()
//     }
//
//     fn next_value_seed<V>(&mut self, seed: V) -> std::result::Result<V::Value, Self::Error> where V: DeserializeSeed<'de> {
//         todo!()
//     }
// }