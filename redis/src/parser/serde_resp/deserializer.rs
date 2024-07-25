use nom::{AsBytes, Finish};
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::character::complete::{char, u128};
use nom::sequence::{delimited, terminated};
use serde::{de, Deserialize};
use serde::de::{DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};

use crate::parser::protocol::{null, string};
use crate::parser::protocol::double::Double;
use crate::parser::protocol::integer::{Integer, parse_digits};
use crate::parser::protocol::terminator::terminator;

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

    fn from_str(input: &'de str) -> Self {
        Deserializer { input: input.as_bytes() }
    }

    fn from_reader(input: &'de mut dyn std::io::Read, buf: &'de mut Vec<u8>) -> std::io::Result<Self> {
        input.read_to_end(buf)?;
        Ok(Deserializer { input: buf.as_slice() })
    }
}


pub fn from_slice<'a, T>(s: &'a [u8]) -> Result<T>
    where
        T: serde::de::Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_slice(s);
    let t = T::deserialize(&mut deserializer)?;

    // if deserializer.input.is_empty() {
    //     Ok(t)
    // } else {
    //     Err(Error::TrailingCharacters)
    // }
    Ok(t)
}


pub fn from_str<'a, T>(s: &'a str) -> Result<T>
    where
        T: serde::de::Deserialize<'a>,
{
    from_slice(s.as_bytes())
}

pub fn from_reader<T>(input: &mut dyn std::io::Read) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
{
    let mut buf = Vec::new();
    let mut deserializer = Deserializer::from_reader(input, &mut buf)?;
    let t = T::deserialize(&mut deserializer)?;

    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::TrailingCharacters)
    }
}

const ARRAY: char = '*';
const SET: char = '~';
const PUSH: char = '>';
const TERM: &str = "\r\n";

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        eprintln!("deserialize_any does not make sense for non-self-describing formats like RESP! Check that you are using the correct deserialization method. It is fine for the basic types of RESP like integers, doubles, strings, and arrays but not for e.g. Redis command option parsing.");

        let c = self.input.first().expect("At this point, the input should not be empty");
        let c = *c as char;
        match c {
            '*' | '~' | '>' => self.deserialize_seq(visitor),
            '%' => self.deserialize_map(visitor),
            '+' | '-' | '$' | '(' | '!' | '=' => self.deserialize_str(visitor),
            ':' => self.deserialize_i64(visitor),
            '_' => self.deserialize_unit(visitor),
            '#' => self.deserialize_bool(visitor),
            ',' => self.deserialize_f64(visitor),
            _ => unimplemented!("Unknown type"),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, boolean) = Boolean::try_parse(&self.input)?;
        self.input = i;
        visitor.visit_bool(boolean.into())
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > i8::MAX as i64 || int < i8::MIN as i64 {
            return Err(Error::IntegerOutOfRange(i8::MIN as i128, i8::MAX as i128, int as i128));
        }

        let int = int as i8;

        visitor.visit_i8(int)
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > i16::MAX as i64 || int < i16::MIN as i64 {
            return Err(Error::IntegerOutOfRange(i16::MIN as i128, i16::MAX as i128, int as i128));
        }

        let int = int as i16;


        visitor.visit_i16(int)
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > i32::MAX as i64 || int < i32::MIN as i64 {
            return Err(Error::IntegerOutOfRange(i32::MIN as i128, i32::MAX as i128, int as i128));
        }

        let int = int as i32;


        visitor.visit_i32(int)
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        visitor.visit_i64(int.into())
    }

    fn deserialize_i128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        let int = int as i128;


        visitor.visit_i128(int)
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > u8::MAX as i64 || int < u8::MIN as i64 {
            return Err(Error::IntegerOutOfRange(u8::MIN as i128, u8::MAX as i128, int as i128));
        }

        let int = int as u8;


        visitor.visit_u8(int)
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > u16::MAX as i64 || int < u16::MIN as i64 {
            return Err(Error::IntegerOutOfRange(u16::MIN as i128, u16::MAX as i128, int as i128));
        }

        let int = int as u16;


        visitor.visit_u16(int)
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > u32::MAX as i64 || int < u32::MIN as i64 {
            return Err(Error::IntegerOutOfRange(u32::MIN as i128, u32::MAX as i128, int as i128));
        }

        let int = int as u32;


        visitor.visit_u32(int)
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > u64::MAX as i64 || int < u64::MIN as i64 {
            return Err(Error::IntegerOutOfRange(u64::MIN as i128, u64::MAX as i128, int as i128));
        }

        let int = int as u64;


        visitor.visit_u64(int)
    }

    fn deserialize_u128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, int) = Integer::try_parse(self.input)?;
        self.input = i;

        let int: i64 = int.into();

        if int > u128::MAX as i64 || int < u128::MIN as i64 {
            // TODO: this conversion will panic
            return Err(Error::IntegerOutOfRange(u128::MIN as i128, u128::MAX as i128, int as i128));
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
        let (i, s) = string(self.input).finish()?;
        self.input = i;
        let str = std::str::from_utf8(s)?;
        visitor.visit_borrowed_str(str)
    }

    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        // Consume the entire input and return it as bytes
        // Used to create a copy of the deserializer for deserializing untagged enums of non-self-describing formats like RESP
        visitor.visit_borrowed_bytes(self.input)
    }


    fn deserialize_byte_buf<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_byte_buf(self.input)
        
    }

    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        if let Ok((i, _n)) = null(self.input) {
            self.input = i;
            return visitor.visit_none();
        }

        if let Ok((_, _)) = string(self.input) {
            return visitor.visit_some(self);
        }

        visitor.visit_none()
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, _) = null(self.input).finish()?;
        self.input = i;
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, s) = string(self.input).finish()?;
        self.input = i;
        let s = std::str::from_utf8(s)?;


        if s.to_lowercase() == name.to_lowercase() {
            visitor.visit_unit()
        } else {
            Err(Error::UnitStructNameMismatch(name.to_string(), s.to_string()))
        }
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let seq = alt((char(ARRAY), char(SET), char(PUSH)));

        let mut length = None;

        if let Ok((i, len)) = delimited(seq, is_not(TERM), terminator)(self.input).finish() {
            length = Some(std::str::from_utf8(len).unwrap().parse::<usize>()?);
            self.input = i;
        }


        let slice = Slice::new(self, length);
        visitor.visit_seq(slice)
    }


    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_seq(Slice::new(self, Some(len)))
    }

    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_seq(Slice::new(self, Some(len)))
    }

    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        let (i, num_entries) = delimited(char('%'), is_not("\r\n"), terminator)(self.input).finish()?;
        let (_, num_entries) = parse_digits(num_entries).finish()?;
        self.input = i;

        let map_slice = MapSlice::new(self, num_entries as usize);
        visitor.visit_map(map_slice)
    }

    fn deserialize_struct<V>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_enum(Enum::new(self))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        unimplemented!();
        let (i, _) = terminated(is_not(TERM), terminator)(self.input).finish()?;

        self.input = i;
        visitor.visit_none()
    }
}


struct Enum<'a, 'de> {
    de: &'a mut Deserializer<'de>,
}

impl<'a, 'de> Enum<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        Enum { de }
    }
}

impl<'de, 'a> EnumAccess<'de> for Enum<'a, 'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> std::result::Result<(V::Value, Self), Self::Error> where V: DeserializeSeed<'de> {
        // dbg!(std::str::from_utf8(self.de.input).unwrap());

        let variant = seed.deserialize(&mut *self.de)?;
        dbg!(std::str::from_utf8(self.de.input).unwrap());
        Ok((variant, self))
    }
}

impl<'de, 'a> VariantAccess<'de> for Enum<'a, 'de> {
    type Error = Error;

    fn unit_variant(self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> std::result::Result<T::Value, Self::Error> where T: DeserializeSeed<'de> {
        seed.deserialize(&mut *self.de)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_seq(Slice::new(self.de, Some(len)))
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> std::result::Result<V::Value, Self::Error> where V: Visitor<'de> {
        visitor.visit_seq(Slice::new(self.de, Some(fields.len())))
    }
}

pub struct Slice<'a, 'de> {
    de: &'a mut Deserializer<'de>,
    /// If known, the number of entries in the Array.
    /// If none, the input data will be consumed until empty
    num_items: Option<usize>,
}

impl<'a, 'de> Slice<'a, 'de> {
    pub fn new(de: &'a mut Deserializer<'de>, max: Option<usize>) -> Self {
        Slice { de, num_items: max }
    }
}

impl<'a, 'de> SeqAccess<'de> for Slice<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> std::result::Result<Option<T::Value>, Self::Error> where T: DeserializeSeed<'de> {
        // If there are no more items, return None
        if let Some(num_items) = self.num_items {
            if num_items == 0 {
                return Ok(None);
            }
            self.num_items = Some(num_items - 1);
        }

        seed.deserialize(&mut *self.de).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        self.num_items
    }
}


struct MapSlice<'a, 'de> {
    de: &'a mut Deserializer<'de>,
    num_entries: usize,
}

impl<'a, 'de> MapSlice<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>, num_entries: usize) -> Self {
        MapSlice { de, num_entries }
    }
}

impl<'a, 'de> MapAccess<'de> for MapSlice<'a, 'de> {
    type Error = Error;
    fn next_key_seed<K>(&mut self, seed: K) -> std::result::Result<Option<K::Value>, Self::Error> where K: DeserializeSeed<'de> {
        if self.num_entries == 0 {
            return Ok(None);
        }

        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> std::result::Result<V::Value, Self::Error> where V: DeserializeSeed<'de> {
        self.num_entries -= 1;
        seed.deserialize(&mut *self.de)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.num_entries)
    }
}