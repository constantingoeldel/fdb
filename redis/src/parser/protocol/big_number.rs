use std::fmt::Formatter;

use nom::bytes::complete::is_not;
use nom::character::complete::char;
use nom::combinator::opt;
use nom::Finish;
use nom::IResult;
use nom::sequence::delimited;
use num_bigint::BigInt;
use num_traits::Num;
use serde::{Deserialize, Deserializer};
use serde::de::{Error, Visitor};

use crate::parser::protocol::integer::sign;
use crate::parser::protocol::terminator::terminator;
use crate::parser::protocol::TryParse;

#[derive(Eq, PartialEq, Debug, Hash)]
pub(super) struct BigNumber(BigInt);

struct BigIntVisitor;

impl<'de> Visitor<'de> for BigIntVisitor {
    type Value = BigNumber;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a big number")
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E> where E: Error {
        Ok(BigNumber(BigInt::from(v)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        let num = BigInt::from_str_radix(v, 10).unwrap();
        Ok(BigNumber(num))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> where E: Error {
        let num = BigInt::from_str_radix(v, 10).unwrap();
        Ok(BigNumber(num))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E> where E: Error {
        let num = BigInt::from_str_radix(&v, 10).unwrap();
        Ok(BigNumber(num))
    }
}

impl<'de> Deserialize<'de> for BigNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(BigIntVisitor)
    }
}


impl Into<BigInt> for BigNumber {
    fn into(self) -> BigInt {
        self.0
    }
}

pub fn big_number(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(char('('), is_not("\r\n"), terminator)(i)
}


impl<'a> TryParse<'a> for BigNumber {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, num) = big_number(value).finish()?;
        let (j, sign) = opt(sign)(num).finish()?;
        let sign = sign.unwrap_or(1);

        let num = std::str::from_utf8(j).unwrap();
        let num = BigInt::from_str_radix(num, 10).unwrap();
        let num = num * sign;

        Ok((i, BigNumber(num)))
    }
}


#[test]
fn test_big_number() {
    let s: &[u8] = b"(-3492890328409238509324850943850943825024385\r\n";
    let (rem, int) = BigNumber::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(int.0.to_string(), "-3492890328409238509324850943850943825024385".to_string());
}

#[test]
fn test_big_number_negative() {
    let s: &[u8] = b"(-1000\r\n";
    let (rem, num) = BigNumber::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, BigNumber(BigInt::from(-1000)));
}

#[test]
fn test_big_number_positive() {
    let s: &[u8] = b"(+1000\r\n";
    let (rem, num) = BigNumber::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, BigNumber(BigInt::from(1000)));
}
