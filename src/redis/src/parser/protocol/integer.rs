use nom::{error, Finish};
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::character::complete::{char, digit1};
use nom::combinator::{map_res, opt};
use nom::Err::Error;
use nom::error::{ErrorKind, FromExternalError};
use nom::IResult;
use nom::sequence::delimited;
use serde::Deserialize;

use crate::parser::protocol::{string, TryParse};
use crate::parser::protocol::big_number::big_number;
use crate::parser::protocol::terminator::terminator;

#[derive(Eq, PartialEq, Debug, Hash, Deserialize)]
pub struct Integer(i64);

impl Into<i64> for Integer {
    fn into(self) -> i64 {
        self.0
    }
}

impl From<i64> for Integer {
    fn from(i: i64) -> Self {
        Integer(i)
    }
}


pub(super) fn sign<'a>(i: &'a [u8]) -> IResult<&[u8], i64> {
    map_res(alt((char('-'), char('+'))), |s| {
        Ok::<i64, error::Error<&'a [u8]>>(match s {
            '-' => -1,
            '+' => 1,
            _ => unreachable!()
        })
    })(i)
}

pub fn integer(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(char(':'), is_not("\r\n"), terminator)(i)
}

pub fn parse_digits(i: &[u8]) -> IResult<&[u8], i64> {
    fn parse(i: &str) -> IResult<&str, i64> {
        map_res(digit1, str::parse)(i)
    }

    let i = std::str::from_utf8(i).map_err(|e| Error(nom::error::Error::from_external_error(i, ErrorKind::Digit, e)))?;
    let (i, num) = parse(i).map_err(|e| Error(error::Error::new(i.as_bytes(), ErrorKind::Digit)))?; // TODO: Reuse original error?
    let i = i.as_bytes();

    Ok((i, num))
}

impl<'a> TryParse<'a> for Integer {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, num) = alt((integer, big_number, string))(value).finish()?;
        let (j, sign) = opt(sign)(num).finish()?;
        let (j, digits) = parse_digits(j).finish()?;
        assert_eq!(j.len(), 0);

        // If there is no explicit sign, assume positive int
        let sign = sign.unwrap_or(1);
        let num = sign * digits;

        Ok((i, Integer(num)))
    }
}


#[test]
fn test_integer() {
    let s: &[u8] = b":1000\r\n";
    let (rem, int) = Integer::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(int, Integer(1000));
}

#[test]
fn test_integer_negative() {
    let s: &[u8] = b":-1000\r\n";
    let (rem, num) = Integer::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, Integer(-1000));
}

#[test]
fn test_integer_positive() {
    let s: &[u8] = b":+1000\r\n";
    let (rem, num) = Integer::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, Integer(1000));
}

#[test]
fn test_integer_from_string() {
    let s: &[u8] = b"+1000\r\n";
    let (rem, num) = Integer::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, Integer(1000));
}

#[test]
fn integer_from_bulk_string() {
    let s: &[u8] = b"$4\r\n1000\r\n";
    let (rem, num) = Integer::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, Integer(1000));
}
