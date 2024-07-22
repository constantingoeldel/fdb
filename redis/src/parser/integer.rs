use std::convert::TryFrom;
use std::ops::Deref;

use nom::{error, Finish};
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::character::complete::{char, digit1};
use nom::combinator::{map_res, opt};
use nom::Err::Error;
use nom::error::{ErrorKind, FromExternalError};
use nom::IResult;
use nom::sequence::{delimited, tuple};

use crate::parser::terminator::terminator;
use crate::parser::TryParse;

#[derive(Eq, PartialEq, Debug)]
pub(super) struct Integer(i64);

impl From<i64> for Integer {
    fn from(i: i64) -> Self {
        Integer(i)
    }
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
        let (i, num) = integer(value).finish()?;

        fn sign<'a>(i: &'a [u8]) -> IResult<&[u8], i64> {
            map_res(alt((char('-'), char('+'))), |s| {
                Ok::<i64, error::Error<&'a [u8]>>(match s {
                    '-' => -1,
                    '+' => 1,
                    _ => unreachable!()
                })
            })(i)
        }

        let (j, (sign, digits)) = tuple((opt(sign), parse_digits))(num).finish()?;

        assert_eq!(j.len(), 0);

        // If there is no explicit sign, assume positive int
        let sign = sign.unwrap_or(1);

        Ok((i, Integer(sign * digits)))
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
