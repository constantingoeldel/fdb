use nom::{Finish, IResult};
use nom::bytes::complete::is_not;
use nom::character::complete::char;
use nom::sequence::delimited;

use crate::parser::terminator::terminator;
use crate::parser::TryParse;

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct SimpleError(String);

impl From<&str> for SimpleError {
    fn from(s: &str) -> Self {
        SimpleError(s.to_string())
    }
}

pub fn simple_error(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(char('-'), is_not("\r\n"), terminator)(i)
}

impl<'a> TryParse<'a> for SimpleError {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, str) = simple_error(value).finish()?;
        Ok((i, SimpleError(std::str::from_utf8(str).unwrap().to_string())))
    }
}

#[test]
fn test_simple_error() {
    let s: &[u8] = b"-Error message\r\n";
    let (rem, str) = SimpleError::try_parse(s).unwrap();
    assert_eq!(rem, b"");
    assert_eq!(str, SimpleError(String::from("Error message")));
}