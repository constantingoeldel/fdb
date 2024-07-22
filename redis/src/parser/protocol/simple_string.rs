use nom::{Finish, IResult};
use nom::bytes::complete::is_not;
use nom::character::complete::char;
use nom::sequence::delimited;

use crate::parser::terminator::terminator;
use crate::parser::TryParse;

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct SimpleString(String);

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        SimpleString(s.to_string())
    }
}

pub fn simple_string(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(char('+'), is_not("\r\n"), terminator)(i)
}

impl<'a> TryParse<'a> for SimpleString {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, str) = simple_string(value).finish()?;
        Ok((i, SimpleString(std::str::from_utf8(str).unwrap().to_string())))
    }
}

#[test]
fn test_simple_string() {
    let s: &[u8] = b"+OK\r\n";
    let (rem, str) = SimpleString::try_parse(s).unwrap();
    assert_eq!(rem, b"");
    assert_eq!(str, SimpleString(String::from("OK")));
}