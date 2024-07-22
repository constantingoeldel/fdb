use nom::{Finish, IResult};
use nom::bytes::complete::{is_not, take};
use nom::character::complete::char;
use nom::Err::Error;
use nom::sequence::{delimited, terminated};

use crate::parser::integer::parse_digits;
use crate::parser::terminator::terminator;
use crate::parser::TryParse;

/// A bulk string represents a single binary string. The string can be of any size, but by default,
/// Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).
///
/// TODO: Implement the variable limit
#[derive(Debug, Eq, PartialEq)]
pub(super) struct BulkString(String);

impl From<&str> for BulkString {
    fn from(s: &str) -> Self {
        BulkString(s.to_string())
    }
}

pub fn bulk_string(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let (i, len) = delimited(char('$'), is_not("\r\n"), terminator)(i)?;
    let (_, len) = parse_digits(len)?;
    if len > 512 * 1024 * 1024 {
        return Err(Error(nom::error::Error::new(i, nom::error::ErrorKind::TooLarge)));
    }
    let (i, data) = terminated(take(len as usize), terminator)(i)?;
    Ok((i, data))
}

impl<'a> TryParse<'a> for BulkString {

    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, data) = bulk_string(value).finish()?;
        let data = std::str::from_utf8(data).unwrap().to_string();
        Ok((i, BulkString(data)))
    }
}

#[test]
fn test_bulk_string() {
    let s: &[u8] = b"$6\r\nfoobar\r\n";
    let (rem, str) = BulkString::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(str, BulkString(String::from("foobar")));
}

#[test]
fn test_empty_bulk_string() {
    let s: &[u8] = b"$0\r\n\r\n";
    let (rem, str) = BulkString::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(str, BulkString(String::new()));
}

#[test]
fn test_bulk_string_too_large() {
    let s: &[u8] = b"$536870913\r\n";
    let r = BulkString::try_parse(s);
    assert!(r.is_err());
}

#[test]
#[should_panic]
fn test_bulk_string_with_wrong_size() {
    let s: &[u8] = b"$6\r\nfoobarssss\r\n";
    let (rem, r) = BulkString::try_parse(s).unwrap();
}