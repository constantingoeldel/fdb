use nom::{Finish, IResult};
use nom::bytes::complete::{is_not, take};
use nom::character::complete::char;
use nom::Err::Error;
use nom::sequence::{delimited, terminated};

use crate::parser::integer::parse_digits;
use crate::parser::terminator::terminator;
use crate::parser::TryParse;

// TODO: Variable Size limit? See Bulk String
#[derive(Debug, Eq, PartialEq)]
pub(super) struct BulkError(String);

impl From<&str> for BulkError {
    fn from(s: &str) -> Self {
        BulkError(s.to_string())
    }
}

pub fn bulk_error(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let (i, len) = delimited(char('!'), is_not("\r\n"), terminator)(i)?;
    let (_, len) = parse_digits(len)?;
    if len > 512 * 1024 * 1024 {
        return Err(Error(nom::error::Error::new(i, nom::error::ErrorKind::TooLarge)));
    }
    let (i, data) = terminated(take(len as usize), terminator)(i)?;
    Ok((i, data))
}

impl<'a> TryParse<'a> for BulkError {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, data) = bulk_error(value).finish()?;
        let data = std::str::from_utf8(data).unwrap().to_string();
        Ok((i, BulkError(data)))
    }
}

#[test]
fn test_bulk_error() {
    let s: &[u8] = b"!21\r\nSYNTAX invalid syntax\r\n";
    let (rem, str) = BulkError::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(str, BulkError(String::from("SYNTAX invalid syntax")));
}
