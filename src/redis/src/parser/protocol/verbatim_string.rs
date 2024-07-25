use nom::{Finish, IResult};
use nom::bytes::complete::{is_not, take};
use nom::character::complete::char;
use nom::Err::Error;
use nom::sequence::{delimited, terminated};
use serde::{Deserialize, Serialize};

use crate::parser::protocol::integer::parse_digits;
use crate::parser::protocol::terminator::terminator;
use crate::parser::protocol::TryParse;

#[derive(Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub(super) struct VerbatimString(String);

impl From<&str> for VerbatimString {
    fn from(s: &str) -> Self {
        VerbatimString(s.to_string())
    }
}

impl Into<String> for VerbatimString {
    fn into(self) -> String {
        self.0
    }
}

pub fn verbatim_string(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let (i, len) = delimited(char('='), is_not("\r\n"), terminator)(i)?;
    let (_, len) = parse_digits(len)?;
    if len > 512 * 1024 * 1024 {
        return Err(Error(nom::error::Error::new(i, nom::error::ErrorKind::TooLarge)));
    }
    // TODO: What do we do with the encoding?
    let (i, _encoding) = terminated(take(3usize), char(':'))(i)?;
    let (i, data) = terminated(take(len as usize - 4usize), terminator)(i)?;
    Ok((i, data))
}

impl<'a> TryParse<'a> for VerbatimString {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, data) = verbatim_string(value).finish()?;
        let data = std::str::from_utf8(data).unwrap().to_string();
        Ok((i, VerbatimString(data)))
    }
}

#[test]
fn test_verbatim_string() {
    let s: &[u8] = b"=15\r\ntxt:Some string\r\n";
    let (rem, str) = VerbatimString::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(str, VerbatimString(String::from("Some string")));
}