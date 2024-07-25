use std::convert::TryFrom;

use nom::{Finish, IResult};
use nom::bytes::complete::tag;
use serde::{Deserialize, Serialize};

use crate::parser::protocol::TryParse;

#[derive(Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub(super) struct Terminator;

pub fn terminator(i: &[u8]) -> IResult<&[u8], &[u8]> {
    tag("\r\n")(i)
}

impl<'a> TryParse<'a> for Terminator {
    type Output = Self;

    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, _) = terminator(value).finish()?;
        Ok((i, Self))
    }
}

#[test]
fn test_terminator() {
    let s: &[u8] = b"\r\n";
    let (rem, t) = Terminator::try_parse(s).unwrap();
    assert_eq!(rem, b"");
    assert_eq!(t, Terminator);
}