use nom::{Finish, IResult};
use nom::bytes::complete::tag;
use serde::{Deserialize, Serialize};

use crate::parser::protocol::TryParse;

#[derive(Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct Null;

pub fn null(i: &[u8]) -> IResult<&[u8], &[u8]> {
    tag("_\r\n")(i)
}

impl<'a> TryParse<'a> for Null {
    type Output = Self;

    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, _) = null(value).finish()?;
        Ok((i, Null))
    }
}


#[test]
fn test_null() {
    let s: &[u8] = b"_\r\n";
    let (rem, res) = Null::try_parse(s).unwrap();
    assert_eq!(rem, b"");
    assert_eq!(res, Null);
}

