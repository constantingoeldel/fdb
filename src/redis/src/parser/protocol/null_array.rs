use nom::{Finish, IResult};
use nom::bytes::complete::tag;
use serde::{Deserialize, Serialize};

use crate::parser::protocol::TryParse;

#[derive(Eq, Debug, PartialEq, Hash, Deserialize, Serialize)]
pub struct NullArray;

pub fn null_array(i: &[u8]) -> IResult<&[u8], &[u8]> {
    tag("*-1\r\n")(i)
}


impl<'a> TryParse<'a> for NullArray {
    type Output = Self;

    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, _) = null_array(value).finish()?;
        Ok((i, NullArray))
    }
}


#[test]
fn test_null_array() {
    let s: &[u8] = b"*-1\r\n";
    let (rem, res) = NullArray::try_parse(s).unwrap();
    assert_eq!(rem, b"");
    assert_eq!(res, NullArray);
}