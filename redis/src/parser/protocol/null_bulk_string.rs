use nom::{Finish, IResult};
use nom::bytes::complete::tag;
use crate::parser::TryParse;

#[derive(Eq, Debug, PartialEq, Hash)]
pub(super) struct NullBulkString;

pub fn null_bulk_string(i: &[u8]) -> IResult<&[u8], &[u8]> {
    tag("$-1\r\n")(i)
}


impl<'a> TryParse<'a> for NullBulkString {
    type Output = Self;

    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, _) = null_bulk_string(value).finish()?;
        Ok((i, NullBulkString))
    }
}


#[test]
fn test_null_bulk_string() {
    let s: &[u8] = b"$-1\r\n";
    let (rem, res) = NullBulkString::try_parse(s).unwrap();
    assert_eq!(rem, b"");
    assert_eq!(res, NullBulkString);
}