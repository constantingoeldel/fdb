use std::ops::Deref;

use nom::{Finish, IResult};
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::character::complete::char;
use nom::sequence::delimited;

use crate::parser::{ParsedValues, TryParse};
use crate::parser::bulk_string::bulk_string;
use crate::parser::integer::integer;
use crate::parser::null_bulk_string::null_bulk_string;
use crate::parser::simple_error::simple_error;
use crate::parser::simple_string::simple_string;
use crate::parser::terminator::terminator;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Array(Vec<ParsedValues>);

impl Deref for Array {
    type Target = Vec<ParsedValues>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn array(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (i, len) = delimited(char('*'), is_not("\r\n"), terminator)(input)?;
    let len = std::str::from_utf8(len).unwrap().parse::<usize>().unwrap();

    let start_idx = input.len() - i.len();

    let mut j = i;
    for _ in 0..len {
        let (k, _) = alt((bulk_string, integer, simple_string, simple_error, null_bulk_string, array))(j)?;
        j = k;
    }

    let end_idx = input.len() - j.len();

    Ok((j, &input[start_idx..end_idx]))
}

impl<'a> TryParse<'a> for Array {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (rem, array) = array(value).finish()?;


        let mut res: Vec<ParsedValues> = Vec::new();
        let mut i = array;
        while !i.is_empty() {
            let (j, parsed_value) = ParsedValues::try_parse(i)?;
            res.push(parsed_value);
            i = j;
        }

        Ok((rem, Array(res)))
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::bulk_string::BulkString;
    use crate::parser::integer::Integer;
    use crate::parser::null_bulk_string::NullBulkString;
    use crate::parser::simple_error::SimpleError;
    use crate::parser::simple_string::SimpleString;

    use super::*;
    use super::ParsedValues;

    #[test]
    fn test_empty_array() {
        let s = b"*0\r\n";
        let (rem, res) = Array::try_parse(s.as_ref()).unwrap();
        assert_eq!(res.len(), 0);
        assert_eq!(rem, b"");
    }

    #[test]
    fn test_string_array() {
        let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let (rem, res) = Array::try_parse(s.as_ref()).unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(rem, b"");
        assert_eq!(res[0], ParsedValues::BulkString(BulkString::from("hello")));
        assert_eq!(res[1], ParsedValues::BulkString(BulkString::from("world")));
    }

    #[test]
    fn test_int_array() {
        let s = b"*3\r\n:1\r\n:2\r\n:3\r\n";
        let (rem, res) = Array::try_parse(s.as_ref()).unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(rem, b"");
        assert_eq!(res[0], ParsedValues::Integer(Integer::from(1)));
        assert_eq!(res[1], ParsedValues::Integer(Integer::from(2)));
        assert_eq!(res[2], ParsedValues::Integer(Integer::from(3)));
    }

    #[test]
    fn test_mixed_array() {
        let s = b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n";
        let (rem, res) = Array::try_parse(s.as_ref()).unwrap();
        assert_eq!(rem, b"");
        assert_eq!(res.len(), 5);
        assert_eq!(res[0], ParsedValues::Integer(Integer::from(1)));
        assert_eq!(res[1], ParsedValues::Integer(Integer::from(2)));
        assert_eq!(res[2], ParsedValues::Integer(Integer::from(3)));
        assert_eq!(res[3], ParsedValues::Integer(Integer::from(4)));
        assert_eq!(res[4], ParsedValues::BulkString(BulkString::from("hello")));
    }

    #[test]
    fn test_nested_array() {
        let s = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let (rem, res) = Array::try_parse(s.as_ref()).unwrap();
        assert_eq!(rem, b"");
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], ParsedValues::Array(Array(vec![
            ParsedValues::Integer(Integer::from(1)),
            ParsedValues::Integer(Integer::from(2)),
            ParsedValues::Integer(Integer::from(3)),
        ])));
        assert_eq!(res[1], ParsedValues::Array(Array(vec![
            ParsedValues::SimpleString(SimpleString::from("Hello")),
            ParsedValues::SimpleError(SimpleError::from("World")),
        ])));
    }

    #[test]
    fn test_null_element_in_array() {
        let s = b"*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n";
        let (rem, res) = Array::try_parse(s.as_ref()).unwrap();
        
        assert_eq!(rem, b"");
        assert_eq!(res.len(), 3);
        assert_eq!(res[0], ParsedValues::BulkString(BulkString::from("hello")));
        assert_eq!(res[1], ParsedValues::NullBulkString(NullBulkString));
        assert_eq!(res[2], ParsedValues::BulkString(BulkString::from("world")));
    }
}