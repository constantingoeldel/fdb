use std::collections::HashMap;
use std::ops::Deref;

use nom::{Finish, IResult};
use nom::bytes::complete::is_not;
use nom::character::complete::char;
use nom::sequence::delimited;

use crate::parser::{parsed_value, ParsedValues, TryParse};
use crate::parser::integer::{Integer, parse_digits};
use crate::parser::simple_string::SimpleString;
use crate::parser::terminator::terminator;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Map(HashMap<ParsedValues, ParsedValues>);

impl Deref for Map {
    type Target = HashMap<ParsedValues, ParsedValues>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn map(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (i, num_entries) = delimited(char('%'), is_not("\r\n"), terminator)(input)?;
    let (_, num_entries) = parse_digits(num_entries)?;

    let start_idx = input.len() - i.len();

    let mut j = i;
    for _ in 0..num_entries * 2 {
        let (k, _) = parsed_value(j)?;
        j = k;
    }

    let end_idx = input.len() - j.len();

    Ok((j, &input[start_idx..end_idx]))
}

impl<'a> TryParse<'a> for Map {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (rem, map) = map(value).finish()?;


        let mut res = HashMap::new();
        let mut i = map;
        while !i.is_empty() {
            let (j, key) = ParsedValues::try_parse(i)?;
            let (k, value) = ParsedValues::try_parse(j)?;
            res.insert(key, value);
            i = k;
        }

        Ok((rem, Map(res)))
    }
}

#[test]
fn test_map() {
    let s = b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";
    let (rem, map) = Map::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(map.0.len(), 2);
    assert_eq!(map.0.get(&ParsedValues::SimpleString(SimpleString::from("first"))).unwrap(), &ParsedValues::Integer(Integer::from(1)));
    assert_eq!(map.0.get(&ParsedValues::SimpleString(SimpleString::from("second"))).unwrap(), &ParsedValues::Integer(Integer::from(2)));
}
