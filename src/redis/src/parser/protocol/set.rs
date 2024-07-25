use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Deref;

use nom::{Finish, IResult};
use nom::bytes::complete::is_not;
use nom::character::complete::char;
use nom::sequence::delimited;
use serde::Deserialize;

use crate::parser::protocol::{parsed_value, ParsedValues, TryParse};
use crate::parser::protocol::integer::parse_digits;
use crate::parser::protocol::simple_string::SimpleString;
use crate::parser::protocol::terminator::terminator;

#[derive(Debug, Eq, PartialEq, Deserialize)]
pub struct Set(pub HashSet<ParsedValues>);

impl Hash for Set {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        todo!()
    }
}

impl Deref for Set {
    type Target = HashSet<ParsedValues>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn set(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (i, num_entries) = delimited(char('~'), is_not("\r\n"), terminator)(input)?;
    let (_, num_entries) = parse_digits(num_entries)?;

    let start_idx = input.len() - i.len();

    let mut j = i;
    for _ in 0..num_entries {
        let (k, _) = parsed_value(j)?;
        j = k;
    }

    let end_idx = input.len() - j.len();

    Ok((j, &input[start_idx..end_idx]))
}

impl<'a> TryParse<'a> for Set {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (rem, set) = set(value).finish()?;


        let mut res = HashSet::new();
        let mut i = set;
        while !i.is_empty() {
            let (j, element) = ParsedValues::try_parse(i)?;
            res.insert(element);
            i = j;
        }
        Ok((rem, Set(res)))
    }
}

#[test]
fn test_set() {
    let s = b"~2\r\n+first\r\n+second\r\n";
    let (rem, map) = Set::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(map.0.len(), 2);
    assert!(map.0.contains(&ParsedValues::SimpleString(SimpleString::from("first"))));
    assert!(map.0.contains(&ParsedValues::SimpleString(SimpleString::from("second"))));
}
