use nom::{Finish, IResult};
use nom::bytes::complete::is_not;
use nom::character::complete::char;
use nom::sequence::delimited;

use crate::parser::terminator::terminator;
use crate::parser::TryParse;

#[derive(Debug, Eq, PartialEq)]
pub struct Boolean(bool);

pub fn boolean(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(char('#'), is_not("\r\n"), terminator)(i)
}


impl<'a> TryParse<'a> for Boolean {
    type Output = Self;

    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, b) = boolean(value).finish()?;

        let b = match b {
            b"f" => false,
            b"t" => true,
            _ => unreachable!("booleans can only be f or t")
        };

        Ok((i, Boolean(b)))
    }
}


#[test]
fn test_true() {
    let s: &[u8] = b"#t\r\n";
    let (rem, res) = Boolean::try_parse(s).unwrap();
    assert_eq!(rem, b"");
    assert_eq!(res, Boolean(true));
    
}

#[test]
fn test_false() {
    let s: &[u8] = b"#f\r\n";
    let (rem, res) = Boolean::try_parse(s).unwrap();
    assert_eq!(rem, b"");
    assert_eq!(res, Boolean(false));
}