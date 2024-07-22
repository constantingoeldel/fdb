use nom::{AsChar, Finish};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, take_till};
use nom::character::complete::char;
use nom::combinator::opt;
use nom::IResult;
use nom::sequence::{delimited, tuple};

use crate::parser::integer::{parse_digits, sign};
use crate::parser::terminator::terminator;
use crate::parser::TryParse;

#[derive(PartialEq, Debug)]
pub(super) struct Double(f64);

impl From<f64> for Double {
    fn from(i: f64) -> Self {
        Double(i)
    }
}

pub fn double(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(char(','), is_not("\r\n"), terminator)(i)
}

impl<'a> TryParse<'a> for Double {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, num) = double(value).finish()?;


        fn integral(i: &[u8]) -> IResult<&[u8], i64> {
            let (i, digits) = take_till(|c: u8| !c.as_char().is_ascii_digit())(i)?;
            let (_, num) = parse_digits(digits)?;

            Ok((i, num))
        }

        fn fractional(i: &[u8]) -> IResult<&[u8], i64> {
            let (i, _) = tag(b".")(i)?;
            let (i, digits) = take_till(|c: u8| !c.as_char().is_ascii_digit())(i)?;
            let (_, num) = parse_digits(digits)?;

            Ok((i, num))
        }

        fn exponent(i: &[u8]) -> IResult<&[u8], i64> {
            let (i, _) = alt((tag(b"e"), tag(b"E")))(i)?;
            let (i, sign) = opt(sign)(i)?;
            let (i, digits) = parse_digits(i)?;
            Ok((i, sign.unwrap_or(1) * digits))
        }

        let (j, sign) = opt(sign)(num).finish()?;
        let sign = sign.unwrap_or(1) as f64;


        let (j, inf) = opt(tag(b"inf"))(j).finish()?;
        if inf.is_some() {
            return Ok((j, Double(sign * f64::INFINITY)));
        }

        let (j, nan) = opt(tag(b"nan"))(j).finish()?;
        if nan.is_some() {
            return Ok((j, Double(f64::NAN)));
        }
        let (j, (integral, fractional, exponent)) = tuple((integral, opt(fractional), opt(exponent)))(num).finish()?;
        assert_eq!(j.len(), 0);

        let fractional = fractional.unwrap_or(0);
        let exponent = exponent.unwrap_or(0);

        let num = format!("{integral}.{fractional}e{exponent}");
        let num = num.parse::<f64>().unwrap();


        Ok((i, Double(sign * num)))
    }
}


#[test]
fn test_double() {
    let s: &[u8] = b",1.23\r\n";
    let (rem, int) = Double::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(int, Double(1.23));
}

#[test]
fn test_double_without_fractional_part() {
    let s: &[u8] = b",10\r\n";
    let (rem, num) = Double::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, Double(10.0));
}

#[test]
fn test_double_positive_inf() {
    let s: &[u8] = b",inf\r\n";
    let (rem, num) = Double::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, Double(f64::INFINITY));
}

#[test]
fn test_double_negative_inf() {
    let s: &[u8] = b",-inf\r\n";
    let (rem, num) = Double::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, Double(f64::NEG_INFINITY));
}

#[test]
fn test_double_nan() {
    let s: &[u8] = b",nan\r\n";
    let (rem, num) = Double::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert!(num.0.is_nan());
}
