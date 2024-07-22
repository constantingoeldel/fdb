use nom::bytes::complete::is_not;
use nom::character::complete::char;
use nom::combinator::opt;
use nom::Finish;
use nom::IResult;
use nom::sequence::delimited;
use num_bigint::BigInt;
use num_traits::Num;

#[derive(Eq, PartialEq, Debug, Hash)]
pub(super) struct BigNumber(num_bigint::BigInt);


pub fn big_number(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(char('('), is_not("\r\n"), terminator)(i)
}


impl<'a> TryParse<'a> for BigNumber {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let (i, num) = big_number(value).finish()?;
        let (j, sign) = opt(sign)(num).finish()?;
        let sign = sign.unwrap_or(1);

        let num = std::str::from_utf8(j).unwrap();
        let num = BigInt::from_str_radix(num, 10).unwrap();
        let num = num * sign;

        Ok((i, BigNumber(num)))
    }
}


#[test]
fn test_big_number() {
    let s: &[u8] = b"(-3492890328409238509324850943850943825024385\r\n";
    let (rem, int) = BigNumber::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(int.0.to_string(), "-3492890328409238509324850943850943825024385".to_string());
}

#[test]
fn test_big_number_negative() {
    let s: &[u8] = b"(-1000\r\n";
    let (rem, num) = BigNumber::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, BigNumber(BigInt::from(-1000)));
}

#[test]
fn test_big_number_positive() {
    let s: &[u8] = b"(+1000\r\n";
    let (rem, num) = BigNumber::try_parse(s).unwrap();
    assert_eq!(rem.len(), 0);
    assert_eq!(num, BigNumber(BigInt::from(1000)));
}
