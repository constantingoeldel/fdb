use std::cmp::max;
use nom::{Finish, IResult};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, take, digit1};
use nom::character::complete::{char, digit1};
use nom::combinator::{map_opt, map_res, opt};
use nom::Err::Error;
use nom::sequence::{delimited, tuple};

struct Terminator;

impl Parse for Terminator {
    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        tag("\r\n")(i)
    }

    fn parse(i: &[u8]) -> IResult<&[u8], Self> {
        let (i, _) = Self::find(i)?;
        Ok((i, Terminator))
    }
}

struct BulkString(String);

impl Parse for BulkString {
    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        let (i, len) = delimited(char('$'), is_not("\r\n"), Terminator::find)(i)?;
        let len = std::str::from_utf8(len).unwrap().parse::<usize>().unwrap();
        if len > 512 * 1024 * 1024 {
            return Err(Error(nom::error::Error::new(i, nom::error::ErrorKind::TooLarge)));
        }
        let (i, data) = take(len)(i)?;
        let (j, _) = Terminator::find(i)?;
        Ok((j, data))
    }

    /// A bulk string represents a single binary string. The string can be of any size, but by default,
    /// Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).
    ///
    /// TODO: Implement the variable limit
    fn parse(i: &[u8]) -> IResult<&[u8], Self> {
        let (i, data) = Self::find(i)?;
        let data = std::str::from_utf8(data).unwrap().to_string();
        Ok((i, BulkString(data)))
    }
}

struct NullBulkString;

impl Parse for NullBulkString {
    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        tag("$-1\r\n")(i)
    }

    fn parse(i: &[u8]) -> IResult<&[u8], Self> {
        let (i, _) = Self::find(i)?;
        Ok((i, NullBulkString))
    }
}

struct Array(Vec<ParsedValues>);

impl Parse for Array {
    fn find(input: &[u8]) -> IResult<&[u8], &[u8]> {
        let (i, len) = delimited(char('*'), is_not("\r\n"), Terminator::find)(input)?;
        let len = std::str::from_utf8(len).unwrap().parse::<usize>().unwrap();

        let start_idx = input.len() - i.len();

        let mut j = i;
        for _ in 0..len {
            let (k, _) = alt((BulkString::find, Integer::find, SimpleString::find, SimpleError::find, NullBulkString::find, Array::find))(j)?;
            j = k;
        }

        let end_idx = input.len() - j.len();

        Ok((j, &input[start_idx..end_idx]))
    }
    fn parse(i: &[u8]) -> IResult<&[u8], Self> {
        let (i, array) = Self::find(i)?;

        let mut res: Vec<ParsedValues> = Vec::new();
        let mut j = array;
        while !j.is_empty() {

            map_res(array, | )



            let (k, item) = alt((bulk_string, integer_finder, simple_string, simple_error, null_bulk_string_finder))(j)?;
            res.push(item);
            j = k;

        }

        Ok((i, res))

    }


}




enum ParsedValues {
    SimpleString(SimpleString),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    NullBulkString,
    Array(Vec<ParsedValues>)
}
struct SimpleString(String);

trait Parse {
    fn parse(i: &[u8]) -> IResult<&[u8], dyn Self>;

    fn find(i: &[u8]) -> IResult<&[u8], &[u8]>;
}

impl Parse for SimpleString  {
    fn parse(i: &[u8]) -> IResult<&[u8], Self> {
        let (i, str) = Self::find(i)?;
        Ok((i, SimpleString(std::str::from_utf8(str).unwrap().to_string())))
    }

    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        delimited(char('+'), is_not("\r\n"), Terminator::find)(i)
    }
}

struct SimpleError(String);

impl Parse for SimpleError {
    fn parse(i: &[u8]) -> IResult<&[u8], Self> {
        let (i, str) = Self::find(i)?;
        Ok((i, SimpleError(std::str::from_utf8(str).unwrap().to_string())))
    }

    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        delimited(char('-'), is_not("\r\n"), Terminator::find)(i)
    }

}

struct Integer(i64);

fn integer(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(char(':'), is_not("\r\n"), Terminator::find)(i)
}

impl<'a > TryFrom<&'a [u8]> for Integer {
    type Error = nom::error::Error<&'a [u8]>;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let (i, num) = integer(value)?;

        fn sign (i: &[u8]) -> IResult<&[u8], i64> {
            map_res(alt((char('-'), char('+'))), |s| {
                match s {
                    '-' => Ok(-1),
                    '+' => Ok(1),
                    _ => unreachable!()
                }})(i)
        }

        fn parse_digits(i: &[u8]) -> IResult<&[u8], i64> {
            map_res(digit1, str::parse)(i.as_char())
        }

        let (j, (sign, digits))  = tuple((opt(sign),parse_digits))(num)?;

        assert_eq!(j.len(), 0);

        // If there is no explicit sign, assume positive int
        let sign = sign.unwrap_or(1);

        Ok(Integer(sign* digits))
    }
}



impl Parse for Integer {
    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
    }

    fn parse(i: &[u8]) -> IResult<&[u8], i64> {


    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_simple_string() {
        let s = b"+OK\r\n";
        let (i, str) = SimpleString::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(str, b"OK");
    }

    #[test]
    fn test_simple_error() {
        let s = b"-Error message\r\n";
        let (i, str) = SimpleError::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(str, b"Error message");
    }

    #[test]
    fn test_integer() {
        let s = b":1000\r\n";
        let num = Integer::try_from(s).unwrap();
        assert_eq!(num, 1000);
    }

    #[test]
    fn test_integer_negative() {
        let s = b":-1000\r\n";
        let (i, num) = Integer::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(num, -1000);
    }

    #[test]
    fn test_integer_positive() {
        let s = b":+1000\r\n";
        let (i, num) = Integer::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(num, 1000);
    }

    #[test]
    fn test_bulk_string() {
        let s = b"$6\r\nfoobar\r\n";
        let (i, str) = BulkString::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(str, b"foobar");
    }

    #[test]
    fn test_empty_bulk_string() {
        let s = b"$0\r\n\r\n";
        let (i, str) = BulkString::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(str, b"");
    }

    #[test]
    fn test_bulk_string_too_large() {
        let s = b"$536870913\r\n";
        let r = BulkString::parse(s);
        assert!(r.is_err());
    }

    #[test]
    #[should_panic]
    fn test_bulk_string_with_wrong_size() {
        let s = b"$6\r\nfoobarssss\r\n";
        let r = BulkString::parse(s).unwrap();
    }

    #[test]
    fn test_null_bulk_string() {
        let s = b"$-1\r\n";
        let (i, res) = NullBulkString::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(res, ());
    }

    #[test]
    fn test_empty_array() {
        let s = b"*0\r\n";
        let (i, res) = Array::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn test_string_array() {
        let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let (i, res) = Array::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], b"hello");
        assert_eq!(res[1], b"world");
    }

    #[test]
    fn test_int_array() {
        let s = b"*3\r\n:1\r\n:2\r\n:3\r\n";
        let (i, res) = Array::parse(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(res.len(), 3);
        assert_eq!(res[0], b"1");
        assert_eq!(res[1], b"2");
        assert_eq!(res[2], b"3");
    }

    #[test]
    fn test_mixed_array() {
        let s  = b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n";
        let (i, res) = Array::parse(s).unwrap();

        assert_eq!(i, b"");
        assert_eq!(res.len(), 5);
        assert_eq!(res[0], b"1");
        assert_eq!(res[1], b"2");
        assert_eq!(res[2], b"3");
        assert_eq!(res[3], b"4");
        assert_eq!(res[4], b"hello");

    }

    #[test]
    fn test_nested_array() {
        let s = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let (i, res) = array(s).unwrap();

        assert_eq!(i, b"");
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].len(), 3);
        assert_eq!(res[1].len(), 2);

    }


}