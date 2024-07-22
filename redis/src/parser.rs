use std::cmp::max;
use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, take};
use nom::character::complete::char;
use nom::combinator::{map_opt, opt};
use nom::Err::Error;
use nom::sequence::delimited;

struct Terminator;

impl Parse for Terminator {
    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        tag("\r\n")(i)
    }

    fn parse(i: &[u8]) -> IResult<&[u8], Box<Self>> {
        let (i, _) = Self::find(i)?;
        Ok((i, Box::new(Terminator)))
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
    fn parse(i: &[u8]) -> IResult<&[u8], Box<Self>> {
        let (i, data) = Self::find(i)?;
        let data = std::str::from_utf8(data).unwrap().to_string();
        Ok((i, Box::new(BulkString(data))))
    }
}

struct NullBulkString;

impl Parse for NullBulkString {
    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        tag("$-1\r\n")(i)
    }

    fn parse(i: &[u8]) -> IResult<&[u8], Box<Self>> {
        let (i, _) = Self::find(i)?;
        Ok((i, Box::new(NullBulkString)))
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
            let (k, _) = alt((BulkString::find, Integer::find, SimpleString::find, SimpleError::find, NullBulkString::find, Array::find)(j)?;
            j = k;
        }

        let end_idx = input.len() - j.len();

        Ok((j, &input[start_idx..end_idx]))
    }
    fn parse(i: &[u8]) -> IResult<&[u8], Vec<&[u8]>> {
        let (i, array) = Self::find(i)?;

        let mut res: Vec<ParsedValues> = Vec::new();
        let mut j = array;
        while !j.is_empty() {

            map_opt(Array::find,)



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
    fn parse(i: &[u8]) -> IResult<&[u8], Box<Self>>;

    fn find(i: &[u8]) -> IResult<&[u8], &[u8]>;
}

impl Parse for SimpleString  {
    fn parse(i: &[u8]) -> IResult<&[u8], Box<Self>> {
        let (i, str) = Self::find(i)?;
        Ok((i, Box::new(SimpleString(std::str::from_utf8(str).unwrap().to_string()))))
    }

    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        delimited(char('+'), is_not("\r\n"), terminator)(i)
    }
}

struct SimpleError(String);

impl Parse for SimpleError {
    fn parse(i: &[u8]) -> IResult<&[u8], Box<Self>> {
        let (i, str) = Self::find(i)?;
        Ok((i, Box::new(SimpleError(std::str::from_utf8(str).unwrap().to_string()))))
    }

    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        delimited(char('-'), is_not("\r\n"), terminator)(i)
    }

}

struct Integer(i64);

impl Parse for Integer {
    fn find(i: &[u8]) -> IResult<&[u8], &[u8]> {
        delimited(char(':'), is_not("\r\n"), terminator)(i)
    }

    fn parse(i: &[u8]) -> IResult<&[u8], Box<i64>> {
        let (i, num) = Self::find(i)?;

        fn sign (i: &[u8]) -> IResult<&[u8], char> {
            alt((char('-'), char('+')))(i)
        }

        let (j, sign) = opt(sign)(num)?;

        let sign = match sign {
            Some('-') => -1,
            Some('+') => 1,
            None => 1,
            _ => unreachable!()
        };

        let num = std::str::from_utf8(j).unwrap().parse::<i64>().unwrap();
        Ok((i, Box::new(sign * num)))

    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_simple_string() {
        let s = b"+OK\r\n";
        let (i, str) = simple_string(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(str, b"OK");
    }

    #[test]
    fn test_simple_error() {
        let s = b"-Error message\r\n";
        let (i, str) = simple_error(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(str, b"Error message");
    }

    #[test]
    fn test_integer() {
        let s = b":1000\r\n";
        let (i, num) = integer(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(num, 1000);
    }

    #[test]
    fn test_integer_negative() {
        let s = b":-1000\r\n";
        let (i, num) = integer(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(num, -1000);
    }

    #[test]
    fn test_integer_positive() {
        let s = b":+1000\r\n";
        let (i, num) = integer(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(num, 1000);
    }

    #[test]
    fn test_bulk_string() {
        let s = b"$6\r\nfoobar\r\n";
        let (i, str) = bulk_string(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(str, b"foobar");
    }

    #[test]
    fn test_empty_bulk_string() {
        let s = b"$0\r\n\r\n";
        let (i, str) = bulk_string(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(str, b"");
    }

    #[test]
    fn test_bulk_string_too_large() {
        let s = b"$536870913\r\n";
        let r = bulk_string(s);
        assert!(r.is_err());
    }

    #[test]
    #[should_panic]
    fn test_bulk_string_with_wrong_size() {
        let s = b"$6\r\nfoobarssss\r\n";
        let r = bulk_string(s).unwrap();
    }

    #[test]
    fn test_null_bulk_string() {
        let s = b"$-1\r\n";
        let (i, res) = null_bulk_string(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(res, ());
    }

    #[test]
    fn test_empty_array() {
        let s = b"*0\r\n";
        let (i, res) = array(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn test_string_array() {
        let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let (i, res) = array(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], b"hello");
        assert_eq!(res[1], b"world");
    }

    #[test]
    fn test_int_array() {
        let s = b"*3\r\n:1\r\n:2\r\n:3\r\n";
        let (i, res) = array(s).unwrap();
        assert_eq!(i, b"");
        assert_eq!(res.len(), 3);
        assert_eq!(res[0], b"1");
        assert_eq!(res[1], b"2");
        assert_eq!(res[2], b"3");
    }

    #[test]
    fn test_mixed_array() {
        let s  = b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n";
        let (i, res) = array(s).unwrap();

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