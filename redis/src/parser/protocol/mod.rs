use nom::{error, IResult};
use nom::branch::alt;

pub use integer::integer;

use crate::parser::protocol::array::Array;
use crate::parser::protocol::big_number::BigNumber;
use crate::parser::protocol::boolean::Boolean;
use crate::parser::protocol::bulk_error::BulkError;
use crate::parser::protocol::bulk_string::{bulk_string, BulkString};
use crate::parser::protocol::double::Double;
use crate::parser::protocol::integer::Integer;
use crate::parser::protocol::map::Map;
use crate::parser::protocol::null::Null;
use crate::parser::protocol::null_array::NullArray;
use crate::parser::protocol::null_bulk_string::NullBulkString;
use crate::parser::protocol::push::Push;
use crate::parser::protocol::set::Set;
use crate::parser::protocol::simple_error::SimpleError;
use crate::parser::protocol::simple_string::{simple_string, SimpleString};
use crate::parser::protocol::terminator::Terminator;
use crate::parser::protocol::verbatim_string::{verbatim_string, VerbatimString};

pub mod integer;
pub mod terminator;
pub mod bulk_string;
pub mod null_bulk_string;
pub mod simple_string;
pub mod simple_error;
pub mod array;
pub mod null_array;
pub mod null;
pub mod boolean;
pub mod double;
pub mod big_number;
pub mod bulk_error;
pub mod verbatim_string;
pub mod map;
pub mod set;
pub mod push;


pub trait TryParse<'a> {
    /// The type of the parsed value
    ///
    /// This is a hacky workaround to avoid requiring implementing protocol to be :sized
    ///
    /// Please always use `Self` as the type of the parsed value
    ///
    /// Example:
    ///
    /// ```
    ///
    /// impl<'a> TryParse<'a> for Integer {
    ///     type Output = Self;
    ///     fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
    ///         // implementation
    ///    }
    /// }
    /// ```
    ///
    type Output;

    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), error::Error<&'a [u8]>>;
}


#[derive(Debug, Eq, PartialEq, Hash)]
pub enum ParsedValues {
    Integer(Integer),
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    BulkString(BulkString),
    NullBulkString(NullBulkString),
    Array(Array),
    Terminator(Terminator),
    NullArray(NullArray),
    BigNumber(BigNumber),
    Boolean(Boolean),
    BulkError(BulkError),
    Double(Double),
    Map(Map),
    Null(Null),
    Push(Push),
    Set(Set),
    VerbatimString(VerbatimString),
}

fn parsed_value(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((integer, simple_string, simple_error, bulk_string, null_bulk_string, array, terminator))(i)
}

// TODO: Macros hierfür
impl<'a> TryParse<'a> for ParsedValues {
    type Output = Self;
    fn try_parse(value: &'a [u8]) -> Result<(&'a [u8], Self::Output), nom::error::Error<&'a [u8]>> {
        let null_bulk_string = NullBulkString::try_parse(value);

        if let Ok((i, null_bulk_string)) = null_bulk_string {
            return Ok((i, ParsedValues::NullBulkString(null_bulk_string)));
        }

        let terminator = Terminator::try_parse(value);

        if let Ok((i, terminator)) = terminator {
            return Ok((i, ParsedValues::Terminator(terminator)));
        }

        let null_array = NullArray::try_parse(value);
        if let Ok((i, null_array)) = null_array {
            return Ok((i, ParsedValues::NullArray(null_array)));
        }


        let integer = Integer::try_parse(value);

        if let Ok((i, integer)) = integer {
            return Ok((i, ParsedValues::Integer(integer)));
        }

        let simple_string = SimpleString::try_parse(value);

        if let Ok((i, simple_string)) = simple_string {
            return Ok((i, ParsedValues::SimpleString(simple_string)));
        }

        let simple_error = SimpleError::try_parse(value);

        if let Ok((i, simple_error)) = simple_error {
            return Ok((i, ParsedValues::SimpleError(simple_error)));
        }

        let bulk_string = BulkString::try_parse(value);

        if let Ok((i, bulk_string)) = bulk_string {
            return Ok((i, ParsedValues::BulkString(bulk_string)));
        }


        let array = Array::try_parse(value);

        if let Ok((i, array)) = array {
            return Ok((i, ParsedValues::Array(array)));
        }

        Err(Error::new(value, ErrorKind::NoneOf))
    }
}

pub fn string(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((simple_string, bulk_string, verbatim_string))(i)
}