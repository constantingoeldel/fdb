use std::ops::Deref;

use nom::{error, Finish, IResult};
use nom::branch::alt;
use nom::error::{Error, ErrorKind};

use simple_error::SimpleError;

use crate::parser::array::{Array, array};
use crate::parser::bulk_string::{bulk_string, BulkString};
use crate::parser::integer::{Integer, integer};
use crate::parser::null_array::NullArray;
use crate::parser::null_bulk_string::{null_bulk_string, NullBulkString};
use crate::parser::simple_error::simple_error;
use crate::parser::simple_string::{simple_string, SimpleString};
use crate::parser::terminator::{Terminator, terminator};

mod integer;
mod terminator;
mod bulk_string;
mod null_bulk_string;
mod simple_string;
mod simple_error;
mod array;
mod null_array;
mod null;
mod boolean;
mod double;
mod big_number;
mod bulk_error;
mod verbatim_string;
mod maps;
mod set;
mod push;
mod handshake;

pub use handshake::ClientHandshake;

pub trait TryParse<'a> {
    /// The type of the parsed value
    ///
    /// This is a hacky workaround to avoid requiring implementing types to be :sized
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