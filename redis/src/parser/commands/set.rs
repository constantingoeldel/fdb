use serde::Deserialize;

/// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
/// EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
///
/// NX -- Only set the key if it does not already exist.
///
/// XX -- Only set the key if it already exists.
///
/// GET -- Return the old string stored at key, or nil if key did not exist.
/// An error is returned and SET aborted if the value stored at key is not a string.
///
/// EX seconds -- Set the specified expire time, in seconds (a positive integer).
///
/// PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
///
/// EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
///
/// KEEPTTL -- Retain the time to live associated with the key.
#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct Set {
    pub key: String,
    pub value: String,
    pub existence_options: Option<NXorXX>,
    pub get: Option<()>,
    pub expire: Option<Expiry>
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum NXorXX {
    NX,
    XX,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum Expiry {
    EX(u64),
    PX(u64),
    EXAT(u64),
    KEEPTTL,
}
