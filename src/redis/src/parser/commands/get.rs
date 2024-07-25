use serde::Deserialize;

/// # GET
///
/// Syntax
///
/// GET key
///
/// Available since:
///     1.0.0
/// Time complexity:
///     O(1)
/// ACL categories:
///     @read, @string, @fast
///
/// Get the value of key. If the key does not exist the special value nil is returned. An error is returned if the value stored at key is not a string, because GET only handles string values.
#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct Get {
    cmd: GET,
    pub key: String,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct GET;


#[cfg(test)]
mod test {
    use crate::parser::from_slice;
    use crate::parser::Commands;
    
    use super::*;

    #[test]
    fn test_get() {
        let s = b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Get(Get { cmd: GET, key: "hello".to_string() }));
    }
}
