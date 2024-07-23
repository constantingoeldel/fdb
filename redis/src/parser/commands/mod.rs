use serde::Deserialize;

use crate::parser::commands::get::Get;
use crate::parser::commands::set::Set;

mod get;
mod hello;
mod set;
mod getdel;
mod xadd;


#[derive(Deserialize, Debug, Eq, PartialEq)]
#[serde(tag = "untagged")]
pub enum Commands {
    #[serde(alias = "GET")]
    #[serde(alias = "get")]
    Get(Get),
    #[serde(alias = "SET")]
    #[serde(alias = "set")]
    Set(Set),
    // GetDel(GetDel),
    // Hello(Hello),
}

#[cfg(test)]
mod test {
    use crate::parser::from_slice;

    use super::*;

    #[test]
    fn test_get() {
        let s = b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Get(Get { key: "hello".to_string() }));
    }
    
    #[test]
    fn test_basic_set() {
        let s = b"*3\r\n$3\r\nSet\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        
        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { key: "hello".to_string(), value: "world".to_string(), existence_options: None, get: None, expire: None }));

    }
}




