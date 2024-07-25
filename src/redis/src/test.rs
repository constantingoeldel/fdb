use serde::Deserialize;

use crate::parser::from_slice;

fn test() {
    #[derive(Deserialize, Debug, Eq, PartialEq)]
    #[serde(untagged)]
    enum Options {
        Existence(NXorXX),
        Expiry(Expiry),
    }
    
    #[derive(Deserialize, Debug, Eq, PartialEq)]
    enum NXorXX {
        NX,
        XX,
    }

    #[derive(Deserialize, Debug, Eq, PartialEq)]
    enum Expiry {
        EX(String),
        KEEPTTL,
    }

    let s = b"$2\r\nEX\r\n$4\r\ntest\r\n";
    let res: Options = from_slice(s).unwrap();
}