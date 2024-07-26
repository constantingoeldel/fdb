use serde::Deserialize;

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct Auth {
    cmd: AUTH,
    #[serde(default)]
    username: Option<String>,
    password: String,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct AUTH;

#[cfg(test)]
mod tests {
    use crate::parser::from_slice;

    use super::*;

    #[test]
    fn test_auth() {
        let s = b"*3\r\n$4\r\nAUTH\r\n$1\r\nc\r\n$1\r\ng\r\n";
        let res: Auth = from_slice(s).unwrap();
        assert_eq!(res, Auth {
            cmd: AUTH,
            username: Some("c".to_string()),
            password: "g".to_string(),
        });
    }

    #[test]
    fn test_auth_no_username() {
        // TODO: How to deal with the username being optional?
        // Apparently the standard for non ACL-Logins
        let s = b"*2\r\n$4\r\nAUTH\r\n$1\r\ng\r\n";
        let res: Auth = from_slice(s).unwrap();
        assert_eq!(res, Auth {
            cmd: AUTH,
            username: None,
            password: "g".to_string(),
        });
    }
}