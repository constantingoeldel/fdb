use serde::Deserialize;

use macro_derive::*;

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub struct Command {
    cmd: COMMAND,
    #[serde(default)]
    docs: Option<DOCS>,
    #[serde(default)]
    options: Options,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct COMMAND;

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct DOCS;

#[derive(Options, Debug, Eq, PartialEq, Default)]
struct Options {
    command_name: Option<CommandName>,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct CommandName {
    #[serde(rename = "command-name")]
    command_name: String,
}

#[cfg(test)]
mod tests {
    use crate::parser::{Commands, from_slice};

    use super::*;

    #[test]
    fn test_command() {
        let s = b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Command(Command {
            cmd: COMMAND,
            docs: Some(DOCS),
            options: Options {
                command_name: None,
            }
        }));
    }
    
    #[test]
    fn test_only_command() {
        let s = b"*1\r\n$7\r\nCOMMAND\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Command(Command {
            cmd: COMMAND,
            docs: None,
            options: Options {
                command_name: None,
            }
        }));
    }
    
    #[test]
    fn test_command_name() {
        let s = b"*3\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n$1\r\na\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Command(Command {
            cmd: COMMAND,
            docs: Some(DOCS),
            options: Options {
                command_name: Some(CommandName {
                    command_name: "a".to_string(),
                }),
            }
        }));
        
    }
    
    #[test]
    fn test_multiple_command_names() {
        let s = b"*3\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n$1\r\na\r\n$1\r\nb\r\n";
        // TODO: Allow multiple command names
        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Command(Command {
            cmd: COMMAND,
            docs: None,
            options: Options {
                command_name: Some(CommandName {
                    command_name: "b".to_string(),
                }),
            }
        }));
    }
}