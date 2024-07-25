use serde::Deserialize;

pub struct GetDel {
    key: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Test {
    A, B
}