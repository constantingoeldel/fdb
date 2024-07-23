// struct XADD {
//     key: String,
//     nomkstream: Option<()>,
//     #[serde(anonymous)]
//     threshold: Option<Threshold>,
//     #[serde(anonymous)]
//     matching: Matching,
//     #[serde(minimum = 1)]
//     entries: Vec<Entry>
//
// }
//
// struct Entry { 
//     field: String,
//     value: String
// }
//
// enum Matching {
//     #[serde(match = "*")]
//     ANY,
//     #[serde(match = "id")]
//     ID
// }
//
// struct Threshold {
//     #[serde(anonymous)]
//     first: MaxlenOrMinId,
//     #[serde(anonymous)]
//     comparison: Option<Comparison>,
//     threshold: String,
//     limit: Option<i64>
// }
//
// enum MaxlenOrMinId {
//     MAXLEN,
//     MINID
// }
//
// enum Comparison {
//     #[serde(match = "=")]
//     Equal,
//     #[serde(match = "~")]
//     Approx,
// }