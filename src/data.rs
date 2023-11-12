// use std::mem::size_of;
//
// use crate::types::SizedOnDisk;
//
// enum Key {
//     NEGATIVE_INFINITY,
//     USER_KEY(Vec<u8>),
//     POSITIVE_INFINITY,
// }
//
// impl Key {
//     fn len(&self) -> usize {
//         match self {
//             Self::USER_KEY(v) => v.len(),
//             _ => 0,
//         }
//     }
// }
//
// #[allow(non_camel_case_types)]
// pub enum MessageType {
//     INVALID = 0,
//     INSERT,
//     UPDATE,
//     DELETE,
//     MAX_VALID_USER_TYPE,
//     PIVOT_DATA = 1000,
// }
