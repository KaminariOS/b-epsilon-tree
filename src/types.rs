use core::fmt::Debug;
use core::mem::size_of;
use derive_more::{Deref, DerefMut};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use ser_derive::SizedOnDisk;
use std::{cmp::Ordering, collections::BTreeMap};

pub type OndiskKeyLength = u16;
pub type OndiskValueLength = u16;
pub type OndiskFlags = u8;
pub type OndiskMessageLength = u16;
pub type OndiskMapLength = u16;
pub type PageOffset = usize;
pub type Comparator = fn(&[u8], &[u8]) -> Ordering;

pub trait SizedOnDisk: Clone {
    fn size(&self) -> PageOffset;
    fn is_packed() -> Option<usize> {
        None
    }
}

impl SizedOnDisk for String {
    fn size(&self) -> PageOffset {
        self.len() + size_of::<OndiskKeyLength>()
    }
}

impl From<String> for VectorOnDisk<u8, OndiskKeyLength> {
    fn from(value: String) -> Self {
        VectorOnDisk::new(value.into_bytes(), 1 as _)
    }
}

impl From<VectorOnDisk<u8, OndiskKeyLength>> for String {
    fn from(value: VectorOnDisk<u8, OndiskKeyLength>) -> Self {
        let VectorOnDisk { elements, .. } = value;
        String::from_utf8(elements).unwrap()
    }
}

impl<T: Serializable> From<Vec<T>> for VectorOnDisk<T, OndiskKeyLength> {
    fn from(value: Vec<T>) -> Self {
        Self::new(value, 1 as _)
    }
}

#[macro_export]
macro_rules! SizedOnDiskImplForPrimitive {
    ($primitive_ty:ty) => {
        impl SizedOnDisk for $primitive_ty {
            fn size(&self) -> PageOffset {
                size_of::<Self>() as PageOffset
            }

            fn is_packed() -> Option<usize> {
                /*     #[cfg(target_endian = "little")]
                return Some(size_of::<Self>());

                #[cfg(not(target_endian = "little"))]*/
                return None;
            }
        }
    };
}

// #[macro_export]
// macro_rules! SizedOnDiskImplForComposite {
//         (
//             $(#[$($attrss:tt)*])*
//             $vis:vis struct $name:ident {
//
//                 $(
//
//                     $(#[$($attrss_f:tt)*])*
//                     $field_vis:vis $field_name:ident: $field_type:ty),*$(,)?
//             }
//         ) => {
//             $(#[$($attrss)*])*
//             #[derive(Clone)]
//             $vis struct $name {
//                 $(
//                     $(#[$($attrss_f)*])*
//                     $field_vis $field_name: $field_type,)*
//             }
//
//             impl SizedOnDisk for $name {
//                 fn size(&self) -> PageOffset {
//                     0 $( + self.$field_name.size())*
//                 }
//             }
//         }
// }

#[macro_export]
macro_rules! serialize {
    ($s:expr, $des: ident, $cursor:ident) => {
        $s.serialize(&mut $des[$cursor..$cursor + $s.size()]);
        $cursor += $s.size();
    };
}

#[macro_export]
macro_rules! deserialize {
    ($s:ty, $src: ident, $cursor:ident) => {{
        let v = <$s>::deserialize(&$src[$cursor..]);
        $cursor += v.size();
        v
    }};
}

#[macro_export]
macro_rules! deserialize_with_var {
    ($var: ident, $s:ty, $src: ident, $cursor:ident) => {
        let $var = deserialize!($s, $src, $cursor);
    };
}

impl Serializable for String {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        let v = VectorOnDisk::<u8, OndiskKeyLength>::from(self.clone());
        serialize!(v, destination, cursor);
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        deserialize_with_var!(v, VectorOnDisk::<u8, OndiskKeyLength>, src, cursor);
        v.into()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Deref, DerefMut)]
pub struct VectorOnDisk<T: Serializable, L: num::PrimInt + Serializable> {
    #[deref]
    #[deref_mut]
    elements: Vec<T>,
    _p: std::marker::PhantomData<L>,
}

const ASCII_MOD: u8 = 127;
impl Debug for OnDiskKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v: Vec<_> = self.elements.iter().map(|i| *i % ASCII_MOD).collect();
        write!(f, "key")
        // write!(f, "\"{}\"", core::str::from_utf8(&v).unwrap())
    }
}

impl Debug for OnDiskValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v: Vec<_> = self.elements.iter().map(|i| *i % ASCII_MOD).collect();
        write!(f, "\"{}\"", core::str::from_utf8(&v).unwrap())
    }
}

impl<T: Serializable, L: num::PrimInt + Serializable> VectorOnDisk<T, L> {
    pub fn new(elements: Vec<T>, _u: L) -> Self {
        Self {
            elements,
            _p: std::marker::PhantomData,
        }
    }
}

impl<T: Serializable, L: num::PrimInt + Serializable> SizedOnDisk for VectorOnDisk<T, L> {
    fn size(&self) -> PageOffset {
        (size_of::<L>() + self.elements.iter().map(|e| e.size()).sum::<PageOffset>()) as PageOffset
    }
}

#[derive(PartialEq, Eq, Ord, PartialOrd, DerefMut, Clone, Deref, SizedOnDisk)]
pub struct OnDiskKey {
    // pub flags: OndiskFlags,
    #[deref]
    #[deref_mut]
    pub bytes: VectorOnDisk<u8, OndiskKeyLength>,
}

impl OnDiskKey {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            bytes: VectorOnDisk::new(key, 0 as OndiskKeyLength),
        }
    }

    #[cfg(test)]
    fn random() -> Self {
        use rand::{distributions::Alphanumeric, Rng};
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(17)
            .map(char::from)
            .collect();
        Self {
            bytes: VectorOnDisk::new(s.as_bytes().to_vec(), 0 as OndiskKeyLength),
        }
    }
}

#[derive(SizedOnDisk, Clone, Deref, DerefMut)]
pub struct OnDiskValue {
    // pub flags: OndiskFlags,
    pub bytes: VectorOnDisk<u8, OndiskValueLength>,
}

impl OnDiskValue {
    pub fn new(val: Vec<u8>) -> Self {
        Self {
            bytes: VectorOnDisk::new(val, 1 as OndiskValueLength),
        }
    }
}

impl<K: SizedOnDisk, V: SizedOnDisk> SizedOnDisk for BTreeMap<K, V> {
    fn size(&self) -> PageOffset {
        self.iter()
            .map(|(k, v)| k.size() + v.size())
            .sum::<PageOffset>()
            + size_of::<OndiskMapLength>()
    }
}

#[derive(SizedOnDisk, Clone)]
pub struct OndiskTuple {
    key: OnDiskKey,
    flags: OndiskFlags,
    message: VectorOnDisk<u8, OndiskMessageLength>,
}

#[derive(FromPrimitive, Clone, Copy, Debug)]
pub enum MessageType {
    Insert = 1,
    Delete,
    Upsert,
}

impl Serializable for MessageType {
    fn serialize(&self, destination: &mut [u8]) {
        (*self as u8).serialize(destination)
    }

    fn deserialize(src: &[u8]) -> Self {
        let num = u8::deserialize(src);
        Self::from_u8(num).unwrap()
    }
}

impl MessageData {
    pub fn new(ty: MessageType, val: Vec<u8>) -> Self {
        Self {
            ty,
            val: OnDiskValue::new(val),
        }
    }
}

impl SizedOnDisk for MessageType {
    fn size(&self) -> PageOffset {
        1
    }
}

impl Serializable for bool {
    fn serialize(&self, destination: &mut [u8]) {
        (*self as u8).serialize(destination)
    }

    fn deserialize(src: &[u8]) -> Self {
        let num = u8::deserialize(src);
        num != 0
    }
}

impl SizedOnDisk for bool {
    fn size(&self) -> PageOffset {
        1
    }
}

#[derive(SizedOnDisk, Clone, Debug)]
pub struct MessageData {
    pub val: OnDiskValue,
    pub ty: MessageType,
}

impl Serializable for MessageData {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        serialize!(self.ty, destination, cursor);
        serialize!(self.val, destination, cursor);
        debug_assert_eq!(cursor, self.size());
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        deserialize_with_var!(ty, MessageType, src, cursor);
        deserialize_with_var!(val, OnDiskValue, src, cursor);

        let s = Self { ty, val };
        debug_assert_eq!(s.size(), cursor as PageOffset);
        s
    }
}

#[derive(SizedOnDisk, Clone)]
pub struct Message {
    key: OnDiskKey,
    data: MessageData,
}

pub trait Serializable: SizedOnDisk {
    fn serialize(&self, destination: &mut [u8]);
    fn deserialize(src: &[u8]) -> Self;
}

macro_rules! SerializeImplForNumber {
    ($primitive_ty:ty) => {
        impl Serializable for $primitive_ty {
            fn serialize(&self, destination: &mut [u8]) {
                let bytes = self.to_le_bytes();
                destination[..bytes.len()].copy_from_slice(&bytes);
            }
            fn deserialize(src: &[u8]) -> Self {
                const SIZE: usize = size_of::<$primitive_ty>();
                let mut bytes = [0; SIZE];
                bytes.copy_from_slice(&src[0..SIZE]);
                Self::from_le_bytes(bytes)
            }
        }

        SizedOnDiskImplForPrimitive!($primitive_ty);
    };
}

// impl<T: PrimInt> Serializable for T {
//     fn serialize(&self, destination: &mut [u8]) -> usize {
//         let size = size_of::<T>();
//         let num = <PageOffset as NumCast>::from(*self).unwrap();
//         let bytes = num.to_be().to_be_bytes();
//         destination.copy_from_slice(&bytes[..size]);
//         size
//     }
// }
//
//
// impl<T: PrimInt> Deserializable for T {
//     type Target = Self;
//     fn deserialize(src: &[u8]) -> Self::Target {
//         let mut bytes = [0; size_of::<PageOffset>()];
//         let size = size_of::<T>();
//         (&mut bytes[0..size]).copy_from_slice(&src[0..size]);
//         let num = PageOffset::from_le_bytes(bytes);
//         T::from(num).unwrap()
//     }
// }

SerializeImplForNumber!(u8);
SerializeImplForNumber!(i8);
SerializeImplForNumber!(u16);
SerializeImplForNumber!(i16);
SerializeImplForNumber!(u32);
SerializeImplForNumber!(i32);
SerializeImplForNumber!(u64);
SerializeImplForNumber!(PageOffset);
SerializeImplForNumber!(f32);

impl<T: Serializable, L: Serializable + num::PrimInt> Serializable for VectorOnDisk<T, L> {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor: usize = 0;
        let len = self.len();
        let l = L::from(len).unwrap();
        serialize!(l, destination, cursor);
        if let Some(size) = T::is_packed() {
            let total_bytes = size * len;
            let slice = unsafe {
                core::slice::from_raw_parts(self.as_slice() as *const [T] as *const u8, total_bytes)
            };
            destination[cursor..cursor + total_bytes].copy_from_slice(slice);
            cursor += total_bytes;
        } else {
            self.iter().for_each(|i| {
                serialize!(i, destination, cursor);
            });
        }
        debug_assert_eq!(cursor as PageOffset, self.size());
    }

    fn deserialize(destination: &[u8]) -> Self {
        let mut cursor: usize = 0;
        let l_size = size_of::<L>();
        let len1 = L::deserialize(&destination[cursor..]);
        let len = <usize as num::NumCast>::from(len1).unwrap();
        cursor += l_size;
        let bytes_on_disk;
        if let Some(size) = T::is_packed() {
            let total_bytes = len * size;
            let v = (unsafe {
                core::slice::from_raw_parts(
                    &destination[cursor..cursor + total_bytes] as *const [u8] as *const T,
                    len,
                )
            })
            .to_vec();
            bytes_on_disk = VectorOnDisk::new(v, len1);
            cursor += total_bytes;
        } else {
            let v: Vec<_> = (0..len)
                .map(|_| deserialize!(T, destination, cursor))
                .collect();
            bytes_on_disk = VectorOnDisk::new(v, len1);
        }
        debug_assert_eq!(cursor as PageOffset, bytes_on_disk.size());
        bytes_on_disk
    }
}

impl Serializable for OnDiskKey {
    fn serialize(&self, destination: &mut [u8]) {
        self.bytes.serialize(destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        Self {
            bytes: VectorOnDisk::deserialize(src),
        }
    }
}

impl Serializable for OnDiskValue {
    fn serialize(&self, destination: &mut [u8]) {
        self.bytes.serialize(destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        Self {
            bytes: VectorOnDisk::deserialize(src),
        }
    }
}

impl<K: Serializable + Ord, V: Serializable> Serializable for BTreeMap<K, V> {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        let len = self.len();
        let len1 = len as OndiskMapLength;
        serialize!(len1, destination, cursor);
        self.iter().for_each(|(k, v)| {
            serialize!(k, destination, cursor);
            serialize!(v, destination, cursor);
        });
        debug_assert_eq!(cursor, self.size());
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        let len = usize::from(OndiskMapLength::deserialize(&src[cursor..]));
        cursor += size_of::<OndiskMapLength>();
        let map: Self = (0..len)
            .map(|_| {
                deserialize_with_var!(k, K, src, cursor);
                deserialize_with_var!(v, V, src, cursor);
                (k, v)
            })
            .collect();
        map
    }
}

impl Serializable for Message {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor = 0;
        serialize!(self.key, destination, cursor);
        serialize!(self.data, destination, cursor);
        debug_assert_eq!(cursor, self.size());
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;

        let key = deserialize!(OnDiskKey, src, cursor);
        let data = deserialize!(MessageData, src, cursor);
        let s = Self { key, data };
        debug_assert_eq!(s.size(), cursor as PageOffset);
        s
    }
}

pub type LeafEntry = OndiskTuple;

#[test]
fn endian() {
    let a = 1u32;
    let mut bytes = [0; 4];
    a.serialize(&mut bytes);
    assert_eq!(u32::deserialize(&bytes), a);

    let a = -21i32;
    let mut bytes = [0; 4];
    a.serialize(&mut bytes);
    assert_eq!(i32::deserialize(&bytes), a);

    let k = VectorOnDisk::new(vec![1], 1u8);
}

#[test]
fn test_serialization() {
    let a: Vec<_> = (-1000..1000).collect();
    let bs = VectorOnDisk::new(a, 1u16);
    let mut b = vec![0u8; 84000];
    bs.serialize(&mut b);
    let bs1: VectorOnDisk<i32, u16> = VectorOnDisk::deserialize(&b);
    assert_eq!(bs.elements, bs1.elements);
}

#[test]
fn test_map_serialization() {
    let cap = 100;
    let btree: BTreeMap<_, _> = (0..cap)
        .map(|_| (OnDiskKey::random(), OnDiskKey::random()))
        .collect();
    let mut page = [0; 4000];
    btree.serialize(&mut page);
    let db = BTreeMap::<OnDiskKey, OnDiskKey>::deserialize(&page);
    assert_eq!(btree, db);
}
