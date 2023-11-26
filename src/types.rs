use core::fmt::Debug;
use core::mem::size_of;
use derive_more::{Deref, DerefMut};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use ser_derive::SizedOnDisk;
use std::collections::BTreeMap;

pub type OndiskKeyLength = u16;
pub type OndiskValueLength = u16;
pub type OndiskFlags = u8;
pub type OndiskMessageLength = u16;
pub type OndiskMapLength = u16;
pub type PageOffset = usize;
// pub type Comparator = fn(&[u8], &[u8]) -> Ordering;

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
        value.into_bytes().into()
    }
}

impl From<VectorOnDisk<u8, OndiskKeyLength>> for String {
    fn from(value: VectorOnDisk<u8, OndiskKeyLength>) -> Self {
        let VectorOnDisk { elements, .. } = value;
        String::from_utf8(elements).unwrap()
    }
}

// impl<T: Serializable> From<Vec<T>> for VectorOnDisk<T, OndiskKeyLength> {
//     fn from(value: Vec<T>) -> Self {
//         Self::new(value)
//     }
// }

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
//                     $(#[$($attrss_f:tt)*])* $field_vis:vis $field_name:ident: $field_type:ty),*$(,)?
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
    // ([$($s:expr), +], $des: ident, $cursor:ident) => {
    //     $(
    //     $s.serialize(&mut $des[$cursor..$cursor + $s.size()]);
    //     $cursor += $s.size();
    //     )+
    // };
    ($s:expr, $des: ident) => {
        $s.serialize(&mut $des[..$s.size()]);
    };
}

#[macro_export]
macro_rules! deserialize {
    ($s:ty, $src: ident, $cursor:ident) => {{
        let v = <$s>::deserialize(&$src[$cursor..]);
        $cursor += v.size();
        v
    }};
    ($s:ty, $src: ident) => {
        <$s>::deserialize($src)
    };
}

#[macro_export]
macro_rules! deserialize_with_var {
    ($var: ident, $s:ty, $src: ident, $cursor:ident) => {
        let $var = deserialize!($s, $src, $cursor);
    };
    ($var: ident, $s:ty, $src: ident) => {
        let $var = deserialize!($s, $src);
    };
}

impl Serializable for String {
    fn serialize(&self, destination: &mut [u8]) {
        let v = VectorOnDisk::<u8, OndiskKeyLength>::from(self.clone());
        serialize!(v, destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        deserialize_with_var!(v, VectorOnDisk::<u8, OndiskKeyLength>, src);
        v.into()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Deref)]
pub struct VectorOnDisk<T: Serializable, L: num::PrimInt + Serializable> {
    #[deref]
    elements: Vec<T>,
    size: PageOffset,
    _p: std::marker::PhantomData<L>,
}

const ASCII_MOD: u8 = 127;
impl Debug for OnDiskKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let v: Vec<_> = self.elements.iter().map(|i| *i % ASCII_MOD).collect();
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

impl<T: Serializable, L: num::PrimInt + Serializable> SizedOnDisk for VectorOnDisk<T, L> {
    fn size(&self) -> PageOffset {
        self.size
    }
}

#[derive(PartialEq, Eq, Ord, PartialOrd, DerefMut, Clone, Deref, SizedOnDisk)]
pub struct OnDiskKey {
    // pub flags: OndiskFlags,
    #[deref]
    #[deref_mut]
    pub bytes: VectorOnDisk<u8, OndiskKeyLength>,
}

impl<T: Serializable, L: num::PrimInt + Serializable> From<Vec<T>> for VectorOnDisk<T, L> {
    fn from(elements: Vec<T>) -> Self {
        let size = size_of::<L>() + elements.iter().map(|e| e.size()).sum::<PageOffset>();
        Self {
            elements,
            size,
            _p: std::marker::PhantomData,
        }
    }
}

impl OnDiskKey {
    pub fn new(key: Vec<u8>) -> Self {
        Self { bytes: key.into() }
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
            bytes: s.into_bytes().into(),
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
        Self { bytes: val.into() }
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
        let mut _cursor = 0;
        serialize!(self.ty, destination, _cursor);
        serialize!(self.val, destination, _cursor);
        debug_assert_eq!(_cursor, self.size());
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut _cursor = 0;
        deserialize_with_var!(ty, MessageType, src, _cursor);
        deserialize_with_var!(val, OnDiskValue, src, _cursor);

        let s = Self { ty, val };
        debug_assert_eq!(s.size(), _cursor as PageOffset);
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
        let mut _cursor: usize = 0;
        let len = self.len();
        let l = L::from(len).unwrap();
        serialize!(l, destination, _cursor);
        if let Some(size) = T::is_packed() {
            let total_bytes = size * len;
            let slice = unsafe {
                core::slice::from_raw_parts(self.as_slice() as *const [T] as *const u8, total_bytes)
            };
            destination[_cursor.._cursor + total_bytes].copy_from_slice(slice);
            _cursor += total_bytes;
        } else {
            self.iter().for_each(|i| {
                serialize!(i, destination, _cursor);
            });
        }
        debug_assert_eq!(_cursor as PageOffset, self.size());
    }

    fn deserialize(destination: &[u8]) -> Self {
        let mut _cursor: usize = 0;
        let l_size = size_of::<L>();
        let len1 = L::deserialize(&destination[_cursor..]);
        let len = <usize as num::NumCast>::from(len1).unwrap();
        _cursor += l_size;
        let bytes_on_disk: Self;
        if let Some(size) = T::is_packed() {
            let total_bytes = len * size;
            let v = (unsafe {
                core::slice::from_raw_parts(
                    &destination[_cursor.._cursor + total_bytes] as *const [u8] as *const T,
                    len,
                )
            })
            .to_vec();
            bytes_on_disk = v.into();
            _cursor += total_bytes;
        } else {
            let v: Vec<_> = (0..len)
                .map(|_| deserialize!(T, destination, _cursor))
                .collect();
            bytes_on_disk = v.into();
        }
        debug_assert_eq!(_cursor as PageOffset, bytes_on_disk.size());
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
        let mut _cursor = 0;
        let len = self.len();
        let len1 = len as OndiskMapLength;
        serialize!(len1, destination, _cursor);
        self.iter().for_each(|(k, v)| {
            serialize!(k, destination, _cursor);
            serialize!(v, destination, _cursor);
        });
        debug_assert_eq!(_cursor, self.size());
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut _cursor = 0;
        let len = usize::from(OndiskMapLength::deserialize(&src[_cursor..]));
        _cursor += size_of::<OndiskMapLength>();
        let map: Self = (0..len)
            .map(|_| {
                deserialize_with_var!(k, K, src, _cursor);
                deserialize_with_var!(v, V, src, _cursor);
                (k, v)
            })
            .collect();
        map
    }
}

impl Serializable for Message {
    fn serialize(&self, destination: &mut [u8]) {
        let mut _cursor = 0;
        serialize!(self.key, destination, _cursor);
        serialize!(self.data, destination, _cursor);
        debug_assert_eq!(_cursor, self.size());
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut _cursor = 0;

        deserialize_with_var!(key, OnDiskKey, src, _cursor);
        deserialize_with_var!(data, MessageData, src, _cursor);
        let s = Self { key, data };
        debug_assert_eq!(s.size(), _cursor as PageOffset);
        s
    }
}

#[derive(Deref, Clone, Debug)]
pub struct BTreeMapOnDisK<K: Serializable, V: Serializable> {
    #[deref]
    inner: BTreeMap<K, V>,
    size: PageOffset,
}
impl<K: Serializable, V: Serializable> BTreeMapOnDisK<K, V> {
    // type InnerMap = BTreeMap<K, V>;
    pub fn new() -> Self {
        let inner = BTreeMap::<K, V>::new();
        Self {
            size: inner.size(),
            inner,
        }
    }

    pub fn to_inner(self) -> BTreeMap<K, V> {
        let Self { inner, .. } = self;
        inner
    }

    pub fn refresh_size(&mut self) {
        self.size = self.inner.size();
    }
}

impl<K: Serializable + Ord, V: Serializable> BTreeMapOnDisK<K, V> {
    // type InnerMap = BTreeMap<K, V>;
    pub fn pop_last(&mut self) -> Option<(K, V)> {
        let last = self.inner.pop_last();
        self.size -= last.size();
        last
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        let k_size = k.size();
        self.size += v.size();
        let res = self.inner.insert(k, v);
        if res.is_some() {
            self.size -= res.size();
        } else {
            self.size += k_size;
        };
        res
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let res = self.inner.remove(key);
        if res.is_some() {
            self.size -= key.size() + res.size();
        }
        res
    }

    pub fn append(&mut self, other: &mut BTreeMap<K, V>) {
        if self.inner.len() < other.len() {
            core::mem::swap(&mut self.inner, other);
        }
        self.inner.append(other);
        self.size = self.inner.size();
    }

    pub fn split_off(&mut self, key: &K) -> BTreeMap<K, V> {
        let new_map = self.inner.split_off(key);
        self.size = self.inner.size();
        new_map
    }
}

impl<T: SizedOnDisk, V: SizedOnDisk> SizedOnDisk for (T, V) {
    fn size(&self) -> PageOffset {
        self.0.size() + self.1.size()
    }
}

impl<T: SizedOnDisk> SizedOnDisk for Option<T> {
    fn size(&self) -> PageOffset {
        if let Some(e) = self {
            e.size()
        } else {
            0
        }
    }
}

impl<K: Serializable, V: Serializable> SizedOnDisk for BTreeMapOnDisK<K, V> {
    fn size(&self) -> PageOffset {
        self.size
    }
}

impl<K: Serializable + Ord, V: Serializable> Serializable for BTreeMapOnDisK<K, V> {
    fn serialize(&self, destination: &mut [u8]) {
        serialize!(self.inner, destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        let mut _cursor = 0;
        deserialize_with_var!(inner, BTreeMap<K, V>, src, _cursor);
        Self {
            inner,
            size: _cursor,
        }
    }
}

impl<K: Serializable + Ord, V: Serializable> From<BTreeMap<K, V>> for BTreeMapOnDisK<K, V> {
    fn from(inner: BTreeMap<K, V>) -> Self {
        let size = inner.size();
        Self { inner, size }
    }
}

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

    // let k = VectorOnDisk::new(vec![1]);
}

#[test]
fn test_serialization() {
    let a: Vec<_> = (-1000..1000).collect();
    let bs = VectorOnDisk::<_, u16>::from(a);
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
