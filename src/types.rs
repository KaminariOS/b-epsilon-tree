use core::mem::size_of;
use core::ops::{Deref, DerefMut};
use std::collections::BTreeMap;
use num_derive::FromPrimitive;    
use num_traits::FromPrimitive;

pub type OndiskKeyLength = u16;
pub type OndiskValueLength = u16;
pub type OndiskFlags = u8;
pub type OndiskMessageLength = u16;
pub type PageOffset = usize;

pub trait SizedOnDisk {
   fn size(&self) -> PageOffset;
}

#[macro_export]
macro_rules! SizedOnDiskImplForPrimitive {
    ($primitive_ty:ty) => {
        impl SizedOnDisk for $primitive_ty {
            fn size(&self) -> PageOffset {
                size_of::<Self>() as PageOffset
            }
        }
    };
}


#[macro_export]
macro_rules! SizedOnDiskImplForComposite {
        (
            $(#[$($attrss:tt)*])*
            $vis:vis struct $name:ident {

                $(

                    $(#[$($attrss_f:tt)*])*
                    $field_vis:vis $field_name:ident: $field_type:ty),*$(,)?
            }
        ) => {
            $(#[$($attrss)*])*
            $vis struct $name {
                $(
                    $(#[$($attrss_f)*])*
                    $field_vis $field_name: $field_type,)*
            }
            
            impl SizedOnDisk for $name {
                fn size(&self) -> PageOffset {
                    0 $( + self.$field_name.size())*
                }
            }
        }
}

pub struct BytesOnDisk<T: Serializable, L: num::PrimInt + Serializable> {
    elements: Vec<T>,
    _p: std::marker::PhantomData<L>
}

impl<T: Serializable, L: num::PrimInt + Serializable> BytesOnDisk<T, L> {
    fn new(elements: Vec<T>, _u: L) -> Self {
        Self {
            elements,
            _p: std::marker::PhantomData, 
        }
    }
}

impl<T: Serializable, U: num::PrimInt + Serializable> Deref for BytesOnDisk<T, U> {
    type Target = Vec<T>; 
    fn deref(&self) -> &Self::Target {
        &self.elements
    }
}

impl<T: Serializable, U: num::PrimInt + Serializable> DerefMut for BytesOnDisk<T, U> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.elements
    }
}

impl<T: Serializable, L: num::PrimInt + Serializable> SizedOnDisk for BytesOnDisk<T, L> {
    fn size(&self) -> PageOffset {
        (size_of::<L>() + self.elements.len() * size_of::<T>()) as PageOffset
    }
}



SizedOnDiskImplForComposite!{
    pub struct OnDiskKey {
        // pub flags: OndiskFlags,
        pub bytes: BytesOnDisk<u8, OndiskKeyLength>
    }
}

impl Deref for OnDiskKey {
    type Target = BytesOnDisk<u8, OndiskKeyLength>;
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for OnDiskKey {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

SizedOnDiskImplForComposite!{
    pub struct OnDiskValue {
        // pub flags: OndiskFlags,
        pub bytes: BytesOnDisk<u8, OndiskValueLength>
    }
}


impl Deref for OnDiskValue {
    type Target = BytesOnDisk<u8, OndiskValueLength>;
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for OnDiskValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

impl<K: SizedOnDisk, V: SizedOnDisk> SizedOnDisk for BTreeMap<K, V> {
    fn size(&self) -> PageOffset {
        self.iter().map(|(k,v)| k.size() + v.size()).sum()
    }
}

SizedOnDiskImplForComposite!{
    pub struct OndiskTuple {
        key: OnDiskKey,
        flags: OndiskFlags,
        message: BytesOnDisk<u8, OndiskMessageLength>,
    }
}

#[derive(FromPrimitive, Clone, Copy)]
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

impl SizedOnDisk for MessageType {
    fn size(&self) -> PageOffset {
        1 
    }
}



SizedOnDiskImplForComposite! {
    pub struct Message {
        key: OnDiskKey,
        val: OnDiskValue,
        ty: MessageType,
    }
}

pub trait Serializable: SizedOnDisk {
    fn serialize(&self, destination: &mut [u8]);
    fn deserialize(src: &[u8]) -> Self;
}


macro_rules! SerializeImplForInteger {
    ($primitive_ty:ty) => {
        impl Serializable for $primitive_ty {
            fn serialize(&self, destination: &mut [u8]) {
                let bytes = self.to_be_bytes();
                
                destination[..bytes.len()].copy_from_slice(&bytes);
            }
            fn deserialize(src: &[u8]) -> Self {
                const SIZE: usize = size_of::<$primitive_ty>();
                let mut bytes = [0; SIZE];
                bytes.copy_from_slice(&src[0..SIZE]);
                Self::from_be_bytes(bytes)
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

SerializeImplForInteger!(u8);
SerializeImplForInteger!(i8);
SerializeImplForInteger!(u16);
SerializeImplForInteger!(i16);
SerializeImplForInteger!(u32);
SerializeImplForInteger!(i32);
SerializeImplForInteger!(u64);
SerializeImplForInteger!(PageOffset);

impl<T: Serializable + num::PrimInt, L: Serializable + num::PrimInt> Serializable for BytesOnDisk<T, L> {
    fn serialize(&self, destination: &mut [u8]) {
        let mut cursor: usize = 0;
        let len = self.len();
        let l = L::from(len).unwrap(); 
        l.serialize(&mut destination[cursor..]);
        cursor += l.size() as usize;
        let total_bytes = self.size() as usize - size_of::<L>();
        let slice = unsafe {
             core::slice::from_raw_parts(self.as_slice() as *const [T] as *const u8, total_bytes)
        };
        destination[cursor..cursor + total_bytes].copy_from_slice(slice);
        cursor += total_bytes;
        debug_assert_eq!(cursor as PageOffset, self.size());
    }

    fn deserialize(destination: &[u8]) -> Self {
        let mut cursor: usize = 0;
        let l_size = size_of::<L>();
        let len1 = L::deserialize(&destination[cursor..]); 
        let len = <usize as num::NumCast>::from(len1).unwrap();
        cursor += l_size;
        let total_bytes = len * size_of::<T>();
        let v = (unsafe {
            core::slice::from_raw_parts(&destination[cursor..cursor + total_bytes] as *const [u8] as *const T, len)
        }).to_vec();
        let bytes_on_disk = BytesOnDisk::new(v, len1);
        cursor += total_bytes;
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
            bytes: BytesOnDisk::deserialize(src)
        }
    }
}


impl Serializable for OnDiskValue {
    fn serialize(&self, destination: &mut [u8]) {
        self.bytes.serialize(destination);
    }

    fn deserialize(src: &[u8]) -> Self {
        Self {
            bytes: BytesOnDisk::deserialize(src)
        }
    }
}

impl Serializable for Message {
    fn serialize(&self, destination: &mut [u8]) {
           let mut cursor = 0;
           self.ty.serialize(&mut destination[cursor..]);
           cursor += self.ty.size() as usize;
           self.key.serialize(&mut destination[cursor..]);
           cursor += self.key.size() as usize;
           self.val.serialize(&mut destination[cursor..]);
           cursor += self.val.size() as usize;
           debug_assert_eq!(cursor, self.size());
    }   

    fn deserialize(src: &[u8]) -> Self {
        let mut cursor = 0;
        let ty = MessageType::deserialize(&src[cursor..]);
        cursor += ty.size() as usize;

        let key = OnDiskKey::deserialize(&src[cursor..]);
        cursor += key.size() as usize;

        let val = OnDiskValue::deserialize(&src[cursor..]);
        cursor += val.size() as usize;
        
        let s = Self {ty, key, val};
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

    let k = BytesOnDisk::new(  vec![1], 1u8);
}

#[test]
fn test_serialization() {
    let a: Vec<_> = (-1000..1000).collect();
    let bs = BytesOnDisk::new(a, 1u16);
    let mut b = vec![0u8; 84000];
    bs.serialize(&mut b);
    let bs1: BytesOnDisk<i32, u16> = BytesOnDisk::deserialize(&b);
    assert_eq!(bs.elements, bs1.elements);
}



