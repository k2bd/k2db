#[derive(Debug, PartialEq, Eq)]
pub enum SerializeError {
    InvalidSize,
    InvalidValue,
}

/// Trait for serializing and deserializing a struct to and from a fixed size
/// byte array.
pub trait BytesSerialize {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError>
    where
        Self: Sized;
    fn serialized_size() -> usize
    where
        Self: Sized,
    {
        std::mem::size_of::<Self>()
    }
}

// Macro will take a list of serializable types and create a recursive tuple
// E.g. tuple![u8, u16, u32] -> (u8, (u16, u32))
#[macro_export]
macro_rules! tuple_type {
    ($head:ty) => {
        $head
    };
    ($head:ty, $($tail:ty),+) => {
        ($head, tuple_type!($($tail),+))
    };
}

// Macro will take a list of serializable values and create a recursive tuple
// E.g. tuple![2u8, 5u16, 10u32] -> (2u8, (5u16, 10u32))
#[macro_export]
macro_rules! tuple {
    ($head:expr) => {
        $head
    };
    ($head:expr, $($tail:expr),+) => {
        ($head, tuple!($($tail),+))
    };
}

impl<H, T> BytesSerialize for (H, T)
where
    H: BytesSerialize,
    T: BytesSerialize,
{
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.0.to_bytes()?);
        bytes.extend_from_slice(&self.1.to_bytes()?);
        Ok(bytes)
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<(H, T), SerializeError> {
        let h_size = H::serialized_size();
        let t_size = T::serialized_size();
        if bytes.len() != h_size + t_size {
            return Err(SerializeError::InvalidSize);
        }
        let h_bytes = bytes[0..h_size].to_vec();
        let t_bytes = bytes[h_size..h_size + t_size].to_vec();
        Ok((H::from_bytes(h_bytes)?, T::from_bytes(t_bytes)?))
    }

    fn serialized_size() -> usize {
        H::serialized_size() + T::serialized_size()
    }
}

impl BytesSerialize for () {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(vec![])
    }

    fn from_bytes(_: Vec<u8>) -> Result<Self, SerializeError> {
        Ok(())
    }
}

impl BytesSerialize for u8 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(vec![*self])
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

// TODO: serialize/deserialize errors instead of panicking

impl BytesSerialize for u16 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for u32 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for u64 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for u128 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for i8 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(vec![*self as u8])
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for i16 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for i32 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for i64 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for i128 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for f32 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_bits().to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for f64 {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        Ok(self.to_bits().to_be_bytes().to_vec())
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self::from_be_bytes(bytes)),
            Err(_) => Err(SerializeError::InvalidSize),
        }
    }
}

impl BytesSerialize for bool {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        if *self {
            Ok(vec![1])
        } else {
            Ok(vec![0])
        }
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SerializeError> {
        match bytes[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(SerializeError::InvalidValue),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case((), vec![])]
    fn test_unit_to_bytes(#[case] input: (), #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![], ())]
    fn test_unit_from_bytes(#[case] input: Vec<u8>, #[case] expected: ()) {
        assert_eq!(<()>::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_unit_serialized_size() {
        assert_eq!(<()>::serialized_size(), 0);
    }

    #[rstest]
    #[case(0u8, vec![0])]
    #[case(1u8, vec![1])]
    #[case(255u8, vec![255])]
    fn test_u8_to_bytes(#[case] input: u8, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0], 0u8)]
    #[case(vec![1], 1u8)]
    #[case(vec![255], 255u8)]
    fn test_u8_from_bytes(#[case] input: Vec<u8>, #[case] expected: u8) {
        assert_eq!(u8::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_u8_from_invalid_bytes() {
        assert_eq!(u8::from_bytes(vec![0, 0]), Err(SerializeError::InvalidSize));
    }

    #[rstest]
    fn test_u8_serialized_size() {
        assert_eq!(u8::serialized_size(), 1);
    }

    #[rstest]
    #[case(0u16, vec![0, 0])]
    #[case(1u16, vec![0, 1])]
    #[case(255u16, vec![0, 255])]
    #[case(256u16, vec![1, 0])]
    #[case(65535u16, vec![255, 255])]
    fn test_u16_to_bytes(#[case] input: u16, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0, 0], 0u16)]
    #[case(vec![0, 1], 1u16)]
    #[case(vec![0, 255], 255u16)]
    #[case(vec![1, 0], 256u16)]
    #[case(vec![255, 255], 65535u16)]
    fn test_u16_from_bytes(#[case] input: Vec<u8>, #[case] expected: u16) {
        assert_eq!(u16::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_u16_from_invalid_bytes() {
        assert_eq!(
            u16::from_bytes(vec![0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_u16_serialized_size() {
        assert_eq!(u16::serialized_size(), 2);
    }

    #[rstest]
    #[case(0u32, vec![0, 0, 0, 0])]
    #[case(1u32, vec![0, 0, 0, 1])]
    #[case(255u32, vec![0, 0, 0, 255])]
    #[case(256u32, vec![0, 0, 1, 0])]
    #[case(65535u32, vec![0, 0, 255, 255])]
    #[case(65536u32, vec![0, 1, 0, 0])]
    #[case(16777215u32, vec![0, 255, 255, 255])]
    #[case(16777216u32, vec![1, 0, 0, 0])]
    #[case(4294967295u32, vec![255, 255, 255, 255])]
    fn test_u32_to_bytes(#[case] input: u32, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0, 0, 0, 0], 0u32)]
    #[case(vec![0, 0, 0, 1], 1u32)]
    #[case(vec![0, 0, 0, 255], 255u32)]
    #[case(vec![0, 0, 1, 0], 256u32)]
    #[case(vec![0, 0, 255, 255], 65535u32)]
    #[case(vec![0, 1, 0, 0], 65536u32)]
    #[case(vec![0, 255, 255, 255], 16777215u32)]
    #[case(vec![1, 0, 0, 0], 16777216u32)]
    #[case(vec![255, 255, 255, 255], 4294967295u32)]
    fn test_u32_from_bytes(#[case] input: Vec<u8>, #[case] expected: u32) {
        assert_eq!(u32::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_u32_from_invalid_bytes() {
        assert_eq!(
            u32::from_bytes(vec![0, 0, 0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_u32_serialized_size() {
        assert_eq!(u32::serialized_size(), 4);
    }

    #[rstest]
    #[case(0u64, vec![0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(1u64, vec![0, 0, 0, 0, 0, 0, 0, 1])]
    #[case(255u64, vec![0, 0, 0, 0, 0, 0, 0, 255])]
    #[case(256u64, vec![0, 0, 0, 0, 0, 0, 1, 0])]
    #[case(65535u64, vec![0, 0, 0, 0, 0, 0, 255, 255])]
    #[case(65536u64, vec![0, 0, 0, 0, 0, 1, 0, 0])]
    #[case(16777215u64, vec![0, 0, 0, 0, 0, 255, 255, 255])]
    #[case(16777216u64, vec![0, 0, 0, 0, 1, 0, 0, 0])]
    #[case(4294967295u64, vec![0, 0, 0, 0, 255, 255, 255, 255])]
    #[case(4294967296u64, vec![0, 0, 0, 1, 0, 0, 0, 0])]
    #[case(1099511627775u64, vec![0, 0, 0, 255, 255, 255, 255, 255])]
    #[case(1099511627776u64, vec![0, 0, 1, 0, 0, 0, 0, 0])]
    #[case(281474976710655u64, vec![0, 0, 255, 255, 255, 255, 255, 255])]
    #[case(281474976710656u64, vec![0, 1, 0, 0, 0, 0, 0, 0])]
    #[case(72057594037927935u64, vec![0, 255, 255, 255, 255, 255, 255, 255])]
    #[case(72057594037927936u64, vec![1, 0, 0, 0, 0, 0, 0, 0])]
    #[case(18446744073709551615u64, vec![255, 255, 255, 255, 255, 255, 255, 255])]
    fn test_u64_to_bytes(#[case] input: u64, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 0], 0u64)]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 1], 1u64)]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 255], 255u64)]
    #[case(vec![0, 0, 0, 0, 0, 0, 1, 0], 256u64)]
    #[case(vec![0, 0, 0, 0, 0, 0, 255, 255], 65535u64)]
    #[case(vec![0, 0, 0, 0, 0, 1, 0, 0], 65536u64)]
    #[case(vec![0, 0, 0, 0, 0, 255, 255, 255], 16777215u64)]
    #[case(vec![0, 0, 0, 0, 1, 0, 0, 0], 16777216u64)]
    #[case(vec![0, 0, 0, 0, 255, 255, 255, 255], 4294967295u64)]
    #[case(vec![0, 0, 0, 1, 0, 0, 0, 0], 4294967296u64)]
    #[case(vec![0, 0, 0, 255, 255, 255, 255, 255], 1099511627775u64)]
    #[case(vec![0, 0, 0, 255, 255, 255, 255, 255], 1099511627775u64)]
    #[case(vec![0, 0, 1, 0, 0, 0, 0, 0], 1099511627776u64)]
    #[case(vec![0, 0, 255, 255, 255, 255, 255, 255], 281474976710655u64)]
    #[case(vec![0, 1, 0, 0, 0, 0, 0, 0], 281474976710656u64)]
    #[case(vec![0, 255, 255, 255, 255, 255, 255, 255], 72057594037927935u64)]
    #[case(vec![1, 0, 0, 0, 0, 0, 0, 0], 72057594037927936u64)]
    #[case(vec![255, 255, 255, 255, 255, 255, 255, 255], 18446744073709551615u64)]
    fn test_u64_from_bytes(#[case] input: Vec<u8>, #[case] expected: u64) {
        assert_eq!(u64::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_u64_from_invalid_bytes() {
        assert_eq!(
            u64::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_u64_serialized_size() {
        assert_eq!(u64::serialized_size(), 8);
    }

    #[rstest]
    #[case(0u128, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(1u128, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])]
    #[case(18446744073709551615u128, vec![0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255])]
    #[case(18446744073709551616u128, vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(u128::MAX, vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255])]
    fn test_u128_to_bytes(#[case] input: u128, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 0u128)]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 1u128)]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255], 18446744073709551615u128)]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0], 18446744073709551616u128)]
    #[case(vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255], u128::MAX)]
    fn test_u128_from_bytes(#[case] input: Vec<u8>, #[case] expected: u128) {
        assert_eq!(u128::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_u128_from_invalid_bytes() {
        assert_eq!(
            u128::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_u128_serialized_size() {
        assert_eq!(u128::serialized_size(), 16);
    }

    #[rstest]
    #[case(0i8, vec![0])]
    #[case(1i8, vec![1])]
    #[case(-1i8, vec![255])]
    #[case(i8::MAX, vec![127])]
    #[case(i8::MIN, vec![128])]
    fn test_i8_to_bytes(#[case] input: i8, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0], 0i8)]
    #[case(vec![1], 1i8)]
    #[case(vec![255], -1i8)]
    #[case(vec![127], i8::MAX)]
    #[case(vec![128], i8::MIN)]
    fn test_i8_from_bytes(#[case] input: Vec<u8>, #[case] expected: i8) {
        assert_eq!(i8::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_i8_serialized_size() {
        assert_eq!(i8::serialized_size(), 1);
    }

    #[rstest]
    fn test_i8_from_invalid_bytes() {
        assert_eq!(i8::from_bytes(vec![0, 0]), Err(SerializeError::InvalidSize));
    }

    #[rstest]
    #[case(0i16, vec![0, 0])]
    #[case(1i16, vec![0, 1])]
    #[case(-1i16, vec![255, 255])]
    #[case(i16::MAX, vec![127, 255])]
    #[case(i16::MIN, vec![128, 0])]
    fn test_i16_to_bytes(#[case] input: i16, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0, 0], 0i16)]
    #[case(vec![0, 1], 1i16)]
    #[case(vec![255, 255], -1i16)]
    #[case(vec![127, 255], i16::MAX)]
    #[case(vec![128, 0], i16::MIN)]
    fn test_i16_from_bytes(#[case] input: Vec<u8>, #[case] expected: i16) {
        assert_eq!(i16::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_i16_from_invalid_bytes() {
        assert_eq!(
            i16::from_bytes(vec![0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_i16_serialized_size() {
        assert_eq!(i16::serialized_size(), 2);
    }

    #[rstest]
    #[case(0i32, vec![0, 0, 0, 0])]
    #[case(1i32, vec![0, 0, 0, 1])]
    #[case(-1i32, vec![255, 255, 255, 255])]
    #[case(i32::MAX, vec![127, 255, 255, 255])]
    #[case(i32::MIN, vec![128, 0, 0, 0])]
    fn test_i32_to_bytes(#[case] input: i32, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0, 0, 0, 0], 0i32)]
    #[case(vec![0, 0, 0, 1], 1i32)]
    #[case(vec![255, 255, 255, 255], -1i32)]
    #[case(vec![127, 255, 255, 255], i32::MAX)]
    #[case(vec![128, 0, 0, 0], i32::MIN)]
    fn test_i32_from_bytes(#[case] input: Vec<u8>, #[case] expected: i32) {
        assert_eq!(i32::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_i32_from_invalid_bytes() {
        assert_eq!(
            i32::from_bytes(vec![0, 0, 0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_i32_serialized_size() {
        assert_eq!(i32::serialized_size(), 4);
    }

    #[rstest]
    #[case(0i64, vec![0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(1i64, vec![0, 0, 0, 0, 0, 0, 0, 1])]
    #[case(-1i64, vec![255, 255, 255, 255, 255, 255, 255, 255])]
    #[case(i64::MAX, vec![127, 255, 255, 255, 255, 255, 255, 255])]
    #[case(i64::MIN, vec![128, 0, 0, 0, 0, 0, 0, 0])]
    fn test_i64_to_bytes(#[case] input: i64, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 0], 0i64)]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 1], 1i64)]
    #[case(vec![255, 255, 255, 255, 255, 255, 255, 255], -1i64)]
    #[case(vec![127, 255, 255, 255, 255, 255, 255, 255], i64::MAX)]
    #[case(vec![128, 0, 0, 0, 0, 0, 0, 0], i64::MIN)]
    fn test_i64_from_bytes(#[case] input: Vec<u8>, #[case] expected: i64) {
        assert_eq!(i64::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_i64_from_invalid_bytes() {
        assert_eq!(
            i64::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_i64_serialized_size() {
        assert_eq!(i64::serialized_size(), 8);
    }

    #[rstest]
    #[case(0i128, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(1i128, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])]
    #[case(-1i128, vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255])]
    #[case(i128::MAX, vec![127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255])]
    #[case(i128::MIN, vec![128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]
    fn test_i128_to_bytes(#[case] input: i128, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 0i128)]
    #[case(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 1i128)]
    #[case(vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255], -1i128)]
    #[case(vec![127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255], i128::MAX)]
    #[case(vec![128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], i128::MIN)]
    fn test_i128_from_bytes(#[case] input: Vec<u8>, #[case] expected: i128) {
        assert_eq!(i128::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_i128_from_invalid_bytes() {
        assert_eq!(
            i128::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_i128_serialized_size() {
        assert_eq!(i128::serialized_size(), 16);
    }

    #[rstest]
    #[case(1.234f32, vec![63, 157, 243, 182])]
    #[case(-1.234f32, vec![191, 157, 243, 182])]
    #[case(f32::MAX, vec![127, 127, 255, 255])]
    #[case(f32::MIN, vec![255, 127, 255, 255])]
    fn test_f32_to_bytes(#[case] input: f32, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![63, 157, 243, 182], 1.234f32)]
    #[case(vec![191, 157, 243, 182], -1.234f32)]
    #[case(vec![127, 127, 255, 255], f32::MAX)]
    #[case(vec![255, 127, 255, 255], f32::MIN)]
    fn test_f32_from_bytes(#[case] input: Vec<u8>, #[case] expected: f32) {
        assert_eq!(f32::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_f32_from_invalid_bytes() {
        assert_eq!(
            f32::from_bytes(vec![0, 0, 0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_f32_serialized_size() {
        assert_eq!(f32::serialized_size(), 4);
    }

    #[rstest]
    #[case(1.234f64, vec![63, 243, 190, 118, 200, 180, 57, 88])]
    #[case(-1.234f64, vec![191, 243, 190, 118, 200, 180, 57, 88])]
    #[case(f64::MAX, vec![127, 239, 255, 255, 255, 255, 255, 255])]
    #[case(f64::MIN, vec![255, 239, 255, 255, 255, 255, 255, 255])]
    fn test_f64_to_bytes(#[case] input: f64, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![63, 243, 190, 118, 200, 180, 57, 88], 1.234f64)]
    #[case(vec![191, 243, 190, 118, 200, 180, 57, 88], -1.234f64)]
    #[case(vec![127, 239, 255, 255, 255, 255, 255, 255], f64::MAX)]
    #[case(vec![255, 239, 255, 255, 255, 255, 255, 255], f64::MIN)]
    fn test_f64_from_bytes(#[case] input: Vec<u8>, #[case] expected: f64) {
        assert_eq!(f64::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_f64_from_invalid_bytes() {
        assert_eq!(
            f64::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(SerializeError::InvalidSize)
        );
    }

    #[rstest]
    fn test_f64_serialized_size() {
        assert_eq!(f64::serialized_size(), 8);
    }

    #[rstest]
    #[case(true, vec![1])]
    #[case(false, vec![0])]
    fn test_bool_to_bytes(#[case] input: bool, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    #[case(vec![1], true)]
    #[case(vec![0], false)]
    fn test_bool_from_bytes(#[case] input: Vec<u8>, #[case] expected: bool) {
        assert_eq!(bool::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_bool_invalid_value() {
        assert_eq!(bool::from_bytes(vec![3]), Err(SerializeError::InvalidValue));
    }

    #[rstest]
    fn test_bool_serialized_size() {
        assert_eq!(bool::serialized_size(), 1);
    }

    #[rstest]
    #[case(tuple![1u8, 2u8], vec![1, 2])]
    #[case(tuple![1u8, 2u8, 3u8], vec![1, 2, 3])]
    #[case(tuple![10u8, true, false, (), 1.234f32], vec![10, 1, 0, 63, 157, 243, 182])]
    #[case(tuple![(), (), (), (), ()], vec![])]
    fn test_compound_to_bytes(#[case] input: impl BytesSerialize, #[case] expected: Vec<u8>) {
        assert_eq!(input.to_bytes(), Ok(expected));
    }

    #[rstest]
    fn test_bytes_to_compound_1() {
        type TType = tuple_type![u8, u8];

        let input = vec![1, 2];
        let expected = tuple![1u8, 2u8];

        assert_eq!(TType::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_bytes_to_compound_2() {
        type TType = tuple_type![u8, u8, u8];

        let input = vec![1, 2, 3];
        let expected = tuple![1u8, 2u8, 3u8];

        assert_eq!(TType::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_bytes_to_compound_3() {
        type TType = tuple_type![u8, bool, bool, (), f32];

        let input = vec![10, 1, 0, 63, 157, 243, 182];
        let expected = tuple![10u8, true, false, (), 1.234f32];

        assert_eq!(TType::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_bytes_to_compound_4() {
        type TType = tuple_type![(), (), (), (), ()];

        let input = vec![];
        let expected = tuple![(), (), (), (), ()];

        assert_eq!(TType::from_bytes(input), Ok(expected));
    }

    #[rstest]
    fn test_bytes_to_compound_invalid_size() {
        type TType = tuple_type![u8, u8, u8];

        let input = vec![1, 2, 3, 4];

        assert_eq!(TType::from_bytes(input), Err(SerializeError::InvalidSize));
    }

    #[rstest]
    fn test_compound_serialized_size_1() {
        assert_eq!(<tuple_type![u8, u8]>::serialized_size(), 2);
    }

    #[rstest]
    fn test_compound_serialized_size_2() {
        assert_eq!(<tuple_type![u8, u8, u8]>::serialized_size(), 3);
    }

    #[rstest]
    fn test_compound_serialized_size_3() {
        assert_eq!(<tuple_type![u8, bool, bool, (), f32]>::serialized_size(), 7);
    }

    #[rstest]
    fn test_compound_serialized_size_4() {
        assert_eq!(<tuple_type![(), (), (), (), ()]>::serialized_size(), 0);
    }
}
