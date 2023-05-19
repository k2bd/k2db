/// Trait for serializing and deserializing a struct to and from a fixed size
/// byte array.
pub trait BytesSerialize<const SIZE: usize> {
    fn to_bytes(&self) -> [u8; SIZE];
    fn from_bytes(bytes: [u8; SIZE]) -> Self;
}

impl BytesSerialize<1> for u8 {
    fn to_bytes(&self) -> [u8; 1] {
        [*self]
    }

    fn from_bytes(bytes: [u8; 1]) -> Self {
        bytes[0]
    }
}

impl BytesSerialize<2> for u16 {
    fn to_bytes(&self) -> [u8; 2] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 2]) -> Self {
        u16::from_be_bytes(bytes)
    }
}

impl BytesSerialize<4> for u32 {
    fn to_bytes(&self) -> [u8; 4] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 4]) -> Self {
        u32::from_be_bytes(bytes)
    }
}

impl BytesSerialize<8> for u64 {
    fn to_bytes(&self) -> [u8; 8] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 8]) -> Self {
        u64::from_be_bytes(bytes)
    }
}

impl BytesSerialize<16> for u128 {
    fn to_bytes(&self) -> [u8; 16] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 16]) -> Self {
        u128::from_be_bytes(bytes)
    }
}

impl BytesSerialize<1> for i8 {
    fn to_bytes(&self) -> [u8; 1] {
        [*self as u8]
    }

    fn from_bytes(bytes: [u8; 1]) -> Self {
        i8::from_be_bytes(bytes)
    }
}

impl BytesSerialize<2> for i16 {
    fn to_bytes(&self) -> [u8; 2] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 2]) -> Self {
        i16::from_be_bytes(bytes)
    }
}

impl BytesSerialize<4> for i32 {
    fn to_bytes(&self) -> [u8; 4] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 4]) -> Self {
        i32::from_be_bytes(bytes)
    }
}

impl BytesSerialize<8> for i64 {
    fn to_bytes(&self) -> [u8; 8] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 8]) -> Self {
        i64::from_be_bytes(bytes)
    }
}

impl BytesSerialize<16> for i128 {
    fn to_bytes(&self) -> [u8; 16] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 16]) -> Self {
        i128::from_be_bytes(bytes)
    }
}

impl BytesSerialize<4> for f32 {
    fn to_bytes(&self) -> [u8; 4] {
        self.to_bits().to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 4]) -> Self {
        f32::from_bits(u32::from_be_bytes(bytes))
    }
}

impl BytesSerialize<8> for f64 {
    fn to_bytes(&self) -> [u8; 8] {
        self.to_bits().to_be_bytes()
    }

    fn from_bytes(bytes: [u8; 8]) -> Self {
        f64::from_bits(u64::from_be_bytes(bytes))
    }
}

impl BytesSerialize<1> for bool {
    fn to_bytes(&self) -> [u8; 1] {
        if *self {
            [1]
        } else {
            [0]
        }
    }

    fn from_bytes(bytes: [u8; 1]) -> Self {
        match bytes[0] {
            0 => false,
            1 => true,
            _ => panic!("Invalid bool value"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case(0u8, [0])]
    #[case(1u8, [1])]
    #[case(255u8, [255])]
    fn test_u8_to_bytes(#[case] input: u8, #[case] expected: [u8; 1]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0], 0u8)]
    #[case([1], 1u8)]
    #[case([255], 255u8)]
    fn test_u8_from_bytes(#[case] input: [u8; 1], #[case] expected: u8) {
        assert_eq!(u8::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0u16, [0, 0])]
    #[case(1u16, [0, 1])]
    #[case(255u16, [0, 255])]
    #[case(256u16, [1, 0])]
    #[case(65535u16, [255, 255])]
    fn test_u16_to_bytes(#[case] input: u16, #[case] expected: [u8; 2]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0, 0], 0u16)]
    #[case([0, 1], 1u16)]
    #[case([0, 255], 255u16)]
    #[case([1, 0], 256u16)]
    #[case([255, 255], 65535u16)]
    fn test_u16_from_bytes(#[case] input: [u8; 2], #[case] expected: u16) {
        assert_eq!(u16::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0u32, [0, 0, 0, 0])]
    #[case(1u32, [0, 0, 0, 1])]
    #[case(255u32, [0, 0, 0, 255])]
    #[case(256u32, [0, 0, 1, 0])]
    #[case(65535u32, [0, 0, 255, 255])]
    #[case(65536u32, [0, 1, 0, 0])]
    #[case(16777215u32, [0, 255, 255, 255])]
    #[case(16777216u32, [1, 0, 0, 0])]
    #[case(4294967295u32, [255, 255, 255, 255])]
    fn test_u32_to_bytes(#[case] input: u32, #[case] expected: [u8; 4]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0, 0, 0, 0], 0u32)]
    #[case([0, 0, 0, 1], 1u32)]
    #[case([0, 0, 0, 255], 255u32)]
    #[case([0, 0, 1, 0], 256u32)]
    #[case([0, 0, 255, 255], 65535u32)]
    #[case([0, 1, 0, 0], 65536u32)]
    #[case([0, 255, 255, 255], 16777215u32)]
    #[case([1, 0, 0, 0], 16777216u32)]
    #[case([255, 255, 255, 255], 4294967295u32)]
    fn test_u32_from_bytes(#[case] input: [u8; 4], #[case] expected: u32) {
        assert_eq!(u32::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0u64, [0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(1u64, [0, 0, 0, 0, 0, 0, 0, 1])]
    #[case(255u64, [0, 0, 0, 0, 0, 0, 0, 255])]
    #[case(256u64, [0, 0, 0, 0, 0, 0, 1, 0])]
    #[case(65535u64, [0, 0, 0, 0, 0, 0, 255, 255])]
    #[case(65536u64, [0, 0, 0, 0, 0, 1, 0, 0])]
    #[case(16777215u64, [0, 0, 0, 0, 0, 255, 255, 255])]
    #[case(16777216u64, [0, 0, 0, 0, 1, 0, 0, 0])]
    #[case(4294967295u64, [0, 0, 0, 0, 255, 255, 255, 255])]
    #[case(4294967296u64, [0, 0, 0, 1, 0, 0, 0, 0])]
    #[case(1099511627775u64, [0, 0, 0, 255, 255, 255, 255, 255])]
    #[case(1099511627776u64, [0, 0, 1, 0, 0, 0, 0, 0])]
    #[case(281474976710655u64, [0, 0, 255, 255, 255, 255, 255, 255])]
    #[case(281474976710656u64, [0, 1, 0, 0, 0, 0, 0, 0])]
    #[case(72057594037927935u64, [0, 255, 255, 255, 255, 255, 255, 255])]
    #[case(72057594037927936u64, [1, 0, 0, 0, 0, 0, 0, 0])]
    #[case(18446744073709551615u64, [255, 255, 255, 255, 255, 255, 255, 255])]
    fn test_u64_to_bytes(#[case] input: u64, #[case] expected: [u8; 8]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0, 0, 0, 0, 0, 0, 0, 0], 0u64)]
    #[case([0, 0, 0, 0, 0, 0, 0, 1], 1u64)]
    #[case([0, 0, 0, 0, 0, 0, 0, 255], 255u64)]
    #[case([0, 0, 0, 0, 0, 0, 1, 0], 256u64)]
    #[case([0, 0, 0, 0, 0, 0, 255, 255], 65535u64)]
    #[case([0, 0, 0, 0, 0, 1, 0, 0], 65536u64)]
    #[case([0, 0, 0, 0, 0, 255, 255, 255], 16777215u64)]
    #[case([0, 0, 0, 0, 1, 0, 0, 0], 16777216u64)]
    #[case([0, 0, 0, 0, 255, 255, 255, 255], 4294967295u64)]
    #[case([0, 0, 0, 1, 0, 0, 0, 0], 4294967296u64)]
    #[case([0, 0, 0, 255, 255, 255, 255, 255], 1099511627775u64)]
    #[case([0, 0, 0, 255, 255, 255, 255, 255], 1099511627775u64)]
    #[case([0, 0, 1, 0, 0, 0, 0, 0], 1099511627776u64)]
    #[case([0, 0, 255, 255, 255, 255, 255, 255], 281474976710655u64)]
    #[case([0, 1, 0, 0, 0, 0, 0, 0], 281474976710656u64)]
    #[case([0, 255, 255, 255, 255, 255, 255, 255], 72057594037927935u64)]
    #[case([1, 0, 0, 0, 0, 0, 0, 0], 72057594037927936u64)]
    #[case([255, 255, 255, 255, 255, 255, 255, 255], 18446744073709551615u64)]
    fn test_u64_from_bytes(#[case] input: [u8; 8], #[case] expected: u64) {
        assert_eq!(u64::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0u128, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(1u128, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])]
    #[case(18446744073709551615u128, [0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255])]
    #[case(18446744073709551616u128, [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(u128::MAX, [255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255])]
    fn test_u128_to_bytes(#[case] input: u128, #[case] expected: [u8; 16]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0, 0, 0, 0, 0, 0, 0, 0, 00, 0, 0, 0, 0, 0, 0, 0], 0u128)]
    #[case([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 1u128)]
    #[case([0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255], 18446744073709551615u128)]
    #[case([0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0], 18446744073709551616u128)]
    #[case([255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255], u128::MAX)]
    fn test_u128_from_bytes(#[case] input: [u8; 16], #[case] expected: u128) {
        assert_eq!(u128::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0i8, [0])]
    #[case(1i8, [1])]
    #[case(-1i8, [255])]
    #[case(i8::MAX, [127])]
    #[case(i8::MIN, [128])]
    fn test_i8_to_bytes(#[case] input: i8, #[case] expected: [u8; 1]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0], 0i8)]
    #[case([1], 1i8)]
    #[case([255], -1i8)]
    #[case([127], i8::MAX)]
    #[case([128], i8::MIN)]
    fn test_i8_from_bytes(#[case] input: [u8; 1], #[case] expected: i8) {
        assert_eq!(i8::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0i16, [0, 0])]
    #[case(1i16, [0, 1])]
    #[case(-1i16, [255, 255])]
    #[case(i16::MAX, [127, 255])]
    #[case(i16::MIN, [128, 0])]
    fn test_i16_to_bytes(#[case] input: i16, #[case] expected: [u8; 2]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0, 0], 0i16)]
    #[case([0, 1], 1i16)]
    #[case([255, 255], -1i16)]
    #[case([127, 255], i16::MAX)]
    #[case([128, 0], i16::MIN)]
    fn test_i16_from_bytes(#[case] input: [u8; 2], #[case] expected: i16) {
        assert_eq!(i16::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0i32, [0, 0, 0, 0])]
    #[case(1i32, [0, 0, 0, 1])]
    #[case(-1i32, [255, 255, 255, 255])]
    #[case(i32::MAX, [127, 255, 255, 255])]
    #[case(i32::MIN, [128, 0, 0, 0])]
    fn test_i32_to_bytes(#[case] input: i32, #[case] expected: [u8; 4]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0, 0, 0, 0], 0i32)]
    #[case([0, 0, 0, 1], 1i32)]
    #[case([255, 255, 255, 255], -1i32)]
    #[case([127, 255, 255, 255], i32::MAX)]
    #[case([128, 0, 0, 0], i32::MIN)]
    fn test_i32_from_bytes(#[case] input: [u8; 4], #[case] expected: i32) {
        assert_eq!(i32::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0i64, [0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(1i64, [0, 0, 0, 0, 0, 0, 0, 1])]
    #[case(-1i64, [255, 255, 255, 255, 255, 255, 255, 255])]
    #[case(i64::MAX, [127, 255, 255, 255, 255, 255, 255, 255])]
    #[case(i64::MIN, [128, 0, 0, 0, 0, 0, 0, 0])]
    fn test_i64_to_bytes(#[case] input: i64, #[case] expected: [u8; 8]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0, 0, 0, 0, 0, 0, 0, 0], 0i64)]
    #[case([0, 0, 0, 0, 0, 0, 0, 1], 1i64)]
    #[case([255, 255, 255, 255, 255, 255, 255, 255], -1i64)]
    #[case([127, 255, 255, 255, 255, 255, 255, 255], i64::MAX)]
    #[case([128, 0, 0, 0, 0, 0, 0, 0], i64::MIN)]
    fn test_i64_from_bytes(#[case] input: [u8; 8], #[case] expected: i64) {
        assert_eq!(i64::from_bytes(input), expected);
    }

    #[rstest]
    #[case(0i128, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]
    #[case(1i128, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])]
    #[case(-1i128, [255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255])]
    #[case(i128::MAX, [127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255])]
    #[case(i128::MIN, [128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]
    fn test_i128_to_bytes(#[case] input: i128, #[case] expected: [u8; 16]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 0i128)]
    #[case([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 1i128)]
    #[case([255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255], -1i128)]
    #[case([127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255], i128::MAX)]
    #[case([128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], i128::MIN)]
    fn test_i128_from_bytes(#[case] input: [u8; 16], #[case] expected: i128) {
        assert_eq!(i128::from_bytes(input), expected);
    }

    #[rstest]
    #[case(1.234f32, [63, 157, 243, 182])]
    #[case(-1.234f32, [191, 157, 243, 182])]
    #[case(f32::MAX, [127, 127, 255, 255])]
    #[case(f32::MIN, [255, 127, 255, 255])]
    fn test_f32_to_bytes(#[case] input: f32, #[case] expected: [u8; 4]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([63, 157, 243, 182], 1.234f32)]
    #[case([191, 157, 243, 182], -1.234f32)]
    #[case([127, 127, 255, 255], f32::MAX)]
    #[case([255, 127, 255, 255], f32::MIN)]
    fn test_f32_from_bytes(#[case] input: [u8; 4], #[case] expected: f32) {
        assert_eq!(f32::from_bytes(input), expected);
    }

    #[rstest]
    #[case(1.234f64, [63, 243, 190, 118, 200, 180, 57, 88])]
    #[case(-1.234f64, [191, 243, 190, 118, 200, 180, 57, 88])]
    #[case(f64::MAX, [127, 239, 255, 255, 255, 255, 255, 255])]
    #[case(f64::MIN, [255, 239, 255, 255, 255, 255, 255, 255])]
    fn test_f64_to_bytes(#[case] input: f64, #[case] expected: [u8; 8]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([63, 243, 190, 118, 200, 180, 57, 88], 1.234f64)]
    #[case([191, 243, 190, 118, 200, 180, 57, 88], -1.234f64)]
    #[case([127, 239, 255, 255, 255, 255, 255, 255], f64::MAX)]
    #[case([255, 239, 255, 255, 255, 255, 255, 255], f64::MIN)]
    fn test_f64_from_bytes(#[case] input: [u8; 8], #[case] expected: f64) {
        assert_eq!(f64::from_bytes(input), expected);
    }

    #[rstest]
    #[case(true, [1])]
    #[case(false, [0])]
    fn test_bool_to_bytes(#[case] input: bool, #[case] expected: [u8; 1]) {
        assert_eq!(input.to_bytes(), expected);
    }

    #[rstest]
    #[case([1], true)]
    #[case([0], false)]
    fn test_bool_from_bytes(#[case] input: [u8; 1], #[case] expected: bool) {
        assert_eq!(bool::from_bytes(input), expected);
    }
}
