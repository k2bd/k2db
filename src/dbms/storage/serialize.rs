
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
