use std::convert::TryInto;

macro_rules! back_to_enum {
    ($(#[$meta:meta])* $vis:vis enum $name:ident {
        $($(#[$vmeta:meta])* $vname:ident $(= $val:expr)?,)*
    }) => {
        $(#[$meta])*
        $vis enum $name {
            $($(#[$vmeta])* $vname $(= $val)?,)*
        }

        impl std::convert::TryFrom<i32> for $name {
            type Error = ();

            fn try_from(v: i32) -> Result<Self, Self::Error> {
                match v {
                    $(x if x == $name::$vname as i32 => Ok($name::$vname),)*
                    _ => Err(()),
                }
            }
        }
    }
}
pub(crate) use back_to_enum;

pub fn read_u8(source: &[u8]) -> u8 {
    u8::from_be_bytes(source[..1].try_into().unwrap())
}

pub fn read_u32(source: &[u8]) -> u32 {
    u32::from_be_bytes(source[..4].try_into().unwrap())
}
