use std::cmp::min;
use std::convert::TryInto;
use std::mem::size_of;

pub const COLUMN_ID_SIZE: usize = size_of::<u32>();
pub const COLUMN_USERNAME_SIZE: usize = 32;
pub const COLUMN_EMAIL_SIZE: usize = 255;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + COLUMN_ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + COLUMN_USERNAME_SIZE;
pub const ROW_SIZE: usize = COLUMN_ID_SIZE + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE;

pub struct Row {
    pub id: u32,
    pub username: String,
    pub email: String,
}

pub fn serialize_row(source: &Row, row: &mut [u8]) {
    row[ID_OFFSET..USERNAME_OFFSET].copy_from_slice(&source.id.to_be_bytes());
    row[USERNAME_OFFSET..min(EMAIL_OFFSET, USERNAME_OFFSET + source.username.len())]
        .copy_from_slice(source.username.as_bytes());
    row[EMAIL_OFFSET..min(ROW_SIZE, EMAIL_OFFSET + source.email.len())]
        .copy_from_slice(source.email.as_bytes());
}

pub fn deserialize_row(source: &[u8]) -> Row {
    let id = u32::from_be_bytes(source[ID_OFFSET..USERNAME_OFFSET].try_into().unwrap());
    // String::from_utf8 do not take \x00 as the end of string.
    // even result string have \x00, \x00 is unseeable in terminal
    let username = read_string_from_slice(&source[USERNAME_OFFSET..EMAIL_OFFSET]);
    let email = read_string_from_slice(&source[EMAIL_OFFSET..ROW_SIZE].to_vec());
    return Row {
        id: id,
        username: username,
        email: email,
    };
}

fn read_string_from_slice(source: &[u8]) -> String {
    let mut temp_vec: Vec<u8> = Vec::new();
    // if not include \x00, all byte in slice will be used to generate the string, which can save one byte.
    for byte in source.iter() {
        if *byte == 0x00 {
            break;
        }
        temp_vec.push(*byte)
    }
    String::from_utf8(temp_vec).unwrap()
}
