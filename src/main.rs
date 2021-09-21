use std::cmp::min;
use std::convert::TryInto;
use std::io::{self, Write};
use std::mem::size_of;
use std::process;

fn main() -> Result<(), String> {
    let mut table = new_table();
    let mut buf = String::new();
    loop {
        buf.clear();
        print!("db > ");
        io::stdout().flush().map_err(|e| e.to_string())?;
        io::stdin().read_line(&mut buf).map_err(|e| e.to_string())?;
        buf = buf.trim().to_string();
        // execute meta command
        if buf.len() > 0 && buf.chars().nth(0).unwrap() == '.' {
            do_meta_command(&buf)
                .unwrap_or_else(|err| println!("{}: Unrecognized command '{}'", err, buf))
        }
        match prepare_statement(&buf) {
            Ok(statement) => {
                execute_statement(&statement, &mut table)
                    .unwrap_or_else(|err| println!("{}: Unrecognized command '{}'", err, buf));
                println!("Executed");
            }
            Err(err) => {
                println!("{}: Unrecognized command '{}'", err, buf);
                continue;
            }
        }
    }
}

fn do_meta_command(input: &String) -> Result<(), String> {
    if input == ".exit" {
        process::exit(0);
    }
    Err("Unrecognized Meta Command".to_string())
}

enum StatementType {
    Insert,
    Select,
}

const COLUMN_ID_SIZE: usize = size_of::<u32>();
const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + COLUMN_ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + COLUMN_USERNAME_SIZE;
const ROW_SIZE: usize = COLUMN_ID_SIZE + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE;

struct Row {
    id: u32,
    username: String,
    email: String,
}

fn serialize_row(source: &Row, row: &mut [u8]) {
    row[ID_OFFSET..USERNAME_OFFSET].copy_from_slice(&source.id.to_be_bytes());
    row[USERNAME_OFFSET..min(EMAIL_OFFSET, USERNAME_OFFSET + source.username.len())]
        .copy_from_slice(source.username.as_bytes());
    row[EMAIL_OFFSET..min(ROW_SIZE, EMAIL_OFFSET + source.email.len())]
        .copy_from_slice(source.email.as_bytes());
}

fn deserialize_row(source: &[u8]) -> Row {
    let id = u32::from_be_bytes(source[ID_OFFSET..USERNAME_OFFSET].try_into().unwrap());
    // it seem that last \x00 in vector will be the end of string
    let username = String::from_utf8(source[USERNAME_OFFSET..EMAIL_OFFSET].to_vec()).unwrap();
    let email = String::from_utf8(source[EMAIL_OFFSET..ROW_SIZE].to_vec()).unwrap();
    return Row {
        id: id,
        username: username,
        email: email,
    };
}

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table {
    num_rows: usize,
    pages: [[u8; PAGE_SIZE]; TABLE_MAX_PAGES],
}

fn new_table() -> Table {
    return Table {
        num_rows: 0,
        pages: [[0; PAGE_SIZE]; TABLE_MAX_PAGES],
    };
}

fn row_slot(table: &mut Table, row_num: usize) -> &mut [u8] {
    let page_num = row_num / ROWS_PER_PAGE;
    let row_offset = row_num % ROWS_PER_PAGE;
    let byte_offset = row_offset * ROW_SIZE;
    &mut table.pages[page_num][byte_offset..]
}

struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>,
}

// do we have a better way to parse statement without repeat code?
fn parse_insert_statement(input: &String) -> Result<(u32, String, String), String> {
    let mut words = input.split_whitespace();
    match words.next() {
        Some(x) => {
            if x != "insert" {
                Err("First word isn't insert")
            } else {
                Ok(())
            }
        }
        None => Err("not find any word in input"),
    }?;
    let mut id: u32 = 0;
    match words.next() {
        Some(x) => {
            id = x.parse::<u32>().map_err(|e| e.to_string())?;
            Ok(())
        }
        None => Err("not find second word"),
    }?;
    let mut username: String = "".to_string();
    match words.next() {
        Some(x) => {
            username = x.to_string();
            Ok(())
        }
        None => Err("not find thrid word"),
    }?;
    let mut email: String = "".to_string();
    match words.next() {
        Some(x) => {
            email = x.to_string();
            Ok(())
        }
        None => Err("not find 4nd word"),
    }?;
    match words.next() {
        Some(x) => Err(format!("unexpect word {} as last", x)),
        None => Ok(()),
    }?;
    Ok((id, username, email))
}

fn prepare_statement(input: &String) -> Result<Statement, String> {
    if input.starts_with("insert") {
        let (id, username, email) = parse_insert_statement(input)?;
        return Ok(Statement {
            statement_type: StatementType::Insert,
            row_to_insert: Some(Row {
                id: id,
                username: username,
                email: email,
            }),
        });
    }
    if input.starts_with("select") {
        return Ok(Statement {
            statement_type: StatementType::Select,
            row_to_insert: None,
        });
    }
    Err("Unrecognized command".to_string())
}

fn execute_statement(statement: &Statement, table: &mut Table) -> Result<(), String> {
    match statement.statement_type {
        StatementType::Insert => {
            return execute_insert(statement, table);
        }
        StatementType::Select => {
            return execute_select(statement, table);
        }
    }
}

fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), String> {
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err(format!("table is full of rows: {}", TABLE_MAX_ROWS));
    }
    serialize_row(
        &statement.row_to_insert.as_ref().unwrap(),
        row_slot(table, table.num_rows),
    );
    table.num_rows += 1;
    Ok(())
}

fn execute_select(statement: &Statement, table: &mut Table) -> Result<(), String> {
    for i in 0..table.num_rows {
        let row = deserialize_row(row_slot(table, i));
        println!("({}, {}, {})", row.id, row.username, row.email);
    }
    Ok(())
}
