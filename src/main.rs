use std::cmp::min;
use std::convert::TryInto;
use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{self, Write};
use std::mem::size_of;
use std::process;

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        return Err("Must supply a database filename".to_string());
    }
    let filename = &args[1];

    let mut table = new_table(filename)?;
    let mut buf = String::new();
    loop {
        buf.clear();
        print!("db > ");
        io::stdout().flush().map_err(|e| e.to_string())?;
        io::stdin().read_line(&mut buf).map_err(|e| e.to_string())?;
        buf = buf.trim().to_string();
        // execute meta command
        if buf.len() > 0 && buf.chars().nth(0).unwrap() == '.' {
            do_meta_command(&buf, table)
                .unwrap_or_else(|err| println!("{}: Unrecognized command '{}'", err, buf));
            // moved table in loop, need break or compiler will complain
            break;
        }
        match prepare_statement(&buf) {
            Ok(statement) => {
                execute_statement(&statement, &mut table)
                    .map_or_else(|err| println!("{}", err), |_| println!("Executed"));
            }
            Err(err) => {
                println!("{}", err);
                continue;
            }
        }
    }
    Ok(())
}

fn do_meta_command(input: &String, mut table: Table) -> Result<(), String> {
    if input == ".exit" {
        close_table(&mut table)?;
        free_table(table);
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

fn deserialize_row(source: &[u8]) -> Row {
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

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table {
    num_rows: usize,
    // pages: [[u8; PAGE_SIZE]; TABLE_MAX_PAGES],
    pager: Pager,
}

fn new_table(filename: &String) -> Result<Table, String> {
    let (pager, row_num) = new_pager(filename)?;
    Ok(Table {
        num_rows: row_num,
        pager: pager,
    })
}

fn close_table(table: &mut Table) -> Result<(), String> {
    // flush full pages
    let num_full_pages = table.num_rows / ROWS_PER_PAGE;
    for i in 0..num_full_pages {
        match table.pager.pages[i] {
            None => continue,
            Some(_) => {
                table.pager.page_flush(i, PAGE_SIZE)?;
            }
        }
    }
    // flush partial page
    let num_additional_rows = table.num_rows % ROWS_PER_PAGE;
    if num_additional_rows > 0 {
        let page_num = num_full_pages;
        table.pager.pages[page_num].map(|_| {
            table
                .pager
                .page_flush(page_num, num_additional_rows * ROW_SIZE)
        });
    }
    Ok(())
}

fn free_table(table: Table) {
    // Files are automatically closed when they go out of scope.
    let _ = table;
}

struct Pager {
    file: File,
    file_length: usize,
    pages: [Option<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES],
}

fn new_pager(path: &str) -> Result<(Pager, usize), String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    let file_length = file.metadata().map_err(|e| e.to_string())?.len();
    let num_rows = file_length as usize / ROW_SIZE;
    let pages = [None::<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES];
    Ok((
        Pager {
            file: file,
            file_length: file_length as usize,
            pages: pages,
        },
        num_rows,
    ))
}

impl Pager {
    fn get_page(&mut self, page_num: usize) -> Result<&mut [u8], String> {
        if page_num >= TABLE_MAX_PAGES {
            return Err(format!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            ));
        }

        let mut new_page: [u8; PAGE_SIZE];
        match self.pages[page_num] {
            Some(_) => {}
            None => {
                new_page = [0; PAGE_SIZE];
                // find how many page in file
                let mut num_pages = self.file_length / PAGE_SIZE;
                if self.file_length % PAGE_SIZE != 0 {
                    num_pages += 1
                }
                // read from file
                if page_num <= num_pages {
                    self.file
                        .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                        .map_err(|e| format!("seek file page error: {}", e))?;
                    self.file
                        .read(&mut new_page)
                        .map_err(|e| format!("read file page error: {}", e))?;
                }
                // cache of alloc
                self.pages[page_num] = Some(new_page);
            }
        }
        return Ok(self.pages[page_num].as_mut().unwrap());
    }

    fn page_flush(&mut self, page_num: usize, size: usize) -> Result<(), String> {
        match self.pages[page_num] {
            None => return Err("Tried to flush null page".to_string()),
            Some(page) => {
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .map_err(|e| format!("seek file page error: {}", e))?;
                self.file
                    .write(&page[..size])
                    .map_err(|e| format!("write file page error: {}", e))?;
                return Ok(());
            }
        }
    }
}

fn row_slot(table: &mut Table, row_num: usize) -> Result<&mut [u8], String> {
    let page_num = row_num / ROWS_PER_PAGE;
    let row_offset = row_num % ROWS_PER_PAGE;
    let byte_offset = row_offset * ROW_SIZE;
    let page = table.pager.get_page(page_num)?;
    Ok(&mut page[byte_offset..])
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
        // add length check
        if username.len() > COLUMN_USERNAME_SIZE || email.len() > COLUMN_EMAIL_SIZE {
            return Err("String is too long".to_string());
        }
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
        row_slot(table, table.num_rows)?,
    );
    table.num_rows += 1;
    Ok(())
}

fn execute_select(_: &Statement, table: &mut Table) -> Result<(), String> {
    for i in 0..table.num_rows {
        let row = deserialize_row(row_slot(table, i)?);
        println!("({}, {}, {})", row.id, row.username, row.email);
    }
    Ok(())
}
