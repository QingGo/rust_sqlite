use crate::row::*;
use crate::table::*;

enum StatementType {
    Insert,
    Select,
}

pub struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>,
}

pub fn prepare_statement(input: &String) -> Result<Statement, String> {
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

impl Statement {
    pub fn execute_statement(&self, table: &mut Table) -> Result<(), String> {
        match self.statement_type {
            StatementType::Insert => {
                return self.execute_insert(table);
            }
            StatementType::Select => {
                return self.execute_select(table);
            }
        }
    }
    fn execute_insert(&self, table: &mut Table) -> Result<(), String> {
        if table.num_rows >= TABLE_MAX_ROWS {
            return Err(format!("table is full of rows: {}", TABLE_MAX_ROWS));
        }
        let cursor = table.table_end();
        serialize_row(
            &self.row_to_insert.as_ref().unwrap(),
            table.row_slot(&cursor)?,
        );
        table.num_rows += 1;
        Ok(())
    }

    fn execute_select(&self, table: &mut Table) -> Result<(), String> {
        let mut cursor = table.table_start();
        while !table.is_end_of_table(&cursor) {
            let row = deserialize_row(table.row_slot(&cursor)?);
            println!("({}, {}, {})", row.id, row.username, row.email);
            cursor.cursor_advance();
        }
        Ok(())
    }
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