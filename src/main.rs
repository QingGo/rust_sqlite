use std::io::{self, Write};
use std::process;

fn main() -> Result<(), String> {
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
                execute_statement(statement);
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

struct Row {
    id: u32,
    username: String,
    email: String,
}

struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>,
}

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

fn execute_statement(statement: Statement) {
    match statement.statement_type {
        StatementType::Insert => println!("This is where we would do an insert"),
        StatementType::Select => println!("This is where we would do an select"),
    }
}
