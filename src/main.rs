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
            match do_meta_command(&buf) {
                MetaCommandResult::Success => continue,
                MetaCommandResult::UnrecognizedCommand => {
                    println!("Unrecognized command '{}'.", buf);
                    continue;
                }
            }
        }
        let (statement, prepare_result) = prepare_statement(&buf);
        match prepare_result {
            PrepareResult::Success => (),
            PrepareResult::UnrecognizedCommand => {
                println!("Unrecognized keyword at start of '{}'.", buf);
                continue;
            }
        }
        execute_statement(statement);
        println!("Executed.");
    }
}

enum MetaCommandResult {
    Success,
    UnrecognizedCommand,
}

enum PrepareResult {
    Success,
    UnrecognizedCommand,
}

fn do_meta_command(input: &String) -> MetaCommandResult {
    if input == ".exit" {
        process::exit(0);
    }
    MetaCommandResult::UnrecognizedCommand
}

enum StatementType {
    Insert,
    Select,
}

struct Statement {
    statement_type: StatementType,
}

fn prepare_statement(input: &String) -> (Statement, PrepareResult) {
    if input.starts_with("insert") {
        return (
            Statement {
                statement_type: StatementType::Insert,
            },
            PrepareResult::Success,
        );
    }
    if input.starts_with("select") {
        return (
            Statement {
                statement_type: StatementType::Select,
            },
            PrepareResult::Success,
        );
    }
    return (
        Statement {
            statement_type: StatementType::Insert,
        },
        PrepareResult::UnrecognizedCommand,
    );
}

fn execute_statement(statement: Statement) {
    match statement.statement_type {
        StatementType::Insert => println!("This is where we would do an insert."),
        StatementType::Select => println!("This is where we would do an select."),
    }
}
