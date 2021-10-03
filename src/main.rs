mod components;
mod util;

use components::statement::prepare_statement;
use components::table::*;
use std::env;
use std::io::{self, Write};

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        return Err("Must supply a database filename".to_string());
    }
    let filename = &args[1];

    let mut table = new_table(filename)?;
    let mut buf = String::new();
    loop {
        // prompt hint and read input
        buf.clear();
        print!("db > ");
        io::stdout().flush().map_err(|e| e.to_string())?;
        io::stdin().read_line(&mut buf).map_err(|e| e.to_string())?;
        buf = buf.trim().to_string();
        // execute meta command
        if buf.len() > 0 && buf.chars().nth(0).unwrap() == '.' {
            if buf == ".exit" {
                close_table(&mut table)?;
                free_table(table);
                // moved table in loop, need break or compiler will complain
                break;
            } else if buf == ".btree" {
                println!("Tree:");
                table.pager.get_page(0).unwrap().print();
            } else if buf == ".constants" {
                println!("Constants:");
                print_constants();
            } else {
                println!("Unrecognized Meta Command");
            }
        } else {
            // execute normal statement
            match prepare_statement(&buf) {
                Ok(statement) => {
                    statement
                        .execute_statement(&mut table)
                        .map_or_else(|err| println!("{}", err), |_| println!("Executed"));
                }
                Err(err) => {
                    println!("{}", err);
                    continue;
                }
            }
        }
    }
    Ok(())
}
