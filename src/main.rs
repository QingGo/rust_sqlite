use std::io::{self, Write};
fn main() -> Result<(), String> {
    let mut buf = String::new();
    loop {
        print!("db > ");
        io::stdout().flush().map_err(|e| e.to_string())?;
        io::stdin().read_line(&mut buf).map_err(|e| e.to_string())?;
        buf = buf.trim().to_string();
        if buf == ".exit"{
            return Ok(());
        }
        println!("Unrecognized command '{}'.", buf);
        buf.clear();
    }
}
