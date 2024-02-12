use std::fs::{File, OpenOptions};
use std::io::{Error, Write};
use std::path::Path;

const LOGS_DIR: &str = "/Users/yareko/github/logging-service/core/logs";

fn create_or_open_log_stream(name: &str) -> Result<File, Error> {
    let path = Path::new(LOGS_DIR).join(format!("{}", name));
    OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
}

pub fn create(name: &str, message: &str) -> Result<String, Error> {
    println!("name: {}, message: {}", name, message);
    let mut file = create_or_open_log_stream(name).unwrap();
    file.write(message.as_bytes()).unwrap();
    Ok(name.to_string())
}