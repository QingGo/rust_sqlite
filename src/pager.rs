use crate::row::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub struct Pager {
    pub file: File,
    pub file_length: usize,
    pub pages: [Option<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES],
}

pub fn new_pager(path: &str) -> Result<(Pager, usize), String> {
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
    pub fn get_page(&mut self, page_num: usize) -> Result<&mut [u8], String> {
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

    pub fn page_flush(&mut self, page_num: usize, size: usize) -> Result<(), String> {
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
