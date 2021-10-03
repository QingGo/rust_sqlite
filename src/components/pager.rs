// use super::row::*;
use super::node::*;
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
    pub pages: [Option<Node>; TABLE_MAX_PAGES],
    pub num_pages: usize,
}

pub fn new_pager(path: &str) -> Result<(Pager, u32), String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    let file_length = file.metadata().map_err(|e| e.to_string())?.len();
    // can not use Default::default() because the count of elements exceed 31
    #[warn(non_upper_case_globals)]
    const TEMP_NONE_PAGE: Option<Node> = None::<Node>;
    let pages = [TEMP_NONE_PAGE; TABLE_MAX_PAGES];
    Ok((
        Pager {
            file: file,
            file_length: file_length as usize,
            pages: pages,
            num_pages: 0,
        },
        0,
    ))
}

impl Pager {
    pub fn get_page(&mut self, page_num: usize) -> Result<&mut Node, String> {
        if page_num >= TABLE_MAX_PAGES {
            return Err(format!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            ));
        }

        // let mut new_page: [u8; PAGE_SIZE];
        match self.pages[page_num] {
            Some(_) => {}
            None => {
                let node: Node;
                // find how many page in file
                self.num_pages = self.file_length / PAGE_SIZE;
                // read from file
                if page_num < self.num_pages {
                    self.file
                        .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                        .map_err(|e| format!("seek file page error: {}", e))?;
                    let mut node_raw = [0; PAGE_SIZE];
                    self.file
                        .read(&mut node_raw)
                        .map_err(|e| format!("read file page error: {}", e))?;
                    node = deserialize_node(&node_raw)
                } else {
                    node = new_node(NodeType::NodeLeaf, page_num == 0, 0);
                    self.num_pages += 1;
                }
                // cache of alloc
                self.pages[page_num] = Some(node);
            }
        }
        return Ok(self.pages[page_num].as_mut().unwrap());
    }

    pub fn page_flush(&mut self, page_num: usize) -> Result<(), String> {
        match &self.pages[page_num] {
            None => return Err("Tried to flush null page".to_string()),
            Some(node) => {
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .map_err(|e| format!("seek file page error: {}", e))?;
                let mut node_raw = [0; PAGE_SIZE];
                node.serialize_node(&mut node_raw);
                self.file
                    .write(&node_raw)
                    .map_err(|e| format!("write file page error: {}", e))?;
                return Ok(());
            }
        }
    }
}
