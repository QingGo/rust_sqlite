use crate::pager::*;
use crate::row::*;

pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    pub num_rows: usize,
    // pages: [[u8; PAGE_SIZE]; TABLE_MAX_PAGES],
    pub pager: Pager,
}

pub fn new_table(filename: &String) -> Result<Table, String> {
    let (pager, row_num) = new_pager(filename)?;
    Ok(Table {
        num_rows: row_num,
        pager: pager,
    })
}

pub fn close_table(table: &mut Table) -> Result<(), String> {
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

pub fn free_table(table: Table) {
    // Files are automatically closed when they go out of scope.
    let _ = table;
}
