use super::pager::*;
use super::row::*;

// pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
// pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    pub root_page_num: u32,
    pub pager: Pager,
}

pub fn new_table(filename: &String) -> Result<Table, String> {
    let (pager, root_page_num) = new_pager(filename)?;
    Ok(Table {
        root_page_num: root_page_num,
        pager: pager,
    })
}

pub fn close_table(table: &mut Table) -> Result<(), String> {
    for i in 0..table.pager.num_pages {
        match table.pager.pages[i] {
            None => continue,
            Some(_) => {
                table.pager.page_flush(i)?;
            }
        }
    }
    Ok(())
}

pub fn free_table(table: Table) {
    // Files are automatically closed when they go out of scope.
    let _ = table;
}

impl Table {
    pub fn table_start(&self) -> Cursor {
        Cursor {
            page_num: self.root_page_num,
            cell_num: 0,
        }
    }
    pub fn table_end(&mut self) -> Result<Cursor, String> {
        let node = self.pager.get_page(self.root_page_num as usize).unwrap();
        if node.is_node_full() {
            return Err("Node is full".to_string());
        }
        Ok(Cursor {
            page_num: self.root_page_num,
            cell_num: node.get_num_cells(),
        })
    }
    pub fn is_end_of_table(&mut self, cursor: &Cursor) -> bool {
        let node = self.pager.get_page(self.root_page_num as usize).unwrap();
        cursor.cell_num == node.get_num_cells()
    }
    pub fn cursor_value(&self, cursor: &Cursor) -> Option<&Row> {
        let node = self.pager.pages[cursor.page_num as usize].as_ref()?;
        let cell = node.get_cell(cursor.cell_num as usize)?;
        Some(&cell.value)
    }

    pub fn insert_row(&mut self, row: Row) -> Result<(), String> {
        let cursor = self.table_end()?;
        let node = self.pager.pages[cursor.page_num as usize]
            .as_mut()
            .ok_or("page not exist")?;
        if node.is_node_full() {
            return Err("Fail to insert, Node is full".to_string());
        }
        node.insert_row(row);
        Ok(())
    }
}

pub struct Cursor {
    page_num: u32,
    cell_num: u32,
}

impl Cursor {
    pub fn cursor_advance(&mut self) {
        self.cell_num += 1;
    }
}

pub fn print_constants() {
    super::node::print_constants();
}
