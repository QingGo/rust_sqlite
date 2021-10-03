use super::pager::PAGE_SIZE;
use super::row::*;
use crate::util::*;
use std::convert::TryInto;
use std::mem::size_of;

back_to_enum! {
    #[derive(Copy, Clone)]
    pub enum NodeType {
        NodeInternal = 1,
        NodeLeaf,
    }
}

// Common Node Header Layout
const NODE_TYPE_SIZE: usize = size_of::<u8>();
const NODE_TYPE_OFFSET: usize = 0;

const IS_ROOT_SIZE: usize = size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;

const PARENT_POINTER_SIZE: usize = size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;

const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf Node Header Layout
const LEAF_NODE_NUM_CELLS_SIZE: usize = size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf Node Body Layout
const LEAF_NODE_KEY_SIZE: usize = size_of::<u32>();
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;

const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

pub struct KeyValuePair {
    pub key: u32,
    pub value: Row,
}

pub struct Node {
    node_type: NodeType,
    is_root: bool,
    parent_pointer: u32,
    num_cells: u32,
    cells: [Option<KeyValuePair>; LEAF_NODE_MAX_CELLS],
}

pub fn new_node(node_type: NodeType, is_root: bool, parent_pointer: u32) -> Node {
    Node {
        node_type: node_type,
        is_root: is_root,
        parent_pointer: parent_pointer,
        num_cells: 0,
        cells: Default::default(),
    }
}

pub fn deserialize_node(source: &[u8]) -> Node {
    let node_type_num = read_u8(&source[NODE_TYPE_OFFSET..]) as i32;
    let node_type: NodeType = node_type_num.try_into().unwrap();
    let is_root = read_u8(&source[IS_ROOT_OFFSET..]) != 0;
    let parent_pointer = read_u32(&source[PARENT_POINTER_OFFSET..]);
    let num_cells = read_u32(&source[LEAF_NODE_NUM_CELLS_OFFSET..]);
    let mut cells: [Option<KeyValuePair>; LEAF_NODE_MAX_CELLS] = Default::default();
    for _i in 0..num_cells {
        let i = _i as usize;
        let key = read_u32(&source[LEAF_NODE_HEADER_SIZE + LEAF_NODE_CELL_SIZE * i..]);
        let value = deserialize_row(
            &source[LEAF_NODE_HEADER_SIZE + LEAF_NODE_CELL_SIZE * i + LEAF_NODE_KEY_SIZE
                ..LEAF_NODE_HEADER_SIZE + LEAF_NODE_CELL_SIZE * (i + 1)],
        );
        cells[i] = Some(KeyValuePair {
            key: key,
            value: value,
        })
    }
    Node {
        node_type: node_type,
        is_root: is_root,
        parent_pointer: parent_pointer,
        num_cells: num_cells,
        cells: cells,
    }
}

impl Node {
    pub fn serialize_node(&self, page: &mut [u8]) {
        page[NODE_TYPE_OFFSET..IS_ROOT_OFFSET]
            .copy_from_slice(&(self.node_type as u8).to_be_bytes());
        page[IS_ROOT_OFFSET..PARENT_POINTER_OFFSET]
            .copy_from_slice(&(self.is_root as u8).to_be_bytes());
        page[PARENT_POINTER_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET]
            .copy_from_slice(&self.parent_pointer.to_be_bytes());
        page[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_HEADER_SIZE]
            .copy_from_slice(&self.num_cells.to_be_bytes());
        let mut offset = LEAF_NODE_HEADER_SIZE;
        for i in 0..self.num_cells {
            let cell = self.cells[i as usize].as_ref().unwrap();
            page[offset..offset + LEAF_NODE_KEY_SIZE].copy_from_slice(&cell.key.to_be_bytes());
            offset += LEAF_NODE_KEY_SIZE;
            cell.value.serialize_row(&mut page[offset..]);
            offset += LEAF_NODE_VALUE_SIZE;
        }
    }

    pub fn is_node_full(&self) -> bool {
        self.num_cells as usize >= LEAF_NODE_MAX_CELLS
    }

    pub fn get_cell(&self, cell_num: usize) -> Option<&KeyValuePair> {
        self.cells[cell_num].as_ref()
    }

    pub fn insert_row(&mut self, row: Row) {
        let kv = KeyValuePair {
            key: row.id,
            value: row,
        };
        self.cells[self.num_cells as usize] = Some(kv);
        self.num_cells += 1;
    }

    pub fn get_num_cells(&self) -> u32 {
        self.num_cells
    }

    pub fn print(&self) {
        println!("leaf (size {})", self.num_cells);
        for i in 0..self.num_cells {
            println!(
                "  - {} : {}",
                i,
                self.cells[i as usize].as_ref().unwrap().key
            );
        }
    }
}

pub fn print_constants() {
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}
