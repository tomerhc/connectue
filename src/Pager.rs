use std::io::{Write, Read, Seek, SeekFrom};
use std::fs::{File,metadata};
use crate::table::*;
use std::io::Error;

#[derive(Debug)]
pub enum PageError {
    RowNotInPage,
    PageFull,
    BufferTooLong
} 



#[derive(Debug)]
pub struct Page {
    row_length: u32,
    max_rows: u32,
    row_count: u32, 
    buffer: Vec<u8>
}

impl Page {

    pub fn new(row_length:u32, max_rows:u32) -> Page {
        Page {
            row_length: row_length,
            max_rows: max_rows,
            row_count: 0,
            buffer: vec![]
        }
    }

    pub fn write_row(&mut self, row: Vec<u8>) -> Result<String, PageError> {
        if self.is_full(){
            return Err(PageError::PageFull);
        } else {
            self.buffer.append(&mut row);
            self.row_count += 1;
            return Ok(String::from("inserted row"));
        }
    }

    pub fn write_page(&mut self, buff: &mut Vec<u8>) -> Result<(), PageError>{
        if buff.len() > (self.row_length * self.max_rows) as usize {
            return Err(PageError::BufferTooLong);
        } else {
            self.buffer.append(buff);
        }
        Ok(())
    }

    pub fn read_all(&self) -> Vec<u8>{
        self.buffer.clone()
    }

    pub fn read_row(&self, row_num: u32) -> Result<Vec<u8>, PageError> {
        if row_num > self.row_count || row_num < 0 {
            return Err(PageError::RowNotInPage);
        }
        let start = row_num * self.row_length;
        let row = &self.buffer[start as usize .. self.row_length as usize];
        Ok(Vec::from(row))
    }

    pub fn is_full(&self) -> bool{
        self.row_count >= self.max_rows
    }
}


pub struct Pager {
    pages_in_mem: Vec<(i32,Page)>,
    page_size: usize,
    file_handler: File,
    pages_in_file: i32
}

impl Pager {
    pub fn new(path: String, page_size: usize) -> Result<Self, String>{
        let file = File::open(path);
        match file {
            Ok(_) => (),
            Err(e) => return Err(format!("{}", e))
        }
        let meta = metadata(path).unwrap();

        Ok(Pager {
            pages_in_mem: Vec::new(),
            page_size: page_size,
            file_handler: file.unwrap(),
            pages_in_file: (meta.len() / page_size as u64)as i32
        })
    }

    fn load_page(&mut self, page_num: i32, table: Table) -> Result<(),Error> {
        // let mut buff: [u8;self.page_size] = [0;self.page_size];
        let mut buff_vec: Vec<u8> = vec![0;self.page_size];
        let offset = page_num * self.page_size as i32;
        self.file_handler.seek(SeekFrom::Start(offset as u64));
        self.file_handler.read_exact(&mut buff_vec)?;
        let page = Page::new(table.schema.row_length as u32, table.rows_per_page as u32);
        page.write_page(&mut buff_vec); 
        self.pages_in_mem.push((page_num, page));
        Ok(())
    }

    // pub fn get_last_page() -> Page{

    // }

    // pub fn get_page(page_num: u8) -> Page{

    // }
}



