use std::io::{Write, Read, Seek, SeekFrom};
use std::fs::{File,metadata};
use crate::table::*;
use std::io::Error;

#[derive(Debug)]
pub struct Page {
    rows_per_page: i32,
    row_count: i32, 
    buffer: Vec<Vec<u8>>
}

impl Page {
    pub fn write_row(&mut self, row: Vec<u8>) -> Result<String, TableError> {
        if self.is_full(){
            return Err(TableError::PageFull);
        } else {
            self.buffer.push(row);
            self.row_count += 1;
            return Ok(String::from("inserted row"));
        }
    }

    pub fn read_rows(&self) -> Vec<Vec<u8>>{
        self.buffer.clone()
    }


    pub fn is_full(&self) -> bool{
        self.row_count >= self.rows_per_page
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

    fn load_page(&mut self, page_num: i32, ) -> Result<(),Error> {
        let mut buff: [u8;self.page_size] = [0;self.page_size];
        let offset = page_num * self.page_size as i32;
        self.file_handler.seek(SeekFrom::Start(offset as u64));
        self.file_handler.read_exact(&mut buff)?;
        self.pages_in_mem.push((page_num, self.make_page(buff)));
        Ok(())
    }


}



