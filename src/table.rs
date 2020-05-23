use crate::schema::{Schema, Type};
use crate::Pager::*;
use std::io::Cursor;
use byteorder::{BigEndian,WriteBytesExt, ReadBytesExt}; // 1.3.4


#[derive(Debug)]
pub enum TableError {
    TableFull,
    SerializeErr, 
    WriteError
}


pub struct Table { 
    pub schema: Schema,
    pager: Pager,
    max_pages: u32,
    page_size: u32,
    pub rows_per_page: u32,
    pub max_rows: u32,
    pub pages: Vec<Page>
}

impl Table {
    pub fn new(s:Schema, max_pages: u32, page_size: u32, path: String) -> Self {
        let rows_per_page = page_size / s.row_length;
        let max_rows = max_pages * rows_per_page;
        let init_page = Page{
            row_length: s.row_length,
            max_rows: rows_per_page,
            row_count: 0,
            buffer: vec![]
        };

        Self {
            schema: s,
            pager: Pager::new(path, page_size).unwrap(),
            max_pages: max_pages,
            page_size: page_size,
            rows_per_page: rows_per_page,
            max_rows: max_rows,
            pages: vec![init_page]
        }
    }

    pub fn write_row(&mut self, row: Vec<String>) -> Result<String,TableError>{
        if self.pages.is_empty() || self.pages.last().unwrap().is_full(){
            self.pages.push(self.pager.new_page(self.schema.row_length, self.rows_per_page));
        }
        let serial_row = self.serialize_row(row)?;
        let writable_page = self.pages.last_mut().unwrap();
        match writable_page.write_row(serial_row) {
            Ok(s) => Ok(s),
            Err(e) => {
                println!("{:?}", e);
                return Err(TableError::WriteError)
            }
        }
    }


    pub fn read_all(&self) -> Vec<Vec<String>> {
        let mut res: Vec<Vec<String>> = Vec::new();
        for page in self.pages.iter() {
            let rows = page.read_all();
            for row in rows.into_iter(){ 
                let deserilized = self.deserialize_row(row);
                res.push(deserilized);
            }
        }
        res // revesed!!!! 
    }


    fn serialize_row(&self, row: Vec<String>) -> Result<Vec<u8>, TableError> {
        let mut buffer: Vec<u8> = Vec::with_capacity(self.schema.row_length as usize);
        for (index,val) in row.into_iter().enumerate(){
            let (_name, t, len) = &self.schema.fields[index];
            match t {
                Type::Varchar => {
                   let mut v = val.into_bytes();
                   if v.len() > *len as usize {
                        return Err(TableError::SerializeErr);
                   } else if v.len() < *len as usize {
                        v.append(&mut vec![0; *len as usize -v.len() ]);
                   }
                   buffer.append(&mut v);
                },
                Type::Integer => {
                    let v = val.parse::<i32>();
                    match v { 
                        Ok(i) => buffer.write_i32::<BigEndian>(i).unwrap(),
                        Err(_) => return Err(TableError::SerializeErr)
                   }
                },
                Type::Float => {
                    let v = val.parse::<f32>();
                    match v { 
                        Ok(f) => buffer.write_f32::<BigEndian>(f).unwrap(),
                        Err(_) => return Err(TableError::SerializeErr)
                   }
                }
            }
        }
        if buffer.len() == self.schema.row_length as usize {
            return Ok(buffer);
        } else {
            return Err(TableError::SerializeErr);
        }
    }

    fn deserialize_row(&self, mut row: Vec<u8>) -> Vec<String> {
        let mut res: Vec<String> = Vec::with_capacity(self.schema.fields.len());
        for (_, t, offset) in self.schema.offsets.iter().rev() {
            let buff = row.split_off(*offset as usize);
            match t {
                Type::Varchar => {
                    res.push(String::from_utf8_lossy(&buff[..]).to_mut().replace("\u{0}",""));        
                },
                Type::Integer => {
                    let int = Cursor::new(&buff[..]).read_i32::<BigEndian>().unwrap();
                    res.push(format!("{}",int));
                },
                Type::Float => {
                    let float = Cursor::new(&buff[..]).read_f32::<BigEndian>().unwrap();
                    res.push(format!("{}",float));
                }
           }
       }
       res    
    }


}

