use crate::schema::{Schema, Type};
use std::io::Cursor;
use byteorder::{BigEndian,WriteBytesExt, ReadBytesExt}; // 1.3.4


#[derive(Debug)]
pub enum TableError {
    PageFull,
    TableFull,
    SerializeErr
}


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


pub struct Table { 
    pub schema: Schema,
    max_pages: i32,
    page_size: i32,
    rows_per_page: i32,
    max_rows: i32,
    pub pages: Vec<Page>
}

impl Table {
    pub fn new(s:Schema, max_pages: i32, page_size: i32) -> Self {
        let rows_per_page = page_size / s.row_length;
        let max_rows = max_pages * rows_per_page;
        let init_page = Page{
            rows_per_page: rows_per_page,
            row_count: 0,
            buffer: vec![]
        };

        Self {
            schema: s,
            max_pages: max_pages,
            page_size: page_size,
            rows_per_page: rows_per_page,
            max_rows: max_rows,
            pages: vec![init_page]
        }
    }

    pub fn write_row(&mut self, row: Vec<String>) -> Result<String,TableError>{
        if self.pages.last().unwrap().is_full() {
            self.new_page()?;
        }
        let serial_row = self.serialize_row(row)?;
        let writable_page = self.pages.last_mut().unwrap();
        writable_page.write_row(serial_row)
    }


    pub fn read_all(&self) -> Vec<Vec<String>> {
        let mut res: Vec<Vec<String>> = Vec::new();
        for page in self.pages.iter() {
            let rows = page.read_rows();
            for row in rows.into_iter(){
                let deserilized = self.deserialize_row(row);
                res.push(deserilized);
            }
        }
        res // revesed!!!! 
    }

    fn new_page(&mut self) -> Result<(),TableError> {
       if self.pages.len() >= self.max_pages as usize {
            return Err(TableError::TableFull);
       } else {
           let page = Page {
               rows_per_page: self.rows_per_page,
               row_count: 0,
               buffer: vec![]
           };
           self.pages.push(page);
       }
       Ok(())
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

