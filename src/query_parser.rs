use crate::schema::{Schema, Type};
use crate::table::Table;
use regex::Regex;
use std::collections::HashMap;

pub fn parse_query(inp: String, table: &mut Table) -> Result<String, String> {
    let mut words: Vec<&str> = inp.split_whitespace().collect();
    let key_word = words.remove(0).to_string();
    if words.is_empty() {
        return Err(String::from("not enough elements in query"))
    }
    let table_name = words.pop().unwrap().to_string();
    if &key_word == "insert" {
        let row = parse_insert(inp, &table.schema, &table_name)?;
        let res = table.write_row(row);
        match res {
            Ok(s) => return Ok(s),
            Err(e) => return Err(format!("faild insertion due to {:?}", e))
        }

    } else if &key_word == "select" {
        return Ok(String::from("selecting... "));
    }

    Err(format!("unknown keyword {}", key_word))
}

fn parse_insert(mut inp: String, schema: &Schema, table_name: &str) -> Result<Vec<String>, String> {
    let mut re_vec: Vec<(&str, Regex)> = vec![];
    let ins_re = Regex::new("^insert ").unwrap();
    let tn_re = Regex::new(format!("into {}$", table_name).as_str()).unwrap();
    
    inp = ins_re.replace(&inp, "").to_string();
    inp = tn_re.replace(&inp, "").to_string();
    
    for (field_name, t, _) in schema.fields.iter() {
        let mut re = format!("{}: ", field_name);
        let type_re = match t {
            Type::Varchar => r"'([\s*\w*\s*]*)'",
            Type::Integer => r"(\d*)",
            Type::Float => r"(\d*\.\d*)"
        };
        re.push_str(type_re);
        re_vec.push((field_name, Regex::new(&re).unwrap()));
    }

    let mut map: HashMap<String, String> = HashMap::new();

    for (field_name, reg) in re_vec.into_iter() {
        // insert an item to hashmap (field: value to insert)
        // then remove the processed words so a valide string will be completly comsumed.
        match reg.captures(&inp) {
            None => return Err(String::from("not all fields specified")),
            Some(cap) => {
                map.insert(field_name.to_string(), cap.get(1).unwrap().as_str().to_string());
            }
        }
        inp = reg.replace(&inp, "").to_string();
    }
    
    inp.retain(|c| !c.is_whitespace());
    if inp.len() > 0 {
        return Err(format!("unrecognized field {}", inp));
    }

    let mut res: Vec<String> = Vec::with_capacity(schema.fields.len());
    for (field, _, _) in schema.fields.iter() {
        res.push(map.get(field).unwrap().to_string());
    }

    Ok(res)
}
