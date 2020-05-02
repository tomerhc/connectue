use dialoguer::Input;
mod query_parser;
mod meta_parser;
mod schema;
mod table;
use table::Table;
use schema::{Type, Schema};


fn main() {
 
    let table_name = String::from("tomer");
    let fields = vec![
        (String::from("id"), Type::Integer, 4),
        (String::from("username"), Type::Varchar, 40),
        (String::from("grade"), Type::Float, 4),
    ];

    let schema = Schema::new(table_name, fields);
    let mut table = Table::new(schema, 10, 4096);
    main_loop(&mut table);
}

fn command_handler(inp: String, table: &mut Table) -> String {
    if meta_parser::check_meta(&inp) {
        let result = meta_parser::parse_meta(&inp);
        match result {
            Ok(out) => return out,
            Err(e) => return format!("invalid meta command \n{}", e)
        }
    } else {
        let result = query_parser::parse_query(inp, table);
        match result {
            Ok(out) => return out,
            Err(e) => return format!("invlid query \n {}", e)
        }
    }
}

fn main_loop(table: &mut Table) {
    loop {
            let inp = Input::<String>::new().with_prompt("> ").interact();
            let mut output: String = String::new();
            
            match inp {
                Err(e) => output = format!("invalid input \n{}", e),
                Ok(usr_string) =>  output = command_handler(usr_string, table)
            }
    
            println!("{}", output);
    } 
}
