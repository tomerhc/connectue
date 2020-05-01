use dialoguer::Input;
mod query_parser;
mod meta_parser;
mod schema;
use schema::{Type, Schema};


fn main() {
 
    let table_name = String::from("tomer");
    let fields = vec![
        (String::from("id"), Type::Integer, 16),
        (String::from("username"), Type::Varchar, 32),
        (String::from("grade"), Type::Float, 32),
    ];

    let schema = Schema::new(table_name, fields);
    main_loop(&schema);
}

fn command_handler(inp: String, schema: &Schema) -> String {
    if meta_parser::check_meta(&inp) {
        let result = meta_parser::parse_meta(&inp);
        match result {
            Ok(out) => return out,
            Err(e) => return format!("invalid meta command \n{}", e)
        }
    } else {
        let result = query_parser::parse_query(inp, schema);
        match result {
            Ok(out) => return out,
            Err(e) => return format!("invlid query \n {}", e)
        }
    }
}

fn main_loop(schema: &Schema) {
    loop {
            let inp = Input::<String>::new().with_prompt("> ").interact();
            let mut output: String = String::new();
            
            match inp {
                Err(e) => output = format!("invalid input \n{}", e),
                Ok(usr_string) =>  output = command_handler(usr_string, schema)
            }
    
            println!("{}", output);
    } 
}
