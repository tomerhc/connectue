use dialoguer::Input;
mod query_parser;
mod meta_parser;
mod schema;
mod table;
mod Pager;
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
            println!("{:?}", table.read_all());
    } 
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn insertion_test(){
        let table_name = String::from("tomer");
        let fields = vec![
            (String::from("id"), Type::Integer, 4),
            (String::from("username"), Type::Varchar, 40),
            (String::from("grade"), Type::Float, 4),
        ];

        let schema = Schema::new(table_name, fields);
        let mut table = Table::new(schema, 10, 4096);

       let input = String::from("insert id: 1 username: 'tomerh' grade: 1.2 into tomer");
        assert_eq!(command_handler(input, &mut table), "inserted row"); 

    }

    #[test]
    fn fill_table_test(){
        let table_name = String::from("tomer");
        let fields = vec![
            (String::from("id"), Type::Integer, 4),
            (String::from("username"), Type::Varchar, 40),
            (String::from("grade"), Type::Float, 4),
        ];

        let schema = Schema::new(table_name, fields);
        let mut table = Table::new(schema, 5, 100);
        for i in 0..20 {
            let input = format!("insert id: {} username: 'tomerh{}' grade: {}.2 into tomer", i,i,i);
            command_handler(input,&mut table);
        }
        let input = String::from("insert id: 1 username: 'tomerh' grade: 1.2 into tomer");
        assert_eq!(command_handler(input, &mut table), "invlid query \n faild insertion due to TableFull"); 
    }
    
    #[test]
    fn string_too_long(){
        let table_name = String::from("tomer");
        let fields = vec![
            (String::from("id"), Type::Integer, 4),
            (String::from("username"), Type::Varchar, 40),
            (String::from("grade"), Type::Float, 4),
        ];

        let schema = Schema::new(table_name, fields);
        let mut table = Table::new(schema, 10, 4096);

       let input = String::from("insert id: 1 username: 't claknsclkasnlksnclkasnclkasnlcksnlskanc   cnananana  omerh' grade: 1.2 into tomer");
        assert_eq!(command_handler(input, &mut table), "invlid query \n faild insertion due to SerializeErr"); 
    }
}
