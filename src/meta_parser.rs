use std::process;

pub fn check_meta(inp: &str) -> bool {
    inp.starts_with("~")
}

pub fn parse_meta(inp: &str) -> Result<String, String> {
    let inp = &inp[1..];
    if inp == "exit" {
        process::exit(1);
    }
    Err(format!("unknown command {}", inp))
}
