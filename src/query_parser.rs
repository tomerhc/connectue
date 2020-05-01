pub fn parse_query(inp: &str) -> Result<String, String> {
    let words: Vec<&str> = inp.split_whitespace().collect();
    if words[0] == "select" {
        return Ok(String::from("making a selection..."));
    } else if words[0] == "insert" {
        return Ok(String::from("iserting somthing"));
    }

    Err(format!("unknown keyword {}", words[0]))
}

