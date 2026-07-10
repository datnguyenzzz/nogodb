/// A SQL parser 
pub struct Parser {}

impl Parser {
    pub fn new() -> Self {
        Self {}
    }

    pub fn parse_sql(&self, sql: &str) {
        println!("Parsing SQL query: {}", sql)
    }
}