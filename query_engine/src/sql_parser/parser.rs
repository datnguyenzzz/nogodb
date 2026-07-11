use log::debug;

use super::tokenizer::Tokenizer;

pub struct Parser {}

impl Parser {
    pub fn new() -> Self {
        Self {}
    }

    pub fn parse_sql(&self, sql: &str) {
        debug!("Parsing SQL query: {}", sql);
        let tokenizer = Tokenizer::new(sql);
    }
}