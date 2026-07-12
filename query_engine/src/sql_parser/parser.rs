use log::debug;

use super::tokenizer::Tokenizer;

#[derive(Default)]
pub struct Parser {}

impl Parser {
    pub fn parse_sql(&self, sql: &str) {
        debug!("Parsing SQL query: {}", sql);
        let tokenizer = Tokenizer::new(sql);
    }
}
