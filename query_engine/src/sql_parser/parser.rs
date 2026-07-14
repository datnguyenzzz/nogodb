use crate::sql_parser::{
    ast::Statement,
    tokenizer::{Tokenizer, TokenizerError},
};
use log::debug;

pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::TokenizerError(e.message)
    }
}

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
    debug!("Parsing SQL query: {}", sql);
    let tokens = Tokenizer::new(sql).tokenize();

    Err(ParserError::ParserError("unimplemented".to_string()))
}
