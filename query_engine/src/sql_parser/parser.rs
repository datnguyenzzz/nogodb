use crate::sql_parser::{
    ast::Statement,
    keywords::{
        Keyword,
        Token::{self, Whitespace},
    },
    tokenizer::{EOF_TOKEN, TokenWithSpan, Tokenizer, TokenizerError},
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

pub struct Parser {
    /// The unprocessed_index (0-indexed) of the first unprocessed token
    unprocessed_index: usize,
    /// The tokens
    tokens: Vec<TokenWithSpan>,
}

impl Default for Parser {
    fn default() -> Self {
        Parser {
            unprocessed_index: 0,
            tokens: Vec::new(),
        }
    }
}

impl Parser {
    /// Peek the (self.index + n)-th non-whitespace token that has not yet been processed
    fn peek_nth_token(&self, mut n: usize) -> &TokenWithSpan {
        let mut index = self.unprocessed_index;
        loop {
            match self.tokens.get(index) {
                Some(TokenWithSpan {
                    token: Whitespace(_),
                    span: _,
                }) => continue,
                non_whitespace => {
                    if n == 0 {
                        return non_whitespace.unwrap_or(&EOF_TOKEN);
                    }
                    n -= 1;
                }
            }
            index += 1;
        }
    }

    /// Advances the current token to the next non-whitespace token
    fn advance_token(&mut self) {
        loop {
            match self.tokens.get(self.unprocessed_index) {
                Some(TokenWithSpan {
                    token: Whitespace(_),
                    span: _,
                }) => self.unprocessed_index += 1,
                _ => break,
            }
        }
    }

    /// Check the next token if it matches an expected token, then advance
    /// it if it does
    fn check_then_consume(&mut self, expected: &Token) -> bool {
        if self.peek_nth_token(0) == expected {
            self.advance_token();
            true
        } else {
            false
        }
    }

    fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        let next_token = self.peek_nth_token(0);
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::CREATE => panic!("implement me"),
                Keyword::DELETE => panic!("implement me"),
                Keyword::INSERT => panic!("implement me"),
                Keyword::UPDATE => panic!("implement me"),
                Keyword::SELECT => panic!("implement me"),
                _ => Err(ParserError::ParserError(format!(
                    "expected a SQL statement, but got {}",
                    next_token
                ))),
            },

            // TODO: Parse query inside the (...)
            _ => Err(ParserError::ParserError(format!(
                "expected a SQL statement, but got {}",
                next_token
            ))),
        }
    }

    pub fn parse_sql(&mut self, sql: &str) -> Result<Vec<Statement>, ParserError> {
        debug!("Parsing SQL query: {}", sql);
        let tokens = Tokenizer::new(sql).tokenize()?;
        // reset the parser state
        self.unprocessed_index = 0;
        self.tokens = tokens;

        let mut stmts: Vec<Statement> = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements
            while self.check_then_consume(&Token::Colon) {
                expecting_statement_delimiter = false;
            }
            match self.peek_nth_token(0).token {
                Token::EOF => break,
                _ => {}
            }
            if expecting_statement_delimiter {
                return Err(ParserError::ParserError(
                    "Expected end of statement, but it doesn't".to_string(),
                ));
            }

            stmts.push(self.parse_statement()?);
            expecting_statement_delimiter = true;
        }

        Ok(stmts)
    }
}
