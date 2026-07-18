use core::fmt;
use std::{fmt::format, str::FromStr};

use crate::sql_parser::{
    ast::{
        Statement,
        data_type::{DataType, ExactNumberInfo},
        ddl::{ColumnDef, CreateTable},
        expr::Ident,
    },
    keywords::{
        Keyword,
        Token::{self, RParen, Whitespace},
    },
    tokenizer::{EOF_TOKEN, Location, TokenWithSpan, Tokenizer, TokenizerError},
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
    fn cast<T: FromStr>(s: String, loc: Location) -> Result<T, ParserError>
    where
        <T as FromStr>::Err: fmt::Display,
    {
        s.parse::<T>().map_err(|e| {
            ParserError::ParserError(format!(
                "Could not parse '{s}' as {}: {e}{loc}",
                core::any::type_name::<T>(),
            ))
        })
    }

    /// Peek the (self.index + n)-th non-whitespace token that is unprocessed
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

    /// Returns a reference to the current token
    fn peek_current_token(&self) -> &TokenWithSpan {
        return self
            .tokens
            .get(self.unprocessed_index.saturating_sub(1))
            .unwrap_or(&EOF_TOKEN);
    }

    /// Advances to the next non-whitespace token and returns a copy.
    ///
    /// Please use [`Self::advance_token`] and [`Self::peek_current_token`] to
    /// avoid the copy.
    fn next_token(&mut self) -> TokenWithSpan {
        self.advance_token();
        self.peek_current_token().clone()
    }

    /// Check the next token if it matches an expected token, then advance
    /// it if it does
    fn check_then_consume(&mut self, expected: &Token) -> Result<(), ParserError> {
        if self.peek_nth_token(0) == expected {
            self.advance_token();
            Ok(())
        } else {
            Err(ParserError::ParserError(format!(
                "Expected token: {}, but got {}",
                expected,
                self.peek_nth_token(0)
            )))
        }
    }

    /// If the current token is the `expected` keyword, consume it and returns
    /// true. Otherwise, no tokens are consumed and returns false.
    fn parse_keyword(&mut self, expected: Keyword) -> Result<(), ParserError> {
        if matches!(&self.peek_nth_token(0).token, Token::Word(w) if w.keyword == expected) {
            self.advance_token();
            Ok(())
        } else {
            Err(ParserError::ParserError(format!(
                "Expected keyword: {}, but got {}",
                expected,
                self.peek_nth_token(0)
            )))
        }
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    fn parse_ident(&mut self) -> Result<Ident, ParserError> {
        let curr_token = self.next_token();
        match curr_token.token {
            Token::Word(w) => Ok(w.into_ident(curr_token.span)),
            _ => Err(ParserError::ParserError(format!(
                "Expected ident token, but {}",
                curr_token
            ))),
        }
    }

    /// Parse an unsigned literal integer/long
    fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        let token = self.next_token();
        match token.token {
            Token::Number(s, _) => Self::cast::<u64>(s, token.span.start),
            _ => Err(ParserError::ParserError(format!(
                "expected literal int, got {}",
                token
            ))),
        }
    }

    /// Parse an optionally signed integer literal.
    fn parse_signed_integer(&mut self) -> Result<i64, ParserError> {
        let is_negative = self.check_then_consume(&Token::Minus).is_ok();

        if !is_negative {
            let _ = self.check_then_consume(&Token::Plus);
        }

        let current_token = self.peek_current_token();
        match &current_token.token {
            Token::Number(s, _) => {
                let v = Self::cast::<i64>(s.clone(), current_token.span.start)?;
                self.advance_token();
                Ok(if is_negative { -v } else { v })
            }
            t => Err(ParserError::ParserError(format!(
                "expected number, got {}",
                t
            ))),
        }
    }

    /// Parse an optional character length specification `(n)`.
    fn parse_character_length(&mut self) -> Result<Option<u64>, ParserError> {
        match self.check_then_consume(&Token::LParen) {
            Ok(_) => {
                let length = self.parse_literal_uint()?;
                let _ = self.check_then_consume(&Token::RParen)?;
                Ok(Some(length))
            }
            Err(_) => Ok(None),
        }
    }

    /// Parse number precision/scale info like `(precision[, scale])` for decimal types.
    fn parse_precision_scale(&mut self) -> Result<ExactNumberInfo, ParserError> {
        match self.check_then_consume(&Token::LParen) {
            Ok(_) => {
                let precision = self.parse_literal_uint()?;
                let scale = match self.check_then_consume(&Token::Comma) {
                    Ok(_) => Some(self.parse_signed_integer()?),
                    Err(_) => None,
                };

                let _ = self.check_then_consume(&Token::RParen)?;

                match scale {
                    Some(scale) => Ok(ExactNumberInfo::PrecisionAndScale(precision, scale)),
                    None => Ok(ExactNumberInfo::Precision(precision)),
                }
            }
            Err(_) => Ok(ExactNumberInfo::None),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    fn parse_data_type(&mut self) -> Result<DataType, ParserError> {
        self.advance_token();
        match &self.peek_current_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::BIGINT => match self.parse_precision_scale()? {
                    ExactNumberInfo::PrecisionAndScale(_, _) => Err(ParserError::ParserError(
                        format!("do not allow `scale` in the BIGINT precision"),
                    )),
                    ExactNumberInfo::Precision(p) => Ok(DataType::BigInt(Some(p))),
                    ExactNumberInfo::None => Ok(DataType::BigInt(None)),
                },
                Keyword::BOOLEAN => Ok(DataType::Boolean),
                Keyword::DATE => Ok(DataType::Date),
                Keyword::DOUBLE => match self.parse_keyword(Keyword::PRECISION) {
                    Ok(_) => {
                        if let Ok(_) = self.parse_keyword(Keyword::UNSIGNED) {
                            Ok(DataType::DoublePrecisionUnsigned)
                        } else {
                            Ok(DataType::DoublePrecision)
                        }
                    }
                    Err(_) => {
                        let precision = self.parse_precision_scale()?;
                        Ok(DataType::Double(precision))
                    }
                },
                Keyword::FLOAT => {
                    let precision = self.parse_precision_scale()?;
                    Ok(DataType::Float(precision))
                }
                Keyword::INT => match self.parse_precision_scale()? {
                    ExactNumberInfo::PrecisionAndScale(_, _) => Err(ParserError::ParserError(
                        format!("do not allow `scale` in the INT precision"),
                    )),
                    ExactNumberInfo::Precision(p) => Ok(DataType::Int(Some(p))),
                    ExactNumberInfo::None => Ok(DataType::Int(None)),
                },
                Keyword::VARCHAR => Ok(DataType::Varchar(self.parse_character_length()?)),
                _ => Err(ParserError::ParserError(format!(
                    "Unrecognised data type keyword, got {}",
                    w.keyword
                ))),
            },
            _ => Err(ParserError::ParserError(format!(
                "Expected data type token, but {}",
                self.peek_current_token()
            ))),
        }
    }

    /// Parse column definition.
    pub fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        Ok(ColumnDef {
            name: self.parse_ident()?,
            data_type: self.parse_data_type()?,
        })
    }

    /// Parse columns
    // TODO: Support `constraint`
    fn parse_columns(&mut self) -> Result<Vec<ColumnDef>, ParserError> {
        self.check_then_consume(&Token::LParen)?;
        let mut columns: Vec<ColumnDef> = vec![];

        loop {
            match &self.peek_current_token().token {
                Token::Word(_) => columns.push(self.parse_column_def()?),
                Token::RParen => {
                    self.advance_token();
                    break;
                }
                Token::Comma => {
                    self.advance_token();
                }
                _ => {
                    return Err(ParserError::ParserError(format!(
                        "Expected column, got {}",
                        self.peek_current_token()
                    )));
                }
            }
        }

        Ok(columns)
    }

    /// Parse `CREATE TABLE` statement
    /// Create a new table:
    ///     CREATE TABLE table_name (
    ///         column1 datatype constraint,
    ///         column2 datatype constraint,
    ///         column3 datatype constraint,
    ///         ....
    ///     );
    // TODO: Create a new table from an existing one
    //     CREATE TABLE new_table AS
    //     SELECT column1, column2,...
    //     FROM existing_table
    //     WHERE ....;
    fn parse_create_table(&mut self) -> Result<CreateTable, ParserError> {
        let table_name = self.parse_ident()?;
        let columns = self.parse_columns()?;
        Ok(CreateTable {
            table_name: table_name,
            columns: columns,
        })
    }

    /// Parse `CREATE <something>`` statement
    fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if let Ok(_) = self.parse_keyword(Keyword::TABLE) {
            self.parse_create_table().map(Into::into)
        } else {
            Err(ParserError::ParserError(format!(
                "Unrecognised object for creating, but got {}",
                self.peek_nth_token(0),
            )))
        }
    }

    fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        let next_token = self.peek_nth_token(0);
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::CREATE => {
                    self.advance_token();
                    self.parse_create()
                }
                Keyword::DELETE => panic!("implement me"),
                Keyword::INSERT => panic!("implement me"),
                Keyword::UPDATE => panic!("implement me"),
                Keyword::SELECT => panic!("implement me"),
                _ => Err(ParserError::ParserError(format!(
                    "expected a SQL statement, but got {}",
                    next_token
                ))),
            },

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
            while let Ok(_) = self.check_then_consume(&Token::Colon) {
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
