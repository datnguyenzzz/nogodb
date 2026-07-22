use core::fmt;
use std::str::FromStr;

use crate::sql_parser::{
    ast::{
        Statement,
        data_type::{DataType, ExactNumberInfo},
        ddl::{ColumnDef, CreateTable},
        dml::Delete,
        expr::{CastKind, Expr, Ident, Value},
        operators::{BinaryOperator, UnaryOperator},
        query::TableFactor,
    },
    keywords::{
        Keyword,
        Token::{self, Whitespace},
    },
    precedence::{self, prec_unknown},
    tokenizer::{EOF_TOKEN, Location, TokenWithSpan, Tokenizer, TokenizerError},
};
use log::debug;

#[derive(Debug)]
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

macro_rules! p {
    ($prec:ident) => {
        precedence::prec_value(precedence::Precedence::$prec)
    };
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
            self.unprocessed_index += 1;
            match self.tokens.get(self.unprocessed_index) {
                Some(TokenWithSpan {
                    token: Whitespace(_),
                    span: _,
                }) => continue,
                _ => break,
            }
        }
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
    fn check_then_consume_keyword(&mut self, expected: Keyword) -> Result<(), ParserError> {
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
        let curr_token = self.peek_nth_token(0).clone();
        self.advance_token();
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
        let token = self.peek_nth_token(0).clone();
        self.advance_token();
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

        let current_token = self.peek_nth_token(0);
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
        match &self.peek_nth_token(0).token {
            Token::Word(w) => match w.keyword {
                Keyword::BIGINT => {
                    self.advance_token();
                    match self.parse_precision_scale()? {
                        ExactNumberInfo::PrecisionAndScale(_, _) => Err(ParserError::ParserError(
                            format!("do not allow `scale` in the BIGINT precision"),
                        )),
                        ExactNumberInfo::Precision(p) => Ok(DataType::BigInt(Some(p))),
                        ExactNumberInfo::None => Ok(DataType::BigInt(None)),
                    }
                }
                Keyword::BOOLEAN => {
                    self.advance_token();
                    Ok(DataType::Boolean)
                }
                Keyword::DATE => {
                    self.advance_token();
                    Ok(DataType::Date)
                }
                Keyword::DOUBLE => {
                    self.advance_token();
                    match self.check_then_consume_keyword(Keyword::PRECISION) {
                        Ok(_) => {
                            if let Ok(_) = self.check_then_consume_keyword(Keyword::UNSIGNED) {
                                Ok(DataType::DoublePrecisionUnsigned)
                            } else {
                                Ok(DataType::DoublePrecision)
                            }
                        }
                        Err(_) => {
                            let precision = self.parse_precision_scale()?;
                            Ok(DataType::Double(precision))
                        }
                    }
                }
                Keyword::FLOAT => {
                    self.advance_token();
                    let precision = self.parse_precision_scale()?;
                    Ok(DataType::Float(precision))
                }
                Keyword::INT => {
                    self.advance_token();
                    match self.parse_precision_scale()? {
                        ExactNumberInfo::PrecisionAndScale(_, _) => Err(ParserError::ParserError(
                            format!("do not allow `scale` in the INT precision"),
                        )),
                        ExactNumberInfo::Precision(p) => Ok(DataType::Int(Some(p))),
                        ExactNumberInfo::None => Ok(DataType::Int(None)),
                    }
                }
                Keyword::VARCHAR => {
                    self.advance_token();
                    Ok(DataType::Varchar(self.parse_character_length()?))
                }
                _ => Err(ParserError::ParserError(format!(
                    "Unrecognised data type keyword, got {}",
                    w.keyword
                ))),
            },
            t => Err(ParserError::ParserError(format!(
                "Expected data type token, but {}",
                t,
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
            match &self.peek_nth_token(0).token {
                Token::Word(_) => columns.push(self.parse_column_def()?),
                Token::RParen => {
                    self.advance_token();
                    break;
                }
                Token::Comma => {
                    self.advance_token();
                }
                t => {
                    return Err(ParserError::ParserError(format!(
                        "Expected column, got {}",
                        t,
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
        Ok(CreateTable {
            table_name: self.parse_ident()?,
            columns: self.parse_columns()?,
        })
    }

    /// Parse `CREATE <something>`` statement
    fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if let Ok(_) = self.check_then_consume_keyword(Keyword::TABLE) {
            self.parse_create_table().map(Into::into)
        } else {
            Err(ParserError::ParserError(format!(
                "Unrecognised object for creating, got {}",
                self.peek_nth_token(0),
            )))
        }
    }

    /// Get the precedence of the token
    fn get_prec(&self) -> u8 {
        match &self.peek_nth_token(0).token {
            Token::Word(w) => match w.keyword {
                Keyword::AND => p!(And),
                Keyword::OR => p!(Or),
                Keyword::XOR => p!(Xor),
                Keyword::NOT => match &self.peek_nth_token(1).token {
                    Token::Word(w) if w.keyword == Keyword::IN => p!(Between),
                    Token::Word(w) if w.keyword == Keyword::BETWEEN => p!(Between),
                    Token::Word(w) if w.keyword == Keyword::LIKE => p!(Like),
                    Token::Word(w) if w.keyword == Keyword::REGEXP => p!(Like),
                    _ => prec_unknown(),
                },
                Keyword::IN => p!(Between),
                Keyword::BETWEEN => p!(Between),
                Keyword::LIKE => p!(Like),
                Keyword::REGEXP => p!(Like),
                _ => prec_unknown(),
            },
            Token::Eq
            | Token::Lt
            | Token::LtEq
            | Token::Neq
            | Token::Gt
            | Token::GtEq
            | Token::DoubleEq => p!(Eq),
            Token::Plus | Token::Minus => p!(PlusMinus),
            Token::Mul | Token::Div | Token::Mod | Token::StringConcat => p!(MulDivMod),
            Token::LBracket => p!(DoubleColon),

            _ => prec_unknown(),
        }
    }

    /// Parse a new expression.
    /// Implementation of a Pratt operator precedence parser, https://en.wikipedia.org/wiki/Operator-precedence_parser
    fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        self.parse_expr_by_prec(0)
    }

    fn parse_expr_by_prec(&mut self, min_prec: u8) -> Result<Expr, ParserError> {
        // parse left prefix node
        let mut lhs = self.parse_expr_prefix()?;

        // infix parse loop: keep consuming while the operations bind tighter than min_prec

        loop {
            let prec = self.get_prec();
            if min_prec >= prec {
                break;
            }

            self.advance_token();
            lhs = self.parse_expr_infix(lhs, prec + 1)?; // left associativity
        }

        Ok(lhs)
    }

    /// Parse an operator following an expression
    fn parse_expr_infix(&mut self, lhs: Expr, prec: u8) -> Result<Expr, ParserError> {
        let span = &self.peek_nth_token(0).span;
        let binary_op = match &self.peek_nth_token(0).token {
            Token::DoubleEq => Some(BinaryOperator::Eq),
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mul => Some(BinaryOperator::Multiply),
            Token::Mod => Some(BinaryOperator::Modulo),
            Token::StringConcat => Some(BinaryOperator::StringConcat),
            Token::Div => Some(BinaryOperator::Divide),
            Token::Word(w) => match w.keyword {
                Keyword::AND => Some(BinaryOperator::And),
                Keyword::OR => Some(BinaryOperator::Or),
                Keyword::XOR => Some(BinaryOperator::Xor),
                _ => None,
            },
            _ => None,
        };

        match binary_op {
            Some(op) => {
                self.advance_token();
                Ok(Expr::BinaryOp {
                    left: Box::new(lhs),
                    op,
                    right: Box::new(self.parse_expr_by_prec(prec)?),
                })
            }
            None => Err(ParserError::ParserError(format!(
                "no infix expression at {}",
                span.start,
            ))),
        }
    }

    /// Parse an expression prefix. Such as leading atom or unary op produces left node
    fn parse_expr_prefix(&mut self) -> Result<Expr, ParserError> {
        match &self.peek_nth_token(0).token {
            Token::Word(w) => match w.keyword {
                Keyword::CAST => {
                    self.advance_token();
                    self.parse_cast_expr(CastKind::Cast)
                }
                Keyword::TRY_CAST => {
                    self.advance_token();
                    self.parse_cast_expr(CastKind::TryCast)
                }
                Keyword::CEIL => {
                    self.advance_token();
                    self.parse_ceil_floor_expr(true)
                }
                Keyword::FLOOR => {
                    self.advance_token();
                    self.parse_ceil_floor_expr(false)
                }
                // TODO: Support parsing interval expression, e.g INTERVAL '1' DAY
                Keyword::NOT => {
                    self.advance_token();
                    self.parse_not_expr()
                }
                k => Err(ParserError::ParserError(format!(
                    "not supported keyword as a prefix of an expression, got {}",
                    k,
                ))),
            },
            tok @ Token::Plus | tok @ Token::Minus => {
                let op = if tok == &Token::Plus {
                    UnaryOperator::Plus
                } else {
                    UnaryOperator::Minus
                };

                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_expr_by_prec(p!(MulDivMod))?),
                })
            }
            Token::Number(_, _) | Token::SingleQuotedString(_) | Token::DoubleQuotedString(_) => {
                self.advance_token();
                self.parse_value()
            }
            Token::LParen => {
                self.advance_token();
                let inner = self.parse_expr()?;
                self.check_then_consume(&Token::RParen)?;
                Ok(Expr::Nested(Box::new(inner)))
            }
            e => Err(ParserError::ParserError(format!(
                "expected an expression, got {}",
                e,
            ))),
        }
    }

    /// Parse a literal value (numbers, strings, date/time, booleans)
    fn parse_value(&mut self) -> Result<Expr, ParserError> {
        let to_expr = |v: Value| Ok(Expr::Value(v));
        let span = &self.peek_nth_token(0).span;
        match &self.peek_nth_token(0).token {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => to_expr(Value::Boolean(true)),
                Keyword::FALSE => to_expr(Value::Boolean(false)),
                Keyword::NULL => to_expr(Value::Null),
                Keyword::NoKeyWord if w.quote.is_some() => match w.quote {
                    Some('"') => to_expr(Value::DoubleQuotedString(w.value.clone())),
                    Some('\'') => to_expr(Value::SingleQuotedString(w.value.clone())),
                    _ => Err(ParserError::ParserError(format!(
                        "unknown quote in a concrete value, got {}",
                        w,
                    ))),
                },
                e => Err(ParserError::ParserError(format!(
                    "expected a concrete value, got {}",
                    e,
                ))),
            },
            Token::Number(n, l) => to_expr(Value::Number(Self::cast(n.clone(), span.start)?, *l)),
            Token::SingleQuotedString(s) => to_expr(Value::SingleQuotedString(s.clone())),
            Token::DoubleQuotedString(s) => to_expr(Value::DoubleQuotedString(s.clone())),

            e => Err(ParserError::ParserError(format!(
                "expected a concrete value, got {}",
                e,
            ))),
        }
    }

    /// Parse a `NOT` expression.
    fn parse_not_expr(&mut self) -> Result<Expr, ParserError> {
        Ok(Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(self.parse_expr_by_prec(p!(UnaryNot))?),
        })
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    fn parse_cast_expr(&mut self, cast_kind: CastKind) -> Result<Expr, ParserError> {
        self.check_then_consume(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.check_then_consume_keyword(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        self.check_then_consume(&Token::RParen)?;

        Ok(Expr::Cast {
            kind: cast_kind,
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse a CEIL/FLOOR(expr)
    fn parse_ceil_floor_expr(&mut self, is_ceil: bool) -> Result<Expr, ParserError> {
        self.check_then_consume(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.check_then_consume(&Token::RParen)?;
        if is_ceil {
            Ok(Expr::Ceil {
                expr: Box::new(expr),
            })
        } else {
            Ok(Expr::Floor {
                expr: Box::new(expr),
            })
        }
    }

    /// Parse `DELETE FROM <table>` statement
    /// Syntax:
    /// DELETE FROM table_name WHERE condition;
    fn parse_delete_from_table(&mut self) -> Result<Delete, ParserError> {
        let from = vec![TableFactor::Table {
            name: self.parse_ident()?,
            alias: None,
        }];
        let mut selection: Option<Expr> = None;
        if let Ok(_) = self.check_then_consume_keyword(Keyword::WHERE) {
            selection = Some(self.parse_expr()?);
        }

        Ok(Delete {
            from: from,
            selection: selection,
        })
    }

    /// Parse `DELETE ...` statement
    fn parse_delete(&mut self) -> Result<Statement, ParserError> {
        self.advance_token();
        match self.check_then_consume_keyword(Keyword::FROM) {
            Ok(_) => {
                self.advance_token();
                self.parse_delete_from_table().map(Into::into)
            }
            Err(_) => Err(ParserError::ParserError(format!(
                "Expected FROM in Delete statement, got {}",
                self.peek_nth_token(0)
            ))),
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
                Keyword::DELETE => {
                    self.advance_token();
                    self.parse_delete()
                }
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
