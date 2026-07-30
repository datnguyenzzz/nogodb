use core::fmt;
use std::str::FromStr;

use crate::sql_parser::{
    ast::{
        Statement,
        data_type::{DataType, ExactNumberInfo},
        ddl::{ColumnDef, CreateTable},
        dml::{Delete, Insert, Update},
        expr::{Assignment, CastKind, Expr, Ident, Parens, SetExpr, Value},
        operators::{BinaryOperator, UnaryOperator},
        query::{
            Join, JoinConstraint, JoinOperator, OrderByExpr, OrderBySort, Query, Select,
            SelectItem, TableFactor, TableWithJoins,
        },
    },
    keywords::{
        Keyword,
        Token::{self, Whitespace},
        search_keyword,
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
            index += 1;
            match self.tokens.get(index - 1) {
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

    /// Peek the current token and advance past it. Use self.peek_nth_token()
    /// then self.advance() if don't want to clone a token
    fn peek_then_advance(&mut self) -> TokenWithSpan {
        loop {
            match self.tokens.get(self.unprocessed_index) {
                Some(TokenWithSpan {
                    token: Whitespace(_),
                    span: _,
                }) => self.unprocessed_index += 1,
                Some(t) => {
                    loop {
                        self.unprocessed_index += 1;
                        if let Some(tws) = self.tokens.get(self.unprocessed_index)
                            && matches!(tws.token, Token::Whitespace(_))
                        {
                            continue;
                        }

                        break;
                    }
                    return t.clone();
                }
                None => {
                    return EOF_TOKEN.clone();
                }
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
        let curr_token = self.peek_then_advance();
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
        let token = self.peek_then_advance();
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
    fn parse_columns(&mut self) -> Result<Vec<ColumnDef>, ParserError> {
        self.check_then_consume(&Token::LParen)?;
        let column_def = self.parse_separated(&Token::Comma, |p| p.parse_column_def())?;
        self.check_then_consume(&Token::RParen)?;

        Ok(column_def)
    }

    /// Parse `CREATE TABLE` statement
    /// Create a new table:
    ///     CREATE TABLE table_name (
    ///         column1 datatype constraint,
    ///         column2 datatype constraint,
    ///         column3 datatype constraint,
    ///         ....
    ///     );
    fn parse_create_table(&mut self) -> Result<CreateTable, ParserError> {
        Ok(CreateTable {
            table_name: TableFactor::Table {
                name: self.parse_ident()?,
                alias: None,
            },
            columns: self.parse_columns()?,
        })
    }

    /// Parse `CREATE <something>`` statement
    fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if self.check_then_consume_keyword(Keyword::TABLE).is_ok() {
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
                Keyword::TRUE | Keyword::FALSE => self.parse_value(),
                Keyword::NULL => self.parse_value(),
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
                _ => {
                    // either Identifier or CompoundIdentifier
                    let idents = self.parse_separated(&Token::Period, |p| p.parse_ident())?;
                    if idents.len() == 1 {
                        Ok(Expr::Identifier(idents[0].clone()))
                    } else {
                        Ok(Expr::CompoundIdentifier(idents))
                    }
                }
            },
            tok @ Token::Plus | tok @ Token::Minus => {
                let op = if tok == &Token::Plus {
                    UnaryOperator::Plus
                } else {
                    UnaryOperator::Minus
                };

                self.advance_token();
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_expr_by_prec(p!(MulDivMod))?),
                })
            }
            Token::Number(_, _) | Token::SingleQuotedString(_) | Token::DoubleQuotedString(_) => {
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
        let current_token = self.peek_then_advance();
        let span = current_token.span;
        match current_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => to_expr(Value::Boolean(true)),
                Keyword::FALSE => to_expr(Value::Boolean(false)),
                Keyword::NULL => to_expr(Value::Null),
                e => Err(ParserError::ParserError(format!(
                    "expected a concrete value, got {}",
                    e,
                ))),
            },
            Token::Number(n, l) => to_expr(Value::Number(Self::cast(n, span.start)?, l)),
            Token::SingleQuotedString(s) => to_expr(Value::SingleQuotedString(s)),
            Token::DoubleQuotedString(s) => to_expr(Value::DoubleQuotedString(s)),

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
        match self.check_then_consume_keyword(Keyword::FROM) {
            Ok(_) => self.parse_delete_from_table().map(Into::into),
            Err(_) => Err(ParserError::ParserError(format!(
                "Expected FROM in Delete statement, got {}",
                self.peek_nth_token(0)
            ))),
        }
    }

    /// Parse a separated list of 1+ items by a given delimiter,
    /// then apply the sub-parser F into each item
    fn parse_separated<T, F>(&mut self, delimiter: &Token, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);

            if self.check_then_consume(delimiter).is_err() {
                break;
            }
        }
        Ok(values)
    }

    /// Parse an alias for a select list item and return it
    fn parse_select_item_alias(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.check_then_consume_keyword(Keyword::AS).is_err() {
            return Ok(None);
        }

        let curr = self.peek_then_advance();
        match curr.token {
            Token::Word(w) if search_keyword(&w.value) == Keyword::NoKeyWord => {
                Ok(Some(w.into_ident(curr.span)))
            }
            e => Err(ParserError::ParserError(format!(
                "expected an identifier after AS, got {}",
                e
            ))),
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    fn parse_select_item(&mut self) -> Result<SelectItem, ParserError> {
        // SELECT * ....
        if self.check_then_consume(&Token::Mul).is_ok() {
            if matches!(self.peek_nth_token(0).token, Token::Comma) {
                return Err(ParserError::ParserError(format!(
                    "syntax error, SELECT * ,"
                )));
            }
            return Ok(SelectItem::Wildcard);
        }

        match self.parse_expr()? {
            Expr::Identifier(id) if id.value.to_lowercase() == "from" => {
                Err(ParserError::ParserError(format!(
                    "there is no select item, must be SELECT <something> FROM ..."
                )))
            }
            expr => match self.parse_select_item_alias()? {
                Some(alias) => Ok(SelectItem::NamedExpr { expr, alias }),
                None => Ok(SelectItem::Expr(expr)),
            },
        }
    }

    fn parse_join_constraint(&mut self) -> Result<JoinConstraint, ParserError> {
        self.check_then_consume_keyword(Keyword::ON)?;
        Ok(JoinConstraint::On(self.parse_expr()?))
    }

    /// Parse Joins syntax <op>JOIN <table> ON <condition>
    fn parse_joins(&mut self) -> Result<Option<Vec<Join>>, ParserError> {
        let mut joins = vec![];
        loop {
            let join_op = match &self.peek_nth_token(0).token {
                Token::Word(w) => match w.keyword {
                    Keyword::JOIN => {
                        self.advance_token();
                        JoinOperator::Join
                    }
                    kw @ Keyword::LEFT | kw @ Keyword::RIGHT | kw @ Keyword::INNER => {
                        self.advance_token();
                        self.check_then_consume_keyword(Keyword::JOIN)?;
                        match kw {
                            Keyword::LEFT => JoinOperator::Left,
                            Keyword::RIGHT => JoinOperator::Right,
                            Keyword::INNER => JoinOperator::Inner,
                            _ => break,
                        }
                    }
                    Keyword::FULL => {
                        self.advance_token();
                        self.check_then_consume_keyword(Keyword::OUTER)?;
                        self.check_then_consume_keyword(Keyword::JOIN)?;
                        JoinOperator::FullOuter
                    }
                    _ => break,
                },
                _ => break,
            };

            joins.push(Join {
                relation: TableFactor::Table {
                    name: self.parse_ident()?,
                    alias: None,
                },
                join_operator: join_op(self.parse_join_constraint()?),
            })
        }

        if joins.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(joins))
        }
    }

    /// Parse a table factor followed by any join clauses
    fn parse_table_with_join(&mut self) -> Result<TableWithJoins, ParserError> {
        Ok(TableWithJoins {
            relation: TableFactor::Table {
                name: self.parse_ident()?,
                alias: None,
            },
            joins: self.parse_joins()?,
        })
    }

    /// Parse an optional `GROUP BY` clause, used in SELECT statement
    fn parse_optional_group_by(&mut self) -> Result<Option<Vec<Expr>>, ParserError> {
        if self.check_then_consume_keyword(Keyword::GROUP).is_err() {
            return Ok(None);
        }

        if self.check_then_consume_keyword(Keyword::BY).is_err() {
            return Ok(None);
        }

        let idents = self.parse_separated(&Token::Comma, |p| p.parse_expr())?;

        if idents.len() == 0 {
            return Err(ParserError::ParserError(format!(
                "expect some columns after GROUP BY"
            )));
        }

        Ok(Some(idents))
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`)
    fn parse_select(&mut self) -> Result<Select, ParserError> {
        let projection = self.parse_separated(&Token::Comma, |p| p.parse_select_item())?;
        if projection.len() == 0 {
            return Err(ParserError::ParserError(format!(
                "select items can not be empty in SELECT statement",
            )));
        }
        self.check_then_consume_keyword(Keyword::FROM)?;
        let table = self.parse_table_with_join()?;
        let mut selection = None;
        if self.check_then_consume_keyword(Keyword::WHERE).is_ok() {
            selection = Some(self.parse_expr()?);
        }
        let group_by = self.parse_optional_group_by()?;

        Ok(Select {
            projections: projection,
            from: table,
            selection,
            group_by,
        })
    }

    /// Parse a "query body". At the moment it has not supported set operation
    /// of multiple query body yet, such as UNION, EXCEPT, ...
    fn parse_query_body(&mut self) -> Result<Box<SetExpr>, ParserError> {
        match &self.peek_nth_token(0).token {
            Token::Word(w) => match w.keyword {
                Keyword::SELECT => {
                    self.advance_token();
                    Ok(Box::new(SetExpr::Select(
                        self.parse_select().map(Box::new)?,
                    )))
                }
                Keyword::VALUES => {
                    self.advance_token();
                    self.check_then_consume(&Token::LParen)?;
                    let values = self.parse_separated(&Token::Comma, |p| p.parse_expr())?;
                    self.check_then_consume(&Token::RParen)?;
                    Ok(Box::new(SetExpr::Values(Parens { content: values })))
                }
                kw => Err(ParserError::ParserError(format!(
                    "unrecognised keyword in a query body, got {}",
                    kw
                ))),
            },
            t => Err(ParserError::ParserError(format!(
                "unrecognised token in a query body, got {}",
                t
            ))),
        }
    }

    /// Parse a single `ORDER BY` expression
    fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParserError> {
        let expr = self.parse_expr()?;
        let sort = match &self.peek_nth_token(0).token {
            Token::Word(w) => match w.keyword {
                Keyword::ASC => Some(OrderBySort::Asc),
                Keyword::DESC => Some(OrderBySort::Desc),
                _ => None,
            },
            _ => None,
        };

        if sort.is_some() {
            self.advance_token();
        }

        Ok(OrderByExpr { expr, sort })
    }

    /// Parse an optional `ORDER BY` clause
    fn parse_optional_order_by(&mut self) -> Result<Option<Vec<OrderByExpr>>, ParserError> {
        if self.check_then_consume_keyword(Keyword::ORDER).is_err() {
            return Ok(None);
        }

        self.check_then_consume_keyword(Keyword::BY)?;
        Ok(Some(self.parse_separated(&Token::Comma, |p| {
            p.parse_order_by_expr()
        })?))
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`.
    fn parse_query(&mut self) -> Result<Box<Query>, ParserError> {
        Ok(Box::new(Query {
            body: self.parse_query_body()?,
            order_by: self.parse_optional_order_by()?,
        }))
    }

    /// The SQL INSERT INTO <table> Statement
    /// Syntax:
    ///   INSERT INTO table_name [Optional(column1, column2, column3, ...)]
    ///   VALUES (value1, value2, value3, ...);
    fn parse_insert_into_table(&mut self) -> Result<Insert, ParserError> {
        let table_name = TableFactor::Table {
            name: self.parse_ident()?,
            alias: None,
        };
        let mut columns = vec![];
        if self.check_then_consume(&Token::LParen).is_ok() {
            columns = self.parse_separated(&Token::Comma, |p| p.parse_ident())?;
            self.check_then_consume(&Token::RParen)?;
        }
        let source = Some(self.parse_query()?);
        Ok(Insert {
            table: table_name,
            columns: columns,
            source: source,
        })
    }

    /// Parse `INSERT <something>` statement
    fn parse_insert(&mut self) -> Result<Statement, ParserError> {
        match self.check_then_consume_keyword(Keyword::INTO) {
            Ok(_) => self.parse_insert_into_table().map(Into::into),
            Err(_) => Err(ParserError::ParserError(format!(
                "Expected INTO in INSERT statement, got {}",
                self.peek_nth_token(0)
            ))),
        }
    }

    /// Parse a `var = expr` assignment
    fn parse_assignment(&mut self) -> Result<Assignment, ParserError> {
        let lhs = self.parse_ident()?;
        self.check_then_consume(&Token::Eq)?;
        let rhs = self.parse_expr()?;
        Ok(Assignment {
            target: lhs,
            value: rhs,
        })
    }

    /// Parse an `UPDATE` statement
    /// Syntax
    ///   UPDATE table_name
    ///   SET column1 = value1, column2 = value2, ...
    ///   WHERE condition;
    fn parse_update(&mut self) -> Result<Update, ParserError> {
        let table_name = TableFactor::Table {
            name: self.parse_ident()?,
            alias: None,
        };
        self.check_then_consume_keyword(Keyword::SET)?;
        let assignments = self.parse_separated(&Token::Comma, |p| p.parse_assignment())?;
        let mut conds = None;
        if self.check_then_consume_keyword(Keyword::WHERE).is_ok() {
            conds = Some(self.parse_expr()?);
        }

        Ok(Update {
            table: table_name,
            assignments: assignments,
            selection: conds,
        })
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
                Keyword::INSERT => {
                    self.advance_token();
                    self.parse_insert()
                }
                Keyword::UPDATE => {
                    self.advance_token();
                    self.parse_update().map(Into::into)
                }
                Keyword::SELECT | Keyword::WITH => self.parse_query().map(Into::into),
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

        if matches!(
            self.tokens.first().map(|t| &t.token),
            Some(Token::Whitespace(_))
        ) {
            self.advance_token();
        }

        let mut stmts: Vec<Statement> = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements
            while let Ok(_) = self.check_then_consume(&Token::SemiColon) {
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
