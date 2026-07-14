use std::hash::Hash;

use crate::sql_parser::{
    ast::{
        operators::{BinaryOperator, UnaryOperator},
        query::Select,
    },
    tokenizer::{Span, TokenWithSpan},
};

/// An identifier, decomposed into its value or character data and the quote style.
#[derive(Debug, Clone)]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub value: String,
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: Option<char>,
    /// The span of the identifier in the original SQL string.
    pub span: Span,
}

impl PartialEq for Ident {
    fn eq(&self, other: &Self) -> bool {
        // we ignore spans in comparisons
        self.value == other.value && self.quote_style == other.quote_style
    }
}

impl Eq for Ident {}

impl Hash for Ident {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.quote_style.hash(state);
    }
}

/// An SQL expression of any type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    /// Identifier e.g. table name or column name
    Identifier(Ident),
    /// `IS FALSE` operator
    IsFalse(Box<Expr>),
    /// `IS NOT FALSE` operator
    IsNotFalse(Box<Expr>),
    /// `IS TRUE` operator
    IsTrue(Box<Expr>),
    /// `IS NOT TRUE` operator
    IsNotTrue(Box<Expr>),
    /// `IS NULL` operator
    IsNull(Box<Expr>),
    /// `IS NOT NULL` operator
    IsNotNull(Box<Expr>),
    // TODO: Support [NOT] IN(...)
    // TODO: Support [NOT] BETWEEN(...)
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        /// Left operand.
        left: Box<Expr>,
        /// Operator between operands.
        op: BinaryOperator,
        /// Right operand.
        right: Box<Expr>,
    },
    /// Unary operation e.g. `NOT foo`
    UnaryOp {
        /// The unary operator (e.g., `NOT`, `-`).
        op: UnaryOperator,
        /// Operand expression.
        expr: Box<Expr>,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    // TODO: Support sub-query
    /// An unqualified `*` wildcard token (e.g. `*`).
    Wildcard(TokenWithSpan),
}

/// A item `T` enclosed in a pair of parentheses
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Parens<T> {
    /// the opening parenthesis token, i.e. `(`
    pub opening_token: TokenWithSpan,
    /// content enclosed in parentheses
    pub content: T,
    /// the closing parenthesis token, i.e. `)`
    pub closing_token: TokenWithSpan,
}

/// Represent a query expression body
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#query%20expression%20body
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetExpr {
    /// Restricted SELECT .. FROM .. HAVING
    Select(Box<Select>),
    // TODO: Support UNION/EXCEPT/INTERSECT of two queries
    /// `VALUES (...)`
    Values(Parens<Vec<Expr>>),
}

/// SQL assignment `foo = expr` as used in SQL Update
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#set%20clause%20list
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Assignment {
    /// The left-hand side of the assignment, a single column
    target: Ident,
    /// The expression assigned to the target.
    value: Expr,
}
