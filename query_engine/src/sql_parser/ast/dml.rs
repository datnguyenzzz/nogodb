use crate::sql_parser::ast::{
    expr::{Assignment, Expr, Ident},
    query::{Query, TableFactor},
};

/// INSERT statement
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#insert%20statement
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Insert {
    /// TABLE
    pub table: TableFactor,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// A SQL query expression or VALUES(...) that specifies what to insert
    pub source: Option<Box<Query>>,
}

/// UPDATE statement.
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#update%20statement:%20searched
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Update {
    /// TABLE
    pub table: TableFactor,
    /// Column assignments
    pub assignments: Vec<Assignment>,
    /// WHERE
    pub selection: Option<Expr>,
}

/// DELETE statement.
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#delete%20statement:%20searched
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Delete {
    /// FROM
    pub from: Vec<TableFactor>,
    /// WHERE
    pub selection: Option<Expr>,
}
