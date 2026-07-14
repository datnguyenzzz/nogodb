use crate::sql_parser::{
    ast::{
        expr::{Assignment, Expr, Ident},
        query::{Query, TableWithJoins},
    },
    tokenizer::TokenWithSpan,
};

/// INSERT statement
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#insert%20statement
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Insert {
    /// Token for the `INSERT` keyword
    insert_token: TokenWithSpan,
    /// INTO - optional keyword
    into: bool,
    /// TABLE
    table: Ident,
    /// COLUMNS
    columns: Vec<Ident>,
    /// A SQL query expression or VALUES(...) that specifies what to insert
    source: Option<Box<Query>>,
}

/// UPDATE statement.
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#update%20statement:%20searched
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Update {
    /// Token for the `UPDATE` keyword
    update_token: TokenWithSpan,
    /// TABLE
    table: Ident,
    /// Column assignments
    assignments: Vec<Assignment>,
    /// Statement where the 'FROM' clause is after the 'SET' keyword
    /// For Example: `UPDATE SET t1.name='aaa' FROM t1`
    from: Vec<TableWithJoins>,
    /// WHERE
    selection: Option<Expr>,
}

/// DELETE statement.
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#delete%20statement:%20searched
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Delete {
    /// Token for the `DELETE` keyword
    delete_token: TokenWithSpan,
    /// FROM
    from: Vec<TableWithJoins>,
    /// WHERE
    selection: Option<Expr>,
}
