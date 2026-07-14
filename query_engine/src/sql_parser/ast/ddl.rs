use crate::sql_parser::ast::{data_type::DataType, expr::Ident, query::Query};

/// SQL column definition
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#table%20contents%20source
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef {
    /// Column name.
    name: Ident,
    /// Column data type.
    data_type: DataType,
    // TODO: Support default options
}

/// CREATE TABLE statement.
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#table%20definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTable {
    /// Table name
    name: Ident,
    /// Column definitions
    columns: Vec<ColumnDef>,
    /// Query used to populate the table
    query: Option<Box<Query>>,
}
