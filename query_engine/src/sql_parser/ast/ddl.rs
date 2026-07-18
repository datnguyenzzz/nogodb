use crate::sql_parser::ast::{data_type::DataType, expr::Ident};

/// SQL column definition
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#table%20contents%20source
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef {
    /// Column name.
    pub name: Ident,
    /// Column data type.
    pub data_type: DataType,
    // TODO: Support default/constraint options
}

/// CREATE TABLE statement.
/// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#table%20definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTable {
    /// Table name
    pub table_name: Ident,
    pub columns: Vec<ColumnDef>,
}
