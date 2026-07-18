use crate::sql_parser::ast::{
    ddl::CreateTable,
    dml::{Delete, Insert, Update},
    expr::Ident,
    query::Query,
};

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement {
    /// ```sql
    /// SELECT
    /// ```
    Query(Box<Query>),
    /// ```sql
    /// INSERT
    /// ```
    Insert(Insert),
    /// ```sql
    /// UPDATE
    /// ```
    Update(Update),
    /// ```sql
    /// DELETE
    /// ```
    Delete(Delete),
    /// ```sql
    /// CREATE TABLE
    /// ```
    CreateTable(CreateTable),
    // TODO: Support ALTER TABLE
    /// ```sql
    /// DROP [TABLE, VIEW, ...]
    /// ```
    Drop {
        /// One or more tables to drop
        names: Vec<Ident>,
    },
    /// ```sql
    /// SHOW TABLES
    /// ```
    ShowTables, // TODO: Support Transaction
}

impl From<CreateTable> for Statement {
    fn from(value: CreateTable) -> Self {
        Self::CreateTable(value)
    }
}
