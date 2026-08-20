use crate::arrow::DataType;

pub mod provider;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<(String, DataType)>,
}
