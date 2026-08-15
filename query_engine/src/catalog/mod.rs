use crate::arrow::DataType;

pub mod provider;

pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<(String, DataType)>,
}
