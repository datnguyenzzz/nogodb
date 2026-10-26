use std::sync::Arc;

use anyhow::{Result, bail};

use crate::{
    arrow::{
        ArrayRef, DataType, RecordBatch, SchemaRef,
        array::{BooleanArray, PrimitiveArray, StringArray},
        ipc::record_batch::Writer,
    },
    execution::pipeline::{CombineResult, PhysicalSink, SinkContext, SinkResult},
    planner::LogicalExpr,
    storage::DataStorage,
};

pub struct WriteCoordinator {
    pub storage: Arc<dyn DataStorage>,
}

impl WriteCoordinator {
    pub fn new(storage: Arc<dyn DataStorage>) -> Self {
        Self { storage }
    }

    pub async fn handle_insert(&self, table_name: &str, batch: RecordBatch) -> Result<()> {
        let mut buf = Vec::new();
        let mut writer = Writer::new(&mut buf);
        writer.write(&batch).unwrap();

        self.storage.append_chunk(table_name, buf).await?;

        // flush the written rows immediately. They are minor,
        // and infrequent segments from this path are an accepted cost.
        self.storage.flush(table_name).await?;

        Ok(())
    }
}

/// Transposes literal SQL VALUES rows into an Arrow-Lite RecordBatch
pub fn values_to_record_batch(
    schema: SchemaRef,
    values: &[Vec<LogicalExpr>],
) -> Result<RecordBatch> {
    let num_rows = values.len();
    let num_cols = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let field = &schema.fields()[col_idx];
        match field.data_type {
            DataType::Int8 => {
                let mut data = Vec::with_capacity(num_rows);
                for row in values {
                    match &row[col_idx] {
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Number(s, _)) => {
                            data.push(Some(s.parse::<i8>()?));
                        }
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Null) => {
                            data.push(None)
                        }
                        _ => bail!(
                            "Unsupported expression for Int8 column: {:?}",
                            &row[col_idx]
                        ),
                    }
                }
                columns.push(Arc::new(PrimitiveArray::from(data)));
            }
            DataType::Int16 => {
                let mut data = Vec::with_capacity(num_rows);
                for row in values {
                    match &row[col_idx] {
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Number(s, _)) => {
                            data.push(Some(s.parse::<i16>()?));
                        }
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Null) => {
                            data.push(None)
                        }
                        _ => bail!(
                            "Unsupported expression for Int16 column: {:?}",
                            &row[col_idx]
                        ),
                    }
                }
                columns.push(Arc::new(PrimitiveArray::from(data)));
            }
            DataType::Int32 => {
                let mut data = Vec::with_capacity(num_rows);
                for row in values {
                    match &row[col_idx] {
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Number(s, _)) => {
                            data.push(Some(s.parse::<i32>()?));
                        }
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Null) => {
                            data.push(None)
                        }
                        _ => bail!(
                            "Unsupported expression for Int32 column: {:?}",
                            &row[col_idx]
                        ),
                    }
                }
                columns.push(Arc::new(PrimitiveArray::from(data)));
            }
            DataType::Int64 => {
                let mut data = Vec::with_capacity(num_rows);
                for row in values {
                    match &row[col_idx] {
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Number(s, _)) => {
                            data.push(Some(s.parse::<i64>()?));
                        }
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Null) => {
                            data.push(None)
                        }
                        _ => bail!(
                            "Unsupported expression for Int64 column: {:?}",
                            &row[col_idx]
                        ),
                    }
                }
                columns.push(Arc::new(PrimitiveArray::from(data)));
            }
            DataType::Float64 => {
                let mut data = Vec::with_capacity(num_rows);
                for row in values {
                    match &row[col_idx] {
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Number(s, _)) => {
                            data.push(Some(s.parse::<f64>()?));
                        }
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Null) => {
                            data.push(None)
                        }
                        _ => bail!(
                            "Unsupported expression for Float64 column: {:?}",
                            &row[col_idx]
                        ),
                    }
                }
                columns.push(Arc::new(PrimitiveArray::from(data)));
            }
            DataType::Utf8 => {
                let mut data: Vec<Option<&str>> = Vec::with_capacity(num_rows);
                for row in values {
                    match &row[col_idx] {
                        LogicalExpr::Value(
                            crate::sql_parser::ast::expr::Value::SingleQuotedString(s),
                        ) => {
                            data.push(Some(s.as_str()));
                        }
                        LogicalExpr::Value(
                            crate::sql_parser::ast::expr::Value::DoubleQuotedString(s),
                        ) => {
                            data.push(Some(s.as_str()));
                        }
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Null) => {
                            data.push(None)
                        }
                        _ => bail!(
                            "Unsupported expression for Utf8 column: {:?}",
                            &row[col_idx]
                        ),
                    }
                }
                columns.push(Arc::new(StringArray::from(data)));
            }
            DataType::Boolean => {
                let mut data = Vec::with_capacity(num_rows);
                for row in values {
                    match &row[col_idx] {
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Boolean(b)) => {
                            data.push(Some(*b));
                        }
                        LogicalExpr::Value(crate::sql_parser::ast::expr::Value::Null) => {
                            data.push(None)
                        }
                        _ => bail!(
                            "Unsupported expression for Boolean column: {:?}",
                            &row[col_idx]
                        ),
                    }
                }
                columns.push(Arc::new(BooleanArray::from(data)));
            }
            _ => bail!(
                "Unsupported column data type for values conversion: {:?}",
                field.data_type
            ),
        }
    }

    RecordBatch::try_new(schema, columns)
}

/// Physical Sink for INSERT operations: buffers and writes incoming batches to storage via handle_insert
pub struct PhysicalInsertSink {
    pub table_name: String,
    pub coordinator: Arc<WriteCoordinator>,
}

impl PhysicalInsertSink {
    pub fn new(table_name: String, coordinator: Arc<WriteCoordinator>) -> Self {
        Self {
            table_name,
            coordinator,
        }
    }
}

impl PhysicalSink for PhysicalInsertSink {
    fn sink(&self, _ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult> {
        futures::executor::block_on(self.coordinator.handle_insert(&self.table_name, input))?;
        Ok(SinkResult::NeedMoreInput)
    }

    fn combine(&self) -> Result<CombineResult> {
        Ok(CombineResult::Empty)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Physical Sink for UPDATE operations: appends modified row versions via handle_insert
pub struct PhysicalUpdateSink {
    pub table_name: String,
    pub coordinator: Arc<WriteCoordinator>,
}

impl PhysicalUpdateSink {
    pub fn new(table_name: String, coordinator: Arc<WriteCoordinator>) -> Self {
        Self {
            table_name,
            coordinator,
        }
    }
}

impl PhysicalSink for PhysicalUpdateSink {
    fn sink(&self, _ctx: &mut SinkContext, _input: RecordBatch) -> Result<SinkResult> {
        todo!()
    }

    fn combine(&self) -> Result<CombineResult> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Physical Sink for DELETE operations
pub struct PhysicalDeleteSink {
    pub table_name: String,
    pub coordinator: Arc<WriteCoordinator>,
}

impl PhysicalDeleteSink {
    pub fn new(table_name: String, coordinator: Arc<WriteCoordinator>) -> Self {
        Self {
            table_name,
            coordinator,
        }
    }
}

impl PhysicalSink for PhysicalDeleteSink {
    fn sink(&self, _ctx: &mut SinkContext, _input: RecordBatch) -> Result<SinkResult> {
        todo!()
    }

    fn combine(&self) -> Result<CombineResult> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::object::ColumnStat;
    use crate::arrow::{Field, Schema, array::PrimitiveArray};
    use crate::storage::ChunkMetaData;
    use crate::storage::embedded::tests::MockDataStorage;

    #[tokio::test]
    async fn test_write_coordinator_handle_insert_populates_stats() {
        let storage = Arc::new(MockDataStorage::new());
        let coordinator = WriteCoordinator::new(storage.clone());

        let schema = Arc::new(Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ]));

        let id_col = Arc::new(PrimitiveArray::from(vec![10i32, 5, 42, 2]));
        let name_col = Arc::new(StringArray::from(vec!["banana", "apple", "cherry", "date"]));
        let batch = RecordBatch::try_new(schema, vec![id_col, name_col]).unwrap();

        coordinator
            .handle_insert("test_table", batch)
            .await
            .unwrap();

        assert_eq!(storage.get_chunk_count("test_table").await.unwrap(), 1);
        let mut it = storage.new_iterator("test_table").await.unwrap();
        let meta_bytes = it.next_chunk_meta().await.unwrap().unwrap();
        let meta = ChunkMetaData::try_new(meta_bytes).unwrap();

        assert_eq!(meta.num_rows, 4);
        match meta.get_column_stat(0).unwrap() {
            ColumnStat::Int64 { min, max } => {
                assert_eq!(*min, 2i64);
                assert_eq!(*max, 42i64);
            }
            _ => panic!("Expected Int64 stat"),
        }
        match meta.get_column_stat(1).unwrap() {
            ColumnStat::Utf8 { min, max } => {
                assert_eq!(min, "apple");
                assert_eq!(max, "datf");
            }
            _ => panic!("Expected Utf8 stat"),
        }
    }
}
