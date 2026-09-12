use std::sync::Arc;

use anyhow::{Result, anyhow};

use crate::{
    arrow::{
        Array, ArrayRef, BooleanBuffer, Buffer, DataType, RecordBatch, Scalar,
        array::{BooleanArray, select::filter_record_batch},
        ord::{self, Datum},
    },
    execution::pipeline::{PhysicalOperator, Pipeline},
    planner::{LogicalPlan, ast::operators::BinaryOperator},
};

pub trait PhysicalExpr: Send + Sync {
    /// Evaluates this expression over a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}

/// A physical expression referencing a column in the schema by its physical index
pub struct PhysicalColumn {
    pub index: usize,
}

impl PhysicalExpr for PhysicalColumn {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        if self.index >= batch.num_columns() {
            return Err(anyhow!(
                "Column index out of bounds: {} >= {}",
                self.index,
                batch.num_columns()
            ));
        }

        Ok(batch.column(self.index).clone())
    }
}

/// A physical expression representing a constant, scalar literal value
pub struct PhysicalValue {
    pub value: ArrayRef,
}

impl PhysicalExpr for PhysicalValue {
    fn evaluate(&self, _batch: &RecordBatch) -> Result<ArrayRef> {
        Ok(self.value.clone())
    }
}

pub struct PhysicalComparison {
    pub lhs: Arc<dyn PhysicalExpr>,
    pub op: BinaryOperator,
    pub rhs: Arc<dyn PhysicalExpr>,
}

impl PhysicalExpr for PhysicalComparison {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let lhs = self.lhs.evaluate(batch)?;
        let rhs = self.rhs.evaluate(batch)?;

        let lhs_datum: &dyn Datum = if lhs.len() == 1 {
            &Scalar::new(lhs)
        } else {
            &lhs.as_ref()
        };

        let rhs_datum: &dyn Datum = if rhs.len() == 1 {
            &Scalar::new(rhs)
        } else {
            &rhs.as_ref()
        };

        match &self.op {
            BinaryOperator::Gt => Ok(Arc::new(ord::gt(lhs_datum, rhs_datum)?)),
            BinaryOperator::Lt => Ok(Arc::new(ord::lt(lhs_datum, rhs_datum)?)),
            BinaryOperator::GtEq => Ok(Arc::new(ord::gt_eq(lhs_datum, rhs_datum)?)),
            BinaryOperator::LtEq => Ok(Arc::new(ord::lt_eq(lhs_datum, rhs_datum)?)),
            BinaryOperator::Eq => Ok(Arc::new(ord::eq(lhs_datum, rhs_datum)?)),
            BinaryOperator::NotEq => Ok(Arc::new(ord::neq(lhs_datum, rhs_datum)?)),
            o => Err(anyhow!(
                "Physical comparison operator not yet supported: {:?}",
                o
            )),
        }
    }
}

/// A physical expression evaluating `expr IS NULL` or `expr IS NOT NULL`
pub struct PhysicalNullable {
    pub expr: Arc<dyn PhysicalExpr>,
    /// Indicates an operator is either IS NULL (true) / IS NOT NULL (false)
    pub is_null: bool,
}

impl PhysicalExpr for PhysicalNullable {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let expr = self.expr.evaluate(batch)?;
        let len = expr.len();

        let value_buf = match expr.nulls() {
            Some(n) => {
                if self.is_null {
                    n.inner().bitwise_bin_op(n.inner(), |a, _b| !a)
                } else {
                    n.inner().clone()
                }
            }
            None => {
                let v = if self.is_null { 0u8 } else { 0xffu8 };

                BooleanBuffer::new(Buffer::from(vec![v; (len + 7) / 8]), 0, len)
            }
        };

        Ok(Arc::new(BooleanArray::new(value_buf, None)))
    }
}

/// A physical expression evaluating `expr IS TRUE` or `expr is FALSE`
pub struct PhysicalBool {
    pub expr: Arc<dyn PhysicalExpr>,
    pub is_true: bool,
}

impl PhysicalExpr for PhysicalBool {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let expr = self.expr.evaluate(batch)?;
        if *expr.data_type() != DataType::Boolean {
            return Err(anyhow!(
                "PhysicalBool can only be evaluated over Boolean arrays, found {:?}",
                expr.data_type()
            ));
        }

        let expr = expr.as_any().downcast_ref::<BooleanArray>().unwrap();

        let value_buf = match expr.nulls() {
            Some(n) => {
                if self.is_true {
                    expr.values().bitwise_bin_op(&expr.values(), |a, b| a & b)
                } else {
                    let not_values: BooleanBuffer =
                        expr.values().bitwise_bin_op(&expr.values(), |a, _b| !a);
                    not_values.bitwise_bin_op(n.inner(), |a, b| a & b)
                }
            }
            None => {
                if self.is_true {
                    expr.values().clone()
                } else {
                    expr.values().bitwise_bin_op(&expr.values(), |a, _b| !a)
                }
            }
        };

        Ok(Arc::new(BooleanArray::new(value_buf, None)))
    }
}

pub struct PhysicalFilter {
    pub predicate: Arc<dyn PhysicalExpr>,
}

impl PhysicalOperator for PhysicalFilter {
    fn execute(&self, input: &RecordBatch) -> Result<Option<RecordBatch>> {
        let eval = self.predicate.evaluate(input)?;
        if *eval.data_type() != DataType::Boolean {
            return Err(anyhow!(
                "PhysicalFilter's predicate must be a Boolean arrays, found {:?}",
                eval.data_type()
            ));
        }

        let eval = eval.as_any().downcast_ref::<BooleanArray>().unwrap();
        Ok(Some(filter_record_batch(input, eval).unwrap()))
    }
}

/// Helper structure to accumulate pipelines and assign pipeline IDs during translation
pub struct PlanBuilder {
    pub pipelines: Vec<Pipeline>,
    pub next_pipeline_id: usize,
}

impl PlanBuilder {
    pub fn new() -> Self {
        todo!("implement me")
    }
}

/// A physical plan is a tree of physical operators. Every operator must know how to translate
/// itself into execution pipelines via `build()` hook
pub trait PhysicalPlanNode {
    /// Recursively registers executing pipeline(s) for this physical node and its children
    fn build(&self, builder: &mut PlanBuilder) -> Result<()>;
}

// Implement PhysicalPlanNode for PhysicalScan, PhysicalFilter
// ...

#[derive(Default)]
pub struct PhysicalPlanGenerator;

impl PhysicalPlanGenerator {
    pub fn create_plan(&self, logical_plan: &LogicalPlan) -> Result<Box<dyn PhysicalPlanNode>> {
        todo!("implement me")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{Field, Schema, array::PrimitiveArray};

    #[test]
    fn test_physical_filter_with_comparison() {
        let schema = Arc::new(Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "age".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ]));

        let col_id: ArrayRef = Arc::new(PrimitiveArray::from(vec![1i32, 2, 3, 4]));
        let col_age: ArrayRef = Arc::new(PrimitiveArray::from(vec![18i32, 45, 12, 32]));
        let batch = RecordBatch::try_new(schema, vec![col_id, col_age]).unwrap();

        // Target: WHERE age > 30
        let expr_col = Arc::new(PhysicalColumn { index: 1 });
        let expr_lit = Arc::new(PhysicalValue {
            value: Arc::new(PrimitiveArray::from(vec![30i32])), // Scalar of length 1
        });
        let predicate = Arc::new(PhysicalComparison {
            lhs: expr_col,
            op: BinaryOperator::Gt,
            rhs: expr_lit,
        });

        let filter_op = PhysicalFilter { predicate };

        // Execute the vectorized physical filter!
        let filtered_batch = filter_op.execute(&batch).unwrap().unwrap();

        assert_eq!(filtered_batch.num_rows(), 2); // 45 and 32 matched
        assert_eq!(filtered_batch.num_columns(), 2);

        let out_id = filtered_batch
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();
        assert_eq!(out_id.value(0), 2);
        assert_eq!(out_id.value(1), 4);
    }

    #[test]
    fn test_physical_nullable_expressions() {
        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: true,
        }]));

        let col_id: ArrayRef = Arc::new(PrimitiveArray::from(vec![Some(10i32), None, Some(30)]));
        let batch = RecordBatch::try_new(schema, vec![col_id]).unwrap();

        // 1. IS NULL check
        let expr_col = Arc::new(PhysicalColumn { index: 0 });
        let expr_is_null = Arc::new(PhysicalNullable {
            expr: expr_col.clone(),
            is_null: true,
        });

        let res_null = expr_is_null.evaluate(&batch).unwrap();
        let parsed_null = res_null.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            parsed_null.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(true), Some(false)]
        );

        // 2. IS NOT NULL check
        let expr_is_not_null = Arc::new(PhysicalNullable {
            expr: expr_col,
            is_null: false,
        });

        let res_not_null = expr_is_not_null.evaluate(&batch).unwrap();
        let parsed_not_null = res_not_null
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(
            parsed_not_null.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(true)]
        );
    }
}
