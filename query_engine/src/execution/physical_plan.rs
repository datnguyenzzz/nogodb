use std::sync::Arc;

use anyhow::{Result, anyhow};

use crate::{
    arrow::{
        Array, ArrayRef, BooleanBuffer, Buffer, DataType, RecordBatch, Scalar,
        array::BooleanArray,
        ord::{self, Datum},
    },
    execution::pipeline::Pipeline,
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

// Note for future implementation: For the Hash Join, the build side should be the smaller one
// compared relatively to the probe side

#[derive(Default)]
pub struct PhysicalPlanGenerator;

impl PhysicalPlanGenerator {
    pub fn create_plan(&self, logical_plan: &LogicalPlan) -> Result<Box<dyn PhysicalPlanNode>> {
        todo!("implement me")
    }
}
