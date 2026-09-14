use std::sync::Arc;

use anyhow::{Result, anyhow};

use crate::{
    arrow::{
        DataType, RecordBatch,
        array::{BooleanArray, select::filter_record_batch},
    },
    execution::{physical_plan::PhysicalExpr, pipeline::PhysicalOperator},
};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        arrow::{ArrayRef, Field, Schema, array::PrimitiveArray},
        execution::physical_plan::{
            PhysicalColumn, PhysicalComparison, PhysicalNullable, PhysicalValue,
        },
        planner::ast::operators::BinaryOperator,
    };

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
