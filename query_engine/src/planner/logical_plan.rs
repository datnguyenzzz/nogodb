use arrow::{array::ArrayRef, datatypes::DataType};

use crate::planner::ast::operators::BinaryOperator;

pub enum ScalaValue {
    Null(DataType),
    Boolean(Option<bool>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float64(Option<f64>),
    Utf8(Option<String>),
}

impl ScalaValue {
    pub fn to_arrow(&self) -> ArrayRef {
        todo!("implement me")
    }
}

pub enum LogicalExpr {
    Column {
        name: String,
        data_type: DataType,
    },
    Literal(ScalaValue),
    BinaryOp {
        left: Box<LogicalExpr>,
        op: BinaryOperator,
        right: Box<LogicalExpr>,
    },
}

pub enum LogicalPlan {
    Scan {
        table_name: String,
        projections: Vec<String>,
    },
}
