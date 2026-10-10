use core::slice;
use std::{
    cmp::{self, Ordering},
    sync::Arc,
};

use anyhow::{Result, anyhow};

use crate::{
    arrow::{
        Array, ArrayRef, BooleanBuffer, Buffer, DataType, RecordBatch, Scalar,
        array::BooleanArray,
        ord::{self, Datum},
    },
    planner::ast::operators::BinaryOperator,
};

pub mod aggregate;
pub mod dispatcher;
pub mod filters;
pub mod hash_join;
pub mod limit;
pub mod numa_topology;
pub mod pipeline;
pub mod plan_builder;
pub mod scan;
pub mod scheduler;
pub mod sort;

/// Represents a NUMA-aware dynamic execution block of roughly [`MORSEL_SIZE`] rows.
#[derive(Clone, Copy)]
pub struct Morsel {
    pub start_row: usize,
    pub num_rows: usize,
    pub numa_node: usize,
}

/// A 16-byte German String
/// - If len <= 12: Inlined string bytes stored entirely within the 16-byte struct.
/// - If len > 12: 4-byte prefix + 8-byte pointer (or coordinate) to the string buffer.
/// Reference: https://cedardb.com/blog/german_strings/
#[derive(Clone, Copy)]
pub struct GermanString {
    pub length: u32,
    pub prefix: [u8; 4],
    pub trailing: Trailing,
}

#[derive(Clone, Copy)]
pub union Trailing {
    pub buf: [u8; 8],
    pub ptr: *const u8,
}

unsafe impl Send for GermanString {}
unsafe impl Sync for GermanString {}

impl GermanString {
    pub fn from_str(s: &str) -> Self {
        let bytes = s.as_bytes();
        let length = bytes.len() as u32;
        let mut prefix = [0u8; 4];

        if length <= 12 {
            let mut buf = [0u8; 8];
            let prefix_len = cmp::min(bytes.len(), 4);
            prefix[..prefix_len].copy_from_slice(&bytes[..prefix_len]);

            if length > 4 {
                let rest_len = (length - 4) as usize;
                buf[..rest_len].copy_from_slice(&bytes[4..4 + rest_len]);
            }

            Self {
                length,
                prefix,
                trailing: Trailing { buf },
            }
        } else {
            prefix.copy_from_slice(&bytes[..4]);

            Self {
                length,
                prefix,
                trailing: Trailing {
                    ptr: bytes.as_ptr(),
                },
            }
        }
    }

    pub fn as_str(&self) -> &str {
        if self.length <= 12 {
            unsafe {
                let ptr = &self.prefix as *const u8;
                str::from_utf8_unchecked(slice::from_raw_parts(ptr, self.length as usize))
            }
        } else {
            unsafe {
                str::from_utf8_unchecked(slice::from_raw_parts(
                    self.trailing.ptr,
                    self.length as usize,
                ))
            }
        }
    }
}

impl PartialOrd for GermanString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GermanString {
    fn cmp(&self, other: &Self) -> Ordering {
        let p1 = u32::from_be_bytes(self.prefix);
        let p2 = u32::from_be_bytes(other.prefix);
        let prefix_ord = p1.cmp(&p2);
        if prefix_ord != Ordering::Equal {
            return prefix_ord;
        }

        if self.length <= 4 && other.length <= 4 {
            return self.length.cmp(&other.length);
        }

        if self.length <= 12 && other.length <= 12 {
            let t1 = (self.length - 4) as usize;
            let t2 = (other.length - 4) as usize;
            unsafe {
                let s1 = &self.trailing.buf[..t1];
                let s2 = &other.trailing.buf[..t2];
                let ord = s1.cmp(&s2);
                if ord != Ordering::Equal {
                    return ord;
                }
                return self.length.cmp(&other.length);
            }
        }

        unsafe {
            let s1 = if self.length <= 12 {
                &self.trailing.buf[..(self.length - 4) as usize]
            } else {
                slice::from_raw_parts(self.trailing.ptr.add(4), (self.length - 4) as usize)
            };

            let s2 = if other.length <= 12 {
                &other.trailing.buf[..(other.length - 4) as usize]
            } else {
                slice::from_raw_parts(other.trailing.ptr.add(4), (other.length - 4) as usize)
            };

            let ord = s1.cmp(&s2);
            if ord != Ordering::Equal {
                return ord;
            }
            return self.length.cmp(&other.length);
        }
    }
}

impl PartialEq for GermanString {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for GermanString {}

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
