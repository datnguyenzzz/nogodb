pub mod dynamic_join_pushdown;
pub mod equivalent_filter;
pub mod projection_pushdown;

use std::collections::HashSet;

use anyhow::Result;

use crate::optimiser::dynamic_join_pushdown::DynamicJoinFilter;
use crate::optimiser::equivalent_filter::EquivalenceFilterPropagation;
use crate::optimiser::projection_pushdown::ProjectionPushdown;
use crate::planner::ast::operators::BinaryOperator;
use crate::planner::{LogicalExpr, LogicalPlan};

// Helper functions

/// Extracts the canonical dot-separated column name (e.g. "users.id")
pub fn get_column_canonical_name(expr: &LogicalExpr) -> Option<String> {
    match expr {
        LogicalExpr::Column(name) => Some(name.clone()),
        LogicalExpr::CompoundColumn(parts) => Some(parts.join(".")),
        _ => None,
    }
}

/// Builds a Column or CompoundColumn from a canonical name
pub fn make_column_from_canonical_name(name: String) -> LogicalExpr {
    if name.contains('.') {
        LogicalExpr::CompoundColumn(name.split('.').map(|s| s.to_string()).collect())
    } else {
        LogicalExpr::Column(name)
    }
}

/// Recursively extracts all column names referenced inside a LogicalExpr.
pub fn accumulate_columns(predicate: &LogicalExpr, cols: &mut HashSet<String>) {
    match predicate {
        LogicalExpr::Column(col) => {
            cols.insert(col.to_string());
        }
        LogicalExpr::CompoundColumn(parts) => {
            cols.insert(parts.join("."));
        }
        LogicalExpr::BinaryOp { left, right, .. } => {
            accumulate_columns(left, cols);
            accumulate_columns(right, cols);
        }
        LogicalExpr::IsNull(expr)
        | LogicalExpr::IsNotNull(expr)
        | LogicalExpr::IsTrue(expr)
        | LogicalExpr::IsFalse(expr) => {
            accumulate_columns(expr, cols);
        }
        _ => {}
    }
}

/// Recursively splits an expression tree of `AND` conjuncts into a flat
/// list of independent expressions. Consume the requested expr
fn split_conjunction(expr: LogicalExpr, acc: &mut Vec<LogicalExpr>) {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } if op == BinaryOperator::And => {
            split_conjunction(*left, acc);
            split_conjunction(*right, acc);
        }
        other => acc.push(other),
    }
}

/// Reconstructs a flat list of independent conjuncts back into a single
/// `AND` expression tree.
pub fn rebuild_conjunction(mut conjuncts: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    if conjuncts.is_empty() {
        return None;
    }
    let mut root = conjuncts.pop().unwrap();
    while let Some(expr) = conjuncts.pop() {
        root = LogicalExpr::BinaryOp {
            left: Box::new(expr),
            op: BinaryOperator::And,
            right: Box::new(root),
        }
    }

    Some(root)
}

/// Shared trait implemented by every individual optimizer pass
pub trait OptimiserRule: Send + Sync {
    /// Unique identifier for the rule
    fn name(&self) -> &str;

    /// Analyzes and rewrites the logical plan, returning an optimized version.
    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
}

pub struct Optimiser {
    rules: Vec<Box<dyn OptimiserRule>>,
}

impl Default for Optimiser {
    fn default() -> Self {
        // Sequentially execute registered optimization rules!
        Optimiser {
            rules: vec![
                Box::new(EquivalenceFilterPropagation),
                Box::new(DynamicJoinFilter::default()),
                Box::new(ProjectionPushdown::default()),
            ],
        }
    }
}

impl Optimiser {
    pub fn new(rules: Vec<Box<dyn OptimiserRule>>) -> Self {
        Optimiser { rules }
    }

    pub fn optimise(&self, mut plan: LogicalPlan) -> Result<LogicalPlan> {
        for rule in &self.rules {
            plan = rule.rewrite(plan)?;
        }
        Ok(plan)
    }
}
