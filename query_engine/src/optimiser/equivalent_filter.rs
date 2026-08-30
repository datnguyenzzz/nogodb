use std::{
    collections::{HashMap, HashSet},
    mem,
};

use anyhow::Result;

use crate::{
    optimiser::{
        OptimiserRule, accumulate_columns, get_column_canonical_name,
        make_column_from_canonical_name, rebuild_conjunction, split_conjunction,
    },
    planner::{LogicalExpr, LogicalPlan},
};

/// Manage column equivalence relations in near-constant O(α(N)) time
#[derive(Default)]
struct EquivalenceClass {
    parent: HashMap<String, String>,
    rank: HashMap<String, usize>,
}

impl EquivalenceClass {
    /// Finds the representative root of a column
    fn find(&mut self, col: &str) -> String {
        if !self.parent.contains_key(col) {
            self.parent.insert(col.to_string(), col.to_string());
            self.rank.insert(col.to_string(), 0);
        }

        let parent = self.parent.get(col).unwrap().clone();
        if parent == col {
            parent.to_string()
        } else {
            let root = self.find(&parent);
            self.parent.insert(col.to_string(), root.clone());
            root
        }
    }

    pub fn union(&mut self, col1: &str, col2: &str) {
        let mut root1 = self.find(col1);
        let mut root2 = self.find(col2);
        if root1 != root2 {
            let rank1 = *self.rank.get(&root1).unwrap();
            let rank2 = *self.rank.get(&root2).unwrap();
            if rank1 < rank2 {
                mem::swap(&mut root1, &mut root2);
            }

            self.parent.insert(root2, root1.clone());
            if rank1 == rank2 {
                self.rank.insert(root1, rank1 + 1);
            }
        }
    }

    /// Collects and returns all columns equivalent to the given column in the disjoint set
    fn get_equivalence(&mut self, col: &str) -> HashSet<String> {
        let mut class = HashSet::new();
        if !self.parent.contains_key(col) {
            return class;
        }

        let root = self.find(col);
        let keys: Vec<String> = self.parent.keys().cloned().collect();
        for key in keys {
            if self.find(&key) == root {
                class.insert(key);
            }
        }

        class
    }

    /// Propagates a binary comparison predicate to equivalent columns
    pub fn propagate(&mut self, predicate: &LogicalExpr) -> Vec<LogicalExpr> {
        let mut propagated = vec![];
        // comparison predicate
        match predicate {
            LogicalExpr::BinaryOp { left, op, right } => {
                if let Some(col) = get_column_canonical_name(left) {
                    for eq_col in self.get_equivalence(&col) {
                        if eq_col != col {
                            propagated.push(LogicalExpr::BinaryOp {
                                left: Box::new(make_column_from_canonical_name(eq_col)),
                                op: op.clone(),
                                right: right.clone(),
                            });
                        }
                    }
                }
            }
            LogicalExpr::IsNull(expr) => {
                if let Some(col) = get_column_canonical_name(expr) {
                    for eq_col in self.get_equivalence(&col) {
                        if eq_col != col {
                            propagated.push(LogicalExpr::IsNull(Box::new(
                                make_column_from_canonical_name(eq_col),
                            )));
                        }
                    }
                }
            }
            LogicalExpr::IsNotNull(expr) => {
                if let Some(col) = get_column_canonical_name(expr) {
                    for eq_col in self.get_equivalence(&col) {
                        if eq_col != col {
                            propagated.push(LogicalExpr::IsNotNull(Box::new(
                                make_column_from_canonical_name(eq_col),
                            )));
                        }
                    }
                }
            }
            LogicalExpr::IsTrue(expr) => {
                if let Some(col) = get_column_canonical_name(expr) {
                    for eq_col in self.get_equivalence(&col) {
                        if eq_col != col {
                            propagated.push(LogicalExpr::IsTrue(Box::new(
                                make_column_from_canonical_name(eq_col),
                            )));
                        }
                    }
                }
            }
            LogicalExpr::IsFalse(expr) => {
                if let Some(col) = get_column_canonical_name(expr) {
                    for eq_col in self.get_equivalence(&col) {
                        if eq_col != col {
                            propagated.push(LogicalExpr::IsFalse(Box::new(
                                make_column_from_canonical_name(eq_col),
                            )));
                        }
                    }
                }
            }
            _ => {}
        }

        propagated
    }
}

/// Equivalence Filter Propagation ("Pull-Up, then Push-Down")
/// When a query joins users and orders on users.id = orders.user_id and
/// contains a filter WHERE users.id > 42:
///  1. Pull-Up: The optimizer analyzes the equijoin constraint and forms
///     an Equivalence Class indicating that users.id and orders.user_id
///     are identical for all successfully joined rows: {users.id, orders.user_id}.
///  2. Propagate & Push-Down: It copies the filter users.id > 42, replaces
///     users.id with its equivalent companion orders.user_id, and pushes
///     both filters down into their respective table scans!
///  3. The Result: Both users and orders are filtered before the join
///     executes, cutting memory allocation and probing steps by orders
///     of magnitude
pub struct EquivalenceFilterPropagation;

impl EquivalenceFilterPropagation {
    /// Recursively collect equality constraints (from Joins) bottom-up
    fn collect_equivalences(&self, plan: &LogicalPlan, eq_class: &mut EquivalenceClass) {
        match plan {
            LogicalPlan::HashJoin {
                left, right, on, ..
            } => {
                for (l_expr, r_expr) in on {
                    if let (Some(l_col), Some(r_col)) = (
                        get_column_canonical_name(l_expr),
                        get_column_canonical_name(r_expr),
                    ) {
                        eq_class.union(&l_col, &r_col);
                    }
                }
                self.collect_equivalences(left, eq_class);
                self.collect_equivalences(right, eq_class);
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Projection { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => {
                self.collect_equivalences(input, eq_class);
            }
            _ => {}
        }
    }

    /// Pushes down propagated equivalent predicates recursively into left/right branches
    fn pushdown_propagated_filters(
        &self,
        plan: LogicalPlan,
        eq_class: &mut EquivalenceClass,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { predicate, input } => {
                let optimized = self.pushdown_propagated_filters(*input, eq_class)?;
                match optimized {
                    LogicalPlan::HashJoin {
                        left,
                        right,
                        on,
                        join_type,
                        schema,
                    } => {
                        let mut conjuncts = vec![];
                        split_conjunction(predicate, &mut conjuncts);

                        let mut left_pushed = vec![];
                        let mut right_pushed = vec![];
                        let mut leftover = vec![];
                        let left_fields: HashSet<String> = left
                            .schema()
                            .unwrap()
                            .fields()
                            .iter()
                            .map(|f| f.name.clone())
                            .collect();
                        let right_fields: HashSet<String> = right
                            .schema()
                            .unwrap()
                            .fields()
                            .iter()
                            .map(|f| f.name.clone())
                            .collect();

                        for conj in conjuncts {
                            let mut filter_cols = HashSet::new();
                            accumulate_columns(&conj, &mut filter_cols);

                            if filter_cols.iter().all(|c| left_fields.contains(c)) {
                                // push to the right if possible
                                for alt_pred in eq_class.propagate(&conj) {
                                    let mut alt_cols = HashSet::new();
                                    accumulate_columns(&alt_pred, &mut alt_cols);
                                    if alt_cols.iter().all(|c| right_fields.contains(c)) {
                                        right_pushed.push(alt_pred)
                                    }
                                }
                                left_pushed.push(conj);
                            } else if filter_cols.iter().all(|c| right_fields.contains(c)) {
                                // push to the left if possible
                                for alt_pred in eq_class.propagate(&conj) {
                                    let mut alt_cols = HashSet::new();
                                    accumulate_columns(&alt_pred, &mut alt_cols);
                                    if alt_cols.iter().all(|c| left_fields.contains(c)) {
                                        left_pushed.push(alt_pred)
                                    }
                                }
                                right_pushed.push(conj);
                            } else {
                                leftover.push(conj);
                            }
                        }

                        // Pushing the filter down
                        let mut left_plan = *left;
                        if !left_pushed.is_empty() {
                            left_plan = LogicalPlan::Filter {
                                predicate: rebuild_conjunction(left_pushed).unwrap(),
                                input: Box::new(left_plan),
                            }
                        }

                        let mut right_plan = *right;
                        if !right_pushed.is_empty() {
                            right_plan = LogicalPlan::Filter {
                                predicate: rebuild_conjunction(right_pushed).unwrap(),
                                input: Box::new(right_plan),
                            }
                        }

                        let mut optimised = LogicalPlan::HashJoin {
                            left: Box::new(self.pushdown_propagated_filters(left_plan, eq_class)?),
                            right: Box::new(
                                self.pushdown_propagated_filters(right_plan, eq_class)?,
                            ),
                            on,
                            join_type,
                            schema,
                        };

                        if !leftover.is_empty() {
                            optimised = LogicalPlan::Filter {
                                predicate: rebuild_conjunction(leftover).unwrap(),
                                input: Box::new(optimised),
                            }
                        }

                        Ok(optimised)
                    }

                    _ => Ok(LogicalPlan::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(optimized),
                    }),
                }
            }
            LogicalPlan::Projection {
                exprs,
                input,
                schema,
            } => Ok(LogicalPlan::Projection {
                exprs: exprs.to_vec(),
                input: Box::new(self.pushdown_propagated_filters(*input, eq_class)?),
                schema: schema.clone(),
            }),
            LogicalPlan::HashJoin {
                left,
                right,
                on,
                join_type,
                schema,
            } => Ok(LogicalPlan::HashJoin {
                left: Box::new(self.pushdown_propagated_filters(*left, eq_class)?),
                right: Box::new(self.pushdown_propagated_filters(*right, eq_class)?),
                on: on.to_vec(),
                join_type,
                schema: schema.clone(),
            }),
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } => Ok(LogicalPlan::Limit {
                limit,
                offset,
                input: Box::new(self.pushdown_propagated_filters(*input, eq_class)?),
            }),
            LogicalPlan::Sort { sort_exprs, input } => Ok(LogicalPlan::Sort {
                sort_exprs,
                input: Box::new(self.pushdown_propagated_filters(*input, eq_class)?),
            }),
            other => Ok(other),
        }
    }
}

impl OptimiserRule for EquivalenceFilterPropagation {
    fn name(&self) -> &str {
        "EquivalenceFilterPropagationRule"
    }

    fn rewrite(&self, plan: crate::planner::LogicalPlan) -> Result<LogicalPlan> {
        let mut eq_class = EquivalenceClass::default();
        self.collect_equivalences(&plan, &mut eq_class);
        self.pushdown_propagated_filters(plan, &mut eq_class)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{DataType, Field, Schema};
    use crate::planner::{JoinType, LogicalExpr, LogicalPlan};
    use crate::sql_parser::ast::expr::Value;
    use crate::sql_parser::ast::operators::BinaryOperator;
    use std::sync::Arc;

    fn col(name: &str) -> LogicalExpr {
        LogicalExpr::Column(name.to_string())
    }

    fn compound_col(parts: Vec<&str>) -> LogicalExpr {
        LogicalExpr::CompoundColumn(parts.into_iter().map(|p| p.to_string()).collect())
    }

    fn lit_int(val: i32) -> LogicalExpr {
        LogicalExpr::Value(Value::Number(val.to_string(), false))
    }

    fn eq(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Eq,
            right: Box::new(right),
        }
    }

    fn and(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        }
    }

    #[test]
    fn test_equivalence_filter_propagation_basic() {
        // Left Schema: [id: Int32, name: Utf8]
        let left_schema = Arc::new(Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ]));

        // Right Schema: [user_id: Int32, amount: Float64]
        let right_schema = Arc::new(Schema::new(vec![
            Field {
                name: "user_id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]));

        let dummy_fields: Vec<Field> = vec![];

        // Plan: Filter [id = 42] -> HashJoin [id = user_id] -> (Scan left, Scan right)
        let left_scan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: left_schema.clone(),
            projections: None,
            pruner: None,
        };

        let right_scan = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: right_schema.clone(),
            projections: None,
            pruner: None,
        };

        let join_node = LogicalPlan::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: vec![(col("id"), col("user_id"))],
            join_type: JoinType::Inner,
            schema: Arc::new(Schema::new(dummy_fields)), // Dummy combined schema
        };

        let filter_plan = LogicalPlan::Filter {
            predicate: eq(col("id"), lit_int(42)),
            input: Box::new(join_node),
        };

        // Run rewrite optimization rule!
        let rule = EquivalenceFilterPropagation;
        let optimized_plan = rule.rewrite(filter_plan).unwrap();

        // Assert that the optimized plan contains Filter [id = 42] on Left Scan,
        // AND Filter [user_id = 42] on Right Scan!
        match optimized_plan {
            LogicalPlan::HashJoin { left, right, .. } => {
                // Verify Left filter pushed exactly!
                match *left {
                    LogicalPlan::Filter { predicate, .. } => {
                        assert_eq!(predicate, eq(col("id"), lit_int(42)));
                    }
                    _ => panic!("Expected Filter on Left branch"),
                }

                // Verify Right filter propagated and pushed exactly!
                match *right {
                    LogicalPlan::Filter { predicate, .. } => {
                        assert_eq!(predicate, eq(col("user_id"), lit_int(42)));
                    }
                    _ => panic!("Expected Propagated Filter on Right branch"),
                }
            }
            _ => panic!("Expected HashJoin at root"),
        }
    }

    #[test]
    fn test_equivalence_filter_propagation_compound() {
        // Left Schema: [users.id: Int32, users.name: Utf8]
        let left_schema = Arc::new(Schema::new(vec![
            Field {
                name: "users.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "users.name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ]));

        // Right Schema: [orders.user_id: Int32, orders.amount: Float64]
        let right_schema = Arc::new(Schema::new(vec![
            Field {
                name: "orders.user_id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]));

        let dummy_fields: Vec<Field> = vec![];

        let left_scan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: left_schema.clone(),
            projections: None,
            pruner: None,
        };

        let right_scan = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: right_schema.clone(),
            projections: None,
            pruner: None,
        };

        let join_node = LogicalPlan::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: vec![(
                compound_col(vec!["users", "id"]),
                compound_col(vec!["orders", "user_id"]),
            )],
            join_type: JoinType::Inner,
            schema: Arc::new(Schema::new(dummy_fields)),
        };

        let filter_plan = LogicalPlan::Filter {
            predicate: eq(compound_col(vec!["users", "id"]), lit_int(42)),
            input: Box::new(join_node),
        };

        let rule = EquivalenceFilterPropagation;
        let optimized_plan = rule.rewrite(filter_plan).unwrap();

        match optimized_plan {
            LogicalPlan::HashJoin { left, right, .. } => {
                // Left should have compound filter users.id = 42
                match *left {
                    LogicalPlan::Filter { predicate, .. } => {
                        assert_eq!(
                            predicate,
                            eq(compound_col(vec!["users", "id"]), lit_int(42))
                        );
                    }
                    _ => panic!("Expected Filter on Left branch"),
                }

                // Right should have propagated compound filter orders.user_id = 42
                match *right {
                    LogicalPlan::Filter { predicate, .. } => {
                        assert_eq!(
                            predicate,
                            eq(compound_col(vec!["orders", "user_id"]), lit_int(42))
                        );
                    }
                    _ => panic!("Expected Propagated Filter on Right branch"),
                }
            }
            o => panic!("Expected HashJoin at root, but found {:#?}", o),
        }
    }

    #[test]
    fn test_equivalence_conjunction_split_propagation() {
        let left_schema = Arc::new(Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
        ]));

        // Right Schema: [user_id: Int32, amount: Float64]
        let right_schema = Arc::new(Schema::new(vec![
            Field {
                name: "user_id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]));

        let dummy_fields: Vec<Field> = vec![];

        let left_scan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: left_schema.clone(),
            projections: None,
            pruner: None,
        };

        let right_scan = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: right_schema.clone(),
            projections: None,
            pruner: None,
        };

        let join_node = LogicalPlan::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: vec![(col("id"), col("user_id"))],
            join_type: JoinType::Inner,
            schema: Arc::new(Schema::new(dummy_fields)),
        };

        // Filter: (id = 42) AND (amount > 100) AND (age < 30)
        // Note: (id = 42) propagates to (user_id = 42).
        // (amount > 100) pushes to Right scan.
        // (age < 30) pushes to Left scan.
        let complex_predicate = and(
            eq(col("id"), lit_int(42)),
            and(
                LogicalExpr::BinaryOp {
                    left: Box::new(col("amount")),
                    op: BinaryOperator::Gt,
                    right: Box::new(lit_int(100)),
                },
                LogicalExpr::BinaryOp {
                    left: Box::new(col("age")),
                    op: BinaryOperator::Lt,
                    right: Box::new(lit_int(30)),
                },
            ),
        );

        let filter_plan = LogicalPlan::Filter {
            predicate: complex_predicate,
            input: Box::new(join_node),
        };

        let rule = EquivalenceFilterPropagation;
        let optimized_plan = rule.rewrite(filter_plan).unwrap();

        match optimized_plan {
            LogicalPlan::HashJoin { left, right, .. } => {
                // Left should have (id = 42) AND (age < 30)
                match *left {
                    LogicalPlan::Filter { predicate, .. } => {
                        let expected = and(
                            eq(col("id"), lit_int(42)),
                            LogicalExpr::BinaryOp {
                                left: Box::new(col("age")),
                                op: BinaryOperator::Lt,
                                right: Box::new(lit_int(30)),
                            },
                        );
                        assert_eq!(predicate, expected);
                    }
                    _ => panic!("Expected Filter on Left branch"),
                }

                // Right should have (user_id = 42) AND (amount > 100)
                match *right {
                    LogicalPlan::Filter { predicate, .. } => {
                        let expected = and(
                            eq(col("user_id"), lit_int(42)),
                            LogicalExpr::BinaryOp {
                                left: Box::new(col("amount")),
                                op: BinaryOperator::Gt,
                                right: Box::new(lit_int(100)),
                            },
                        );
                        assert_eq!(predicate, expected);
                    }
                    _ => panic!("Expected Propagated Filter on Right branch"),
                }
            }
            _ => panic!("Expected HashJoin at root"),
        }
    }

    #[test]
    fn test_equivalence_multi_join_and_predicates() {
        // Table A: users [users.id, users.age]
        let schema_a = Arc::new(Schema::new(vec![
            Field {
                name: "users.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "users.age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
        ]));

        // Table B: orders [orders.id, orders.amount]
        let schema_b = Arc::new(Schema::new(vec![
            Field {
                name: "orders.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]));

        // Table C: shipments [shipments.id, shipments.active]
        let schema_c = Arc::new(Schema::new(vec![
            Field {
                name: "shipments.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "shipments.active".to_string(),
                data_type: DataType::Boolean,
                nullable: true,
            },
        ]));

        let scan_a = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: schema_a.clone(),
            projections: None,
            pruner: None,
        };

        let scan_b = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: schema_b.clone(),
            projections: None,
            pruner: None,
        };

        let scan_c = LogicalPlan::Scan {
            table_name: "shipments".to_string(),
            schema: schema_c.clone(),
            projections: None,
            pruner: None,
        };

        // Combined Schema AB: [users.id, users.age, orders.id, orders.amount]
        let schema_ab = Arc::new(Schema::new(vec![
            Field {
                name: "users.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "users.age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]));

        // Combined Schema ABC: [users.id, users.age, orders.id, orders.amount, shipments.id, shipments.active]
        let schema_abc = Arc::new(Schema::new(vec![
            Field {
                name: "users.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "users.age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
            Field {
                name: "shipments.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "shipments.active".to_string(),
                data_type: DataType::Boolean,
                nullable: true,
            },
        ]));

        // Join A and B: users.id = orders.id
        let join_ab = LogicalPlan::HashJoin {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            on: vec![(
                compound_col(vec!["users", "id"]),
                compound_col(vec!["orders", "id"]),
            )],
            join_type: JoinType::Inner,
            schema: schema_ab,
        };

        // Join AB and C: users.id = shipments.id
        let join_abc = LogicalPlan::HashJoin {
            left: Box::new(join_ab),
            right: Box::new(scan_c),
            on: vec![(
                compound_col(vec!["users", "id"]),
                compound_col(vec!["shipments", "id"]),
            )],
            join_type: JoinType::Inner,
            schema: schema_abc,
        };

        // Predicates: (users.id = 42) AND (orders.amount > 100) AND (shipments.active = true) AND (users.age < 30)
        // Note: (users.id = 42) should propagate to all three tables (users, orders, shipments)!
        let predicate = and(
            eq(compound_col(vec!["users", "id"]), lit_int(42)),
            and(
                LogicalExpr::BinaryOp {
                    left: Box::new(compound_col(vec!["orders", "amount"])),
                    op: BinaryOperator::Gt,
                    right: Box::new(lit_int(100)),
                },
                and(
                    eq(
                        compound_col(vec!["shipments", "active"]),
                        LogicalExpr::Value(Value::Boolean(true)),
                    ),
                    LogicalExpr::BinaryOp {
                        left: Box::new(compound_col(vec!["users", "age"])),
                        op: BinaryOperator::Lt,
                        right: Box::new(lit_int(30)),
                    },
                ),
            ),
        );

        let filter_plan = LogicalPlan::Filter {
            predicate,
            input: Box::new(join_abc),
        };

        let rule = EquivalenceFilterPropagation;
        let optimized_plan = rule.rewrite(filter_plan).unwrap();

        // Assert that all three scans received their respective pushed and propagated filters!
        match optimized_plan {
            LogicalPlan::HashJoin { left, right, .. } => {
                // Right scan (shipments) should have (shipments.id = 42) AND (shipments.active = true)
                match *right {
                    LogicalPlan::Filter { predicate, .. } => {
                        let expected = and(
                            eq(compound_col(vec!["shipments", "id"]), lit_int(42)),
                            eq(
                                compound_col(vec!["shipments", "active"]),
                                LogicalExpr::Value(Value::Boolean(true)),
                            ),
                        );
                        assert_eq!(predicate, expected);
                    }
                    _ => panic!(
                        "Expected Filter on Right scan (shipments), but found {:#?}",
                        *right
                    ),
                }

                // Left sub-plan (HashJoin AB)
                match *left {
                    LogicalPlan::HashJoin {
                        left: sub_left,
                        right: sub_right,
                        ..
                    } => {
                        // Sub-Left scan (users) should have (users.id = 42) AND (users.age < 30)
                        match *sub_left {
                            LogicalPlan::Filter { predicate, .. } => {
                                let expected = and(
                                    eq(compound_col(vec!["users", "id"]), lit_int(42)),
                                    LogicalExpr::BinaryOp {
                                        left: Box::new(compound_col(vec!["users", "age"])),
                                        op: BinaryOperator::Lt,
                                        right: Box::new(lit_int(30)),
                                    },
                                );
                                assert_eq!(predicate, expected);
                            }
                            _ => panic!(
                                "Expected Filter on sub-Left scan (users), but found {:#?}",
                                *sub_left
                            ),
                        }

                        // Sub-Right scan (orders) should have (orders.id = 42) AND (orders.amount > 100)
                        match *sub_right {
                            LogicalPlan::Filter { predicate, .. } => {
                                let expected = and(
                                    eq(compound_col(vec!["orders", "id"]), lit_int(42)),
                                    LogicalExpr::BinaryOp {
                                        left: Box::new(compound_col(vec!["orders", "amount"])),
                                        op: BinaryOperator::Gt,
                                        right: Box::new(lit_int(100)),
                                    },
                                );
                                assert_eq!(predicate, expected);
                            }
                            _ => panic!(
                                "Expected Filter on sub-Right scan (orders), but found {:#?}",
                                *sub_right
                            ),
                        }
                    }
                    _ => panic!("Expected sub-HashJoin, but found {:#?}", *left),
                }
            }
            o => panic!("Expected HashJoin at root, but found {:#?}", o),
        }
    }

    #[test]
    fn test_equivalence_multi_class_and_many_propagations() {
        // Table A: users [users.id, users.type, users.age]
        let schema_a = Arc::new(Schema::new(vec![
            Field {
                name: "users.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "users.type".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "users.age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
        ]));

        // Table B: orders [orders.id, orders.type, orders.status, orders.amount]
        let schema_b = Arc::new(Schema::new(vec![
            Field {
                name: "orders.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.type".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "orders.status".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "orders.amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]));

        // Table C: shipments [shipments.id, shipments.status, shipments.active]
        let schema_c = Arc::new(Schema::new(vec![
            Field {
                name: "shipments.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "shipments.status".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "shipments.active".to_string(),
                data_type: DataType::Boolean,
                nullable: true,
            },
        ]));

        let scan_a = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: schema_a.clone(),
            projections: None,
            pruner: None,
        };

        let scan_b = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: schema_b.clone(),
            projections: None,
            pruner: None,
        };

        let scan_c = LogicalPlan::Scan {
            table_name: "shipments".to_string(),
            schema: schema_c.clone(),
            projections: None,
            pruner: None,
        };

        // Combined Schema AB: [users.id, users.type, users.age, orders.id, orders.type, orders.status, orders.amount]
        let schema_ab = Arc::new(Schema::new(vec![
            Field {
                name: "users.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "users.type".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "users.age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.type".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "orders.status".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "orders.amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]));

        // Combined Schema ABC
        let schema_abc = Arc::new(Schema::new(vec![
            Field {
                name: "users.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "users.type".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "users.age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.type".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "orders.status".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "orders.amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
            Field {
                name: "shipments.id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "shipments.status".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "shipments.active".to_string(),
                data_type: DataType::Boolean,
                nullable: true,
            },
        ]));

        // Join AB on users.id = orders.id AND users.type = orders.type (Equivalence classes 1 & 2!)
        let join_ab = LogicalPlan::HashJoin {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            on: vec![
                (
                    compound_col(vec!["users", "id"]),
                    compound_col(vec!["orders", "id"]),
                ),
                (
                    compound_col(vec!["users", "type"]),
                    compound_col(vec!["orders", "type"]),
                ),
            ],
            join_type: JoinType::Inner,
            schema: schema_ab,
        };

        // Join ABC on users.id = shipments.id AND orders.status = shipments.status (Equivalence classes 1 & 3!)
        let join_abc = LogicalPlan::HashJoin {
            left: Box::new(join_ab),
            right: Box::new(scan_c),
            on: vec![
                (
                    compound_col(vec!["users", "id"]),
                    compound_col(vec!["shipments", "id"]),
                ),
                (
                    compound_col(vec!["orders", "status"]),
                    compound_col(vec!["shipments", "status"]),
                ),
            ],
            join_type: JoinType::Inner,
            schema: schema_abc,
        };

        // Predicates:
        // 1. (users.id = 42) -> Propagates to orders.id = 42 AND shipments.id = 42
        // 2. (users.type = 'Premium') -> Propagates to orders.type = 'Premium'
        // 3. (shipments.status = 'Dispatched') -> Propagates to orders.status = 'Dispatched'
        // 4. (orders.amount > 100) -> Local orders filter
        // 5. (users.age < 30) -> Local users filter
        // 6. (shipments.active = true) -> Local shipments filter
        let val_premium = LogicalExpr::Value(Value::SingleQuotedString("Premium".to_string()));
        let val_dispatched =
            LogicalExpr::Value(Value::SingleQuotedString("Dispatched".to_string()));

        let predicate = and(
            eq(compound_col(vec!["users", "id"]), lit_int(42)),
            and(
                eq(compound_col(vec!["users", "type"]), val_premium.clone()),
                and(
                    eq(
                        compound_col(vec!["shipments", "status"]),
                        val_dispatched.clone(),
                    ),
                    and(
                        LogicalExpr::BinaryOp {
                            left: Box::new(compound_col(vec!["orders", "amount"])),
                            op: BinaryOperator::Gt,
                            right: Box::new(lit_int(100)),
                        },
                        and(
                            LogicalExpr::BinaryOp {
                                left: Box::new(compound_col(vec!["users", "age"])),
                                op: BinaryOperator::Lt,
                                right: Box::new(lit_int(30)),
                            },
                            eq(
                                compound_col(vec!["shipments", "active"]),
                                LogicalExpr::Value(Value::Boolean(true)),
                            ),
                        ),
                    ),
                ),
            ),
        );

        let filter_plan = LogicalPlan::Filter {
            predicate,
            input: Box::new(join_abc),
        };

        let rule = EquivalenceFilterPropagation;
        let optimized_plan = rule.rewrite(filter_plan).unwrap();

        // Let's assert the exact compiled, split, and propagated filters on all three scans!
        match optimized_plan {
            LogicalPlan::HashJoin { left, right, .. } => {
                // Right scan (shipments) should have:
                // (shipments.id = 42) AND (shipments.status = 'Dispatched') AND (shipments.active = true)
                match *right {
                    LogicalPlan::Filter { predicate, .. } => {
                        let expected = and(
                            eq(compound_col(vec!["shipments", "id"]), lit_int(42)),
                            and(
                                eq(
                                    compound_col(vec!["shipments", "status"]),
                                    val_dispatched.clone(),
                                ),
                                eq(
                                    compound_col(vec!["shipments", "active"]),
                                    LogicalExpr::Value(Value::Boolean(true)),
                                ),
                            ),
                        );
                        assert_eq!(predicate, expected);
                    }
                    _ => panic!("Expected Filter on shipments, but found {:#?}", *right),
                }

                // Left plan (HashJoin AB)
                match *left {
                    LogicalPlan::HashJoin {
                        left: sub_left,
                        right: sub_right,
                        ..
                    } => {
                        // Sub-Left scan (users) should have:
                        // (users.id = 42) AND (users.type = 'Premium') AND (users.age < 30)
                        match *sub_left {
                            LogicalPlan::Filter { predicate, .. } => {
                                let expected = and(
                                    eq(compound_col(vec!["users", "id"]), lit_int(42)),
                                    and(
                                        eq(
                                            compound_col(vec!["users", "type"]),
                                            val_premium.clone(),
                                        ),
                                        LogicalExpr::BinaryOp {
                                            left: Box::new(compound_col(vec!["users", "age"])),
                                            op: BinaryOperator::Lt,
                                            right: Box::new(lit_int(30)),
                                        },
                                    ),
                                );
                                assert_eq!(predicate, expected);
                            }
                            _ => panic!("Expected Filter on users, but found {:#?}", *sub_left),
                        }

                        // Sub-Right scan (orders) should have:
                        // (orders.id = 42) AND (orders.type = 'Premium') AND (orders.status = 'Dispatched') AND (orders.amount > 100)
                        match *sub_right {
                            LogicalPlan::Filter { predicate, .. } => {
                                let expected = and(
                                    eq(compound_col(vec!["orders", "id"]), lit_int(42)),
                                    and(
                                        eq(compound_col(vec!["orders", "type"]), val_premium),
                                        and(
                                            eq(
                                                compound_col(vec!["orders", "status"]),
                                                val_dispatched,
                                            ),
                                            LogicalExpr::BinaryOp {
                                                left: Box::new(compound_col(vec![
                                                    "orders", "amount",
                                                ])),
                                                op: BinaryOperator::Gt,
                                                right: Box::new(lit_int(100)),
                                            },
                                        ),
                                    ),
                                );
                                assert_eq!(predicate, expected);
                            }
                            _ => panic!("Expected Filter on orders, but found {:#?}", *sub_right),
                        }
                    }
                    _ => panic!("Expected sub-HashJoin"),
                }
            }
            _ => panic!("Expected HashJoin at root"),
        }
    }
}
