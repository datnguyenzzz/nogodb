use std::collections::HashSet;

use anyhow::Result;

use crate::{
    optimiser::{OptimiserRule, accumulate_columns},
    planner::LogicalPlan,
};

/// This rule identifies exactly which columns are referenced by the entire query,
/// and pushes this set of columns down into the `LogicalPlan::Scan::projections` field.
#[derive(Default)]
pub struct ProjectionPushdown;

impl ProjectionPushdown {
    fn push_down(&self, plan: LogicalPlan, required: &mut HashSet<String>) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projections: _,
                pruner,
            } => {
                // If the required set is empty, we must select at least 1 column (e.g. SELECT COUNT(*))
                let projections = if required.is_empty() {
                    schema.fields().first().map(|f| vec![f.name.clone()])
                } else {
                    Some(
                        schema
                            .fields()
                            .iter()
                            .map(|f| f.name.clone())
                            .filter(|name| {
                                // Match either the full qualified name ("users.id")
                                // or the bare column suffix ("id" matches field "users.id")
                                required.contains(name)
                                    || name
                                        .split('.')
                                        .last()
                                        .map_or(false, |suffix| required.contains(suffix))
                            })
                            .collect(),
                    )
                };

                Ok(LogicalPlan::Scan {
                    table_name,
                    schema,
                    projections,
                    pruner,
                })
            }
            LogicalPlan::Projection {
                exprs,
                input,
                schema,
            } => {
                let mut sub_required = HashSet::new();
                for col in &exprs {
                    accumulate_columns(col, &mut sub_required);
                }

                Ok(LogicalPlan::Projection {
                    exprs,
                    input: Box::new(self.push_down(*input, &mut sub_required)?),
                    schema,
                })
            }
            LogicalPlan::Filter { predicate, input } => {
                accumulate_columns(&predicate, required);

                Ok(LogicalPlan::Filter {
                    predicate,
                    input: Box::new(self.push_down(*input, required)?),
                })
            }
            LogicalPlan::HashJoin {
                left,
                right,
                on,
                join_type,
                schema,
            } => {
                for (l_key, r_key) in &on {
                    accumulate_columns(&l_key, required);
                    accumulate_columns(&r_key, required);
                }

                Ok(LogicalPlan::HashJoin {
                    left: Box::new(self.push_down(*left, &mut required.clone())?),
                    right: Box::new(self.push_down(*right, &mut required.clone())?),
                    on,
                    join_type,
                    schema,
                })
            }
            LogicalPlan::Aggregate {
                group_by,
                input,
                schema,
            } => {
                for col in &group_by {
                    accumulate_columns(col, required);
                }
                Ok(LogicalPlan::Aggregate {
                    group_by,
                    input: Box::new(self.push_down(*input, required)?),
                    schema,
                })
            }
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } => Ok(LogicalPlan::Limit {
                limit,
                offset,
                input: Box::new(self.push_down(*input, required)?),
            }),
            LogicalPlan::Sort { sort_exprs, input } => {
                for col in &sort_exprs {
                    accumulate_columns(col, required);
                }
                Ok(LogicalPlan::Sort {
                    sort_exprs,
                    input: Box::new(self.push_down(*input, required)?),
                })
            }
            other => Ok(other),
        }
    }
}

impl OptimiserRule for ProjectionPushdown {
    fn name(&self) -> &str {
        "ProjectionPushdownRule"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut required = HashSet::new();
        self.push_down(plan, &mut required)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{DataType, Field, Schema};
    use crate::planner::{JoinType, LogicalExpr};
    use std::sync::Arc;

    fn compound_col(parts: Vec<&str>) -> LogicalExpr {
        LogicalExpr::CompoundColumn(parts.into_iter().map(|p| p.to_string()).collect())
    }

    #[test]
    fn test_projection_pushdown_basic() {
        let schema = Arc::new(Schema::new(vec![
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
            Field {
                name: "users.age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
        ]));

        let scan_node = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: schema.clone(),
            projections: None,
            pruner: None,
        };

        // Project only users.id and users.name!
        let proj_node = LogicalPlan::Projection {
            exprs: vec![
                compound_col(vec!["users", "id"]),
                compound_col(vec!["users", "name"]),
            ],
            input: Box::new(scan_node),
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        let rule = ProjectionPushdown::default();
        let optimized = rule.rewrite(proj_node).unwrap();

        // Assert that Scan's projections has been pruned exactly to users.id and users.name!
        match optimized {
            LogicalPlan::Projection { input, .. } => {
                match *input {
                    LogicalPlan::Scan { projections, .. } => {
                        let cols = projections.unwrap();
                        assert_eq!(cols.len(), 2);
                        assert!(cols.contains(&"users.id".to_string()));
                        assert!(cols.contains(&"users.name".to_string()));
                        assert!(!cols.contains(&"users.age".to_string())); // Age is pruned!
                    }
                    _ => panic!("Expected Scan child"),
                }
            }
            _ => panic!("Expected Projection root"),
        }
    }

    #[test]
    fn test_projection_pushdown_complex_multi_join() {
        use crate::planner::ast::operators::BinaryOperator;

        // Table A: users [users.id, users.name, users.age, users.email]
        let schema_a = Arc::new(Schema::new(vec![
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
            Field {
                name: "users.age".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "users.email".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            }, // Unused!
        ]));

        // Table B: orders [orders.user_id, orders.status, orders.date, orders.amount, orders.notes]
        let schema_b = Arc::new(Schema::new(vec![
            Field {
                name: "orders.user_id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.status".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "orders.date".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "orders.amount".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
            Field {
                name: "orders.notes".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            }, // Unused!
        ]));

        // Table C: shipments [shipments.status, shipments.active, shipments.carrier]
        let schema_c = Arc::new(Schema::new(vec![
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
            Field {
                name: "shipments.carrier".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            }, // Unused!
        ]));

        let scan_a = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: schema_a,
            projections: None,
            pruner: None,
        };

        let scan_b = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: schema_b,
            projections: None,
            pruner: None,
        };

        let scan_c = LogicalPlan::Scan {
            table_name: "shipments".to_string(),
            schema: schema_c,
            projections: None,
            pruner: None,
        };

        // Join AB: users.id = orders.user_id
        let join_ab = LogicalPlan::HashJoin {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            on: vec![(
                compound_col(vec!["users", "id"]),
                compound_col(vec!["orders", "user_id"]),
            )],
            join_type: JoinType::Inner,
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        // Join ABC: orders.status = shipments.status
        let join_abc = LogicalPlan::HashJoin {
            left: Box::new(join_ab),
            right: Box::new(scan_c),
            on: vec![(
                compound_col(vec!["orders", "status"]),
                compound_col(vec!["shipments", "status"]),
            )],
            join_type: JoinType::Inner,
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        // Filter C (shipments.active = true)
        let filter_c = LogicalPlan::Filter {
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(compound_col(vec!["shipments", "active"])),
                op: BinaryOperator::Eq,
                right: Box::new(LogicalExpr::Value(
                    crate::sql_parser::ast::expr::Value::Boolean(true),
                )),
            },
            input: Box::new(join_abc),
        };

        // Filter B (orders.date = '2026-08-17')
        let filter_b = LogicalPlan::Filter {
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(compound_col(vec!["orders", "date"])),
                op: BinaryOperator::Eq,
                right: Box::new(LogicalExpr::Value(
                    crate::sql_parser::ast::expr::Value::SingleQuotedString(
                        "2026-08-17".to_string(),
                    ),
                )),
            },
            input: Box::new(filter_c),
        };

        // Filter A (users.age > 30)
        let filter_a = LogicalPlan::Filter {
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(compound_col(vec!["users", "age"])),
                op: BinaryOperator::Gt,
                right: Box::new(LogicalExpr::Value(
                    crate::sql_parser::ast::expr::Value::Number("30".to_string(), false),
                )),
            },
            input: Box::new(filter_b),
        };

        // Projection: users.name, orders.amount
        let proj_node = LogicalPlan::Projection {
            exprs: vec![
                compound_col(vec!["users", "name"]),
                compound_col(vec!["orders", "amount"]),
            ],
            input: Box::new(filter_a),
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        let rule = ProjectionPushdown::default();
        let optimized = rule.rewrite(proj_node).unwrap();

        // Assert that the physical column projections for all three table scans match exactly!
        match optimized {
            LogicalPlan::Projection {
                input: filter_a_opt,
                ..
            } => {
                match *filter_a_opt {
                    LogicalPlan::Filter {
                        input: filter_b_opt,
                        ..
                    } => {
                        match *filter_b_opt {
                            LogicalPlan::Filter {
                                input: filter_c_opt,
                                ..
                            } => {
                                match *filter_c_opt {
                                    LogicalPlan::Filter {
                                        input: join_abc_opt,
                                        ..
                                    } => {
                                        match *join_abc_opt {
                                            LogicalPlan::HashJoin {
                                                left: join_ab_opt,
                                                right: scan_c_opt,
                                                ..
                                            } => {
                                                // Verify shipments scan projections:
                                                // Should require: shipments.status (join key) and shipments.active (filter)
                                                // Should NOT require: shipments.carrier (unused!)
                                                match *scan_c_opt {
                                                    LogicalPlan::Scan { projections, .. } => {
                                                        let cols = projections.unwrap();
                                                        assert_eq!(cols.len(), 2);
                                                        assert!(cols.contains(
                                                            &"shipments.status".to_string()
                                                        ));
                                                        assert!(cols.contains(
                                                            &"shipments.active".to_string()
                                                        ));
                                                        assert!(!cols.contains(
                                                            &"shipments.carrier".to_string()
                                                        ));
                                                    }
                                                    _ => panic!("Expected shipments Scan"),
                                                }

                                                // AB Join branch
                                                match *join_ab_opt {
                                                    LogicalPlan::HashJoin {
                                                        left: scan_a_opt,
                                                        right: scan_b_opt,
                                                        ..
                                                    } => {
                                                        // Verify users scan projections:
                                                        // Should require: users.name (projection), users.id (join key), and users.age (filter)
                                                        // Should NOT require: users.email (unused!)
                                                        match *scan_a_opt {
                                                            LogicalPlan::Scan {
                                                                projections,
                                                                ..
                                                            } => {
                                                                let cols = projections.unwrap();
                                                                assert_eq!(cols.len(), 3);
                                                                assert!(cols.contains(
                                                                    &"users.name".to_string()
                                                                ));
                                                                assert!(cols.contains(
                                                                    &"users.id".to_string()
                                                                ));
                                                                assert!(cols.contains(
                                                                    &"users.age".to_string()
                                                                ));
                                                                assert!(!cols.contains(
                                                                    &"users.email".to_string()
                                                                ));
                                                            }
                                                            _ => panic!("Expected users Scan"),
                                                        }

                                                        // Verify orders scan projections:
                                                        // Should require: orders.amount (projection), orders.user_id (join key), orders.status (join key), and orders.date (filter)
                                                        // Should NOT require: orders.notes (unused!)
                                                        match *scan_b_opt {
                                                            LogicalPlan::Scan {
                                                                projections,
                                                                ..
                                                            } => {
                                                                let cols = projections.unwrap();
                                                                assert_eq!(cols.len(), 4);
                                                                assert!(cols.contains(
                                                                    &"orders.amount".to_string()
                                                                ));
                                                                assert!(cols.contains(
                                                                    &"orders.user_id".to_string()
                                                                ));
                                                                assert!(cols.contains(
                                                                    &"orders.status".to_string()
                                                                ));
                                                                assert!(cols.contains(
                                                                    &"orders.date".to_string()
                                                                ));
                                                                assert!(!cols.contains(
                                                                    &"orders.notes".to_string()
                                                                ));
                                                            }
                                                            _ => panic!("Expected orders Scan"),
                                                        }
                                                    }
                                                    _ => panic!("Expected sub-HashJoin AB"),
                                                }
                                            }
                                            _ => panic!("Expected HashJoin ABC"),
                                        }
                                    }
                                    _ => panic!("Expected Filter C"),
                                }
                            }
                            _ => panic!("Expected Filter B"),
                        }
                    }
                    _ => panic!("Expected Filter A"),
                }
            }
            _ => panic!("Expected Projection root"),
        }
    }
}
