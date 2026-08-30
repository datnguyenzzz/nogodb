use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{
    optimiser::{OptimiserRule, get_column_canonical_name},
    planner::{DynamicJoinPruner, LogicalPlan},
};

/// Sideways Information Passing ("Dynamic Join-Filter Pushdown")
/// This rule identifies HashJoins, generates dynamic pruners based on
/// the join-keys, and injects them sideways into the probe-side's table scan.
///
/// The dynamic pruners will be useful during the join process.
/// Join execution proceeds in two (alternating) phases:
///   1. Read rows from build/outer input h. Record min/max
///      values m_b / M_b in column of build side
///   2. While reading rows from the probe/inner input
///      use m_b / M_b  on column of probe side to pre-filter rows.
#[derive(Default)]
pub struct DynamicJoinFilter {
    next_join_id: AtomicUsize,
}

impl DynamicJoinFilter {
    /// Recursively traverses a sub-plan to inject a dynamic pruner
    /// into the matching Scan node
    pub fn inject_pruner(&self, plan: LogicalPlan, pruner: DynamicJoinPruner) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projections,
                pruner: existing_pruner,
            } => {
                let matched = schema.fields().iter().any(|f| {
                    f.name == pruner.probe_col
                        || f.name.ends_with(&format!(".{}", pruner.probe_col))
                });

                let new_prunner = if matched {
                    let mut list = existing_pruner.unwrap_or_default();
                    list.push(pruner);
                    Some(list)
                } else {
                    existing_pruner
                };

                LogicalPlan::Scan {
                    table_name,
                    schema,
                    projections,
                    pruner: new_prunner,
                }
            }
            LogicalPlan::Projection {
                exprs,
                input,
                schema,
            } => LogicalPlan::Projection {
                exprs,
                input: Box::new(self.inject_pruner(*input, pruner)),
                schema,
            },
            LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
                predicate,
                input: Box::new(self.inject_pruner(*input, pruner)),
            },
            LogicalPlan::HashJoin {
                left,
                right,
                on,
                join_type,
                schema,
            } => LogicalPlan::HashJoin {
                left: Box::new(self.inject_pruner(*left, pruner.clone())),
                right: Box::new(self.inject_pruner(*right, pruner)),
                on,
                join_type,
                schema,
            },
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } => LogicalPlan::Limit {
                limit,
                offset,
                input: Box::new(self.inject_pruner(*input, pruner)),
            },
            LogicalPlan::Sort { sort_exprs, input } => LogicalPlan::Sort {
                sort_exprs,
                input: Box::new(self.inject_pruner(*input, pruner)),
            },
            other => other,
        }
    }
}

impl OptimiserRule for DynamicJoinFilter {
    fn name(&self) -> &str {
        "DynamicJoinFilterRule"
    }

    fn rewrite(&self, plan: LogicalPlan) -> anyhow::Result<LogicalPlan> {
        match plan {
            LogicalPlan::HashJoin {
                left,
                right,
                on,
                join_type,
                schema,
            } => {
                let join_id = self.next_join_id.fetch_add(1, Ordering::Relaxed);
                let mut left_plan = self.rewrite(*left)?;
                let right_plan = self.rewrite(*right)?;

                for (left_key, right_key) in &on {
                    if let (Some(left_col), Some(right_col)) = (
                        get_column_canonical_name(&left_key),
                        get_column_canonical_name(&right_key),
                    ) {
                        left_plan = self.inject_pruner(
                            left_plan,
                            DynamicJoinPruner {
                                join_id,
                                build_col: right_col,
                                probe_col: left_col,
                            },
                        )
                    }
                }

                Ok(LogicalPlan::HashJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    on,
                    join_type,
                    schema,
                })
            }
            LogicalPlan::Projection {
                exprs,
                input,
                schema,
            } => Ok(LogicalPlan::Projection {
                exprs,
                input: Box::new(self.rewrite(*input)?),
                schema,
            }),
            LogicalPlan::Filter { predicate, input } => Ok(LogicalPlan::Filter {
                predicate,
                input: Box::new(self.rewrite(*input)?),
            }),
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } => Ok(LogicalPlan::Limit {
                limit,
                offset,
                input: Box::new(self.rewrite(*input)?),
            }),
            LogicalPlan::Sort { sort_exprs, input } => Ok(LogicalPlan::Sort {
                sort_exprs,
                input: Box::new(self.rewrite(*input)?),
            }),
            other => Ok(other),
        }
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
    fn test_join_filter_pushdown_basic() {
        let left_schema = Arc::new(Schema::new(vec![Field {
            name: "users.id".to_string(),
            data_type: DataType::Int32,
            nullable: true,
        }]));
        let right_schema = Arc::new(Schema::new(vec![Field {
            name: "orders.user_id".to_string(),
            data_type: DataType::Int32,
            nullable: true,
        }]));

        let left_scan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: left_schema,
            projections: None,
            pruner: None,
        };

        let right_scan = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: right_schema,
            projections: None,
            pruner: None,
        };

        // Join users.id = orders.user_id
        let join_node = LogicalPlan::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: vec![(
                compound_col(vec!["users", "id"]),
                compound_col(vec!["orders", "user_id"]),
            )],
            join_type: JoinType::Inner,
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        // Run Join Pushdown rule!
        let rule = DynamicJoinFilter::default();
        let optimized = rule.rewrite(join_node).unwrap();

        // Assert that Left Scan (users) received the DynamicJoinPruner!
        match optimized {
            LogicalPlan::HashJoin { left, .. } => match *left {
                LogicalPlan::Scan { pruner, .. } => {
                    let pruners = pruner.unwrap();
                    assert_eq!(pruners.len(), 1);
                    assert_eq!(pruners[0].join_id, 0);
                    assert_eq!(pruners[0].build_col, "orders.user_id");
                    assert_eq!(pruners[0].probe_col, "users.id");
                }
                _ => panic!("Expected Scan on left branch"),
            },
            _ => panic!("Expected HashJoin root"),
        }
    }

    #[test]
    fn test_join_filter_pushdown_composite_keys() {
        let left_schema = Arc::new(Schema::new(vec![
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
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field {
                name: "orders.user_id".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            Field {
                name: "orders.user_type".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ]));

        let left_scan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: left_schema,
            projections: None,
            pruner: None,
        };

        let right_scan = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: right_schema,
            projections: None,
            pruner: None,
        };

        // Join ON users.id = orders.user_id AND users.type = orders.user_type
        let join_node = LogicalPlan::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: vec![
                (
                    compound_col(vec!["users", "id"]),
                    compound_col(vec!["orders", "user_id"]),
                ),
                (
                    compound_col(vec!["users", "type"]),
                    compound_col(vec!["orders", "user_type"]),
                ),
            ],
            join_type: JoinType::Inner,
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        let rule = DynamicJoinFilter::default();
        let optimized = rule.rewrite(join_node).unwrap();

        // Assert that Left Scan (users) received BOTH dynamic join pruners!
        match optimized {
            LogicalPlan::HashJoin { left, .. } => {
                match *left {
                    LogicalPlan::Scan { pruner, .. } => {
                        let pruners = pruner.unwrap();
                        assert_eq!(pruners.len(), 2); // Both pruners pushed!

                        assert_eq!(pruners[0].build_col, "orders.user_id");
                        assert_eq!(pruners[0].probe_col, "users.id");

                        assert_eq!(pruners[1].build_col, "orders.user_type");
                        assert_eq!(pruners[1].probe_col, "users.type");
                    }
                    _ => panic!("Expected Scan on left branch"),
                }
            }
            _ => panic!("Expected HashJoin root"),
        }
    }

    #[test]
    fn test_join_filter_pushdown_multi_join_composite() {
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

        let left_scan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: schema_a,
            projections: None,
            pruner: None,
        };

        let right_scan = LogicalPlan::Scan {
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

        // Join AB: users.id = orders.id AND users.type = orders.type
        let join_ab = LogicalPlan::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
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
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        // Join ABC: users.id = shipments.id AND orders.status = shipments.status
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
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        // Run Optimizer Rule!
        let rule = DynamicJoinFilter::default();
        let optimized = rule.rewrite(join_abc).unwrap();

        // Assert that pruners are pushed down correctly across multiple levels of Joins!
        match optimized {
            LogicalPlan::HashJoin {
                left,
                right: right_scan_c,
                ..
            } => {
                // Table C (shipments) is on the build side of join_abc; it should NOT receive any pruners!
                match *right_scan_c {
                    LogicalPlan::Scan { pruner, .. } => {
                        assert!(pruner.is_none());
                    }
                    _ => panic!("Expected Scan on Right branch (shipments)"),
                }

                // Table AB (HashJoin AB)
                match *left {
                    LogicalPlan::HashJoin {
                        left: sub_left,
                        right: sub_right,
                        ..
                    } => {
                        // Table A (users) should have received 3 pruners:
                        // - 2 from Join AB (id and type, join_id = 0)
                        // - 1 from Join ABC (id, join_id = 1)
                        match *sub_left {
                            LogicalPlan::Scan { pruner, .. } => {
                                let pruners = pruner.unwrap();
                                assert_eq!(pruners.len(), 3);

                                // From Join AB (First join, join_id = 1 due to top-down traversal)
                                assert_eq!(pruners[0].join_id, 1);
                                assert_eq!(pruners[0].build_col, "orders.id");
                                assert_eq!(pruners[0].probe_col, "users.id");

                                assert_eq!(pruners[1].join_id, 1);
                                assert_eq!(pruners[1].build_col, "orders.type");
                                assert_eq!(pruners[1].probe_col, "users.type");

                                // From Join ABC (Second join, join_id = 0 due to top-down traversal)
                                assert_eq!(pruners[2].join_id, 0);
                                assert_eq!(pruners[2].build_col, "shipments.id");
                                assert_eq!(pruners[2].probe_col, "users.id");
                            }
                            _ => panic!("Expected Scan on sub-Left branch (users)"),
                        }

                        // Table B (orders) should have received 1 pruner:
                        // - 1 from Join ABC (status, join_id = 0)
                        match *sub_right {
                            LogicalPlan::Scan { pruner, .. } => {
                                let pruners = pruner.unwrap();
                                assert_eq!(pruners.len(), 1);
                                assert_eq!(pruners[0].join_id, 0);
                                assert_eq!(pruners[0].build_col, "shipments.status");
                                assert_eq!(pruners[0].probe_col, "orders.status");
                            }
                            _ => panic!("Expected Scan on sub-Right branch (orders)"),
                        }
                    }
                    _ => panic!("Expected sub-HashJoin node"),
                }
            }
            _ => panic!("Expected HashJoin root"),
        }
    }
}
