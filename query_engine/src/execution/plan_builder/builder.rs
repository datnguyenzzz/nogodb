use std::{mem, sync::Arc};

use anyhow::{Result, anyhow, bail};

use crate::{
    arrow::SchemaRef,
    execution::{
        PhysicalColumn, PhysicalComparison, PhysicalExpr,
        dispatcher::Dispatcher,
        pipeline::{PhysicalOperator, PhysicalSink, Pipeline, PipelineID},
        plan_builder::{
            PhysicalFilterNode, PhysicalHashJoinNode, PhysicalLimitNode, PhysicalPlanNode,
            PhysicalScanNode, PhysicalSortNode,
        },
        scan::ScanSource,
        scheduler::MailBoxSender,
        sort::SortColumn,
    },
    planner::{LogicalExpr, LogicalPlan},
    storage::DataStorage,
};

pub struct PlanBuilder {
    pub pipelines: Vec<Pipeline>,
    pub next_pipeline_id: PipelineID,
    pub storage: Arc<dyn DataStorage>,
    pub operators: Vec<Box<dyn PhysicalOperator>>,
    pub dispatcher: Arc<Dispatcher>,
    pub current_deps: Vec<PipelineID>,
    pub current_source: Option<Arc<ScanSource>>,
    pub mailboxes: Arc<Vec<MailBoxSender>>,
    pub total_cores: usize,
}

impl PlanBuilder {
    pub fn new(
        storage: Arc<dyn DataStorage>,
        dispatcher: Arc<Dispatcher>,
        mailboxes: Arc<Vec<MailBoxSender>>,
        total_cores: usize,
    ) -> Self {
        Self {
            pipelines: Vec::new(),
            next_pipeline_id: 1,
            storage,
            operators: Vec::new(),
            dispatcher,
            current_deps: Vec::new(),
            current_source: None,
            mailboxes,
            total_cores,
        }
    }

    pub fn next_id(&mut self) -> usize {
        let id = self.next_pipeline_id;
        self.next_pipeline_id += 1;
        id
    }

    /// Emits a completed pipeline into the DAG builder
    pub fn emit_pipeline(
        &mut self,
        id: Option<usize>,
        sink: Box<dyn PhysicalSink>,
        schema: SchemaRef,
        downstream_id: Option<usize>,
    ) -> usize {
        let id = match id {
            Some(id) => id,
            None => self.next_id(),
        };
        let pipeline = Pipeline {
            id,
            source: self.current_source.take(),
            downstream_id,
            operators: mem::take(&mut self.operators),
            sink,
            dependencies: mem::take(&mut self.current_deps),
            partitions: self.total_cores,
            schema,
        };
        self.pipelines.push(pipeline);
        id
    }
}

pub struct PhysicalPlanGenerator {
    pub storage: Option<Arc<dyn DataStorage>>,
    pub total_cores: usize,
}

impl Default for PhysicalPlanGenerator {
    fn default() -> Self {
        Self {
            storage: None,
            total_cores: 1,
        }
    }
}

impl PhysicalPlanGenerator {
    pub fn new(storage: Arc<dyn DataStorage>, total_cores: usize) -> Self {
        Self {
            storage: Some(storage),
            total_cores,
        }
    }

    pub fn estimate_cardinality(&self, plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::Scan { table_name, .. } => {
                if let Some(storage) = &self.storage {
                    storage.get_page_count(table_name).unwrap_or(1)
                } else {
                    1
                }
            }
            LogicalPlan::Filter { input, .. } => self.estimate_cardinality(input),
            LogicalPlan::Projection { input, .. } => self.estimate_cardinality(input),
            LogicalPlan::HashJoin { left, right, .. } => {
                self.estimate_cardinality(left) + self.estimate_cardinality(right)
            }
            LogicalPlan::Limit { limit, .. } => *limit,
            _ => 10,
        }
    }

    pub fn create_plan(&self, logical_plan: &LogicalPlan) -> Result<Box<dyn PhysicalPlanNode>> {
        match logical_plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projections,
                pruner,
            } => Ok(Box::new(PhysicalScanNode {
                table_name: table_name.clone(),
                schema: schema.clone(),
                pruners: pruner.clone().unwrap_or_default(),
                projections: projections.clone(),
            })),
            LogicalPlan::Filter { predicate, input } => {
                let physical_input = self.create_plan(input)?;
                let physical_pred = self.compile_expr(predicate, &physical_input.schema())?;
                Ok(Box::new(PhysicalFilterNode {
                    predicate: physical_pred,
                    input: physical_input,
                }))
            }
            LogicalPlan::HashJoin {
                left,
                right,
                on,
                join_type,
                schema,
            } => {
                let physical_left = self.create_plan(left)?;
                let physical_right = self.create_plan(right)?;

                // Smaller side = Build, Larger side = Probe
                let left_size = self.estimate_cardinality(left);
                let right_size = self.estimate_cardinality(right);
                let left_is_build = left_size <= right_size;

                let on_keys = on
                    .iter()
                    .map(|(l, r)| (Self::expr_to_column_name(l), Self::expr_to_column_name(r)))
                    .collect();

                Ok(Box::new(PhysicalHashJoinNode {
                    left: physical_left,
                    right: physical_right,
                    on: on_keys,
                    join_type: *join_type,
                    schema: schema.clone(),
                    left_is_build,
                }))
            }
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } if matches!(**input, LogicalPlan::Sort { .. }) => {
                if let LogicalPlan::Sort {
                    sort_exprs,
                    input: sort_input,
                } = &**input
                {
                    let physical_input = self.create_plan(sort_input)?;
                    let sort_columns =
                        self.compile_sort_columns(sort_exprs, &physical_input.schema())?;
                    let schema = physical_input.schema();

                    Ok(Box::new(PhysicalSortNode {
                        sort_columns,
                        limit: Some((*offset, *limit)),
                        input: physical_input,
                        schema,
                    }))
                } else {
                    unreachable!()
                }
            }
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } => {
                let physical_input = self.create_plan(input)?;
                let schema = physical_input.schema();
                Ok(Box::new(PhysicalLimitNode {
                    skip: *offset,
                    fetch: *limit,
                    input: physical_input,
                    schema,
                }))
            }
            LogicalPlan::Sort { sort_exprs, input } => {
                let physical_input = self.create_plan(input)?;
                let sort_columns =
                    self.compile_sort_columns(sort_exprs, &physical_input.schema())?;
                let schema = physical_input.schema();
                Ok(Box::new(PhysicalSortNode {
                    sort_columns,
                    limit: None,
                    input: physical_input,
                    schema,
                }))
            }
            _ => bail!("Logical plan node not supported yet: {:?}", logical_plan),
        }
    }

    pub fn expr_to_column_name(expr: &LogicalExpr) -> String {
        match expr {
            LogicalExpr::Column(name) => name.clone(),
            LogicalExpr::CompoundColumn(parts) => parts.join("."),
            _ => format!("{:?}", expr),
        }
    }

    fn compile_sort_columns(
        &self,
        sort_exprs: &[LogicalExpr],
        schema: &crate::arrow::Schema,
    ) -> Result<Vec<SortColumn>> {
        let mut cols = Vec::new();
        for expr in sort_exprs {
            let col_name = Self::expr_to_column_name(expr);
            if let Some(col_idx) = schema
                .fields()
                .iter()
                .position(|f| f.name == col_name || f.name.ends_with(&format!(".{}", col_name)))
            {
                cols.push(SortColumn {
                    col_idx,
                    opt: crate::arrow::ord::SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                });
            }
        }
        Ok(cols)
    }

    fn compile_expr(
        &self,
        expr: &LogicalExpr,
        schema: &crate::arrow::Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            LogicalExpr::Column(name) => {
                let idx = schema
                    .fields()
                    .iter()
                    .position(|f| f.name == *name || f.name.ends_with(&format!(".{}", name)))
                    .ok_or_else(|| anyhow!("Column '{}' not found in schema", name))?;
                Ok(Arc::new(PhysicalColumn { index: idx }))
            }
            LogicalExpr::BinaryOp { left, op, right } => {
                let lhs = self.compile_expr(left, schema)?;
                let rhs = self.compile_expr(right, schema)?;
                Ok(Arc::new(PhysicalComparison {
                    lhs,
                    op: op.clone(),
                    rhs,
                }))
            }
            _ => bail!("Expression {:?} not yet supported in compile_expr", expr),
        }
    }

    pub fn compile_to_pipelines(
        &self,
        logical_plan: &LogicalPlan,
        storage: Arc<dyn DataStorage>,
        dispatcher: Arc<Dispatcher>,
        mailboxes: Arc<Vec<MailBoxSender>>,
    ) -> Result<Vec<Pipeline>> {
        let physical_root = self.create_plan(logical_plan)?;
        let mut builder = PlanBuilder::new(storage, dispatcher, mailboxes, self.total_cores);
        physical_root.build(&mut builder, None)?;

        // If the query was a pure Scan + Filter without terminal sink, emit final pipeline:
        if builder.current_source.is_some() || !builder.operators.is_empty() {
            let schema = physical_root.schema();
            let sink = Box::new(crate::execution::limit::PhysicalLimitSink::new(
                0,
                usize::MAX,
            ));
            builder.emit_pipeline(None, sink, schema, None);
        }

        Ok(builder.pipelines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{Buffer, DataType, Field, Schema};
    use crate::planner::JoinType;
    use crate::storage::{MockDataStorage, PageMetaData, StoragePage};
    use std::collections::HashMap;

    fn make_test_schema(col: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field {
            name: col.to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]))
    }

    #[test]
    fn test_compile_hash_join_smaller_side_is_build() {
        let storage = Arc::new(MockDataStorage::new());

        // Table "large": 10 pages
        for _ in 0..10 {
            storage.add_page(
                "large",
                StoragePage {
                    metadata: PageMetaData {
                        num_rows: 100,
                        column_stats: HashMap::new(),
                        header: Buffer::from(vec![1]),
                    },
                    data: Buffer::from(vec![1]),
                },
            );
        }

        // Table "small": 2 pages
        for _ in 0..2 {
            storage.add_page(
                "small",
                StoragePage {
                    metadata: PageMetaData {
                        num_rows: 100,
                        column_stats: HashMap::new(),
                        header: Buffer::from(vec![2]),
                    },
                    data: Buffer::from(vec![2]),
                },
            );
        }

        let large_schema = make_test_schema("l_id");
        let small_schema = make_test_schema("s_id");

        let left_scan = LogicalPlan::Scan {
            table_name: "large".to_string(),
            schema: large_schema.clone(),
            projections: None,
            pruner: None,
        };

        let right_scan = LogicalPlan::Scan {
            table_name: "small".to_string(),
            schema: small_schema.clone(),
            projections: None,
            pruner: None,
        };

        // Query: large JOIN small ON l_id = s_id
        let joined_schema = Arc::new(Schema::new(vec![
            Field {
                name: "l_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "s_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ]));

        let join_plan = LogicalPlan::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: vec![(
                LogicalExpr::Column("l_id".to_string()),
                LogicalExpr::Column("s_id".to_string()),
            )],
            join_type: JoinType::Inner,
            schema: joined_schema,
        };

        let generator = PhysicalPlanGenerator::new(storage.clone(), 1);
        let dispatcher = Arc::new(Dispatcher::new(1));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mailboxes = Arc::new(vec![MailBoxSender { sender: tx }]);

        let pipelines = generator
            .compile_to_pipelines(&join_plan, storage, dispatcher, mailboxes)
            .unwrap();

        // Must produce 2 pipelines: Build and Probe
        assert_eq!(pipelines.len(), 2);

        // Pipeline 1 (Build): Must be the SMALLER table ("small")!
        let build_pipe = &pipelines[0];
        assert_eq!(build_pipe.source.as_ref().unwrap().table_name, "small");
        assert!(build_pipe.dependencies.is_empty()); // Build starts immediately

        // Pipeline 2 (Probe): Must be the LARGER table ("large")!
        let probe_pipe = &pipelines[1];
        assert_eq!(probe_pipe.source.as_ref().unwrap().table_name, "large");
        // Probe must wait for Build!
        assert_eq!(probe_pipe.dependencies, vec![build_pipe.id]);
    }

    #[test]
    fn test_compile_limit_with_sort_rule_1() {
        let storage = Arc::new(MockDataStorage::new());
        let schema = make_test_schema("val");

        let scan = LogicalPlan::Scan {
            table_name: "nums".to_string(),
            schema: schema.clone(),
            projections: None,
            pruner: None,
        };

        let sort = LogicalPlan::Sort {
            sort_exprs: vec![LogicalExpr::Column("val".to_string())],
            input: Box::new(scan),
        };

        // Query: SELECT val FROM nums ORDER BY val LIMIT 10 OFFSET 5
        let limit_sort = LogicalPlan::Limit {
            limit: 10,
            offset: 5,
            input: Box::new(sort),
        };

        let generator = PhysicalPlanGenerator::new(storage.clone(), 1);
        let dispatcher = Arc::new(Dispatcher::new(1));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mailboxes = Arc::new(vec![MailBoxSender { sender: tx }]);

        let pipelines = generator
            .compile_to_pipelines(&limit_sort, storage, dispatcher, mailboxes)
            .unwrap();

        assert_eq!(pipelines.len(), 1);
        let pipe = &pipelines[0];
        // Must contain PhysicalLimitOperator (is_pre_sort = true) in operators!
        assert_eq!(pipe.operators.len(), 1);
        let limit_op = pipe.operators[0]
            .as_any()
            .downcast_ref::<crate::execution::limit::PhysicalLimitOperator>()
            .unwrap();
        assert_eq!(limit_op.is_pre_sort, true); // Rule 1: is_pre_sort = true!
        assert_eq!(limit_op.skip, 5);
        assert_eq!(limit_op.fetch, 10);
    }

    #[test]
    fn test_compile_limit_without_sort_rule_2() {
        let storage = Arc::new(MockDataStorage::new());
        let schema = make_test_schema("val");

        let scan = LogicalPlan::Scan {
            table_name: "nums".to_string(),
            schema: schema.clone(),
            projections: None,
            pruner: None,
        };

        // Query: SELECT val FROM nums LIMIT 10 OFFSET 5 (no sort)
        let limit_plan = LogicalPlan::Limit {
            limit: 10,
            offset: 5,
            input: Box::new(scan),
        };

        let generator = PhysicalPlanGenerator::new(storage.clone(), 1);
        let dispatcher = Arc::new(Dispatcher::new(1));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mailboxes = Arc::new(vec![MailBoxSender { sender: tx }]);

        let pipelines = generator
            .compile_to_pipelines(&limit_plan, storage, dispatcher, mailboxes)
            .unwrap();

        assert_eq!(pipelines.len(), 1);
        let pipe = &pipelines[0];
        // Rule 2: Must contain PhysicalLimitOperator (is_pre_sort = false)!
        assert_eq!(pipe.operators.len(), 1);
        let limit_op = pipe.operators[0]
            .as_any()
            .downcast_ref::<crate::execution::limit::PhysicalLimitOperator>()
            .unwrap();
        assert_eq!(limit_op.is_pre_sort, false); // Rule 2: is_pre_sort = false!
        assert_eq!(limit_op.skip, 5);
        assert_eq!(limit_op.fetch, 10);
    }

    #[test]
    fn test_compile_complex_multi_operation_dag() {
        let storage = Arc::new(MockDataStorage::new());

        // 1. Populate storage with 3 tables of different sizes:
        // Table "users": 1 page (smallest)
        storage.add_page(
            "users",
            StoragePage {
                metadata: PageMetaData {
                    num_rows: 50,
                    column_stats: HashMap::new(),
                    header: Buffer::from(vec![1]),
                },
                data: Buffer::from(vec![1]),
            },
        );

        // Table "orders": 5 pages (medium)
        for _ in 0..5 {
            storage.add_page(
                "orders",
                StoragePage {
                    metadata: PageMetaData {
                        num_rows: 100,
                        column_stats: HashMap::new(),
                        header: Buffer::from(vec![2]),
                    },
                    data: Buffer::from(vec![2]),
                },
            );
        }

        // Table "lineitem": 20 pages (largest)
        for _ in 0..20 {
            storage.add_page(
                "lineitem",
                StoragePage {
                    metadata: PageMetaData {
                        num_rows: 200,
                        column_stats: HashMap::new(),
                        header: Buffer::from(vec![3]),
                    },
                    data: Buffer::from(vec![3]),
                },
            );
        }

        // Schemas for the 3 tables:
        let users_schema = Arc::new(Schema::new(vec![
            Field {
                name: "u_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "u_age".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ]));

        let orders_schema = Arc::new(Schema::new(vec![
            Field {
                name: "o_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "o_user_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "o_amount".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ]));

        let lineitem_schema = Arc::new(Schema::new(vec![
            Field {
                name: "l_order_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "l_price".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ]));

        // --- Multi-Operation Query Construction ---
        // 1. Scan 1 + Filter 1: users WHERE u_age > 18
        let scan_users = LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: users_schema.clone(),
            projections: None,
            pruner: None,
        };
        let filter_users = LogicalPlan::Filter {
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column("u_age".to_string())),
                op: crate::planner::ast::operators::BinaryOperator::Gt,
                right: Box::new(LogicalExpr::Column("u_age".to_string())),
            },
            input: Box::new(scan_users),
        };

        // 2. Scan 2 + Filter 2: orders WHERE o_amount > 50
        let scan_orders = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: orders_schema.clone(),
            projections: None,
            pruner: None,
        };
        let filter_orders = LogicalPlan::Filter {
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column("o_amount".to_string())),
                op: crate::planner::ast::operators::BinaryOperator::Gt,
                right: Box::new(LogicalExpr::Column("o_amount".to_string())),
            },
            input: Box::new(scan_orders),
        };

        // Join 1: (users JOIN orders ON u_id = o_user_id)
        let join1_schema = Arc::new(Schema::new(vec![
            Field {
                name: "u_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "u_age".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "o_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "o_user_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "o_amount".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ]));
        let join1 = LogicalPlan::HashJoin {
            left: Box::new(filter_users),
            right: Box::new(filter_orders),
            on: vec![(
                LogicalExpr::Column("u_id".to_string()),
                LogicalExpr::Column("o_user_id".to_string()),
            )],
            join_type: JoinType::Inner,
            schema: join1_schema.clone(),
        };

        // 3. Scan 3 + Filter 3: lineitem WHERE l_price > 100
        let scan_lineitem = LogicalPlan::Scan {
            table_name: "lineitem".to_string(),
            schema: lineitem_schema.clone(),
            projections: None,
            pruner: None,
        };
        let filter_lineitem = LogicalPlan::Filter {
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column("l_price".to_string())),
                op: crate::planner::ast::operators::BinaryOperator::Gt,
                right: Box::new(LogicalExpr::Column("l_price".to_string())),
            },
            input: Box::new(scan_lineitem),
        };

        // Join 2: (Join 1 JOIN lineitem ON o_id = l_order_id)
        let mut final_schema_fields = join1_schema.fields().to_vec();
        final_schema_fields.extend(lineitem_schema.fields().to_vec());
        let join2_schema = Arc::new(Schema::new(final_schema_fields));

        let join2 = LogicalPlan::HashJoin {
            left: Box::new(join1),
            right: Box::new(filter_lineitem),
            on: vec![(
                LogicalExpr::Column("o_id".to_string()),
                LogicalExpr::Column("l_order_id".to_string()),
            )],
            join_type: JoinType::Inner,
            schema: join2_schema.clone(),
        };

        // 4. Sort: ORDER BY l_price DESC
        let sort = LogicalPlan::Sort {
            sort_exprs: vec![LogicalExpr::Column("l_price".to_string())],
            input: Box::new(join2),
        };

        // 5. Limit: LIMIT 10 OFFSET 5 (Fused Rule 1)
        let full_query_plan = LogicalPlan::Limit {
            limit: 10,
            offset: 5,
            input: Box::new(sort),
        };

        let generator = PhysicalPlanGenerator::new(storage.clone(), 1);
        let dispatcher = Arc::new(Dispatcher::new(1));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mailboxes = Arc::new(vec![MailBoxSender { sender: tx }]);

        let pipelines = generator
            .compile_to_pipelines(&full_query_plan, storage, dispatcher, mailboxes)
            .unwrap();

        // --- VERIFY DAG DEPENDENCY STRUCTURE & OPERATORS ---
        // Must compile into exactly 3 pipelines in anticipated DAG order:
        // Pipeline 1: Build for Join 1 (users table: smallest = 1 page)
        // Pipeline 2: Probe for Join 1 + Build for Join 2 (orders table: medium = 5 pages)
        // Pipeline 3: Probe for Join 2 + Sort + Limit (lineitem table: largest = 20 pages)
        assert_eq!(pipelines.len(), 3);

        // PIPELINE 1:
        // - Source: "users"
        // - Operators: 1 (PhysicalFilter for u_age > 18)
        // - Sink: PhysicalBuildSink (join_id for Join 1)
        // - Dependencies: None (starts immediately!)
        let p1 = &pipelines[0];
        assert_eq!(p1.source.as_ref().unwrap().table_name, "users");
        assert_eq!(p1.operators.len(), 1); // PhysicalFilter
        assert!(p1.dependencies.is_empty());
        assert_eq!(p1.downstream_id, None);

        // PIPELINE 2:
        // - Source: "orders"
        // - Operators: 2 (PhysicalFilter for o_amount > 50, PhysicalProbeOperator for Join 1)
        // - Sink: PhysicalBuildSink (join_id for Join 2)
        // - Dependencies: [p1.id] (MUST wait for Pipeline 1!)
        let p2 = &pipelines[1];
        assert_eq!(p2.source.as_ref().unwrap().table_name, "orders");
        assert_eq!(p2.operators.len(), 2); // PhysicalFilter + PhysicalProbeOperator
        assert_eq!(p2.dependencies, vec![p1.id]);
        assert_eq!(p2.downstream_id, None);

        // PIPELINE 3:
        // - Source: "lineitem"
        // - Operators: 3 (PhysicalFilter for l_price > 100, PhysicalProbeOperator for Join 2, PhysicalLimitOperator is_pre_sort=true)
        // - Sink: PhysicalSortSink (Top-N Sort Sink)
        // - Dependencies: [p2.id] (MUST wait for Pipeline 2!)
        // - Downstream: None (Root Pipeline!)
        let p3 = &pipelines[2];
        assert_eq!(p3.source.as_ref().unwrap().table_name, "lineitem");
        assert_eq!(p3.operators.len(), 3); // PhysicalFilter + PhysicalProbeOperator + PhysicalLimitOperator
        assert_eq!(p3.dependencies, vec![p2.id]);
        assert_eq!(p3.downstream_id, None); // Root pipeline!
    }
}
