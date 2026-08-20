use std::{fmt::Display, sync::Arc};

use crate::{
    arrow::{DataType, Field, Schema},
    catalog::provider::CatalogProvider,
    planner::{
        JoinType, LogicalColumnDef, LogicalExpr, LogicalPlan,
        ast::{
            Statement,
            ddl::CreateTable,
            dml::{Delete, Insert, Update},
            expr::{Expr, Parens, SetExpr},
            operators::BinaryOperator,
            query::{Join, JoinConstraint, JoinOperator, Query, Select, SelectItem, TableFactor},
        },
    },
};

pub struct Planner {
    catalog: Arc<CatalogProvider>,
}

#[derive(Debug)]
pub enum PlannerError {
    PlannerError(String),
    CatalogError(String),
}

impl Display for PlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlannerError::PlannerError(e) => write!(f, "Planner Error: {}", e),
            PlannerError::CatalogError(e) => write!(f, "Catalog Error: {}", e),
        }
    }
}

impl std::error::Error for PlannerError {}

fn has_aggregation(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::Aggregation { .. } => true,
        LogicalExpr::BinaryOp { left, right, .. } => {
            has_aggregation(left) || has_aggregation(right)
        }
        LogicalExpr::UnaryOp { expr, .. } => has_aggregation(expr),
        LogicalExpr::Alias { expr, .. } => has_aggregation(expr),
        LogicalExpr::IsNull(expr) => has_aggregation(expr),
        LogicalExpr::IsNotNull(expr) => has_aggregation(expr),
        LogicalExpr::IsTrue(expr) => has_aggregation(expr),
        LogicalExpr::IsNotTrue(expr) => has_aggregation(expr),
        LogicalExpr::IsFalse(expr) => has_aggregation(expr),
        LogicalExpr::IsNotFalse(expr) => has_aggregation(expr),
        LogicalExpr::Cast { expr, .. } => has_aggregation(expr),
        LogicalExpr::Ceil(expr) => has_aggregation(expr),
        LogicalExpr::Floor(expr) => has_aggregation(expr),
        _ => false,
    }
}

impl Planner {
    pub fn new(catalog: Arc<CatalogProvider>) -> Self {
        Planner { catalog }
    }

    pub async fn plan_statment(&self, statement: Statement) -> Result<LogicalPlan, PlannerError> {
        match statement {
            Statement::Query(query) => self.plan_query(*query).await,
            Statement::Insert(insert) => self.plan_insert(insert).await,
            Statement::Update(update) => self.plan_update(update).await,
            Statement::Delete(delete) => self.plan_delete(delete).await,
            Statement::CreateTable(create) => self.plan_create_table(create).await,
            Statement::Drop { .. } => Err(PlannerError::PlannerError(
                "DROP statement is not supported yet".to_string(),
            )),
            Statement::ShowTables => Ok(LogicalPlan::ShowTables),
        }
    }

    /// Compiles parsed SQL expressions recursively, validating types
    fn plan_expr(&self, expr: &Expr, schema: &Schema) -> Result<LogicalExpr, PlannerError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = &ident.value;
                if schema.fields().iter().any(|f| f.name == *name) {
                    Ok(LogicalExpr::Column(name.to_string()))
                } else {
                    Err(PlannerError::PlannerError(format!(
                        "Column '{}' not found in table schema",
                        name
                    )))
                }
            }
            Expr::CompoundIdentifier(parts) => {
                let paths: Vec<String> = parts.iter().map(|i| i.value.clone()).collect();
                // If it is a local column reference (e.g. table_name.col_name)
                // we can validate the column suffix
                if let Some(col) = paths.last() {
                    if schema.fields().iter().any(|f| f.name == *col) {
                        Ok(LogicalExpr::CompoundColumn(paths))
                    } else {
                        Err(PlannerError::PlannerError(format!(
                            "Column '{}' not found in table schema",
                            col
                        )))
                    }
                } else {
                    Ok(LogicalExpr::CompoundColumn(paths))
                }
            }
            Expr::Value(val) => Ok(LogicalExpr::Value(val.clone())),
            Expr::BinaryOp { left, op, right } => {
                let lhs = self.plan_expr(&left, schema)?;
                let rhs = self.plan_expr(&right, schema)?;
                Ok(LogicalExpr::BinaryOp {
                    left: Box::new(lhs),
                    op: op.clone(),
                    right: Box::new(rhs),
                })
            }
            // Strips grouping parentheses on compile-time (optimizing query evaluation!)
            Expr::Nested(inner) => self.plan_expr(*&inner, schema),
            Expr::IsNull(inner) => {
                let compiled_inner = self.plan_expr(*&inner, schema)?;
                Ok(LogicalExpr::IsNull(Box::new(compiled_inner)))
            }
            Expr::IsNotNull(inner) => {
                let compiled_inner = self.plan_expr(*&inner, schema)?;
                Ok(LogicalExpr::IsNotNull(Box::new(compiled_inner)))
            }
            Expr::IsTrue(inner) => {
                let compiled_inner = self.plan_expr(*&inner, schema)?;
                Ok(LogicalExpr::IsTrue(Box::new(compiled_inner)))
            }
            Expr::IsNotTrue(inner) => {
                let compiled_inner = self.plan_expr(*&inner, schema)?;
                Ok(LogicalExpr::IsNotTrue(Box::new(compiled_inner)))
            }
            Expr::IsFalse(inner) => {
                let compiled_inner = self.plan_expr(*&inner, schema)?;
                Ok(LogicalExpr::IsFalse(Box::new(compiled_inner)))
            }
            Expr::IsNotFalse(inner) => {
                let compiled_inner = self.plan_expr(*&inner, schema)?;
                Ok(LogicalExpr::IsNotFalse(Box::new(compiled_inner)))
            }
            Expr::Cast {
                kind,
                expr,
                data_type,
            } => {
                let compiled_inner = self.plan_expr(*&expr, schema)?;
                let mapped_type = match data_type {
                    crate::sql_parser::ast::data_type::DataType::Int(_) => {
                        crate::arrow::DataType::Int32
                    }
                    crate::sql_parser::ast::data_type::DataType::Boolean => {
                        crate::arrow::DataType::Boolean
                    }
                    crate::sql_parser::ast::data_type::DataType::Double(_) => {
                        crate::arrow::DataType::Float64
                    }
                    crate::sql_parser::ast::data_type::DataType::Varchar(_) => {
                        crate::arrow::DataType::Utf8
                    }
                    e => {
                        return Err(PlannerError::CatalogError(format!(
                            "unsupported data type: {:?}",
                            e
                        )));
                    }
                };
                Ok(LogicalExpr::Cast {
                    kind: kind.clone(),
                    expr: Box::new(compiled_inner),
                    data_type: mapped_type,
                })
            }
            Expr::Ceil { expr } => {
                let compiled_inner = self.plan_expr(*&expr, schema)?;
                Ok(LogicalExpr::Ceil(Box::new(compiled_inner)))
            }
            Expr::Floor { expr } => {
                let compiled_inner = self.plan_expr(*&expr, schema)?;
                Ok(LogicalExpr::Floor(Box::new(compiled_inner)))
            }
            e => Err(PlannerError::PlannerError(format!(
                "Expression '{e:?}' not supported yet"
            ))),
        }
    }

    /// Compiles a SQL Query AST into a logical operator tree supporting Sort and Limit!
    async fn plan_query(&self, query: Query) -> Result<LogicalPlan, PlannerError> {
        let mut plan = match *query.body {
            SetExpr::Select(select) => self.plan_select(*select).await?,
            SetExpr::Values(parens) => self.plan_values(parens, None).await?,
        };

        if let Some(order_by) = query.order_by {
            if !order_by.is_empty() {
                let schema = match &plan {
                    LogicalPlan::Projection { schema, .. } => schema.clone(),
                    LogicalPlan::Scan { schema, .. } => schema.clone(),
                    LogicalPlan::HashJoin { schema, .. } => schema.clone(),
                    LogicalPlan::Aggregate { schema, .. } => schema.clone(),
                    _ => {
                        return Err(PlannerError::PlannerError(
                            "Cannot resolve ORDER BY schema".to_string(),
                        ));
                    }
                };

                let mut sort_exprs = Vec::with_capacity(order_by.len());
                for item in order_by {
                    // Compile the ORDER BY expression against the output schema
                    let compiled_expr = self.plan_expr(&item.expr, &schema)?;
                    sort_exprs.push(compiled_expr);
                }

                plan = LogicalPlan::Sort {
                    sort_exprs,
                    input: Box::new(plan),
                };
            }
        }

        if query.limit.is_some() || query.offset.is_some() {
            plan = LogicalPlan::Limit {
                limit: query.limit.unwrap_or(usize::MAX),
                offset: query.offset.unwrap_or(0),
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

    /// Plans [`LogicalPlan::Values`], optionally validating against an expected target schema
    async fn plan_values(
        &self,
        parens: Parens<Vec<Expr>>,
        expected_schema: Option<Arc<Schema>>,
    ) -> Result<LogicalPlan, PlannerError> {
        let empty_schema = Schema::new(Vec::<Field>::new());
        let mut row_exprs = Vec::with_capacity(parens.content.len());
        for expr in &parens.content {
            row_exprs.push(self.plan_expr(expr, &empty_schema)?);
        }

        let schema = if let Some(target_schema) = expected_schema {
            if row_exprs.len() != target_schema.fields().len() {
                return Err(PlannerError::PlannerError(format!(
                    "Column count mismatch: target table expects {} columns, but VALUES clause has {}",
                    target_schema.fields().len(),
                    row_exprs.len()
                )));
            }

            for (i, expr) in row_exprs.iter().enumerate() {
                let target_field = &target_schema.fields()[i];
                let expected_type = &target_field.data_type;

                let expr_type = match expr {
                    LogicalExpr::Value(val) => match val {
                        crate::sql_parser::ast::expr::Value::Number(s, _) => {
                            if s.contains('.') {
                                DataType::Float64
                            } else {
                                DataType::Int32
                            }
                        }
                        crate::sql_parser::ast::expr::Value::SingleQuotedString(_) => {
                            DataType::Utf8
                        }
                        crate::sql_parser::ast::expr::Value::DoubleQuotedString(_) => {
                            DataType::Utf8
                        }
                        crate::sql_parser::ast::expr::Value::Boolean(_) => DataType::Boolean,
                        crate::sql_parser::ast::expr::Value::Null => {
                            // NULL is compatible with any target data type!
                            expected_type.clone()
                        }
                    },
                    _ => expected_type.clone(), // Fallback for complex expressions
                };

                if &expr_type != expected_type {
                    return Err(PlannerError::PlannerError(format!(
                        "Type mismatch for column '{}' (index {}): expected {:?}, but found {:?}",
                        target_field.name, i, expected_type, expr_type
                    )));
                }
            }

            target_schema
        } else {
            // Dynamically derive the field schemas based on the literal value types (raw VALUES query)
            let mut fields = Vec::with_capacity(row_exprs.len());
            for (i, expr) in row_exprs.iter().enumerate() {
                let data_type = match expr {
                    LogicalExpr::Value(val) => match val {
                        crate::sql_parser::ast::expr::Value::Number(s, _) => {
                            if s.contains('.') {
                                DataType::Float64
                            } else {
                                DataType::Int32
                            }
                        }
                        crate::sql_parser::ast::expr::Value::SingleQuotedString(_) => {
                            DataType::Utf8
                        }
                        crate::sql_parser::ast::expr::Value::DoubleQuotedString(_) => {
                            DataType::Utf8
                        }
                        crate::sql_parser::ast::expr::Value::Boolean(_) => DataType::Boolean,
                        crate::sql_parser::ast::expr::Value::Null => DataType::Int32,
                    },
                    _ => DataType::Int32,
                };
                fields.push(Field {
                    name: format!("column_{}", i),
                    data_type,
                    nullable: true,
                });
            }
            Arc::new(Schema::new(fields))
        };

        Ok(LogicalPlan::Values {
            schema,
            values: vec![row_exprs],
        })
    }

    /// Compiles a SELECT statement into a relational operator stack supporting Join & Aggregate
    /// Plan order:
    ///   Scan -> Hash Join -> Filter -> Aggregation -> Projection
    async fn plan_select(&self, select: Select) -> Result<LogicalPlan, PlannerError> {
        let (mut plan, mut schema) = self.plan_scan(&select.from.relation).await?;

        if let Some(ref joins) = select.from.joins {
            let (joined_plan, joined_schema) = self.plan_joins(plan, schema, joins).await?;
            plan = joined_plan;
            schema = joined_schema;
        }

        plan = self.plan_filter(plan, select.selection.as_ref(), &schema)?;

        let projection_exprs = self.plan_projection_exprs(&select.projections, &schema)?;

        plan = self.plan_aggregate(
            plan,
            &projection_exprs,
            select.group_by.as_deref(),
            schema.clone(),
        )?;

        plan = LogicalPlan::Projection {
            exprs: projection_exprs,
            input: Box::new(plan),
            schema: schema.clone(),
        };

        Ok(plan)
    }

    /// Plans the [`LogicalPlan::Scan`] that read data from a physical table
    /// returns the plan and the target table's schema
    async fn plan_scan(
        &self,
        table_factor: &TableFactor,
    ) -> Result<(LogicalPlan, Arc<Schema>), PlannerError> {
        let table_name = match table_factor {
            TableFactor::Table { name, .. } => name.value.clone(),
        };

        let table_meta = self
            .catalog
            .get_table_metadata(&table_name)
            .await
            .map_err(|e| PlannerError::CatalogError(e.to_string()))?;

        let fields: Vec<Field> = table_meta
            .columns
            .into_iter()
            .map(|(name, dt)| Field {
                name,
                data_type: dt,
                nullable: true,
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let plan = LogicalPlan::Scan {
            table_name,
            schema: schema.clone(),
            projections: None,
        };

        Ok((plan, schema))
    }

    /// plans the [`LogicalPlan::HashJoin`] to join 2 datasets
    async fn plan_joins(
        &self,
        mut left_plan: LogicalPlan,
        mut left_schema: Arc<Schema>,
        joins: &[Join],
    ) -> Result<(LogicalPlan, Arc<Schema>), PlannerError> {
        for join in joins {
            let (right_plan, right_schema) = self.plan_scan(&join.relation).await?;

            // Combine schemas of left and right inputs by collecting raw Field elements
            let mut combined_fields: Vec<Field> = Vec::new();
            for f in left_schema.fields().iter() {
                combined_fields.push((**f).clone());
            }
            for f in right_schema.fields().iter() {
                combined_fields.push((**f).clone());
            }
            let combined_schema = Arc::new(Schema::new(combined_fields));

            // Resolve Join Type and Join condition
            let (join_type, constraint) = match &join.join_operator {
                JoinOperator::Join(constraint) => (JoinType::Inner, constraint),
                JoinOperator::Inner(constraint) => (JoinType::Inner, constraint),
                JoinOperator::Left(constraint) => (JoinType::Left, constraint),
                JoinOperator::Right(constraint) => (JoinType::Right, constraint),
                JoinOperator::FullOuter(constraint) => (JoinType::Full, constraint),
            };

            let on_expr = match constraint {
                JoinConstraint::On(expr) => expr,
            };

            let on_compiled = self.plan_expr(on_expr, &combined_schema)?;

            let on_pairs = match on_compiled {
                LogicalExpr::BinaryOp { left, op, right } if op == BinaryOperator::Eq => {
                    vec![(*left, *right)]
                }
                _ => {
                    return Err(PlannerError::PlannerError(
                        "Only equality join conditions ON left_col = right_col are supported"
                            .to_string(),
                    ));
                }
            };

            left_plan = LogicalPlan::HashJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                on: on_pairs,
                join_type,
                schema: combined_schema.clone(),
            };

            left_schema = combined_schema;
        }

        Ok((left_plan, left_schema))
    }

    /// Plan [`LogicalPlan::Filter`]
    fn plan_filter(
        &self,
        input_plan: LogicalPlan,
        selection: Option<&Expr>,
        schema: &Schema,
    ) -> Result<LogicalPlan, PlannerError> {
        if let Some(expr) = selection {
            let predicate = self.plan_expr(expr, schema)?;
            Ok(LogicalPlan::Filter {
                predicate,
                input: Box::new(input_plan),
            })
        } else {
            Ok(input_plan)
        }
    }

    /// Plans [`LogicalPlan::Projection`]
    fn plan_projection_exprs(
        &self,
        projections: &[SelectItem],
        schema: &Schema,
    ) -> Result<Vec<LogicalExpr>, PlannerError> {
        let mut exprs = Vec::new();
        for proj in projections {
            match proj {
                SelectItem::Expr(expr) => {
                    exprs.push(self.plan_expr(expr, schema)?);
                }
                SelectItem::NamedExpr { expr, alias } => {
                    let compiled_expr = self.plan_expr(expr, schema)?;
                    exprs.push(LogicalExpr::Alias {
                        expr: Box::new(compiled_expr),
                        alias: alias.value.clone(),
                    });
                }
                SelectItem::Wildcard => {
                    for field in schema.fields().iter() {
                        exprs.push(LogicalExpr::Column(field.name.clone()));
                    }
                }
            }
        }
        Ok(exprs)
    }

    /// Plans [`LogicalPlan::Aggregate`]
    fn plan_aggregate(
        &self,
        input_plan: LogicalPlan,
        projection_exprs: &[LogicalExpr],
        group_by: Option<&[Expr]>,
        schema: Arc<Schema>,
    ) -> Result<LogicalPlan, PlannerError> {
        let mut group_by_exprs = Vec::new();
        if let Some(exprs) = group_by {
            for expr in exprs {
                group_by_exprs.push(self.plan_expr(expr, &schema)?);
            }
        }

        let is_agg_query = group_by.is_some() || projection_exprs.iter().any(has_aggregation);

        if is_agg_query {
            Ok(LogicalPlan::Aggregate {
                group_by: group_by_exprs,
                input: Box::new(input_plan),
                schema,
            })
        } else {
            Ok(input_plan)
        }
    }

    /// Compiles a CreateTable AST into a [`LogicalPlan::CreateTable`]
    async fn plan_create_table(&self, create: CreateTable) -> Result<LogicalPlan, PlannerError> {
        let table_name = match create.table_name {
            TableFactor::Table { name, .. } => name.value.clone(),
        };

        let mut columns = Vec::new();
        for col in create.columns {
            let data_type = match col.data_type {
                crate::sql_parser::ast::data_type::DataType::Int(_) => DataType::Int32,
                crate::sql_parser::ast::data_type::DataType::Boolean => DataType::Boolean,
                crate::sql_parser::ast::data_type::DataType::Double(_) => DataType::Float64,
                crate::sql_parser::ast::data_type::DataType::Varchar(_) => DataType::Utf8,
                _ => DataType::Int32, // Wildcard fallback
            };
            columns.push(LogicalColumnDef {
                name: col.name.value,
                data_type,
            });
        }

        Ok(LogicalPlan::CreateTable {
            table_name,
            columns,
        })
    }

    /// Compiles an Insert AST into an [`LogicalPlan::Insert`]
    async fn plan_insert(&self, insert: Insert) -> Result<LogicalPlan, PlannerError> {
        let table_name = match insert.table {
            TableFactor::Table { name, .. } => name.value.clone(),
        };

        let table_meta = self
            .catalog
            .get_table_metadata(&table_name)
            .await
            .map_err(|e| PlannerError::CatalogError(e.to_string()))?;

        let fields: Vec<Field> = table_meta
            .columns
            .into_iter()
            .map(|(name, dt)| Field {
                name,
                data_type: dt,
                nullable: true,
            })
            .collect();

        let target_schema = Arc::new(Schema::new(fields));
        let insert_columns = insert.columns.iter().map(|id| id.value.clone()).collect();
        let source_query = match insert.source {
            Some(q) => *q,
            None => {
                return Err(PlannerError::PlannerError(
                    "INSERT must specify a source query".to_string(),
                ));
            }
        };

        let values_query_plan = match *source_query.body {
            SetExpr::Values(parens) => self.plan_values(parens, Some(target_schema)).await?,
            _ => self.plan_query(source_query).await?,
        };

        Ok(LogicalPlan::Insert {
            table_name,
            columns: insert_columns,
            input: Box::new(values_query_plan),
        })
    }

    /// Compiles an Update AST into an UPDATE LogicalPlan
    async fn plan_update(&self, update: Update) -> Result<LogicalPlan, PlannerError> {
        let table_name = match update.table {
            TableFactor::Table { name, .. } => name.value.clone(),
        };

        let table_meta = self
            .catalog
            .get_table_metadata(&table_name)
            .await
            .map_err(|e| PlannerError::CatalogError(e.to_string()))?;

        let fields_iter = table_meta.columns.into_iter();
        let fields: Vec<Field> = fields_iter
            .map(|(name, dt)| Field {
                name,
                data_type: dt,
                nullable: true,
            })
            .collect();
        let schema = Schema::new(fields);

        let mut assignments = Vec::new();
        for assign in update.assignments {
            let target_col = assign.target.value.clone();
            let val_expr = self.plan_expr(&assign.value, &schema)?; // Passed as reference!
            assignments.push((target_col, val_expr));
        }

        let selection = match update.selection {
            Some(expr) => Some(self.plan_expr(&expr, &schema)?),
            None => None,
        };

        Ok(LogicalPlan::Update {
            table_name,
            assignments,
            selection,
        })
    }

    /// Compiles an Delete AST into an DELETE LogicalPlan
    async fn plan_delete(&self, delete: Delete) -> Result<LogicalPlan, PlannerError> {
        let table_factor = match delete.from.first() {
            Some(tf) => tf,
            None => {
                return Err(PlannerError::PlannerError(
                    "DELETE queries must specify a table".to_string(),
                ));
            }
        };

        let table_name = match table_factor {
            TableFactor::Table { name, .. } => name.value.clone(),
        };

        let table_meta = self
            .catalog
            .get_table_metadata(&table_name)
            .await
            .map_err(|e| PlannerError::CatalogError(e.to_string()))?;

        let fields_iter = table_meta.columns.into_iter();
        let fields: Vec<Field> = fields_iter
            .map(|(name, dt)| Field {
                name,
                data_type: dt,
                nullable: true,
            })
            .collect();
        let schema = Schema::new(fields);

        let selection = match delete.selection {
            Some(expr) => Some(self.plan_expr(&expr, &schema)?),
            None => None,
        };

        Ok(LogicalPlan::Delete {
            table_name,
            selection,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::DataType;
    use crate::catalog::TableMetadata;
    use crate::storage::CatalogStorage;
    use anyhow::Result;
    use std::collections::HashMap;
    use std::sync::Mutex;

    pub struct MockCatalog {
        tables: Mutex<HashMap<String, TableMetadata>>,
    }

    impl MockCatalog {
        pub fn new() -> Self {
            Self {
                tables: Mutex::new(HashMap::new()),
            }
        }

        pub fn add_table(&self, table_name: &str, columns: Vec<(&str, DataType)>) {
            let cols = columns
                .into_iter()
                .map(|(n, dt)| (n.to_string(), dt))
                .collect();
            let meta = TableMetadata {
                name: table_name.to_string(),
                columns: cols,
            };
            self.tables
                .lock()
                .unwrap()
                .insert(table_name.to_string(), meta);
        }
    }

    #[async_trait::async_trait]
    impl CatalogStorage for MockCatalog {
        async fn fetch_table_meta(&self, table_name: &str) -> Result<TableMetadata> {
            self.tables
                .lock()
                .unwrap()
                .get(table_name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", table_name))
        }

        async fn register_table_meta(
            &self,
            _table_name: &str,
            _metadata: TableMetadata,
        ) -> Result<()> {
            Ok(())
        }

        async fn drop_table_meta(&self, _table_name: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_plan_select_filter_projection_limit() {
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Utf8),
                ("active", DataType::Boolean),
            ],
        );

        let provider = Arc::new(CatalogProvider::new(catalog));
        let planner = Planner::new(provider);

        let sql = "SELECT id, name FROM users WHERE active = true;";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan = planner.plan_statment(statement).await.unwrap();

        let proj_node = match plan {
            LogicalPlan::Projection { exprs, input, .. } => {
                assert_eq!(exprs.len(), 2);
                assert!(matches!(&exprs[0], LogicalExpr::Column(name) if name == "id"));
                assert!(matches!(&exprs[1], LogicalExpr::Column(name) if name == "name"));
                input
            }
            _ => panic!("Expected Projection top-level node"),
        };

        let filter_node = match *proj_node {
            LogicalPlan::Filter { predicate, input } => {
                assert!(matches!(predicate, LogicalExpr::BinaryOp { .. }));
                input
            }
            _ => panic!("Expected Filter node under Projection"),
        };

        match *filter_node {
            LogicalPlan::Scan { table_name, .. } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected Scan leaf node under Filter"),
        }
    }

    #[tokio::test]
    async fn test_plan_inner_join_query() {
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        );
        catalog.add_table(
            "orders",
            vec![
                ("id", DataType::Int32),
                ("user_id", DataType::Int32),
                ("amount", DataType::Float64),
            ],
        );

        let provider = Arc::new(CatalogProvider::new(catalog));
        let planner = Planner::new(provider);

        let sql =
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id;";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan = planner.plan_statment(statement).await.unwrap();

        let proj_node = match plan {
            LogicalPlan::Projection { exprs, input, .. } => {
                assert_eq!(exprs.len(), 2);
                input
            }
            _ => panic!("Expected Projection top-level node"),
        };

        match *proj_node {
            LogicalPlan::HashJoin {
                left,
                right,
                on,
                join_type,
                ..
            } => {
                assert_eq!(join_type, JoinType::Inner);
                assert_eq!(on.len(), 1);

                let (ref l_key, ref r_key) = on[0];
                assert!(
                    matches!(l_key, LogicalExpr::CompoundColumn(parts) if parts[0] == "users" && parts[1] == "id")
                );
                assert!(
                    matches!(r_key, LogicalExpr::CompoundColumn(parts) if parts[0] == "orders" && parts[1] == "user_id")
                );

                assert!(
                    matches!(*left, LogicalPlan::Scan { table_name, .. } if table_name == "users")
                );
                assert!(
                    matches!(*right, LogicalPlan::Scan { table_name, .. } if table_name == "orders")
                );
            }
            _ => panic!("Expected HashJoin node under Projection"),
        }
    }

    #[tokio::test]
    async fn test_plan_aggregate_groupby_query() {
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![("age", DataType::Int32), ("salary", DataType::Float64)],
        );

        let provider = Arc::new(CatalogProvider::new(catalog));
        let planner = Planner::new(provider);

        let sql = "SELECT age FROM users GROUP BY age;";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan = planner.plan_statment(statement).await.unwrap();

        let proj_node = match plan {
            LogicalPlan::Projection { input, .. } => input,
            _ => panic!("Expected Projection top-level node"),
        };

        match *proj_node {
            LogicalPlan::Aggregate {
                group_by, input, ..
            } => {
                assert_eq!(group_by.len(), 1);
                assert!(matches!(&group_by[0], LogicalExpr::Column(name) if name == "age"));
                assert!(
                    matches!(*input, LogicalPlan::Scan { table_name, .. } if table_name == "users")
                );
            }
            _ => panic!("Expected Aggregate node under Projection"),
        }
    }

    #[tokio::test]
    async fn test_plan_select_wildcard_with_filter() {
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Utf8),
                ("active", DataType::Boolean),
            ],
        );

        let provider = Arc::new(CatalogProvider::new(catalog));
        let planner = Planner::new(provider);

        // Uses "SELECT *" combined with a WHERE filter!
        let sql = "SELECT * FROM users WHERE active = true;";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan = planner.plan_statment(statement).await.unwrap();

        // 1. Assert top Projection has expanded ALL 3 columns!
        let proj_node = match plan {
            LogicalPlan::Projection { exprs, input, .. } => {
                assert_eq!(exprs.len(), 3); // id, name, active
                assert!(matches!(&exprs[0], LogicalExpr::Column(name) if name == "id"));
                assert!(matches!(&exprs[1], LogicalExpr::Column(name) if name == "name"));
                assert!(matches!(&exprs[2], LogicalExpr::Column(name) if name == "active"));
                input
            }
            _ => panic!("Expected Projection top-level node"),
        };

        // 2. Assert Filter predicate (active = true) compiled cleanly under Projection
        let filter_node = match *proj_node {
            LogicalPlan::Filter { predicate, input } => {
                assert!(matches!(predicate, LogicalExpr::BinaryOp { .. }));
                input
            }
            _ => panic!("Expected Filter node under Projection"),
        };

        // 3. Assert Scan leaf node under Filter
        match *filter_node {
            LogicalPlan::Scan { table_name, .. } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected Scan leaf node"),
        }
    }

    #[tokio::test]
    async fn test_plan_orderby_query() {
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        );

        let provider = Arc::new(CatalogProvider::new(catalog));
        let planner = Planner::new(provider);

        // Parse query with ORDER BY!
        let sql = "SELECT id, name FROM users ORDER BY id DESC;";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan = planner.plan_statment(statement).await.unwrap();

        // Assert top node is Sort!
        let sort_node = match plan {
            LogicalPlan::Sort { sort_exprs, input } => {
                assert_eq!(sort_exprs.len(), 1);
                assert!(matches!(&sort_exprs[0], LogicalExpr::Column(name) if name == "id"));
                input
            }
            _ => panic!("Expected Sort top-level node"),
        };

        // Assert under Sort is Projection!
        match *sort_node {
            LogicalPlan::Projection { .. } => {}
            _ => panic!("Expected Projection node under Sort"),
        }
    }

    #[tokio::test]
    async fn test_plan_limit_offset_query() {
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        );

        let provider = Arc::new(CatalogProvider::new(catalog));
        let planner = Planner::new(provider);

        // Parse query with LIMIT and OFFSET!
        let sql = "SELECT id, name FROM users LIMIT 10 OFFSET 5;";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan = planner.plan_statment(statement).await.unwrap();

        // Assert top node is Limit!
        match plan {
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } => {
                assert_eq!(limit, 10);
                assert_eq!(offset, 5);

                // Assert under Limit is Projection!
                assert!(matches!(*input, LogicalPlan::Projection { .. }));
            }
            _ => panic!("Expected Limit top-level node"),
        }
    }

    #[tokio::test]
    async fn test_plan_insert_values_query() {
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        );

        let provider = Arc::new(CatalogProvider::new(catalog));
        let planner = Planner::new(provider);

        // Parse INSERT statement!
        let sql = "INSERT INTO users (id, name) VALUES (42, 'Charlie');";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan = planner.plan_statment(statement).await.unwrap();

        // Assert top node is Insert!
        let values_node = match plan {
            LogicalPlan::Insert {
                table_name,
                columns,
                input,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(columns, vec!["id", "name"]);
                input
            }
            _ => panic!("Expected Insert top-level node"),
        };

        // Assert under Insert is Values!
        match *values_node {
            LogicalPlan::Values { schema, values } => {
                assert_eq!(values.len(), 1); // 1 row
                assert_eq!(values[0].len(), 2); // 2 fields

                // Verify derived schema is correct!
                let fields = schema.fields();
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].data_type, DataType::Int32);
                assert_eq!(fields[1].data_type, DataType::Utf8);
            }
            _ => panic!("Expected Values node under Insert"),
        }
    }

    #[tokio::test]
    async fn test_plan_insert_validation_mismatch() {
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        );

        let provider = Arc::new(CatalogProvider::new(catalog));
        let planner = Planner::new(provider);

        // 1. Test Type Mismatch: try to insert string ('Charlie') into id column (Int32)
        let sql_mismatch_type = "INSERT INTO users (id, name) VALUES ('Charlie', 42);";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql_mismatch_type).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan_err = planner.plan_statment(statement).await.unwrap_err();
        assert!(
            plan_err
                .to_string()
                .contains("Type mismatch for column 'id'")
        );

        // 2. Test Column Count Mismatch: try to insert 3 values into a 2-column table
        let sql_mismatch_count = "INSERT INTO users (id, name) VALUES (1, 'Alice', true);";
        let mut parser = crate::sql_parser::Parser::default();
        let statements = parser.parse_sql(sql_mismatch_count).unwrap();
        let statement = statements.into_iter().next().unwrap();

        let plan_err = planner.plan_statment(statement).await.unwrap_err();
        assert!(plan_err.to_string().contains("Column count mismatch"));
    }
}
