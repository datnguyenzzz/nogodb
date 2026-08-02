use std::sync::Arc;

use crate::{
    catalog::provider::CatalogProvider,
    planner::{ast::Statement, logical_plan::LogicalPlan},
};

pub struct Planner {
    catalog: Arc<CatalogProvider>,
}

pub enum PlannerError {
    PlannerError(String),
    CatalogError(String),
}

impl Planner {
    pub fn new(catalog: Arc<CatalogProvider>) -> Self {
        Planner { catalog }
    }

    pub async fn plan_statment(
        &self,
        statements: Vec<Statement>,
    ) -> Result<LogicalPlan, PlannerError> {
        // 1. Call to Catalog provider to get schema
        // let _ = self.catalog.get_table_metadata(table_name)
        // 2. Resolve to LogicalPlan

        todo!("implement me!")
    }
}
