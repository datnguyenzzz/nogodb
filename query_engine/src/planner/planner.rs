use std::{fmt::Display, sync::Arc};

use crate::{
    catalog::provider::CatalogProvider,
    planner::{LogicalPlan, ast::Statement},
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

impl Planner {
    pub fn new(catalog: Arc<CatalogProvider>) -> Self {
        Planner { catalog }
    }

    pub async fn plan_statment(&self, statement: Statement) -> Result<LogicalPlan, PlannerError> {
        // 1. Call to Catalog provider to get schema
        // let _ = self.catalog.get_table_metadata(table_name)
        // 2. Resolve to LogicalPlan

        todo!("implement me!")
    }
}
