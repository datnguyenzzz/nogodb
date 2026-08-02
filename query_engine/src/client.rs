use std::sync::Arc;

use anyhow::Result;

use crate::{
    catalog::provider::CatalogProvider, optimiser::Optimiser, planner::planner::Planner,
    sql_parser::Parser, storage::catalog::GrpcCatalogClient,
};

pub struct Client {
    parser: Parser,
    planner: Planner,
    optimiser: Optimiser,
}

impl Client {
    pub fn new(server_address: String) -> Self {
        let catalog_client = Arc::new(GrpcCatalogClient::new(server_address));
        let catalog_provider = CatalogProvider::new(catalog_client);
        Self {
            parser: Parser::default(),
            planner: Planner::new(catalog_provider),
            optimiser: Optimiser::default(),
        }
    }

    pub async fn execute(&mut self, statement: &str) -> Result<()> {
        let statements = self.parser.parse_sql(statement)?;

        for statement in statements {
            let plan = self.planner.plan_statment(statement).await?;
            let optimised_plan = self.optimiser.optimise(plan)?;
        }

        Ok(())
    }
}
