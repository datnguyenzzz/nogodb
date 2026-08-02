use std::sync::Arc;

use anyhow::Result;

use crate::{
    catalog::provider::CatalogProvider, planner::planner::Planner, sql_parser::Parser,
    storage::catalog::GrpcCatalogClient,
};

pub struct Client {
    parser: Parser,
    planner: Planner,
}

impl Client {
    pub fn new(server_address: String) -> Self {
        let catalog_client = Arc::new(GrpcCatalogClient::new(server_address));
        let catalog_provider = Arc::new(CatalogProvider::new(catalog_client));
        Self {
            parser: Parser::default(),
            planner: Planner::new(catalog_provider),
        }
    }

    pub fn execute(&mut self, statement: &str) -> Result<()> {
        let statements = self.parser.parse_sql(statement)?;
        let _ = self.planner.plan_statment(statements);

        Ok(())
    }
}
