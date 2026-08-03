use std::sync::Arc;

use anyhow::Result;

use crate::{
    db::Database,
    execution::physical_plan::{PhysicalPlanGenerator, PlanBuilder},
    optimiser::Optimiser,
    planner::planner::Planner,
    sql_parser::Parser,
};

pub struct Client {
    db: Arc<Database>,
    parser: Parser,
    planner: Planner,
    optimiser: Optimiser,
    physical_generator: PhysicalPlanGenerator,
}

impl Client {
    pub fn new(db: Arc<Database>) -> Self {
        let catalog = db.catalog_provider.clone();
        Self {
            db,
            parser: Parser::default(),
            planner: Planner::new(catalog),
            optimiser: Optimiser::default(),
            physical_generator: PhysicalPlanGenerator::default(),
        }
    }

    pub async fn execute(&mut self, statement: &str) -> Result<()> {
        let statements = self.parser.parse_sql(statement)?;

        for statement in statements {
            let plan = self.planner.plan_statment(statement).await?;
            let optimised_plan = self.optimiser.optimise(plan)?;
            let physical_node = self.physical_generator.create_plan(&optimised_plan)?;
            let mut builder = PlanBuilder::new();
            physical_node.build(&mut builder)?;
            self.db.scheduler.execute_job(builder.pipelines).await?;
        }

        Ok(())
    }
}
