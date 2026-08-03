use anyhow::Result;

use crate::{execution::pipeline::Pipeline, planner::LogicalPlan};

/// Helper structure to accumulate pipelines and assign pipeline IDs during translation
pub struct PlanBuilder {
    pub pipelines: Vec<Pipeline>,
    pub next_pipeline_id: usize,
}

impl PlanBuilder {
    pub fn new() -> Self {
        todo!("implement me")
    }
}

/// A physical plan is a tree of physical operators. Every operator must know how to translate
/// itself into execution pipelines via `build()` hook
pub trait PhysicalPlanNode {
    /// Recursively registers executing pipeline(s) for this physical node and its children
    fn build(&self, builder: &mut PlanBuilder) -> Result<()>;
}

// Implement PhysicalPlanNode for PhysicalScan, PhysicalFilter
// ...

#[derive(Default)]
pub struct PhysicalPlanGenerator;

impl PhysicalPlanGenerator {
    pub fn create_plan(&self, logical_plan: &LogicalPlan) -> Result<Box<dyn PhysicalPlanNode>> {
        todo!("implement me")
    }
}
