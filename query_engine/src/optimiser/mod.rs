use anyhow::Result;

use crate::planner::logical_plan::LogicalPlan;

/// Shared trait implemented by every individual optimizer pass
pub trait OptimiserRule: Send + Sync {
    /// Unique identifier for the rule
    fn name(&self) -> &str;

    /// Analyzes and rewrites the logical plan, returning an optimized version.
    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
}

pub struct Optimiser {
    rules: Vec<Box<dyn OptimiserRule>>,
}

impl Default for Optimiser {
    fn default() -> Self {
        // TODO: Add more optimiser rules here
        Optimiser { rules: vec![] }
    }
}

impl Optimiser {
    pub fn new(rules: Vec<Box<dyn OptimiserRule>>) -> Self {
        Optimiser { rules }
    }

    pub fn optimise(&self, mut plan: LogicalPlan) -> Result<LogicalPlan> {
        for rule in &self.rules {
            plan = rule.rewrite(plan)?;
        }
        Ok(plan)
    }
}
