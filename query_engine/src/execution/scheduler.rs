use anyhow::Result;

use crate::execution::pipeline::Pipeline;

/// The Scheduler coordinates pipeline execution. It builds a dependency graph, schedules
/// independent tasks on the Tokio thread pool, and when a pipeline finishes, invokes the
/// sink's `combine()` hook to finalize states before unlocking dependent tasks.
pub struct Scheduler {
    max_threads: usize,
}

impl Scheduler {
    pub fn new(max_threads: usize) -> Self {
        todo!("implement me")
    }
    /// Spawns parallel worker tasks on Tokio to run the execution DAG
    pub async fn execute_job(&self, pipelines: Vec<Pipeline>) -> Result<()> {
        todo!("implement me")
    }
}
