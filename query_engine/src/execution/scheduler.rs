use std::sync::Arc;

use anyhow::Result;

use crate::execution::{
    dispatcher::Dispatcher,
    pipeline::{Morsel, Pipeline},
};

pub struct Worker {
    pub id: usize,
    pub numa_node: usize,
    pub dispatcher: Arc<Dispatcher>,
}

impl Worker {
    pub fn run_loop(self) -> Result<()> {
        // we'd pin this thread to Core `self.id`:
        // execute_vectorized_morsel_loop
        todo!("implement me")
    }

    /// The core vectorized push-loop: processes the 100,000-row morsel vector-by-vector (2048 rows)
    fn execute_vectorized_morsel_loop(
        pipeline: &Pipeline,
        morsel: Morsel,
        worker_id: usize,
    ) -> Result<()> {
        todo!("implement me")
    }
}

/// The Scheduler coordinates pipeline execution. It builds a dependency graph, schedules
/// independent tasks on the Tokio thread pool, and when a pipeline finishes, invokes the
/// sink's `combine()` hook to finalize states before unlocking dependent tasks.
// Architecture:
//                                     [ Scheduler ]
//                                           │
//                                           ▼  (Register Pipelines & Dependency DAG)
//                                    [ DISPATCHER ]
//                                           │
//               ┌---------------------------┼---------------------------┐
//               ▼                           ▼                           ▼
//        [ NUMA 0 Queue ]            [ NUMA 1 Queue ]            [ NUMA 2 Queue ]
//       (Morsel, Morsel...)         (Morsel, Morsel...)         (Morsel, Morsel...)
//               ▲                           ▲                           ▲
//               │ (Pull Local Work First)   │                           │
//         [ Worker 0 ]                [ Worker 1 ]                [ Worker 2 ]
//      (Pinned to Core 0)          (Pinned to Core 1)          (Pinned to Core 2)
//               │                           │                           |
//               └------(If Local Empty, Steal from other Node)----------┘
pub struct Scheduler {
    pub numa_nodes: usize,
    pub cores_per_node: usize,
}

impl Scheduler {
    pub fn new(cores: usize) -> Self {
        todo!("implement me")
    }
    /// Spawns parallel worker tasks on Tokio, submitting the query pipeline DAG
    pub fn execute_job(&self, pipelines: Vec<Pipeline>) -> Result<()> {
        todo!("implement me")
    }
}
