use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, atomic::AtomicUsize},
};

use anyhow::Result;

use crate::execution::pipeline::{Morsel, Pipeline};

/// Represents the dispatcher's response to an idle worker thread requesting work.
pub enum DispatchResult {
    /// Execute this specific Morsel of this Pipeline.
    ProcessMorsel {
        pipeline: Arc<Pipeline>,
        morsel: Morsel,
    },
    /// No work is ready right now (pipelines are blocked on dependencies); worker should yield.
    Wait,
    /// All pipelines in the query execution DAG have completed.
    Finished,
}

/// A NUMA-aware work queue for a running Pipeline
pub struct PipelineWorkQueue {
    /// Queues of pending morsels, segmented by their physical NUMA node:
    /// `numa_queues[numa_node_id] = VecDeque<Morsel>`
    pub numa_queues: Vec<VecDeque<Morsel>>,
    /// Number of active/in-flight tasks currently being processed on CPU cores
    pub active_tasks: AtomicUsize,
    /// Total number of morsels in this pipeline
    pub total_morsels: usize,
}

pub struct DispatcherState {
    /// Active pipelines: pipeline_id -> Pipeline
    pub pipelines: HashMap<usize, Arc<Pipeline>>,
    /// Work queues: pipeline_id -> PipelineWorkQueue
    pub work_queues: HashMap<usize, PipelineWorkQueue>,
    /// Set of completed pipeline IDs
    pub completed_pipelines: HashSet<usize>,
}

/// The central, thread-safe Coordinator driving the Morsel Parallel execution DAG.
//                                     [ Scheduler ]
//                                           │
//                                           ▼ (Register Pipelines & Dependency DAG)
//                                    [ DISPATCHER ]
//                                           │
//               ┌───────────────────────────┼───────────────────────────┐
//               ▼                           ▼                           ▼
//      [ NUMA 0 Queue ]            [ NUMA 1 Queue ]            [ NUMA 2 Queue ]
//       (Morsel, Morsel...)         (Morsel, Morsel...)         (Morsel, Morsel...)
//              ▲                           ▲                           ▲
//              │ (Pull Local Work First)   │                           │
//        [ Worker 0 ]                [ Worker 1 ]                [ Worker 2 ]
//        (Pinned to Core 0)          (Pinned to Core 1)          (Pinned to Core 2)
//              │                           │
//              └──────(If Local Empty, Steal from other Node)──────────┘
pub struct Dispatcher {
    pub numa_nodes: usize,
    pub state: Mutex<DispatcherState>,
}

impl Dispatcher {
    /// Pulls the next available Morsel. Prefers local NUMA node; falls back to Work-Stealing!
    pub async fn pull_work(
        &self,
        worker_id: usize,
        worker_numa_node: usize,
    ) -> Result<DispatchResult> {
        todo!("implement me")
    }

    pub async fn mark_morsel_complete(&self, pipeline_id: usize, _morsel: Morsel) -> Result<()> {
        todo!("implement me")
    }
}
