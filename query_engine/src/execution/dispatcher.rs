use std::{
    cmp,
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Result;

use crate::execution::pipeline::{Morsel, Pipeline, PipelineID};

/// 100_000K is an optimal starting number for the morsel size
/// according to the benchmark reported in https://dl.acm.org/doi/epdf/10.1145/2588555.2610507
pub const MORSEL_SIZE: usize = 100_000;

/// Represents the dispatcher's response to an idle worker thread requesting work.
pub enum DispatchResult {
    /// Executing this specific Morsel of this Pipeline.
    ProcessMorsel {
        pipeline: Arc<Pipeline>,
        morsel: Morsel,
    },
    /// No work is ready right now (pipelines are blocked on dependencies); worker should yield.
    Wait,
    /// All pipelines in the query execution DAG have completed.
    Finished,
}

/// A NUMA-aware work queue for a running [Pipeline]
pub struct WorkQueue {
    /// Queues of pending morsels, segmented by their physical NUMA node:
    /// `numa_queues[numa_node_id] = VecDeque<Morsel>`
    pub numa_queues: Vec<VecDeque<Morsel>>,
    /// Number of active/in-flight tasks currently being processed on CPU cores
    pub active_tasks: AtomicUsize,
    pub total_morsels: usize,
}

pub struct DispatcherState {
    pub pipelines: HashMap<PipelineID, Arc<Pipeline>>,
    pub work_queues: HashMap<PipelineID, WorkQueue>,
    pub completed_pipelines: HashSet<PipelineID>,
}

/// The central, thread-safe Coordinator driving the Morsel Parallel execution DAG.
pub struct Dispatcher {
    pub numa_nodes: usize,
    pub state: Mutex<DispatcherState>,
}

impl Dispatcher {
    pub fn new(numa_nodes: usize) -> Self {
        Self {
            numa_nodes,
            state: Mutex::new(DispatcherState {
                pipelines: HashMap::new(),
                work_queues: HashMap::new(),
                completed_pipelines: HashSet::new(),
            }),
        }
    }

    /// Registers an executable pipeline and dynamically slices its total rows into
    /// [MORSEL_SIZE]-row morsels, distributing them evenly across physical NUMA nodes
    pub fn register(&self, pipeline: Pipeline, rows: usize) -> Result<()> {
        let mut numa_queues = vec![VecDeque::new(); self.numa_nodes];
        let mut total_morsels = 0;
        let mut row_offset = 0;

        while row_offset < rows {
            let num_rows = cmp::min(MORSEL_SIZE, rows - row_offset);
            let numa_node = total_morsels % self.numa_nodes;
            numa_queues[numa_node].push_back(Morsel {
                start_row: row_offset,
                num_rows,
                numa_node,
            });
            row_offset += num_rows;
            total_morsels += 1;
        }

        let pipeline_id = pipeline.id;
        let mut state = self.state.lock().unwrap();
        state.pipelines.insert(pipeline_id, Arc::new(pipeline));
        state.work_queues.insert(
            pipeline_id,
            WorkQueue {
                numa_queues,
                active_tasks: AtomicUsize::new(0),
                total_morsels,
            },
        );

        Ok(())
    }

    /// Pulls the next available Morsel. Prefers local NUMA node; falls back to Work-Stealing
    pub async fn pull_work(&self, worker_numa_node: usize) -> Result<DispatchResult> {
        let mut state = self.state.lock().unwrap();
        if state.pipelines.is_empty() {
            return Ok(DispatchResult::Finished);
        }

        let mut runnable_pipeline_ids = Vec::<PipelineID>::new();
        for (&id, pipeline) in &state.pipelines {
            if pipeline
                .dependencies
                .iter()
                .all(|d| state.completed_pipelines.contains(d))
            {
                runnable_pipeline_ids.push(id)
            }
        }

        if runnable_pipeline_ids.is_empty() {
            return Ok(DispatchResult::Wait);
        }

        // Try to pop a morsel from the worker's local NUMA queue (Locality Priority)
        // If the local queue is empty, try to steal from other NUMA node queues
        // It'd give a higher latency penalty, but the worker is not idle
        for id in &runnable_pipeline_ids {
            let queue = state.work_queues.get_mut(id).unwrap();
            if let Some(morsel) = queue.numa_queues[worker_numa_node].pop_front() {
                queue
                    .active_tasks
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Ok(DispatchResult::ProcessMorsel {
                    pipeline: state.pipelines.get(id).unwrap().clone(),
                    morsel,
                });
            }
        }

        for id in &runnable_pipeline_ids {
            let queue = state.work_queues.get_mut(id).unwrap();
            for stolen_numa in 0..self.numa_nodes {
                if let Some(morsel) = queue.numa_queues[stolen_numa].pop_front() {
                    queue.active_tasks.fetch_add(1, Ordering::SeqCst);
                    return Ok(DispatchResult::ProcessMorsel {
                        pipeline: state.pipelines.get(id).unwrap().clone(),
                        morsel,
                    });
                }
            }
        }

        // If no morsels are left to claim, but some cores are still processing active tasks,
        // we must wait for them to finish before we can advance the DAG.
        let active_tasks_exist = runnable_pipeline_ids.iter().any(|id| {
            state
                .work_queues
                .get(id)
                .unwrap()
                .active_tasks
                .load(Ordering::SeqCst)
                > 0
        });

        if active_tasks_exist {
            Ok(DispatchResult::Wait)
        } else {
            for id in runnable_pipeline_ids {
                state.pipelines.remove(&id);
                state.work_queues.remove(&id);
                state.completed_pipelines.insert(id);
            }
            Ok(DispatchResult::Finished)
        }
    }

    pub async fn mark_morsel_complete(&self, pipeline_id: usize, _morsel: Morsel) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(queue) = state.work_queues.get_mut(&pipeline_id) {
            let active = queue.active_tasks.fetch_sub(1, Ordering::SeqCst);

            let all_queues_empty = queue.numa_queues.iter().all(|q| q.is_empty());
            if all_queues_empty && active == 1 {
                if let Some(pipeline) = state.pipelines.get(&pipeline_id) {
                    pipeline.sink.combine()?;
                }

                state.pipelines.remove(&pipeline_id);
                state.work_queues.remove(&pipeline_id);
                state.completed_pipelines.insert(pipeline_id);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::RecordBatch;
    use crate::execution::pipeline::{PhysicalSink, PhysicalSource, SinkContext, SinkResult};

    struct DummySource;
    impl PhysicalSource for DummySource {
        fn next_morsel(&self, _worker_numa_node: usize) -> Result<Option<Morsel>> {
            Ok(None)
        }
        fn get_chunk(&self, _morsel: &Morsel, _batch_offset: usize) -> Result<Option<RecordBatch>> {
            Ok(None)
        }
    }

    struct DummySink;
    impl PhysicalSink for DummySink {
        fn sink(&self, _ctx: &mut SinkContext, _input: RecordBatch) -> Result<SinkResult> {
            Ok(SinkResult::Finished)
        }
        fn combine(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_dispatcher_locality_stealing_and_dag() {
        // Create a 2-socket NUMA Dispatcher
        let dispatcher = Dispatcher::new(2);

        // 1. Register Pipeline A (ID: 10, no dependencies)
        let pipeline_a = Pipeline {
            id: 10,
            source: Box::new(DummySource),
            operators: vec![],
            sink: Box::new(DummySink),
            dependencies: vec![],
            partitions: 1,
        };
        // Register with 150,000 rows (Slices into 2 morsels: 100,000 and 50,000)
        dispatcher.register(pipeline_a, 150_000).unwrap();

        // 2. Register Pipeline B (ID: 20, DEPENDS on A)
        let mut pipeline_b = Pipeline {
            id: 20,
            source: Box::new(DummySource),
            operators: vec![],
            sink: Box::new(DummySink),
            dependencies: vec![],
            partitions: 1,
        };
        pipeline_b.add_dependency(10);
        dispatcher.register(pipeline_b, 50_000).unwrap();

        // --- TEST 1: DAG Blocking ---
        // Request work on NUMA Node 0. Since A is not finished, B is blocked.
        // The dispatcher should return A's morsel (assigned to NUMA Node 0)!
        let work_res = dispatcher.pull_work(0).await.unwrap();
        let morsel_a1 = match work_res {
            DispatchResult::ProcessMorsel { pipeline, morsel } => {
                assert_eq!(pipeline.id, 10);
                assert_eq!(morsel.numa_node, 0);
                morsel
            }
            _ => panic!("Expected Morsel"),
        };

        // --- TEST 2: Work-Stealing ---
        // Request work on NUMA Node 0 again. Node 0's queue of Pipeline A is now empty.
        // Slicing put the 2nd morsel (50,000 rows) on NUMA Node 1 queue.
        // Worker 0 should successfully STEAL Node 1's morsel!
        let work_res2 = dispatcher.pull_work(0).await.unwrap();
        let morsel_a2 = match work_res2 {
            DispatchResult::ProcessMorsel { pipeline, morsel } => {
                assert_eq!(pipeline.id, 10);
                assert_eq!(morsel.numa_node, 1);
                morsel
            }
            _ => panic!("Expected Stolen Morsel"),
        };

        // --- TEST 3: Wait State ---
        // Request work on NUMA Node 0. Since A's morsels are fully claimed but not yet complete,
        // we should get a Wait response (cannot release B yet!).
        let work_res3 = dispatcher.pull_work(0).await.unwrap();
        assert!(matches!(work_res3, DispatchResult::Wait));

        // --- TEST 4: DAG Phase Progression ---
        // Mark both of A's morsels as complete!
        dispatcher
            .mark_morsel_complete(10, morsel_a1)
            .await
            .unwrap();
        dispatcher
            .mark_morsel_complete(10, morsel_a2)
            .await
            .unwrap();

        // Request work on NUMA Node 0. Since A is completed, B is unlocked and released!
        let work_res4 = dispatcher.pull_work(0).await.unwrap();
        match work_res4 {
            DispatchResult::ProcessMorsel { pipeline, morsel } => {
                assert_eq!(pipeline.id, 20);
                assert_eq!(morsel.numa_node, 0);
            }
            _ => panic!("Expected Pipeline B's morsel"),
        }
    }
}
