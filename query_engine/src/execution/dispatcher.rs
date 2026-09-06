use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Result;

use crate::execution::{
    Morsel,
    pipeline::{Pipeline, PipelineID, ScanMessage},
};

/// Represents the dispatcher's response to an idle worker thread requesting work.
pub enum DispatchResult {
    /// Executing this specific Morsel of this Pipeline, loaded with its pre-fetched ScanMessage
    ProcessMorsel {
        pipeline: Arc<Pipeline>,
        morsel: Morsel,
        scan_message: ScanMessage,
    },
    /// No work is ready right now (pipelines are blocked on dependencies); worker should yield.
    Wait,
    /// All pipelines in the query execution DAG have completed.
    Finished,
}

/// A NUMA-aware work queue for a running [Pipeline]
pub struct WorkQueue {
    /// Queues of pending morsels and their ScanMessages, segmented by
    /// their physical NUMA node
    pub numa_queues: Vec<VecDeque<(Morsel, ScanMessage)>>,
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
    // join-id <-> [min_val, max_val]
    pub join_bounds: Mutex<HashMap<usize, (i64, i64)>>,
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
            join_bounds: Mutex::new(HashMap::new()),
        }
    }

    /// Queries the active join key boundaries for a specific join ID.
    pub fn get_join_bounds(&self, join_id: usize) -> Option<(i64, i64)> {
        let guard = self.join_bounds.lock().unwrap();
        guard.get(&join_id).copied()
    }

    /// Publishes/updates the join key boundaries for a specific join ID.
    pub fn publish_join_bounds(&self, join_id: usize, min_val: i64, max_val: i64) {
        let mut guard = self.join_bounds.lock().unwrap();
        guard.insert(join_id, (min_val, max_val));
    }

    /// Pushes a single pre-fetched [ScanMessage] (and its [Morsel]) dynamically
    /// into the target NUMA node's queue. Called asynchronously by the unpinned I/O thread.
    pub fn push_scan_message(
        &self,
        pipeline_id: usize,
        numa_node: usize,
        morsel: Morsel,
        message: ScanMessage,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(queue) = state.work_queues.get_mut(&pipeline_id) {
            queue.numa_queues[numa_node % self.numa_nodes].push_back((morsel, message));
            queue.total_morsels += 1;
        }
        Ok(())
    }

    /// Registers an executable pipeline inside the coordination DAG, instantiating
    /// an empty, ready-to-run WorkQueue. Slices are pushed dynamically via push_scan_message.
    pub fn register(&self, pipeline: Pipeline) -> Result<()> {
        let numa_queues = vec![VecDeque::new(); self.numa_nodes];
        let pipeline_id = pipeline.id;
        let mut state = self.state.lock().unwrap();
        state.pipelines.insert(pipeline_id, Arc::new(pipeline));
        state.work_queues.insert(
            pipeline_id,
            WorkQueue {
                numa_queues,
                active_tasks: AtomicUsize::new(0),
                total_morsels: 0,
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
            if let Some((morsel, message)) = queue.numa_queues[worker_numa_node].pop_front() {
                queue
                    .active_tasks
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Ok(DispatchResult::ProcessMorsel {
                    pipeline: state.pipelines.get(id).unwrap().clone(),
                    morsel,
                    scan_message: message,
                });
            }
        }

        for id in &runnable_pipeline_ids {
            let queue = state.work_queues.get_mut(id).unwrap();
            for stolen_numa in 0..self.numa_nodes {
                if stolen_numa == worker_numa_node {
                    continue;
                }
                if let Some((morsel, message)) = queue.numa_queues[stolen_numa].pop_front() {
                    queue.active_tasks.fetch_add(1, Ordering::SeqCst);
                    return Ok(DispatchResult::ProcessMorsel {
                        pipeline: state.pipelines.get(id).unwrap().clone(),
                        morsel,
                        scan_message: message,
                    });
                }
            }
        }

        // If no morsels are left to claim, but some cores are still processing active tasks,
        // we must wait for them to finish before we can advance the DAG
        let has_active_tasks = runnable_pipeline_ids.iter().any(|id| {
            state
                .work_queues
                .get(id)
                .unwrap()
                .active_tasks
                .load(Ordering::SeqCst)
                > 0
        });

        if has_active_tasks {
            Ok(DispatchResult::Wait)
        } else {
            for id in runnable_pipeline_ids {
                if let Some(pipeline) = state.pipelines.get(&id) {
                    pipeline.sink.combine()?;
                }
                state.pipelines.remove(&id);
                state.work_queues.remove(&id);
                state.completed_pipelines.insert(id);
            }
            Ok(DispatchResult::Wait)
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
    use crate::execution::pipeline::{PhysicalSink, SinkContext, SinkResult};

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
            operators: vec![],
            sink: Box::new(DummySink),
            dependencies: vec![],
            partitions: 1,
            schema: std::sync::Arc::new(crate::arrow::Schema::new(
                Vec::<crate::arrow::Field>::new(),
            )),
        };
        dispatcher.register(pipeline_a).unwrap();

        // Push mock data dynamically matching standard NUMA nodes!
        let morsel_a1 = Morsel {
            start_row: 0,
            num_rows: 100_000,
            numa_node: 0,
        };
        dispatcher
            .push_scan_message(
                10,
                0,
                morsel_a1,
                ScanMessage::Batch(RecordBatch::new_empty()),
            )
            .unwrap();

        let morsel_a2 = Morsel {
            start_row: 100_000,
            num_rows: 50_000,
            numa_node: 1,
        };
        dispatcher
            .push_scan_message(
                10,
                1,
                morsel_a2,
                ScanMessage::Batch(RecordBatch::new_empty()),
            )
            .unwrap();

        // 2. Register Pipeline B (ID: 20, DEPENDS on A)
        let mut pipeline_b = Pipeline {
            id: 20,
            operators: vec![],
            sink: Box::new(DummySink),
            dependencies: vec![],
            partitions: 1,
            schema: std::sync::Arc::new(crate::arrow::Schema::new(
                Vec::<crate::arrow::Field>::new(),
            )),
        };
        pipeline_b.add_dependency(10);
        dispatcher.register(pipeline_b).unwrap();

        // Push Pipeline B's mock data!
        let morsel_b1 = Morsel {
            start_row: 0,
            num_rows: 50_000,
            numa_node: 0,
        };
        dispatcher
            .push_scan_message(
                20,
                0,
                morsel_b1,
                ScanMessage::Batch(RecordBatch::new_empty()),
            )
            .unwrap();

        // --- TEST 1: DAG Blocking ---
        // Request work on NUMA Node 0. Since A is not finished, B is blocked.
        // The dispatcher should return A's morsel (assigned to NUMA Node 0)!
        let work_res = dispatcher.pull_work(0).await.unwrap();
        let fetched_a1 = match work_res {
            DispatchResult::ProcessMorsel {
                pipeline, morsel, ..
            } => {
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
        let fetched_a2 = match work_res2 {
            DispatchResult::ProcessMorsel {
                pipeline, morsel, ..
            } => {
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
            .mark_morsel_complete(10, fetched_a1)
            .await
            .unwrap();
        dispatcher
            .mark_morsel_complete(10, fetched_a2)
            .await
            .unwrap();

        // Request work on NUMA Node 0. Since A is completed, B is unlocked and released!
        let work_res4 = dispatcher.pull_work(0).await.unwrap();
        match work_res4 {
            DispatchResult::ProcessMorsel {
                pipeline, morsel, ..
            } => {
                assert_eq!(pipeline.id, 20);
                assert_eq!(morsel.numa_node, 0);
            }
            _ => panic!("Expected Pipeline B's morsel"),
        }
    }
}
