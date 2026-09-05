use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::{
    runtime::LocalRuntime,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task,
};

use crate::execution::{
    Morsel,
    dispatcher::{DispatchResult, Dispatcher},
    numa_topology::NumaTopology,
    pipeline::{Pipeline, SinkResult, SinkContext},
};

/// An Inter-Core message represented as a boxed closure to be
/// executed on a remote Reactor core.
/// In the thread-per-core architecture, Core 0 is physically forbidden
/// from ever locking or directly reading/writing memory owned by Core 1.
/// Therefore, instead of Core 0 locking Core 1's memory to read it, Core 0
/// uses Inter-Core Message Passing
//     [ Core 0 (Reactor) ]                              [ Core 1 (Reactor) ]
//                  │                                                  │
//                  │ 1. Core 0 compiles Lookup closure                │
//                  │                                                  │
//                  ▼                                                  │
//         [ Mailbox_Sender 1 ] ───(2. submit closure via channel)───► [ Mailbox_Receiver 1 ]
//                                                                     │
//                                                                     │ 3. Core 1 polls receiver
//                                                                     │    and executes lookup locally
//                                                                     ▼
//                                                            [ Core 1's RAM location ]
pub type InterCoreMessage = Box<dyn FnOnce() -> Result<()> + Send>;

/// A Thread-safe mailbox sender handle to submit tasks/messages
/// to a specific Core's Reactor
#[derive(Clone)]
pub struct MailBoxSender {
    sender: UnboundedSender<InterCoreMessage>,
}

impl MailBoxSender {
    pub fn submit<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        self.sender
            .send(Box::new(f))
            .map_err(|_| anyhow!("Failed to submit task to target Core Mailbox"))
    }
}

pub struct Worker {
    pub core_id: core_affinity::CoreId,
    pub numa_node: usize,
    pub dispatcher: Arc<Dispatcher>,
    /// Mailbox receiver, owned exclusively by this worker's local thread
    pub mailbox_rx: UnboundedReceiver<InterCoreMessage>,
    /// Global registry of all core mailboxes to support inter-core submissions
    pub all_mailboxes: Vec<MailBoxSender>,
}

impl Worker {
    pub fn run_loop(&mut self) -> Result<()> {
        core_affinity::set_for_current(self.core_id);

        // Build an isolated, single-threaded Tokio LocalRuntime (similar to LocalSet)
        // and run the cooperative Reactor loop directly on the it
        let rt = LocalRuntime::new().unwrap();
        rt.block_on(async move {
            loop {
                let mut is_done = false;

                // For simplicity, we prioritise the all inter-core tasks
                // over the local tasks. Since those tasks should be relatively
                // lightweight (such as pointer look up, ...), it should be ok
                // (until it isn't :-) )
                while let Ok(msg) = self.mailbox_rx.try_recv() {
                    if let Err(e) = msg() {
                        // TODO: How should we handle error here ?
                        eprintln!(
                            "Error executing inter-core message on Core {}: {:?}",
                            self.core_id.id, e
                        );
                    }
                    is_done = true;
                }

                match self.dispatcher.pull_work(self.numa_node).await? {
                    DispatchResult::ProcessMorsel { pipeline, morsel } => {
                        let pipeline_id = pipeline.id;
                        if let Err(e) = self.execute_vectorized_morsel_loop(&pipeline, morsel) {
                            eprintln!("Error executing pipeline: {:?}", e);
                        }

                        self.dispatcher
                            .mark_morsel_complete(pipeline_id, morsel)
                            .await?;
                    }
                    DispatchResult::Wait => {
                        // pipeline is blocked on active dependencies
                    }
                    DispatchResult::Finished => break,
                }

                if !is_done {
                    task::yield_now().await;
                }
            }

            Ok::<(), anyhow::Error>(())
        })?;

        Ok(())
    }

    /// The core vectorized push-loop: processes the 100,000-row morsel
    /// vector-by-vector (2048 rows)
    fn execute_vectorized_morsel_loop(&self, pipeline: &Pipeline, morsel: Morsel) -> Result<()> {
        let mut offset = 0;
        let mut sink_ctx = SinkContext { thread_id: self.core_id.id };

        while let Some(mut batch) = pipeline.source.get_chunk(&morsel, offset)? {
            let mut is_passed = true;

            for op in &pipeline.operators {
                if let Some(next_batch) = op.execute(&batch)? {
                    batch = next_batch
                } else {
                    is_passed = false;
                    break;
                }
            }

            if is_passed {
                match pipeline.sink.sink(&mut sink_ctx, batch)? {
                    SinkResult::Finished => break,
                    SinkResult::NeedMoreInput => {}
                }
            }

            offset += 1
        }

        Ok(())
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
    pub fn new() -> Self {
        let topo = NumaTopology::detect();
        let numa_nodes = topo.numa_nodes_count();
        let total_cores = topo.cores_count();

        let cores_per_node = (total_cores + numa_nodes - 1) / numa_nodes;

        Self {
            numa_nodes,
            cores_per_node,
        }
    }
    /// Spawns parallel worker tasks on Tokio, submitting the query pipeline DAG
    pub fn execute_job(&self, pipelines: Vec<Pipeline>) -> Result<()> {
        let dispatcher = Arc::new(Dispatcher::new(self.numa_nodes));
        for pipeline in pipelines {
            let rows = pipeline.source.total_rows()?;
            dispatcher.register(pipeline, rows)?;
        }

        let topo = NumaTopology::detect();
        let total_workers = self.cores_per_node * self.numa_nodes;
        let mut mailbox_txs = Vec::with_capacity(total_workers);
        let mut mailbox_rxs = Vec::with_capacity(total_workers);

        for _ in 0..total_workers {
            let (tx, rx) = unbounded_channel();
            mailbox_txs.push(MailBoxSender { sender: tx });
            mailbox_rxs.push(rx)
        }

        let active_core_ids = core_affinity::get_core_ids().unwrap_or_default();
        let mut thread_handles = Vec::new();
        for id in 0..total_workers {
            let core_id = active_core_ids.get(id).unwrap();
            let worker_numa = topo.numa_node(core_id.id);
            let mut worker = Worker {
                core_id: *core_id,
                numa_node: worker_numa,
                dispatcher: dispatcher.clone(),
                mailbox_rx: mailbox_rxs.remove(0),
                all_mailboxes: mailbox_txs.clone(),
            };
            let handle = std::thread::spawn(move || worker.run_loop());

            thread_handles.push(handle);
        }

        // Block caller thread until all worker threads complete
        for handle in thread_handles {
            handle
                .join()
                .map_err(|e| anyhow!("Worker thread panicked: {:?}", e))??;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::pipeline::{PhysicalOperator, PhysicalSink, PhysicalSource, SinkContext, SinkResult};
    use crate::arrow::{Field, Schema, DataType, RecordBatch, ArrayRef, array::PrimitiveArray};
    use std::sync::Mutex;

    struct MockSource;
    impl PhysicalSource for MockSource {
        fn total_rows(&self) -> Result<usize> {
            Ok(30)
        }
        fn next_morsel(&self, _worker_numa_node: usize) -> Result<Option<Morsel>> {
            Ok(None)
        }
        fn get_chunk(&self, morsel: &Morsel, batch_offset: usize) -> Result<Option<RecordBatch>> {
            // Generate mock batch data up to 3 batches
            if batch_offset >= 3 {
                return Ok(None);
            }
            
            let schema = Arc::new(Schema::new(vec![
                Field { name: "val".to_string(), data_type: DataType::Int32, nullable: false },
            ]));
            
            // Symmetrically yield unique offset elements
            let start = (morsel.start_row + batch_offset * 10) as i32;
            let values: Vec<i32> = (0..10).map(|i| start + i).collect();
            let array: ArrayRef = Arc::new(PrimitiveArray::from(values));
            
            let batch = RecordBatch::try_new(schema, vec![array])?;
            Ok(Some(batch))
        }
    }

    struct MockSink {
        accumulated: Arc<Mutex<Vec<i32>>>,
    }

    impl PhysicalSink for MockSink {
        fn sink(&self, _ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult> {
            let mut guard = self.accumulated.lock().unwrap();
            let array = input.column(0).as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
            for val in array.iter().flatten() {
                guard.push(val);
            }
            Ok(SinkResult::NeedMoreInput)
        }
        fn combine(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_scheduler_end_to_end_parallel_job() {
        let scheduler = Scheduler::new();
        assert!(scheduler.numa_nodes >= 1);
        assert!(scheduler.cores_per_node >= 1);

        // Setup shared mock buffers
        let accumulated_state = Arc::new(Mutex::new(Vec::new()));

        let pipeline = Pipeline {
            id: 100,
            source: Box::new(MockSource),
            operators: vec![],
            sink: Box::new(MockSink { accumulated: accumulated_state.clone() }),
            dependencies: vec![],
            partitions: 1,
        };

        // Submit job to parallel Reactors!
        scheduler.execute_job(vec![pipeline]).unwrap();

        let mut final_results = accumulated_state.lock().unwrap().clone();
        final_results.sort(); // Sort because parallel workers process in arbitrary order!

        // Slicing registers 30 rows (Morsel 1: 30 rows).
        // It evaluates 3 batches of 10 rows.
        // Total rows processed = 30 rows!
        assert_eq!(final_results.len(), 30);

        // Assert first and last values are mapped cleanly
        assert_eq!(final_results[0], 0);
        assert_eq!(final_results[29], 29);
    }

    #[test]
    fn test_inter_core_mailbox_submissions() {
        let scheduler = Scheduler::new();
        let _total_workers = scheduler.numa_nodes * scheduler.cores_per_node;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sender = MailBoxSender { sender: tx };

        // Symmetrically submit a closure to Core's mailbox!
        let trigger = Arc::new(Mutex::new(false));
        let trigger_clone = trigger.clone();

        sender.submit(move || {
            let mut guard = trigger_clone.lock().unwrap();
            *guard = true;
            Ok(())
        }).unwrap();

        // Pull message on receiver
        let msg = rx.try_recv().unwrap();
        msg().unwrap();

        // Verify state mutated lock-free!
        assert!(*trigger.lock().unwrap());
    }
}
