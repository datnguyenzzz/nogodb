use std::{io, sync::Arc};

use anyhow::{Result, anyhow, bail};
use tokio::{
    runtime::LocalRuntime,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task,
};

use crate::arrow::{Buffer, RecordBatch, SchemaRef, ipc};
use crate::execution::{
    dispatcher::{DispatchResult, Dispatcher},
    numa_topology::NumaTopology,
    pipeline::{Pipeline, ScanMessage, SinkContext},
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
        // and run the cooperative Reactor loop directly on it
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
                        eprintln!(
                            "Error executing inter-core message on Core {}: {:?}",
                            self.core_id.id, e
                        );
                    }
                    is_done = true;
                }

                match self.dispatcher.pull_work(self.numa_node).await? {
                    DispatchResult::ProcessMorsel {
                        pipeline,
                        morsel,
                        scan_message,
                    } => {
                        let pipeline_id = pipeline.id;
                        if let Err(e) = self.execute_vectorized_quantum(&pipeline, scan_message) {
                            eprintln!("Error executing pipeline: {:?}", e);
                        }

                        self.dispatcher
                            .mark_morsel_complete(pipeline_id, morsel)
                            .await?;
                        is_done = true;
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

    /// Reads compressed body bytes, decompresses them using Zstd, and decodes the FlatBuffer
    /// columns directly into a structured RecordBatch locally on the pinned worker core.
    fn decompress_and_decode(
        compressed_data: Buffer,
        metadata_bytes: Buffer,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let fb_message = flatbuffers::root::<ipc::FbMessage>(metadata_bytes.as_slice())
            .map_err(|e| anyhow!("Failed to parse FlatBuffer header: {:?}", e))?;
        let fb_batch = fb_message
            .header_as_fb_record_batch()
            .ok_or_else(|| anyhow!("Invalid FlatBuffer: expected RecordBatch header"))?;

        let compressed_slice = compressed_data.as_slice();
        if compressed_slice.len() < 8 {
            bail!("Invalid compressed page block: body too short");
        }

        let uncompressed_len = i64::from_le_bytes(compressed_slice[0..8].try_into().unwrap());
        let decompressed_buf = if uncompressed_len == -1 {
            Buffer::from(compressed_slice[8..].to_vec())
        } else {
            let mut decoder = zstd::Decoder::new(&compressed_slice[8..])?;
            let mut output = vec![0u8; uncompressed_len as usize];
            io::Read::read_exact(&mut decoder, &mut output)?;
            Buffer::from(output)
        };

        let mut batch_decoder =
            ipc::record_batch::RecordBatchDecoder::new(&decompressed_buf, fb_batch, schema);
        batch_decoder.try_decode()
    }

    /// Processes the page, decompresses if needed, and pushes through operators.
    fn execute_vectorized_quantum(&self, pipeline: &Pipeline, message: ScanMessage) -> Result<()> {
        let mut sink_ctx = SinkContext {
            core_id: self.core_id.id,
        };
        let mut batch = match message {
            ScanMessage::Batch(b) => b,
            ScanMessage::CompressedPage {
                data: compressed_data,
                meta: metadata_bytes,
            } => Self::decompress_and_decode(
                compressed_data,
                metadata_bytes,
                pipeline.schema.clone(),
            )?,
        };

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
            _ = pipeline.sink.sink(&mut sink_ctx, batch)?;
        }

        Ok(())
    }
}

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
            dispatcher.register(pipeline)?;
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
    use crate::arrow::{ArrayRef, DataType, Field, RecordBatch, Schema, array::PrimitiveArray};
    use crate::execution::Morsel;
    use crate::execution::pipeline::{PhysicalSink, SinkResult};
    use std::sync::Mutex;

    struct MockSink {
        accumulated: Arc<Mutex<Vec<i32>>>,
    }

    impl PhysicalSink for MockSink {
        fn sink(&self, _ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult> {
            let mut guard = self.accumulated.lock().unwrap();
            let array = input
                .column(0)
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
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
            operators: vec![],
            sink: Box::new(MockSink {
                accumulated: accumulated_state.clone(),
            }),
            dependencies: vec![],
            partitions: 1,
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        // Initialize and register inside dispatcher
        let dispatcher = Arc::new(Dispatcher::new(scheduler.numa_nodes));
        dispatcher.register(pipeline).unwrap();

        // Feed mock page buffers into dispatcher queues!
        let mut row_offset = 0;
        let mut count = 0;
        while row_offset < 30 {
            let numa_node = count % scheduler.numa_nodes;
            let morsel = Morsel {
                start_row: row_offset,
                num_rows: 10,
                numa_node,
            };

            // Build mock RecordBatch
            let schema = Arc::new(Schema::new(vec![Field {
                name: "val".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }]));
            let values: Vec<i32> = (0..10).map(|i| (row_offset + i) as i32).collect();
            let array: ArrayRef = Arc::new(PrimitiveArray::from(values));
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

            dispatcher
                .push_scan_message(100, numa_node, morsel, ScanMessage::Batch(batch))
                .unwrap();
            row_offset += 10;
            count += 1;
        }

        // Spawn threads manually for exact execution
        let mut thread_handles = Vec::new();
        let total_workers = scheduler.cores_per_node * scheduler.numa_nodes;
        let active_core_ids = core_affinity::get_core_ids().unwrap_or_default();

        let mut mailbox_txs = Vec::with_capacity(total_workers);
        let mut mailbox_rxs = Vec::with_capacity(total_workers);
        for _ in 0..total_workers {
            let (tx, rx) = unbounded_channel();
            mailbox_txs.push(MailBoxSender { sender: tx });
            mailbox_rxs.push(rx)
        }

        for id in 0..total_workers {
            let core_id = active_core_ids.get(id).unwrap();
            let worker_numa = NumaTopology::detect().numa_node(core_id.id);
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

        for handle in thread_handles {
            handle.join().unwrap().unwrap();
        }

        let mut final_results = accumulated_state.lock().unwrap().clone();
        final_results.sort(); // Sort because parallel workers process in arbitrary order!

        assert_eq!(final_results.len(), 30);
        assert_eq!(final_results[0], 0);
        assert_eq!(final_results[29], 29);
    }

    #[test]
    fn test_inter_core_mailbox_submissions() {
        let scheduler = Scheduler::new();
        let _total_workers = scheduler.numa_nodes * scheduler.cores_per_node;

        let (tx, mut rx) = unbounded_channel();
        let sender = MailBoxSender { sender: tx };

        // Symmetrically submit a closure to Core's mailbox!
        let trigger = Arc::new(Mutex::new(false));
        let trigger_clone = trigger.clone();

        sender
            .submit(move || {
                let mut guard = trigger_clone.lock().unwrap();
                *guard = true;
                Ok(())
            })
            .unwrap();

        // Pull message on receiver
        let msg = rx.try_recv().unwrap();
        msg().unwrap();

        // Verify state mutated lock-free!
        assert!(*trigger.lock().unwrap());
    }
}
