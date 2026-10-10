use std::{
    io,
    sync::{Arc, Mutex, mpsc},
    thread,
};

use anyhow::{Result, anyhow, bail};
use tokio::{
    runtime::LocalRuntime,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task,
};

use crate::execution::{
    Morsel,
    aggregate::PhysicalAggregateSink,
    dispatcher::{DispatchResult, Dispatcher},
    numa_topology::NumaTopology,
    pipeline::{
        CombineResult, OperatorContext, Pipeline, PipelineEvent, ScanMessage, SinkContext,
        SinkResult,
    },
    scan::ScanSource,
};
use crate::{
    arrow::{
        Buffer, RecordBatch, SchemaRef,
        ipc::{self, record_batch::RecordBatchDecoder},
    },
    execution::{
        hash_join::operators::{PhysicalBuildSink, PhysicalProbeOperator},
        pipeline::PipelineID,
    },
};

const IO_THREAD_COUNT: usize = 4;

/// An Inter-Core message represented as a boxed closure to be
/// executed on a remote Reactor core.
pub enum InterCoreMessage {
    JoinBuildShuffle {
        pipeline_id: PipelineID,
        batch: RecordBatch,
    },
    JoinProbeShuffle {
        pipeline_id: PipelineID,
        batch: RecordBatch,
        /// Return channel to send joined RecordBatches back to the originating core
        response_tx: UnboundedSender<RecordBatch>,
    },
    AggregateShuffle {
        pipeline_id: PipelineID,
        batch: RecordBatch,
    },
}

/// A Thread-safe mailbox sender handle to submit tasks/messages
#[derive(Clone)]
pub struct MailBoxSender {
    pub sender: UnboundedSender<InterCoreMessage>,
}

impl MailBoxSender {
    pub fn submit(&self, msg: InterCoreMessage) -> Result<()> {
        self.sender
            .send(msg)
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
        // pin this worker thread to the target core
        core_affinity::set_for_current(self.core_id);

        let rt = LocalRuntime::new().unwrap();
        rt.block_on(async move {
            loop {
                let mut is_done = false;

                // For simplicity, we prioritise the all inter-core tasks
                // over the local tasks. Since those tasks should be relatively
                // lightweight (such as pointer look up, ...), it should be ok
                // (until it isn't :-) )
                while let Ok(msg) = self.mailbox_rx.try_recv() {
                    match msg {
                        InterCoreMessage::JoinBuildShuffle { pipeline_id, batch } => {
                            let pipeline = self.dispatcher.get_pipeline(pipeline_id);
                            if let Some(join_build_sink) =
                                pipeline.sink.as_any().downcast_ref::<PhysicalBuildSink>()
                            {
                                join_build_sink.push_batch_local(batch);
                            }
                        }

                        InterCoreMessage::JoinProbeShuffle {
                            pipeline_id,
                            batch,
                            response_tx,
                        } => {
                            let pipeline = self.dispatcher.get_pipeline(pipeline_id);
                            for op in &pipeline.operators {
                                if let Some(probe_op) =
                                    op.as_any().downcast_ref::<PhysicalProbeOperator>()
                                {
                                    probe_op.probe_and_respond_local(&batch, response_tx)?;
                                    break;
                                }
                            }
                        }
                        InterCoreMessage::AggregateShuffle { pipeline_id, batch } => {
                            let pipeline = self.dispatcher.get_pipeline(pipeline_id);
                            if let Some(agg_sink) = pipeline
                                .sink
                                .as_any()
                                .downcast_ref::<PhysicalAggregateSink>()
                            {
                                agg_sink.accumulate_local(self.core_id.id, batch)?;
                            }
                        }
                    }
                    is_done = true;
                }

                match self.dispatcher.pull_work(self.numa_node).await? {
                    DispatchResult::ProcessMorsel {
                        pipeline,
                        morsel,
                        scan_message,
                    } => {
                        if let Err(e) = self.execute_vectorized_quantum(&pipeline, scan_message) {
                            eprintln!("Error executing pipeline: {:?}", e);
                        }

                        // Check and execute .combine() locally if this thread is the last worker
                        self.mark_morsel_complete_local(&pipeline, morsel).await?;
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

    /// Triggers local combine() and marks the pipeline complete
    async fn mark_morsel_complete_local(&self, pipeline: &Pipeline, _morsel: Morsel) -> Result<()> {
        let is_last = self
            .dispatcher
            .check_and_decrement_active_tasks(pipeline.id)?;
        if is_last {
            let res = pipeline.sink.combine()?;
            if let CombineResult::Materialised(batch) = res {
                if let Some(downstream) = pipeline.downstream_id {
                    // push to the next pipeline. The sink's output RecordBatch already
                    // physically resides in self.numa_node's local RAM of this worker
                    let next_morsel = Morsel {
                        start_row: 0,
                        num_rows: batch.num_rows(),
                        numa_node: self.numa_node,
                    };
                    self.dispatcher.push_scan_message(
                        downstream,
                        self.numa_node,
                        next_morsel,
                        ScanMessage::Batch(batch),
                    )?;
                } else {
                    // root pipeline terminated, collect final result
                    self.dispatcher.collect_final_result(batch);
                }
            }

            self.dispatcher.mark_pipeline_complete(pipeline.id).await?;
        }
        Ok(())
    }

    /// Local-Core Decompression & Decoding (CPU-bound execution path)
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

        let mut batch_decoder = RecordBatchDecoder::new(&decompressed_buf, fb_batch, schema);
        batch_decoder.try_decode()
    }

    fn execute_vectorized_quantum(&self, pipeline: &Pipeline, message: ScanMessage) -> Result<()> {
        let mut sink_ctx = SinkContext {
            core_id: self.core_id.id,
            pipeline_id: pipeline.id,
        };
        let op_ctx = OperatorContext {
            core_id: self.core_id.id,
            pipeline_id: pipeline.id,
        };
        let mut batch = match message {
            ScanMessage::Batch(b) => b,
            ScanMessage::CompressedPage { data, meta } => {
                Self::decompress_and_decode(data, meta, pipeline.schema.clone())?
            }
        };

        let mut is_passed = true;

        for op in &pipeline.operators {
            if let Some(next_batch) = op.execute(&op_ctx, &batch)? {
                batch = next_batch
            } else {
                is_passed = false;
                break;
            }
        }

        if is_passed {
            match pipeline.sink.sink(&mut sink_ctx, batch)? {
                SinkResult::Finished | SinkResult::NeedMoreInput => {}
            }
        }

        Ok(())
    }
}

pub struct ScanTask {
    pub pipeline_id: usize,
    pub source: Arc<ScanSource>,
    pub dispatcher: Arc<Dispatcher>,
}

pub struct Scheduler {
    pub numa_nodes: usize,
    pub cores_per_node: usize,
    pub dispatcher: Arc<Dispatcher>,
    pub mailboxes: Arc<Vec<MailBoxSender>>,
    /// Handles to keep the background Reactor threads alive for the lifetime of the engine
    pub thread_handles: Vec<thread::JoinHandle<Result<()>>>,
    /// Channel to submit scan tasks to the long-lived I/O thread pool
    pub io_sender: mpsc::Sender<ScanTask>,
}

impl Scheduler {
    pub fn new() -> Self {
        let topo = NumaTopology::detect();
        let numa_nodes = topo.numa_nodes_count();
        let total_cores = topo.cores_count();
        let cores_per_node = (total_cores + numa_nodes - 1) / numa_nodes;

        let total_workers = cores_per_node * numa_nodes;
        let mut mailbox_txs = Vec::with_capacity(total_workers);
        let mut mailbox_rxs = Vec::with_capacity(total_workers);
        for _ in 0..total_workers {
            let (tx, rx) = unbounded_channel();
            mailbox_txs.push(MailBoxSender { sender: tx });
            mailbox_rxs.push(rx);
        }

        let mailboxes = Arc::new(mailbox_txs);
        let dispatcher = Arc::new(Dispatcher::new(numa_nodes));
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
                all_mailboxes: mailboxes.as_ref().clone(),
            };
            let handle = std::thread::spawn(move || worker.run_loop());
            thread_handles.push(handle);
        }

        // spawn long-lived unpinned I/O thread pool
        let (io_tx, io_rx) = mpsc::channel::<ScanTask>();
        let shared_rx = Arc::new(Mutex::new(io_rx));
        for io_id in 0..IO_THREAD_COUNT {
            let rx = shared_rx.clone();
            thread::Builder::new()
                .name(format!("io-pool-worker-{}", io_id))
                .spawn(move || {
                    while let Ok(task) = {
                        let guard = rx.lock().unwrap();
                        guard.recv()
                    } {
                        if let Err(e) = task.source.execute(task.pipeline_id, task.dispatcher) {
                            // Need to handle error here
                            eprintln!(
                                "Error in I/O scan task for pipeline {}: {:?}",
                                task.pipeline_id, e
                            );
                        }
                    }
                })
                .expect("Failed to spawn background I/O thread");
        }

        Self {
            numa_nodes,
            cores_per_node,
            dispatcher,
            mailboxes,
            thread_handles,
            io_sender: io_tx,
        }
    }

    /// Submits a query pipeline DAG to the permanently running background reactor pool,
    /// blocking the caller thread safely until the query completes!
    pub fn execute_job(&self, pipelines: Vec<Pipeline>) -> Result<Vec<RecordBatch>> {
        let (tx, rx) = mpsc::channel();

        for pipeline in pipelines {
            let id = pipeline.id;
            let deps = pipeline.dependencies.clone();
            self.dispatcher
                .register(id, deps, Arc::new(pipeline), tx.clone())?;
        }
        self.trigger_ready_scans()?;

        // Drop local sender so receiver closes when last dispatcher task unregisters
        drop(tx);

        // Block caller thread until query completion
        while let Ok(event) = rx.recv() {
            match event {
                PipelineEvent::PipelineFinished(_pid) => {
                    self.trigger_ready_scans()?;
                }
                PipelineEvent::AllDone => break,
            }
        }

        Ok(self.dispatcher.take_final_results())
    }

    pub fn trigger_ready_scans(&self) -> Result<()> {
        let ready = self.dispatcher.take_ready_scan_sources();
        for (pid, source) in ready {
            self.io_sender.send(ScanTask {
                pipeline_id: pid,
                source,
                dispatcher: self.dispatcher.clone(),
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{ArrayRef, DataType, Field, RecordBatch, Schema, array::PrimitiveArray};
    use crate::execution::pipeline::{CombineResult, PhysicalSink, SinkContext, SinkResult};
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
        fn combine(&self) -> Result<CombineResult> {
            Ok(CombineResult::Empty)
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_scheduler_end_to_end_parallel_job() {
        let scheduler = Scheduler::new();

        // Setup shared mock buffers
        let accumulated_state = Arc::new(Mutex::new(Vec::new()));

        let pipeline = Pipeline {
            id: 100,
            downstream_id: Some(50),
            source: None,
            operators: vec![],
            sink: Box::new(MockSink {
                accumulated: accumulated_state.clone(),
            }),
            dependencies: vec![],
            partitions: 1,
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        };

        // Initialize and register inside dispatcher
        let (tx, rx) = std::sync::mpsc::channel();
        scheduler
            .dispatcher
            .register(100, vec![], Arc::new(pipeline), tx)
            .unwrap();

        // Feed mock page page buffers into dispatcher queues!
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

            scheduler
                .dispatcher
                .push_scan_message(100, numa_node, morsel, ScanMessage::Batch(batch))
                .unwrap();
            row_offset += 10;
            count += 1;
        }

        scheduler.dispatcher.finish_pushing(100, count).unwrap();

        // Wait for background worker threads to automatically consume and finish the job!
        let _ = rx.recv();

        let mut final_results = accumulated_state.lock().unwrap().clone();
        final_results.sort(); // Sort because parallel workers process in arbitrary order!

        assert_eq!(final_results.len(), 30);
        assert_eq!(final_results[0], 0);
        assert_eq!(final_results[29], 29);
    }

    #[test]
    fn test_inter_core_mailbox_submissions() {
        let (tx, mut rx) = unbounded_channel();
        let sender = MailBoxSender { sender: tx };

        // Symmetrically submit a typed JoinBuildShuffle message to Core's mailbox!
        sender
            .submit(InterCoreMessage::JoinBuildShuffle {
                pipeline_id: 100,
                batch: RecordBatch::new_empty(Arc::new(Schema::new(Vec::<Field>::new()))),
            })
            .unwrap();

        // Pull message on receiver
        let msg = rx.try_recv().unwrap();

        match msg {
            InterCoreMessage::JoinBuildShuffle { pipeline_id, .. } => {
                assert_eq!(pipeline_id, 100);
            }
            _ => panic!("Expected JoinBuildShuffle message"),
        }
    }

    struct FinalMaterializingSink {
        output_batch: Mutex<Option<RecordBatch>>,
    }

    impl PhysicalSink for FinalMaterializingSink {
        fn sink(&self, _ctx: &mut SinkContext, _input: RecordBatch) -> Result<SinkResult> {
            Ok(SinkResult::Finished)
        }
        fn combine(&self) -> Result<CombineResult> {
            let mut guard = self.output_batch.lock().unwrap();
            let batch = guard.take().unwrap();
            Ok(CombineResult::Materialised(batch))
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_scheduler_root_pipeline_collect_final_result() {
        let scheduler = Scheduler::new();

        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));
        let id_col: ArrayRef = Arc::new(PrimitiveArray::from(vec![42i32]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![id_col]).unwrap();

        let pipeline = Pipeline {
            id: 200,
            downstream_id: None, // ROOT PIPELINE!
            source: None,
            operators: vec![],
            sink: Box::new(FinalMaterializingSink {
                output_batch: Mutex::new(Some(expected_batch)),
            }),
            dependencies: vec![],
            partitions: 1,
            schema: schema.clone(),
        };

        let (tx, rx) = std::sync::mpsc::channel();
        scheduler
            .dispatcher
            .register(200, vec![], Arc::new(pipeline), tx)
            .unwrap();

        // Push a single morsel to trigger execution
        let morsel = Morsel {
            start_row: 0,
            num_rows: 1,
            numa_node: 0,
        };
        scheduler
            .dispatcher
            .push_scan_message(
                200,
                0,
                morsel,
                ScanMessage::Batch(RecordBatch::new_empty(schema)),
            )
            .unwrap();
        scheduler.dispatcher.finish_pushing(200, 1).unwrap();

        // Wait for background worker threads to automatically consume and finish the job!
        while let Ok(event) = rx.recv() {
            if let PipelineEvent::AllDone = event {
                break;
            }
        }

        // Extract and verify the final collected results from the root pipeline!
        let results = scheduler.dispatcher.take_final_results();

        assert_eq!(results.len(), 1);
        let out_id = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();
        assert_eq!(out_id.value(0), 42);
    }
}
