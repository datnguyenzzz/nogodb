use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;

pub enum SinkResult {
    NeedMoreInput,
    Finished,
}

pub struct SourceContext {
    /// source needs to be parallelism-aware, and know how to
    /// partition rows
    pub partition_id: usize,
    pub thread_id: usize,
}

pub struct SinkContext {
    /// sink needs to be parallelism-aware
    pub thread_id: usize,
}

pub trait PhysicalSource {
    /// Pulls batch_size-rows Arrow RecordBatches
    fn get_chunk(&self, ctx: &mut SourceContext) -> Result<Option<RecordBatch>>;
}

pub trait PhysicalOperator {
    /// Performs in-place vectorized transformations
    fn execute(&self, input: RecordBatch) -> Result<Option<RecordBatch>>;
}

pub trait PhysicalSink {
    /// Consumes batches then accumulates to thread-local states
    fn sink(&self, ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult>;
    /// Combines thread-local partition states into a finalized global
    /// state once all threads finish
    fn combine(&self) -> Result<()>;
}

/// A linear pipeline of execution: Source -> [Operators...] -> Sink
/// Execution plans for non-trivial SQL queries are assembled by
/// stitching multiple pipelines together. There are 3 components in
/// every pipeline:
/// 1. A "Source" produces chunks, such as a table scan or the completed
/// build side of a hash join when it is later scanned.
/// 2. An "Operator" transforms a chunk into another chunk, like a filter
/// or a projection.
/// 3. A "Sink" consumes chunks and accumulates state. An aggregate may build
/// a hash table, while an ORDER BY may collect rows into sorted groups. "Sinks"
/// may only be placed at pipeline ends (pipeline breakers).
///
/// A pipeline is depended on other pipelines, meaning it can only be executed
/// when all depended pipelines are finished
pub struct Pipeline {
    pub id: usize,
    // Operators are stateless and in-place vector transformers. And they are never
    // shared between different pipelines.
    // Sources and Sinks are almost isolated, unless for pipeline breakers (such as
    // Joins or Group By), the Source and Sink are distinct, separate objects, and
    // they share their state table internally
    pub source: Box<dyn PhysicalSource>,
    pub operators: Vec<Box<dyn PhysicalOperator>>,
    pub sink: Box<dyn PhysicalSink>,
    /// List of Pipeline IDs that MUST execute and complete before this pipeline can run
    pub dependencies: Vec<usize>,
    /// Number of concurrent partitions (threads) to execute this pipeline on
    pub partitions: usize,
}

impl Pipeline {
    pub fn add_dependency(&mut self, id: usize) {
        self.dependencies.push(id);
    }
}

/// Represents a single thread running a pipeline for a specific data partition.
/// It implements the standard push-based execution loop.
pub struct PipelineTask {
    pub pipeline: Arc<Pipeline>,
    pub partition_id: usize,
}

impl PipelineTask {
    /// Primary execution loop. Drives data from Source -> Operator Chain -> Sink.
    pub fn execute(&self) -> Result<()> {
        // TODO: Push loop: fetch batch, transform in-place, push to sink
        todo!("implement me!")
    }
}
