use anyhow::Result;

use crate::arrow::RecordBatch;

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

/// Represents a NUMA-aware dynamic execution block of roughly 100,000 rows.
pub struct Morsel {
    /// Starting row offset in the physical table
    pub start_row: usize,
    /// Number of active logical rows in this block (typically 100,000)
    pub num_rows: usize,
    /// The physical NUMA socket where this block's memory resides
    pub numa_node: usize,
}

pub trait PhysicalSource: Send + Sync {
    /// Dynamically pulls the next available Morsel from this source.
    /// Prefers returning a Morsel local to the caller's `worker_numa_node` (NUMA-locality).
    /// Returns `None` when all data in the table has been fully claimed.
    fn next_morsel(&self, worker_numa_node: usize) -> Result<Option<Morsel>>;
    /// Pulls a single 2048-row vectorized chunk from a specific, active Morsel.
    fn get_chunk(&self, morsel: &Morsel, batch_offset: usize) -> Result<Option<RecordBatch>>;
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
    pub source: Box<dyn PhysicalSource>,
    pub operators: Vec<Box<dyn PhysicalOperator>>,
    pub sink: Box<dyn PhysicalSink>,
    /// List of Pipeline IDs that MUST execute and complete before this pipeline can run
    pub dependencies: Vec<usize>,
    /// Number of concurrent partitions (morsels) inside this pipeline
    pub partitions: usize,
}

impl Pipeline {
    pub fn add_dependency(&mut self, id: usize) {
        self.dependencies.push(id);
    }
}
