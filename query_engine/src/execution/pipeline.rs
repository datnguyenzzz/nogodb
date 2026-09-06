use anyhow::Result;

use crate::arrow::{Buffer, RecordBatch, SchemaRef};

/// Represents the push-stream data payload received from the DataStorage layer
#[derive(Clone)]
pub enum ScanMessage {
    /// Used for in-memory temporary tables, CTEs, or simply mock testing
    Batch(RecordBatch),
    CompressedPage {
        data: Buffer,
        meta: Buffer,
    },
}

pub enum SinkResult {
    NeedMoreInput,
    Finished,
}

pub struct SinkContext {
    pub core_id: usize,
}

pub trait PhysicalOperator: Send + Sync {
    /// Performs in-place vectorized transformations
    fn execute(&self, input: &RecordBatch) -> Result<Option<RecordBatch>>;
}

pub trait PhysicalSink: Send + Sync {
    /// Consumes batches then accumulates to thread-local states
    fn sink(&self, ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult>;
    /// Combines thread-local partition states into a finalized global
    /// state once all threads finish
    fn combine(&self) -> Result<()>;
}

pub type PipelineID = usize;

/// A linear pipeline of execution: Source -> [Operators...] -> Sink
pub struct Pipeline {
    pub id: PipelineID,
    pub operators: Vec<Box<dyn PhysicalOperator>>,
    pub sink: Box<dyn PhysicalSink>,
    pub dependencies: Vec<PipelineID>,
    /// Number of concurrent partitions (morsels) inside this pipeline
    pub partitions: usize,
    pub schema: SchemaRef,
}

impl Pipeline {
    pub fn add_dependency(&mut self, id: usize) {
        self.dependencies.push(id);
    }
}
