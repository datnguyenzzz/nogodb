use std::{any::Any, sync::Arc};

use anyhow::Result;

use crate::{
    arrow::{Buffer, RecordBatch, SchemaRef},
    execution::scan::ScanSource,
};

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

pub enum CombineResult {
    Materialised(RecordBatch),
    Empty,
}

pub struct SinkContext {
    pub core_id: usize,
    pub pipeline_id: PipelineID,
}

pub struct OperatorContext {
    pub core_id: usize,
    pub pipeline_id: PipelineID,
}

pub trait PhysicalOperator: Send + Sync {
    /// Performs in-place vectorized transformations
    fn execute(&self, ctx: &OperatorContext, input: &RecordBatch) -> Result<Option<RecordBatch>>;
    fn as_any(&self) -> &dyn Any;
}

pub trait PhysicalSink: Send + Sync {
    /// Consumes batches then accumulates to thread-local states
    fn sink(&self, ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult>;
    /// Combines thread-local partition states into a finalized global
    /// state once all threads finish
    fn combine(&self) -> Result<CombineResult>;
    fn as_any(&self) -> &dyn Any;
}

// Note: We need it here plan_builder/mod.rs/PhysicalHashJoinNode/build(...):139
impl<T: PhysicalSink + ?Sized + 'static> PhysicalSink for Arc<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn combine(&self) -> Result<CombineResult> {
        (**self).combine()
    }
    fn sink(&self, ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult> {
        (**self).sink(ctx, input)
    }
}

pub type PipelineID = usize;

/// A linear pipeline of execution: Source -> [Operators...] -> Sink
pub struct Pipeline {
    pub id: PipelineID,
    pub source: Option<Arc<ScanSource>>,
    pub downstream_id: Option<PipelineID>,
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

pub enum PipelineEvent {
    PipelineFinished(PipelineID),
    AllDone,
}
