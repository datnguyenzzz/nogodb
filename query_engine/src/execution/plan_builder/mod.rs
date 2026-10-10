pub mod builder;

use std::sync::Arc;

use anyhow::{Result, anyhow};

use crate::{
    arrow::SchemaRef,
    execution::{
        PhysicalExpr,
        filters::PhysicalFilter,
        hash_join::operators::{PhysicalBuildSink, PhysicalProbeOperator},
        limit::{PhysicalLimitOperator, PhysicalLimitSink},
        pipeline::PipelineID,
        plan_builder::builder::PlanBuilder,
        scan::ScanSource,
        sort::{PhysicalSortSink, SortColumn},
    },
    planner::{DynamicJoinPruner, JoinType},
};

/// A physical plan is a tree of physical operators, where will be translate to a physical execution plan
/// which form a DAG, every node in the DAG must know how to translate itself into execution pipelines via
/// `build()` hook
pub trait PhysicalPlanNode {
    /// Recursively builds pipelines, linking each pipeline to its downstream consumer
    /// [`downstream_id`] is None for the root query, and Some(id) for children pipeline
    fn build(&self, builder: &mut PlanBuilder, downstream_id: Option<PipelineID>) -> Result<()>;
    fn schema(&self) -> SchemaRef;
}

pub struct PhysicalScanNode {
    pub table_name: String,
    pub schema: SchemaRef,
    pub pruners: Vec<DynamicJoinPruner>,
    pub projections: Option<Vec<String>>,
}

impl PhysicalPlanNode for PhysicalScanNode {
    fn build(&self, builder: &mut PlanBuilder, _downstream_id: Option<PipelineID>) -> Result<()> {
        let source = ScanSource::new(
            self.table_name.clone(),
            self.schema.clone(),
            builder.storage.clone(),
            self.pruners.clone(),
            self.projections.clone(),
        );
        builder.current_source = Some(Arc::new(source));
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct PhysicalFilterNode {
    pub predicate: Arc<dyn PhysicalExpr>,
    pub input: Box<dyn PhysicalPlanNode>,
}

impl PhysicalPlanNode for PhysicalFilterNode {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn build(&self, builder: &mut PlanBuilder, downstream_id: Option<PipelineID>) -> Result<()> {
        self.input.build(builder, downstream_id)?;
        let filter_op = Box::new(PhysicalFilter::new(self.predicate.clone()));
        builder.operators.push(filter_op);
        Ok(())
    }
}

/// Physical HashJoin Node (Smaller Side = Build, Larger Side = Probe)
pub struct PhysicalHashJoinNode {
    pub left: Box<dyn PhysicalPlanNode>,
    pub right: Box<dyn PhysicalPlanNode>,
    pub on: Vec<(String, String)>,
    pub join_type: JoinType,
    pub schema: SchemaRef,
    pub left_is_build: bool,
}

impl PhysicalPlanNode for PhysicalHashJoinNode {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn build(&self, builder: &mut PlanBuilder, downstream_id: Option<PipelineID>) -> Result<()> {
        let (build_child, probe_child, build_keys, probe_keys) = if self.left_is_build {
            let b_keys: Vec<&str> = self.on.iter().map(|(l, _)| l.as_str()).collect();
            let p_keys: Vec<&str> = self.on.iter().map(|(_, r)| r.as_str()).collect();
            (&self.left, &self.right, b_keys, p_keys)
        } else {
            let b_keys: Vec<&str> = self.on.iter().map(|(_, r)| r.as_str()).collect();
            let p_keys: Vec<&str> = self.on.iter().map(|(l, _)| l.as_str()).collect();
            (&self.right, &self.left, b_keys, p_keys)
        };

        let build_schema = build_child.schema();
        let probe_schema = probe_child.schema();

        let build_col_indexes: Vec<usize> = build_keys
            .iter()
            .map(|name| {
                build_schema
                    .fields()
                    .iter()
                    .position(|f| f.name == *name || f.name.ends_with(&format!(".{}", name)))
                    .ok_or_else(|| anyhow!("Join key '{}' not found in build schema", name))
            })
            .collect::<Result<Vec<usize>>>()?;

        let probe_col_indexes: Vec<usize> = probe_keys
            .iter()
            .map(|name| {
                probe_schema
                    .fields()
                    .iter()
                    .position(|f| f.name == *name || f.name.ends_with(&format!(".{}", name)))
                    .ok_or_else(|| anyhow!("Join key '{}' not found in probe schema", name))
            })
            .collect::<Result<Vec<usize>>>()?;

        let build_pipeline_id = builder.next_id();
        let join_id = build_pipeline_id;
        let build_sink = Arc::new(PhysicalBuildSink::new(
            join_id,
            build_col_indexes.clone(),
            builder.total_cores,
            builder.dispatcher.clone(),
            builder.mailboxes.clone(),
        ));

        let saved_operators = std::mem::take(&mut builder.operators);
        let saved_source = builder.current_source.take();
        let saved_deps = std::mem::take(&mut builder.current_deps);

        build_child.build(builder, None)?;
        builder.emit_pipeline(
            Some(build_pipeline_id),
            Box::new(build_sink.clone()),
            build_schema,
            None,
        );

        builder.operators = saved_operators;
        builder.current_source = saved_source;
        builder.current_deps = saved_deps;

        // The Probe pipeline must wait for the Build pipeline to finish
        builder.current_deps.push(build_pipeline_id);

        probe_child.build(builder, downstream_id)?;
        let probe_op = PhysicalProbeOperator::new(
            builder.total_cores,
            probe_col_indexes,
            builder.mailboxes.clone(),
            build_sink,
            build_col_indexes,
        );
        builder.operators.push(Box::new(probe_op));

        Ok(())
    }
}

pub struct PhysicalSortNode {
    pub sort_columns: Vec<SortColumn>,
    /// ((offset, limit))
    pub limit: Option<(usize, usize)>,
    pub input: Box<dyn PhysicalPlanNode>,
    pub schema: SchemaRef,
}

impl PhysicalPlanNode for PhysicalSortNode {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn build(&self, builder: &mut PlanBuilder, downstream_id: Option<PipelineID>) -> Result<()> {
        let sink_pid = builder.next_id();
        self.input.build(builder, Some(sink_pid))?;

        let (skip, fetch) = match self.limit {
            Some((offset, limit)) => {
                let limit_op = PhysicalLimitOperator::new(
                    offset,
                    limit,
                    true,
                    builder.total_cores,
                    builder.dispatcher.clone(),
                );
                builder.operators.push(Box::new(limit_op));
                (offset, Some(limit))
            }
            None => (0, None),
        };

        let sort_sink =
            PhysicalSortSink::new(self.sort_columns.clone(), skip, fetch, builder.total_cores);
        builder.emit_pipeline(
            Some(sink_pid),
            Box::new(sort_sink),
            self.schema(),
            downstream_id,
        );
        Ok(())
    }
}

/// For LIMIT operation without ORDER BY
pub struct PhysicalLimitNode {
    pub skip: usize,
    pub fetch: usize,
    pub input: Box<dyn PhysicalPlanNode>,
    pub schema: SchemaRef,
}

impl PhysicalPlanNode for PhysicalLimitNode {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn build(&self, builder: &mut PlanBuilder, downstream_id: Option<PipelineID>) -> Result<()> {
        let limit_pid = builder.next_id();
        self.input.build(builder, Some(limit_pid))?;

        let op = PhysicalLimitOperator::new(
            self.skip,
            self.fetch,
            false,
            builder.total_cores,
            builder.dispatcher.clone(),
        );
        let sink = PhysicalLimitSink::new(self.skip, self.fetch);
        builder.operators.push(Box::new(op));
        builder.emit_pipeline(
            Some(limit_pid),
            Box::new(sink),
            self.schema(),
            downstream_id,
        );

        Ok(())
    }
}
