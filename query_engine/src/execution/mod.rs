pub mod dispatcher;
pub mod numa_topology;
pub mod physical_plan;
pub mod pipeline;
pub mod scheduler;

/// Represents a NUMA-aware dynamic execution block of roughly [`MORSEL_SIZE`] rows.
#[derive(Clone)]
pub struct Morsel {
    pub start_row: usize,
    pub num_rows: usize,
    pub numa_node: usize,
}
