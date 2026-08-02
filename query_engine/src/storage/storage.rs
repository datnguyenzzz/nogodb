/// Dynamic chunk-based Storage Reader that communicates with the
/// remote storage node via gRPC streaming.
pub struct GrpcStorageClient {
    server_address: String,
}

impl GrpcStorageClient {
    pub fn new(server_address: String) -> Self {
        GrpcStorageClient { server_address }
    }

    /// Simulates gRPC streaming fetch of physical 2048-row Arrow chunks for a table partition.
    // pub fn scan_table(&self, table_name: &str, projections: &[String]) -> Resu
}