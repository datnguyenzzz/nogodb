/// Dynamic chunk-based Storage Reader that communicates with the
/// remote storage node via gRPC streaming.
pub struct GrpcStorageClient {
    server_address: String,
}

impl GrpcStorageClient {
    pub fn new(server_address: String) -> Self {
        GrpcStorageClient { server_address }
    }
}

// implement StorageData engine trait
