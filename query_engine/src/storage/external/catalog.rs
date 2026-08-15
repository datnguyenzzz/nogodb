/// Concrete implementation of `catalog::CatalogClient` that communicates with the
/// object storage/metadata layer via gRPC.
pub struct GrpcCatalogClient {
    server_address: String,
}

impl GrpcCatalogClient {
    pub fn new(server_address: String) -> Self {
        GrpcCatalogClient { server_address }
    }
}

// implement Storage Engine trait
