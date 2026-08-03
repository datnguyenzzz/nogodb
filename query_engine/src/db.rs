use std::sync::Arc;

use crate::{catalog::provider::CatalogProvider, execution::scheduler::Scheduler};

pub struct Database {
    pub scheduler: Arc<Scheduler>,
    pub catalog_provider: Arc<CatalogProvider>,
}

impl Database {
    pub fn new(server_address: String) -> Self {
        //  let catalog_client = Arc::new(GrpcCatalogClient::new(server_address));
        // let catalog_provider = CatalogProvider::new(catalog_client);
        todo!("implement me")
    }
}
