use tokio::runtime::Runtime;

use crate::common::BrokerContextType;

pub(crate) struct AktonInner {
    pub(crate) runtime: Runtime,
    pub(crate) broker_pool: Vec<BrokerContextType>,
}