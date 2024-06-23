use crate::common::BrokerContextType;

#[derive(Debug, Clone)]
pub(crate) struct AktonInner {
    pub(crate) broker: BrokerContextType,
}