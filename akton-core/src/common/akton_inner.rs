use crate::common::BrokerContext;

#[derive(Debug, Clone)]
pub(crate) struct AktonInner {
    pub(crate) broker: BrokerContext,
}