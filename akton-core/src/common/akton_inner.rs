use crate::common::BrokerContext;

#[derive(Debug, Clone, Default)]
pub(crate) struct AktonInner {
    pub(crate) broker: BrokerContext,
}