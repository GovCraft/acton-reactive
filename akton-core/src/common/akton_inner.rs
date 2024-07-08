use crate::common::BrokerRef;

#[derive(Debug, Clone, Default)]
pub(crate) struct AktonInner {
    pub(crate) broker: BrokerRef,
}