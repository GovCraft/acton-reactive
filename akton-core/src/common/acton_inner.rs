use crate::common::BrokerRef;

#[derive(Debug, Clone, Default)]
pub(crate) struct ActonInner {
    pub(crate) broker: BrokerRef,
}