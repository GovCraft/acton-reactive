use std::any::Any;
use std::future::Future;
use async_trait::async_trait;
use tracing::*;
use crate::message::BrokerRequestEnvelope;
use crate::prelude::{ActorContext, AktonMessage};

#[async_trait]
pub(crate) trait BrokerContext: ActorContext {
    #[instrument(skip(self), fields(children = self.children().len()))]
    fn broker_emit_async(
        &self,
        broker_request_envelope: BrokerRequestEnvelope,
        pool_name: Option<&str>,
    ) -> impl Future<Output=()> + Send + Sync + '_
    where
        Self: Sync,
    {
        let pool_name = {
            if let Some(pool_id) = pool_name {
                Some(String::from(pool_id))
            } else {
                None
            }
        };
        async move {
            let envelope = self.return_address();
            let type_id= broker_request_envelope.type_id();
            trace!("BEFORE: {type_id:?}");
            let message = broker_request_envelope.message;
            let type_id= message.type_id();
            trace!("AFTER: {type_id:?}");
            envelope.reply_async(message, pool_name).await;
        }
    }
}