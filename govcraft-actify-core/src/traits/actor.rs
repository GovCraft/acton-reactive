use std::fmt::Debug;
use async_trait::{async_trait as govcraft_async};
use tracing::{instrument, Level};
use crate::messages::{SupervisorMessage};

#[govcraft_async]
pub trait GovcraftActor: Debug {
    type T: Send + 'static;
    async fn handle_message(&mut self, message: Self::T) -> anyhow::Result<()>;
    async fn handle_message_internal(&mut self, message: Self::T) -> anyhow::Result<()> {
        self.handle_message(message).await?;
        Ok(())
    }
    #[instrument(skip(self),level="trace")]
    async fn handle_supervisor_message_internal(&mut self, message: SupervisorMessage) -> anyhow::Result<()> {
        self.handle_supervisor_message(&message).await?;
        match message {
            SupervisorMessage::Shutdown => {
                tracing::event!(Level::TRACE, msg=?message);
                self.on_shutdown_request().await?;
                self.on_shutting_down().await?;
            }
            _ => {}
        }
        Ok(())
    }
    #[instrument(skip(self),level="trace")]
    async fn handle_supervisor_message(&mut self, _message: &SupervisorMessage) -> anyhow::Result<()> {
        Ok(())
    }
    #[instrument(skip(self),level="trace")]
    async fn pre_start(&mut self) -> anyhow::Result<()> {
        tracing::trace!("*");
        Ok(())
    }
    #[instrument(skip(self),level="trace")]
    async fn on_start(&mut self) -> anyhow::Result<()> {
        tracing::trace!("*");
        Ok(())
    }
    #[instrument(skip(self),level="trace")]
    async fn post_start(&mut self) -> anyhow::Result<()> {
        tracing::trace!("*");
        Ok(())
    }

    #[instrument(skip(self),level="trace")]
    async fn on_shutdown_request(&mut self) -> anyhow::Result<()> {
        tracing::trace!("*");
        Ok(())
    }

    #[instrument(skip(self),level="trace")]
    async fn on_shutting_down(&mut self) -> anyhow::Result<()> {
        tracing::trace!("*");
        Ok(())
    }

    #[instrument(skip(self),level="trace")]
    async fn post_shutdown(&mut self) -> anyhow::Result<()> {
        tracing::trace!("*");
        Ok(())
    }
}
