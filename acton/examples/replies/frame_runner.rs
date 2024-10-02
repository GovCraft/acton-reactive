use std::time::Duration;

use mti::prelude::MagicTypeId;
use tokio::time::interval;
use tracing::*;

use acton_core::prelude::*;
use acton_macro::acton_message;

#[derive(Default, Debug, Clone)]
pub struct FrameRunner {
    item_id: MagicTypeId,
    spinner_sequence: Vec<char>,
    fps: u64,
}

const SPINNER_SEQUENCE: [char; 4] = ['|', '/', '-', '\\'];

impl FrameRunner {
    pub async fn new(item_id: MagicTypeId, fps: u64, mut app: &mut AgentRuntime) -> anyhow::Result<ManagedAgent<Idle, FrameRunner>> {
        let config = ActorConfig::new_with_name("frame_runner")?;
        let mut agent = app.create_actor_with_config::<FrameRunner>(config).await;
        agent.act_on::<StartSpinning>(move |agent, context| {
            let item_name = agent.model.item_id.clone();
            let mut interval = interval(Duration::from_secs_f64(1.0 / fps as f64));
            let mut index = 0;

            let envelope = context.reply_envelope();
            Box::pin(async move {
                loop {
                    let envelope = envelope.clone();
                    interval.tick().await;
                    // debug!("Tick: {}", index);

                    // Create and send the SpinnerUpdate message to the Printer actor
                    let spinner_char = SPINNER_SEQUENCE[index];
                    index = (index + 1) % SPINNER_SEQUENCE.len();
                    let _ = envelope.reply(SpinnerUpdate {
                        item_id: item_name.clone(),
                        spinner_char,
                    });
                }
            })
        })
            .before_stop(|agent| {
                warn!("FrameRunner stopped fir item_id: {}", agent.model.item_id);
                AgentReply::immediate()
            });
        agent.model.fps = fps;
        agent.model.item_id = item_id.parse()?;
        Ok(agent)
    }
}

#[acton_message]
pub struct NewSpinner(pub(crate) MagicTypeId);

#[acton_message]
pub struct StartSpinning;

#[acton_message]
pub struct StopSpinning;

#[acton_message]
pub struct SpinnerUpdate {
    pub(crate) item_id: MagicTypeId,
    pub(crate) spinner_char: char,
}
