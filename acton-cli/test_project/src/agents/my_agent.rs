use acton::prelude::*;
use crate::messages::InitProject;
use tracing::*;

#[acton_agent]
struct MyAgent;

impl MyAgent {
pub async fn new(runtime: &mut AgentRuntime) -> AgentHandle {
let mut agent = runtime.new_agent::
<MyAgent>().await;
    agent.act_on::
    <MyMessage>(|agent, context| {
        info!("MyMessage received by MyAgent.");

        AgentReply::immediate()
        });

        agent.start().await
        }
        }