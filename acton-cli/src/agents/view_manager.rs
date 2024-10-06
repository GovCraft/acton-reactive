use std::collections::HashMap;

use acton_reactive::prelude::*;

use crate::messages::{MenuMoveDown, MenuMoveUp, MenuSelect};
use crate::screens::{HomeScreen, NewScreen};

#[acton_actor]
pub struct ViewManager {
    screens: HashMap<Ern, AgentHandle>,
    active: AgentHandle,
}

impl ViewManager {
    pub async fn new(runtime: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        let mut agent = runtime.new_agent::<ViewManager>().await;
        let mut runtime = agent.runtime().clone();
        agent
            .act_on::<MenuMoveUp>(|agent, context| {
                let view = agent.model.active.clone();
                let msg = context.message().clone();
                AgentReply::from_async(async move {
                    view.send(msg).await;
                })
            })
            .act_on::<MenuMoveDown>(|agent, context| {
                let view = agent.model.active.clone();
                let msg = context.message().clone();
                AgentReply::from_async(async move {
                    view.send(msg).await;
                })
            })
            .act_on::<MenuSelect>(|agent, context| {
                let view = agent.model.active.clone();
                let msg = context.message().clone();
                AgentReply::from_async(async move {
                    view.send(msg).await;
                })
            });
        let home_screen = HomeScreen::new(&mut runtime).await?;
        agent.model.screens.insert(home_screen.id(), home_screen);

        let new_screen = NewScreen::new(&mut runtime).await?;
        agent.model.screens.insert(new_screen.id(), new_screen);

        // agent.model.active = agent.model.screens[0].clone();

        agent.broker().subscribe::<MenuMoveUp>().await;
        agent.broker().subscribe::<MenuMoveDown>().await;
        Ok(agent.start().await)
    }
}