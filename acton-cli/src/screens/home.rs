use std::io::{stdout, Write};
use acton_reactive::prelude::*;
use crossterm::{cursor, queue};
use crate::{PADLEFT, PADTOP};

#[acton_actor]
pub(crate) struct HomeScreen {
    screens: Vec<AgentHandle>,
}

impl HomeScreen {
    pub async fn new(runtime: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        let mut agent = runtime.new_agent::<HomeScreen>().await;

        agent.before_start(|agent| {
            let mut stdout = stdout();
            queue!(stdout, cursor::MoveTo(PADLEFT, PADTOP)).unwrap();
            let _ = stdout.write("Acton Reactive Framework CLI v1.0.0-alpha.1".as_ref());
            let _ = stdout.flush();
            AgentReply::immediate()
        });
        Ok(agent.start().await)
    } }