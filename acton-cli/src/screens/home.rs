use std::fmt::Display;
use std::io::{stdout, Write};

use acton_reactive::prelude::*;
use ansi_term::Color::RGB;
use ansi_term::Style;
use crossterm::{cursor, queue};
use tracing::*;

use crate::{PADLEFT, PADTOP, TAGLINE, TITLE};
use crate::messages::{MenuMoveDown, MenuMoveUp, MenuSelect};

#[acton_actor]
pub(crate) struct HomeScreen {
    menu: Vec<MenuItem>,
    is_visible: bool,
}

#[derive(Clone)]
struct MenuItem {
    label: String,
    action: String,
    selected: bool,
}

impl Display for MenuItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let selected = Style::new().fg(RGB(255, 141, 204));
        let selected_text = Style::new().fg(RGB(253, 209, 234));
        let normal = Style::new().fg(RGB(96, 96, 96));
        if self.selected {
            write!(f, "{} {:<2}", selected.paint(">"), format!("{}", selected_text.paint(self.label.clone())))
        } else {
            write!(f, "{} {:<2}", " ", format!("{}", normal.paint(self.label.clone())))
        }
    }
}

impl HomeScreen {
    pub async fn new(runtime: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        let mut agent = runtime.new_agent::<HomeScreen>().await;

        agent.before_start(|_agent| {
            let mut stdout = stdout();
            queue!(stdout, cursor::MoveTo(PADLEFT, PADTOP)).unwrap();
            let _ = stdout.write(TITLE.as_ref());
            queue!(stdout, cursor::MoveTo(PADLEFT, PADTOP + 1)).unwrap();
            let _ = stdout.write(TAGLINE.as_ref());
            let _ = stdout.flush();
            AgentReply::immediate()
        })
            .act_on::<MenuMoveDown>(|agent, _context| {
                trace!(" MenuMoveDown");
                let len = agent.model.menu.len();
                if let Some(current_index) = agent.model.menu.iter().position(|item| item.selected) {
                    agent.model.menu[current_index].selected = false;
                    let next_index = (current_index + 1) % len;
                    agent.model.menu[next_index].selected = true;
                }
                agent.model.paint();
                AgentReply::immediate()
            })
            .act_on::<MenuMoveUp>(|agent, _context| {
                trace!(" MenuMoveUp");
                let len = agent.model.menu.len();
                if let Some(current_index) = agent.model.menu.iter().position(|item| item.selected) {
                    agent.model.menu[current_index].selected = false;
                    let prev_index = if current_index == 0 { len - 1 } else { current_index - 1 };
                    agent.model.menu.iter_mut().rev().nth(len - prev_index - 1).unwrap().selected = true;
                }
                agent.model.paint();
                AgentReply::immediate()
            })
            .act_on::<MenuSelect>(|agent, context| {
                //use debug to print the selected menu item label
                debug!(" MenuSelect: {:?}", agent.model.menu.iter().find(|item| item.selected).unwrap().label);
                AgentReply::immediate()
            })
            .after_start(|agent| {
                agent.model.paint();
                AgentReply::immediate()
            });
        agent.model.menu.push(MenuItem { label: "Create a new project".to_string(), action: "create_project".to_string(), selected: true });
        agent.model.menu.push(MenuItem { label: "Update an existing project".to_string(), action: "open_project".to_string(), selected: false });

        Ok(agent.start().await)
    }

    fn paint(&self) {
        let mut stdout = stdout();
        queue!(stdout, cursor::MoveTo(PADLEFT, PADTOP + 3)).unwrap();
        // use crossterm to output the list of menuitems
        for (i, item) in self.menu.iter().enumerate() {
            queue!(stdout, cursor::MoveTo(PADLEFT, PADTOP + 3 + i as u16)).unwrap();
            let _ = stdout.write(item.to_string().as_ref());
        }
        let _ = stdout.flush();
    }
}