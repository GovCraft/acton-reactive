use std::fmt::Display;
use std::io::{stdout, Write};

use acton_reactive::prelude::*;
use ansi_term::Color::RGB;
use ansi_term::Style;
use crossterm::{cursor, queue};
use tracing::*;

use crate::{PADLEFT, PADTOP, TAGLINE, TITLE};
use crate::messages::{MenuMoveDown, MenuMoveUp};

#[acton_actor]
pub(crate) struct NewScreen {
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
        let selected = Style::new().fg(RGB(255,141,204));
        let selected_text = Style::new().fg(RGB(253,209,234));
        let normal = Style::new().fg(RGB(96,96,96));
        if self.selected {
            write!(f, "{} {:<2}", selected.paint(">"), format!("{}", selected_text.paint(self.label.clone())))
        } else {
            write!(f, "{} {:<2}", " ", format!("{}", normal.paint(self.label.clone())))
        }
    }
}

impl NewScreen {
    pub async fn new(runtime: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        let mut agent = runtime.new_agent::<NewScreen>().await;

        agent.before_start(|_agent| {
            let mut stdout = stdout();
            queue!(stdout, cursor::MoveTo(PADLEFT, PADTOP)).unwrap();
            let _ = stdout.write(TITLE.as_ref());
            queue!(stdout, cursor::MoveTo(PADLEFT, PADTOP + 1)).unwrap();
            let _ = stdout.write("Scaffold a new application".as_ref());
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
                agent.model.Paint();
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
                agent.model.Paint();
                AgentReply::immediate()
            })
            .after_start(|agent| {
                agent.model.Paint();
                AgentReply::immediate()
            });
        agent.model.menu.push(MenuItem { label: "Name".to_string(), action: "create_project".to_string(), selected: true });
        agent.model.menu.push(MenuItem { label: "Location".to_string(), action: "open_project".to_string(), selected: false });

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