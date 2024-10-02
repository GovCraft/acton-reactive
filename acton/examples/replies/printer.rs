/*
 * Copyright (c) 2024. Govcraft
 *
 * Licensed under either of
 *   * Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *   * MIT license: http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the applicable License for the specific language governing permissions and
 * limitations under that License.
 */

use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display};
use std::io::{stderr, stdout, Write};
use std::ops::Deref;

use crossterm::{cursor, ExecutableCommand, execute, queue, style::Print, terminal};
use crossterm::terminal::{BeginSynchronizedUpdate, Clear, ClearType, EndSynchronizedUpdate};
use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use mti::prelude::MagicTypeId;
use tokio::io::AsyncWriteExt;
use tracing::*;
use tracing::field::debug;

use acton_core::prelude::*;
use acton_macro::acton_actor;

use crate::{PriceRequest, PriceResponse, PrinterMessage};
use crate::cart_item::{CartItem, Price};

const COLS: u16 = 40;
const LEFT_PAD: u16 = 3;
const MARGIN_TOP: u16 = 4;
const PAD_TOP: u16 = 2;
const PAD_BOTTOM: u16 = 4;
const HEADER_HEIGHT: u16 = 1;
const STATUS_HEIGHT: u16 = 5;
const POWER_ON: &str = "Printer powering on...";
const STARTED: &str = "\u{25CF}";
const LOADING: &str = "loading";
const HELP_TEXT: &str = "q: quit, ?: help";

#[acton_actor]
pub struct Printer {
    current_line: u16,
    height: u16,
    status: String,
    items: DashMap<MagicTypeId, DisplayItem>,
}
#[derive(Clone, Debug, Default)]
enum DisplayItem {
    Item(CartItem),
    Loader(String),
    #[default]
    Startup,
}

impl fmt::Display for DisplayItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DisplayItem::Item(item) => write!(
                f,
                "{}({}) @ {} {:^3} {:>5}",
                item.name(),
                item.quantity(),
                MoneyFmt(**item.cost()),
                "│",
                MoneyFmt(item.price().0)
            ),
            DisplayItem::Loader(what) => write!(
                f,
                "{}( ) @ {} {:^3} {:>5}",
                what,
                MoneyFmt(0),
                "│",
                MoneyFmt(0),
            ),
            DisplayItem::Startup => write!(f, ""),
        }
    }
}


#[derive(Clone, Debug, Default)]
struct MoneyFmt(i32);

impl Display for MoneyFmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cents = self.0;
        if cents <= 0 {
            write!(f, "${: >3}{:>3}", "", "-")?;
        } else {
            write!(f, "${: >3}.{:0>2}", cents / 100, cents % 100)?;
        }
        Ok(())
    }
}

const SPINNER_FPS: u64 = 1;

impl Printer {
    pub async fn power_on(app: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        let mut agent = app.new_agent_with_name::<Printer>("printer".to_string()).await;
        trace!("Agent:{}", agent.name());
        agent
            .act_on::<PriceRequest>(|agent, context| {
                let item = context.message().0.clone();
                let printer = agent.model.clone(); // No need for &mut since DashMap handles interior mutability
                trace!("PriceRequest: {}", &item.name());
                agent.model.items.insert(item.id().clone(), DisplayItem::Loader(item.name().to_string()));
                agent.model.status = LOADING.to_string();
                let handle = agent.handle().clone();
                let printer = agent.model.clone();
                AgentReply::from_async(async move {
                    let _ = Self::repaint(printer);
                }
                )
            })
            .act_on::<PriceResponse>(|agent, context| {
                let printer = &mut agent.model; // No need for &mut since DashMap handles interior mutability
                let item = context.message().item.clone();
                trace!("PriceResponse: {}", item.id());

                // if an item with the same item.id() exists in the printer.items, replace it with the new item by removing it first
                if let Some((old_item, item)) = printer.items.remove(item.id().as_ref()) {
                    warn!("old item removed: {:?}", old_item.as_ref());
                }
                //then insert it
                printer.items.insert(item.id().clone(), DisplayItem::Item(item.clone()));
                trace!("item replaced: {:?}", item.id().as_ref());
                agent.model.status = STARTED.to_string();

                let handle = agent.handle().clone();
                let _ = Self::repaint(agent.model.clone());
                AgentReply::immediate()
            })
            .act_on::<PrinterMessage>(|agent, _context| {
                let handle = agent.handle().clone();
                let _ = Self::repaint(agent.model.clone());
                AgentReply::immediate()
            })
            .after_start(|agent| {
                let mut stdout = stdout();
                let _ = execute!(stdout, cursor::Hide);
                let handle = agent.handle().clone();
                let _ = Self::repaint(agent.model.clone());
                AgentReply::immediate()
            });
        agent.model.status = STARTED.to_string();
        agent.model.height = MARGIN_TOP;
        agent.model.current_line = MARGIN_TOP;

        agent.handle().subscribe::<PriceRequest>().await;
        agent.handle().subscribe::<PriceResponse>().await;
        agent.handle().subscribe::<PrinterMessage>().await;
        Ok(agent.start().await)
    }

    fn repaint(printer: Printer) -> anyhow::Result<()> {
        Self::print_status(&printer)?;
        Self::print_header(&printer)?;
        Self::print_receipt(&printer)?;
        Ok(())
    }

    fn print_header(printer: &Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;
        let top = MARGIN_TOP;

        // Create the horizontal line with intersections ├ and ┤
        let text = format!(
            "{}{}{}",
            "─".repeat((COLS - 12) as usize),
            "┬",
            "─".repeat(12)
        );

        // Position the cursor at LEFT_PAD to ensure the line is correctly aligned
        queue!(stdout, cursor::MoveTo(LEFT_PAD, top))?;
        stdout.write(text.as_bytes())?;
        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }

    // #[instrument(skip(printer), level = "trace")]
    fn print_receipt(printer: &Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;
        let top = MARGIN_TOP + HEADER_HEIGHT;
        // Iterate through all items and print them
        for (i, item) in printer.items.iter().enumerate() {
            let line_item = format!("{}", item.value()); // Use Display trait

            // Calculate the exact position to move the cursor to align right
            let start_col = (LEFT_PAD + COLS).saturating_sub(line_item.len() as u16);

            // Ensure the cursor moves to the correct position for right alignment
            queue!(stdout, cursor::MoveTo(start_col, top + i as u16))?;
            queue!(stdout, Clear(ClearType::CurrentLine))?;
            stdout.write(line_item.as_bytes())?;
            queue!(stdout, cursor::MoveTo(0, 0))?;
        }

        Self::print_subtotal(printer)?;
        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }


    fn print_subtotal(printer: &Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();

        let top = MARGIN_TOP + printer.items.len() as u16;

        // Create the horizontal line with intersections ├ and ┤
        let text = format!(
            "{}{}{}",
            "─".repeat((COLS - 12) as usize),
            "┴",
            "─".repeat(12)
        );

        // Position the cursor at LEFT_PAD to ensure the line is correctly aligned
        queue!(stdout, cursor::MoveTo(LEFT_PAD, top))?;
        queue!(stdout, Clear(ClearType::CurrentLine))?;
        stdout.write(text.as_bytes())?;

        // Sum the price of all items in the printer (only counting CartItems)
        let subtotal = printer.items.iter().map(|kv| match kv.value() {
            DisplayItem::Item(item) => item.price().0,
            _ => 0,
        }).sum::<i32>();

        let total_str = format!("{:<11}{}", "Subtotal", MoneyFmt(subtotal));
        let start_col = LEFT_PAD + COLS - total_str.len() as u16 - 2;

        // Move to the correct position for the subtotal line, right-aligned
        queue!(stdout, cursor::MoveTo(start_col, top + 1))?;
        stdout.write(total_str.as_bytes())?;

        Ok(())
    }

    fn print_status(printer: &Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();
        // execute!(stdout, BeginSynchronizedUpdate)?;
        queue!(stdout, cursor::MoveTo((LEFT_PAD + COLS).saturating_sub(printer.items.len() as u16), 1));
        queue!(stdout, Clear(ClearType::CurrentLine))?;
        stdout.write(STARTED.as_ref())?;

        stdout.flush()?;
        // execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }

    fn print_help(printer: &mut Printer, text: &str) -> anyhow::Result<()> {
        let mut stdout = stdout();
        // execute!(stdout, BeginSynchronizedUpdate)?;

        queue!(stdout, cursor::MoveTo((LEFT_PAD + COLS).saturating_sub(text.len() as u16), printer.current_line + PAD_BOTTOM - 1));
        execute!(stdout, Clear(ClearType::CurrentLine)).expect("clear failed");
        queue!(stdout,  cursor::MoveTo(LEFT_PAD, printer.current_line + PAD_BOTTOM));

        stdout.write(text.as_ref()).expect("write failed");

        // execute!(stdout, EndSynchronizedUpdate)?;

        Ok(())
    }
}
