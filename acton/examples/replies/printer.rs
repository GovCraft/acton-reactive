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
use std::fmt::Display;
use std::io::{stderr, stdout, Write};
use std::ops::Deref;

use crossterm::{cursor, ExecutableCommand, execute, queue, style::Print, terminal};
use crossterm::terminal::{BeginSynchronizedUpdate, Clear, ClearType, EndSynchronizedUpdate};
use dashmap::DashMap;
use mti::prelude::MagicTypeId;
use tokio::io::AsyncWriteExt;
use tracing::*;
use tracing::field::debug;

use acton_core::prelude::*;

use crate::{PriceResponse, PrinterMessage};
use crate::cart_item::{CartItem, Price};
use crate::frame_runner::{FrameRunner, NewSpinner, SpinnerUpdate, StartSpinning, StopSpinning};

const COLS: u16 = 40;
const LEFT_PAD: u16 = 3;
const MARGIN_TOP: u16 = 3;
const PAD_TOP: u16 = 2;
const PAD_BOTTOM: u16 = 4;
const POWER_ON: &str = "Printer powering on...";
const STARTED: &str = "\u{25CF}";
const LOADING: &str = "loading...";
const HELP_TEXT: &str = "q: quit, ?: help";

#[derive(Default, Debug, Clone)]
pub struct Printer {
    current_line: u16,
    items: DashMap<MagicTypeId, DisplayItem>,
    spinners: HashMap<MagicTypeId, bool>,
}

#[derive(Clone, Debug, Default)]
enum DisplayItem {
    Item(CartItem),
    Loader(String),
    #[default]
    Startup,
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
            .act_on::<SpinnerUpdate>(|agent, context| {
                let handle = agent.handle().clone();
                let printer = &mut agent.model;
                let update = context.message().clone();


                // Update the corresponding Loader item with the spinner character
                // if let Some(DisplayItem::Loader(ref mut loader)) = printer.items.get_mut(&update.item_id) {
                //     debug!("Loader:{}", loader);
                //     *loader = update.spinner_char.to_string();
                // }

                // Reprint the receipt to reflect the change
                // let _ = Self::print_receipt(printer, handle);
                //check what the bool value of the corresponding spinner is
                // if false, then we'll send a message to stop the spinner
                let item_id = update.item_id.clone();
                let printer = printer.clone();

                Box::pin(async move {
                    // if let Some(spinner_run_state) = printer.spinners.get(&item_id) {
                    //     if *spinner_run_state {
                    //         debug!("running {:?}", printer.spinners);
                    //     } else {
                    //         warn!("stopping {}", &update.item_id);
                    //         debug!("{:?}", printer.spinners);
                    //     }
                    // } else {
                    //     warn!("spinner not found:{:?}", &printer.spinners);
                    // };
                })

                // AgentReply::immediate()
            })
            .act_on::<PriceResponse>(|agent, context| {
                let app = agent.runtime().clone();
                let handle = agent.handle().clone();
                let printer = &agent.model; // No need for &mut since DashMap handles interior mutability
                let item = context.message().item.clone();
                trace!("PriceResponse: {}", item.id());

                // Check if this item name exists as a Loader display item, if so, convert it to a CartItem, otherwise add it as a CartItem
                if let Some(mut loader) = printer.items.get_mut(&item.id().clone()) {
                    debug!("Loader found: {}", item.name());
                    let cloned_item_name = item.name().clone();

                    match std::mem::replace(&mut *loader, DisplayItem::Startup) { // Temporarily replace the loader to avoid borrow conflict
                        DisplayItem::Loader(what) => {
                            let mut cart_item = CartItem::new(cloned_item_name, item.quantity());
                            cart_item.set_cost(*item.cost().as_ref());

                            *loader = DisplayItem::Item(cart_item);
                        }
                        _ => {
                            let mut cart_item = CartItem::new(cloned_item_name, item.quantity());
                            cart_item.set_cost(*item.cost().as_ref());

                            *loader = DisplayItem::Item(cart_item);
                        }
                    }

                    // set the spinner false
                    // if let Some(spinner) = printer.spinners.get_mut(&item.id().clone()) {
                    //     spinner.clone_from(&false);
                    // }
                } else {
                    warn!("items {:?}", printer.items);
                    let mut cart_item = CartItem::new(item.name().clone(), item.quantity());
                    cart_item.set_cost(*item.cost().as_ref());
                    printer.items.insert(item.id().clone(), DisplayItem::Item(cart_item));
                }

                // trace!("printer.items length: {}", printer.items.len());
                // let _ = Self::print_receipt(printer, handle.clone());
                let app = app.clone();
                AgentReply::immediate()
            })

            .act_on::<PrinterMessage>(|agent, context| {
                let handle = agent.handle().clone();
                let printer = &mut agent.model;
                let message = context.message().clone();
                let app = agent;
                Box::pin(async move {
                    // let _ = Self::handle_printer_message(agent, &message).await;
                })
            })
            .before_start(|agent| {
                let _ = Self::print_status(POWER_ON);
                AgentReply::immediate()
            })
            .act_on::<NewSpinner>(|agent, context| {
                let item_id = context.message().0.clone();
                let runtime = &mut agent.runtime();
                // let spinner = FrameRunner::new(
                //     item_id,
                //     SPINNER_FPS,
                //     runtime
                // ).await?;
                // let spinner_handle = handle.supervise(spinner).await?;
                // let envelope = handle.create_envelope(Some(spinner_handle.reply_address()));

                AgentReply::immediate()
            })
            .after_start(|agent| {
            let mut stdout = stdout();
            let _ = execute!(stdout, cursor::Hide);
            let _ = Self::print_status(STARTED);
            AgentReply::immediate()
        });
        agent.model.current_line = MARGIN_TOP;

        agent.handle().subscribe::<PriceResponse>().await;
        agent.handle().subscribe::<PrinterMessage>().await;
        Ok(agent.start().await)
    }

    async fn handle_printer_message(
        agent: &mut ManagedAgent<Started, Printer>,
        message: &PrinterMessage,
    ) -> anyhow::Result<()> {
        let mut printer = &agent.model;
        let handle = agent.handle().clone();
        match message {
            PrinterMessage::Status(message) => {
                Self::print_status(message)?;
            }
            PrinterMessage::PrintLine(_) => {
                // No operation for PrintLine
            }
            PrinterMessage::Help(help_text) => {
                // Self::print_help(printer, help_text)?;
            }
            PrinterMessage::Loading(item_id) => {
                let cloned_id = item_id.clone();
                printer.items.insert(cloned_id.clone(), DisplayItem::Loader(item_id.clone().parse()?));

                // if let Some(old_value) = printer.spinners.insert(cloned_id, true) {
                //     debug!("existing spinner replaced:{}", old_value);
                // } else {
                //     info!("new spinner:{}", item_id);
                //     debug!("spinner count:{}", printer.spinners.len());
                // }

                handle.send(NewSpinner(item_id.clone())).await;

                Self::print_receipt(printer, handle)?;
            }
        }
        Ok(())
    }


    fn print_header(printer: &mut Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;
        let top = MARGIN_TOP + 1;

        // Create the horizontal line with intersections ├ and ┤
        let text = format!("{}{}{}", "─".repeat((COLS - 11) as usize), "┴", "─".repeat((11) as usize));

        // Position the cursor at LEFT_PAD to ensure the line is correctly aligned
        queue!(stdout, cursor::MoveTo(LEFT_PAD, top))?;
        stdout.write(text.as_bytes())?;
        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }

    // #[instrument(skip(printer), level = "trace")]
    fn print_receipt(printer: &Printer, handle: AgentHandle) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;

        // Iterate through all the items that are CartItems and print them
        for (i, item) in printer.items.iter().filter_map(|kv| match kv.value() {
            DisplayItem::Item(cart_item) => Some(cart_item.clone()), // Clone the CartItem to avoid borrowing issues
            _ => None,
        }).enumerate() {
            let line_item = format!(
                "{} ({}) {} {} {}",
                item.name(),
                item.quantity(),
                MoneyFmt(**item.cost()),
                "│",
                MoneyFmt(item.price().0)
            );

            // Calculate the exact position to move the cursor to align right
            let start_col = (LEFT_PAD + COLS).saturating_sub(line_item.len() as u16);

            // Ensure the cursor moves to the correct position for right alignment
            queue!(stdout, cursor::MoveTo(start_col, MARGIN_TOP + i as u16))?;
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

        let top = printer.items.len() as u16 + MARGIN_TOP;

        // Create the horizontal line with intersections ├ and ┤
        let text = format!(
            "{}{}{}",
            "─".repeat((COLS - 11) as usize),
            "┴",
            "─".repeat(11)
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

    fn print_status(text: &str) -> anyhow::Result<()> {
        let mut stdout = stdout();
        // execute!(stdout, BeginSynchronizedUpdate)?;
        queue!(stdout, cursor::MoveTo((LEFT_PAD + COLS).saturating_sub(text.len() as u16), 1));
        queue!(stdout, Clear(ClearType::CurrentLine))?;
        stdout.write(text.as_ref())?;

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
