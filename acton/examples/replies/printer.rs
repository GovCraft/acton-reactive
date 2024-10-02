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
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, trace};

use acton_core::prelude::*;

use crate::{PriceResponse, PrinterMessage};
use crate::cart_item::{CartItem, Price};

const COLS: u16 = 40;
const LEFT_PAD: u16 = 3;
const MARGIN_TOP: u16 = 3;
const PAD_TOP: u16 = 2;
const PAD_BOTTOM: u16 = 4;
const POWER_ON: &str = "Printer powering on...";
const STARTED: &str = "System running...\n";
const LOADING: &str = "loading...";
const HELP_TEXT: &str = "q: quit, ?: help";

#[derive(Default, Debug, Clone)]
pub struct Printer {
    current_line: u16,
    items: HashMap<String, DisplayItem>,
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

impl Printer {
    pub async fn power_on(app: &mut AgentRuntime) -> AgentHandle {
        let mut agent = app.new_agent::<Printer>().await;

        agent
            .act_on::<PriceResponse>(|agent, context| {
                let mut printer = agent.model.clone();
                let item = context.message().item.clone();

                // see if this item name exists as a Loader display item, if so , convert it to a CartItem, otherwise add it as a CartItem
                if let Some(loader) = printer.items.get_mut(&item.name().clone()) {
                    match loader {
                        DisplayItem::Loader(_what) => {
                            let mut cart_item = CartItem::new(item.name().clone(), item.quantity());
                            cart_item.set_cost(*item.cost().as_ref());
                            *loader = DisplayItem::Item(cart_item);
                        }
                        _ => {
                            let cart_item = CartItem::new(item.name().clone(), item.price().0);
                            printer.items.insert(item.name().clone(), DisplayItem::Item(cart_item));
                        }
                    }
                } else {
                    let cart_item = CartItem::new(item.name().clone(), item.price().0);
                    printer.items.insert(item.name().clone(), DisplayItem::Item(cart_item));
                }

                let _ = Self::print_receipt(&mut printer);
                AgentReply::immediate()
            })
            .act_on::<PrinterMessage>(|agent, context| {
                let mut printer = agent.model.clone();
                let message = context.message().clone();
                Self::handle_printer_message(&mut printer, &message);
                AgentReply::immediate()
            })
            .before_start(|agent| {
                let _ = Self::print_status(POWER_ON);
                AgentReply::immediate()
            })
            .after_start(|agent| {
                let mut stdout = stdout();
                let _ = execute!(stdout, cursor::Hide);
                let _ = Self::print_status(STARTED);
                AgentReply::immediate()
            });
        agent.model.current_line = MARGIN_TOP;


        trace!("Subscribing to PriceResponse and Status");
        agent.handle().subscribe::<PriceResponse>().await;
        agent.handle().subscribe::<PrinterMessage>().await;
        agent.start().await
    }

    fn handle_printer_message(printer: &mut Printer, message: &PrinterMessage) {
        match &message {
            PrinterMessage::Status(message) => {
                let _ = Self::print_status(message);
            }
            PrinterMessage::PrintLine(text) => {
                let _ = Self::print_line(&printer.current_line, text.as_ref());
            }

            PrinterMessage::Help(help_text) => {
                Self::print_help(printer, help_text);
            }
            PrinterMessage::Loading(what) => {
                printer.items.insert((*what.clone()).parse().unwrap(), DisplayItem::Loader(what.clone()));
                let _ = Self::print_receipt(printer);
            }
        }
    }

    fn print_receipt(printer: &mut Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;

        // Iterate through all the items that are CartItems and print them
        for (i, item) in printer.items.iter().filter_map(|(_, item)| Some(item)).enumerate() {
            let line_item = match item {
                DisplayItem::Item(item) => format!("{} x {} @ {} {:^3} {:>5}", item.quantity(), item.name(), item.cost(), "│", item.price()),
                DisplayItem::Loader(what) => format!("? x {} {:^3} {:>5}", what, "│", "$"),
                _ => "".to_string(),
            };

            // Calculate the exact position to move the cursor to align right
            let start_col = LEFT_PAD + COLS - line_item.len() as u16;

            // Ensure the cursor moves to the correct position for right alignment
            queue!(stdout, cursor::MoveTo(start_col, MARGIN_TOP + i as u16))?;
            queue!(stdout, Clear(ClearType::CurrentLine))?;
            stdout.write_all(line_item.as_bytes())?;
        }

        stdout.flush()?;
        Self::print_subtotal(printer)?;
        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }

    fn print_subtotal(printer: &mut Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;

        let top = printer.items.len() as u16 + MARGIN_TOP;
        // let separator_col = LEFT_PAD + COLS / 2; // Position of the vertical line intersection

        // Create the horizontal line with intersections ├ and ┤
        let text = format!("{}{}{}", "─".repeat((COLS - 9) as usize), "┴", "─".repeat((9) as usize));

        // Position the cursor at LEFT_PAD to ensure the line is correctly aligned
        queue!(stdout, cursor::MoveTo(LEFT_PAD, top))?;
        stdout.write_all(text.as_bytes())?;
        stdout.flush()?; // Flush after writing the line

        let subtotal = printer.items.iter().map(|(_, item)| {
            match item {
                DisplayItem::Item(item) => item.price().0,
                _ => 0
            }
        }).sum::<i32>();

        let total_str = format!("{:<11}{}", "Subtotal", MoneyFmt(subtotal));

        // Move to the correct position for the subtotal line, right-aligned
        queue!(stdout, cursor::MoveTo((LEFT_PAD + COLS).saturating_sub(total_str.len() as u16), top + 1))?;
        stdout.write_all(total_str.as_bytes())?;
        stdout.flush()?;

        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }


    fn print_line(line_no: &u16, text: &str) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;
        queue!(stdout, cursor::MoveTo((LEFT_PAD + COLS).saturating_sub(text.len() as u16), *line_no));
        queue!(stdout, Clear(ClearType::CurrentLine))?;

        stdout.write(format!("{text}").as_ref())?;

        // move operation is performed only if we flush the buffer.
        stdout.flush()?;
        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }

    fn print_status(text: &str) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;
        queue!(stdout, cursor::MoveTo((LEFT_PAD + COLS).saturating_sub(text.len() as u16), 1));
        queue!(stdout, Clear(ClearType::CurrentLine))?;
        stdout.write(text.as_ref())?;

        // move operation is performed only if we flush the buffer.
        stdout.flush()?;
        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }

    fn print_help(printer: &mut Printer, text: &str) {
        let mut stdout = stdout();

        queue!(stdout, cursor::MoveTo((LEFT_PAD + COLS).saturating_sub(text.len() as u16), printer.current_line + PAD_BOTTOM - 1));
        execute!(stdout, Clear(ClearType::CurrentLine)).expect("clear failed");
        queue!(stdout,  cursor::MoveTo(LEFT_PAD, printer.current_line + PAD_BOTTOM));

        stdout.write(text.as_ref()).expect("write failed");

        // move operation is performed only if we flush the buffer.
        stdout.flush().expect("flush failed");
    }
}
