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
use std::fmt::{self, Debug, Display};
use std::io::{stdout, Write};

use ansi_term::Color::RGB;
use crossterm::{
    cursor, execute, queue,
    terminal::{BeginSynchronizedUpdate, Clear, ClearType, EndSynchronizedUpdate},
};
use dashmap::DashMap;
use mti::prelude::MagicTypeId;
use tracing::*;

use acton_core::prelude::*;
use acton_macro::{acton_actor, acton_message};

use crate::{ItemScanned, PriceRequest, PriceResponse, PrinterMessage};
use crate::cart_item::{CartItem, Price};

const COLS: u16 = 40;
const PAD_LEFT: u16 = 3;
const MARGIN_TOP: u16 = 1;
const PAD_TOP: u16 = 2;
const HEADER_HEIGHT: u16 = 4;
const TRANSACTION_RECEIPT: &str = "Transaction Receipt";
const STARTED: &str = "\u{2713}";
const HELP_TEXT: &str = "s: scan item, q: quit, ?: toggle help";
const HELP_TEXT_SHORT: &str = "?: toggle help";
const MOCK_TAX_RATE: f64 = 0.07;
const START_HELP: &str = "Press 's' to scan an item.";
const SUBTOTAL_LABEL: &str = "Subtotal";
const TAX_LABEL: &str = "Tax";
const DUE_LABEL: &str = "Due";
const CHECK_MARK_COLOR: (u8, u8, u8) = (113, 208, 131);
const TOTAL_DUE_COLOR_NOT_LOADED: (u8, u8, u8) = (255, 255, 255);
const COLOR_DARK_GREY: (u8, u8, u8) = (58, 58, 58);
const COLOR_LIGHT_BLUE: (u8, u8, u8) = (194, 234, 255);
const COLOR_MEDIUM_BLUE: (u8, u8, u8) = (117, 199, 240);
const COLOR_GREEN: (u8, u8, u8) = (194, 240, 194);
const COLOR_LOADER: (u8, u8, u8) = (73, 71, 78);
const COLOR_HELP_TEXT: (u8, u8, u8) = (96, 96, 96);

#[acton_actor]
pub struct Printer {
    status: String,
    loaded: bool,
    show_help: bool,
    items: DashMap<MagicTypeId, DisplayItem>,
}

#[acton_message]
pub struct ToggleHelp;

#[derive(Clone, Debug, Default)]
enum DisplayItem {
    Item(CartItem),
    Loader(String),
    #[default]
    Startup,
}

impl Display for DisplayItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DisplayItem::Item(item) => {
                write!(
                    f,
                    "{}({}) @ {} │ {:>5}",
                    item.name(),
                    item.quantity(),
                    MoneyFmt(**item.cost()),
                    MoneyFmt(item.price().0)
                )
            }
            DisplayItem::Loader(what) => write!(
                f,
                "{}( ) @ {} │ {:>5}",
                what,
                MoneyFmt(0),
                MoneyFmt(0)
            ),
            DisplayItem::Startup => write!(f, ""),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct FormattedItem(String);

impl From<DisplayItem> for FormattedItem {
    fn from(value: DisplayItem) -> Self {
        match value {
            DisplayItem::Item(item) => {
                let name = item.name().clone();
                FormattedItem(format!(
                    "{}({}) @ {} {} {:>5}",
                    RGB(COLOR_LIGHT_BLUE.0, COLOR_LIGHT_BLUE.1, COLOR_LIGHT_BLUE.2).paint(name),
                    RGB(
                        COLOR_MEDIUM_BLUE.0,
                        COLOR_MEDIUM_BLUE.1,
                        COLOR_MEDIUM_BLUE.2,
                    )
                        .paint(item.quantity().to_string()),
                    MoneyFmt(**item.cost()).to_string(),
                    RGB(COLOR_DARK_GREY.0, COLOR_DARK_GREY.1, COLOR_DARK_GREY.2).paint("│"),
                    RGB(COLOR_GREEN.0, COLOR_GREEN.1, COLOR_GREEN.2)
                        .paint(MoneyFmt(item.price().0).to_string())
                ))
            }
            DisplayItem::Loader(what) => FormattedItem(format!(
                "{}( ) @ {} {} {:>5}",
                RGB(COLOR_LOADER.0, COLOR_LOADER.1, COLOR_LOADER.2).paint(what),
                MoneyFmt(0),
                RGB(COLOR_DARK_GREY.0, COLOR_DARK_GREY.1, COLOR_DARK_GREY.2).paint("│"),
                MoneyFmt(0)
            )),
            DisplayItem::Startup => FormattedItem(String::default()),
        }
    }
}

impl Display for FormattedItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Default)]
struct MoneyFmt(i32);

impl Display for MoneyFmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    pub async fn power_on(app: &mut AgentRuntime) -> anyhow::Result<AgentHandle> {
        let mut agent = app.new_agent_with_name::<Printer>("printer".to_string()).await;
        agent
            .act_on::<PriceRequest>(|agent, context| {
                let item = context.message().0.clone();
                trace!("PriceRequest: {}", item.name());

                let item_exists = agent.model.items.iter().any(|entry| {
                    if let DisplayItem::Item(existing_item) = entry.value() {
                        existing_item.name() == item.name()
                    } else {
                        false
                    }
                });

                if !item_exists {
                    agent
                        .model
                        .items
                        .insert(item.id().clone(), DisplayItem::Loader(item.name().to_string()));
                    agent.model.loaded = false;
                } else {
                    trace!("Item already exists: {}", item.name());
                }
                let printer = agent.model.clone();
                AgentReply::from_async(async move {
                    let _ = Self::repaint(&printer);
                })
            })
            .act_on::<PriceResponse>(|agent, context| {
                let new_item = context.message().item.clone();
                trace!("PriceResponse: {}", new_item.name());

                let existing_key = agent.model.items.iter().find_map(|item| {
                    if let DisplayItem::Item(existing_item) = item.value() {
                        if existing_item.name() == new_item.name() {
                            Some(item.key().clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });

                if let Some(key) = existing_key {
                    if let Some(mut old_item) = agent.model.items.get_mut(&key) {
                        if let DisplayItem::Item(existing_item) = old_item.value_mut() {
                            existing_item
                                .set_quantity(existing_item.quantity() + new_item.quantity());
                            trace!("Updated item: {:?}", existing_item);
                        }
                    }
                } else {
                    agent
                        .model
                        .items
                        .insert(new_item.id().clone(), DisplayItem::Item(new_item.clone()));
                    trace!("Inserted new item: {:?}", new_item.id());
                }

                agent.model.loaded = agent.model.items.iter().all(|item| {
                    matches!(item.value(), DisplayItem::Item(_))
                });

                let _ = Self::repaint(&agent.model);
                AgentReply::immediate()
            })
            .act_on::<ToggleHelp>(|agent, _context| {
                agent.model.show_help = !agent.model.show_help;
                let _ = Self::repaint(&agent.model);
                AgentReply::immediate()
            })
            .act_on::<PrinterMessage>(|agent, _context| {
                let _ = Self::repaint(&agent.model);
                AgentReply::immediate()
            })
            .after_start(|agent| {
                let mut stdout = stdout();
                let _ = execute!(stdout, cursor::Hide);
                let _ = Self::repaint(&agent.model);
                AgentReply::immediate()
            });

        agent.model.status = STARTED.to_string();

        agent.handle().subscribe::<ItemScanned>().await;
        agent.handle().subscribe::<PriceResponse>().await;
        agent.handle().subscribe::<PrinterMessage>().await;
        Ok(agent.start().await)
    }

    fn repaint(printer: &Printer) -> anyhow::Result<()> {
        Self::print_header()?;
        Self::print_items(&printer)?;
        Self::print_totals(&printer)?;
        Self::print_help(&printer)?;
        Ok(())
    }

    fn print_header() -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;
        let top = PAD_TOP;

        let header_border = RGB(COLOR_DARK_GREY.0, COLOR_DARK_GREY.1, COLOR_DARK_GREY.2)
            .paint("─".repeat(COLS as usize + 1))
            .to_string();
        queue!(stdout, cursor::MoveTo(PAD_LEFT, top))?;
        stdout.write(header_border.as_bytes())?;

        let padding = (COLS as usize).saturating_sub(TRANSACTION_RECEIPT.len()) / 2;
        let centered_text = format!("{}{}", " ".repeat(padding), TRANSACTION_RECEIPT);

        queue!(stdout, cursor::MoveTo(PAD_LEFT, top + 1))?;
        stdout.write(centered_text.as_bytes())?;

        queue!(stdout, cursor::MoveTo(PAD_LEFT, top + 2))?;
        stdout.write(
            RGB(COLOR_DARK_GREY.0, COLOR_DARK_GREY.1, COLOR_DARK_GREY.2)
                .paint(format!(
                    "{}{}{}",
                    "─".repeat((COLS - 11) as usize),
                    "┬",
                    "─".repeat(11)
                ))
                .to_string()
                .as_bytes(),
        )?;

        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }

    fn print_items(printer: &Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();
        execute!(stdout, BeginSynchronizedUpdate)?;

        let top = PAD_TOP + HEADER_HEIGHT;

        if printer.items.is_empty() {
            let start_col = (PAD_LEFT + COLS).saturating_sub(START_HELP.len() as u16);
            queue!(stdout, cursor::MoveTo(start_col, top))?;
            queue!(stdout, Clear(ClearType::CurrentLine))?;
            stdout.write_all(START_HELP.as_bytes())?;
            stdout.flush()?;
            execute!(stdout, EndSynchronizedUpdate)?;
            return Ok(());
        }

        let mut sorted_items: Vec<_> = printer.items.iter().collect();
        sorted_items.sort_by_key(|item| item.key().clone());

        for (i, item) in sorted_items.iter().enumerate() {
            let line_item = format!("{}", item.value());
            let start_col = (PAD_LEFT + COLS).saturating_sub(line_item.len() as u16);
            let formatted: FormattedItem = item.value().clone().into();

            queue!(stdout, cursor::MoveTo(start_col, top + i as u16 -1))?;
            queue!(stdout, Clear(ClearType::CurrentLine))?;
            stdout.write_all(formatted.to_string().as_bytes())?;
            queue!(stdout, cursor::MoveTo(0, 0))?;
        }

        execute!(stdout, EndSynchronizedUpdate)?;
        Ok(())
    }

    fn print_totals(printer: &Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();

        if printer.items.is_empty() {
            return Ok(());
        }

        let top = PAD_TOP + HEADER_HEIGHT + printer.items.len() as u16 - 1;
        let separator = RGB(COLOR_DARK_GREY.0, COLOR_DARK_GREY.1, COLOR_DARK_GREY.2)
            .paint(format!(
                "{}{}{}",
                "─".repeat((COLS - 11) as usize),
                "┴",
                "─".repeat(11)
            ))
            .to_string();

        queue!(stdout, cursor::MoveTo(PAD_LEFT, top))?;
        queue!(stdout, Clear(ClearType::CurrentLine))?;
        stdout.write(separator.as_bytes())?;

        let subtotal = printer
            .items
            .iter()
            .map(|kv| match kv.value() {
                DisplayItem::Item(item) => item.price().0,
                _ => 0,
            })
            .sum::<i32>();
        let tax = (subtotal as f64 * MOCK_TAX_RATE).round() as i32;
        let total_due = subtotal + tax;

        let subtotal_str = format!("{:<11}{}", SUBTOTAL_LABEL, MoneyFmt(subtotal));
        let tax_str = format!("{:<11}{}", TAX_LABEL, MoneyFmt(tax));

        let total_due_str = if printer.loaded {
            format!(
                "{:<11}{} {}",
                DUE_LABEL,
                RGB(
                    CHECK_MARK_COLOR.0,
                    CHECK_MARK_COLOR.1,
                    CHECK_MARK_COLOR.2,
                )
                    .paint(MoneyFmt(total_due).to_string()),
                RGB(
                    CHECK_MARK_COLOR.0,
                    CHECK_MARK_COLOR.1,
                    CHECK_MARK_COLOR.2,
                )
                    .paint(STARTED)
            )
        } else {
            format!(
                "{:<11}{}",
                DUE_LABEL,
                RGB(
                    TOTAL_DUE_COLOR_NOT_LOADED.0,
                    TOTAL_DUE_COLOR_NOT_LOADED.1,
                    TOTAL_DUE_COLOR_NOT_LOADED.2,
                )
                    .paint(MoneyFmt(total_due).to_string())
            )
        };

        let start_col = PAD_LEFT + COLS - subtotal_str.len() as u16 - 2;

        queue!(stdout, cursor::MoveTo(start_col, top + 1))?;
        queue!(stdout, Clear(ClearType::CurrentLine))?;
        stdout.write(subtotal_str.as_bytes())?;
        queue!(stdout, cursor::MoveTo(start_col, top + 2))?;
        queue!(stdout, Clear(ClearType::CurrentLine))?;
        stdout.flush()?;
        stdout.write(tax_str.as_bytes())?;
        queue!(stdout, cursor::MoveTo(start_col, top + 3))?;
        queue!(stdout, Clear(ClearType::CurrentLine))?;
        stdout.flush()?;
        stdout.write(total_due_str.as_bytes())?;
        stdout.flush()?;

        Ok(())
    }

    fn print_help(printer: &Printer) -> anyhow::Result<()> {
        let mut stdout = stdout();
        let help_msg = if printer.show_help {
            HELP_TEXT
        } else {
            HELP_TEXT_SHORT
        };


        let top = if printer.items.is_empty(){
            PAD_TOP + HEADER_HEIGHT + printer.items.len() as u16
        } else {
            PAD_TOP + HEADER_HEIGHT + printer.items.len() as u16 + 2
        };
        let start_col = (PAD_LEFT + COLS).saturating_sub(help_msg.len() as u16);

        queue!(stdout, cursor::MoveTo(start_col, top + 1))?;
        queue!(stdout, Clear(ClearType::FromCursorDown))?;
        queue!(stdout, cursor::MoveDown(1))?;
        stdout.write(
            RGB(
                COLOR_HELP_TEXT.0,
                COLOR_HELP_TEXT.1,
                COLOR_HELP_TEXT.2,
            )
                .paint(help_msg)
                .to_string()
                .as_bytes(),
        )?;
        stdout.flush()?;

        Ok(())
    }
}
