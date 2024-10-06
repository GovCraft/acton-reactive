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
use std::io::{stdout, Write};
use std::sync::Once;

use acton_reactive::prelude::*;
use anyhow::Result;
use crossterm::{
    cursor, event::{self, Event, KeyCode, KeyModifiers}, execute,
    queue,
    terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode},
};
use futures::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tracing::*;
use tracing::Level;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan, FmtSubscriber};

use agents::ScaffoldAgent;
use messages::{InitProject, MenuMoveDown, MenuMoveUp};
use screens::HomeScreen;
use crate::agents::ViewManager;
use crate::messages::MenuSelect;

mod screens;
mod agents;
mod messages;

const FAILED_TO_ENABLE_RAW_MODE: &str = "Failed to enable raw mode";
const FAILED_TO_DISABLE_RAW_MODE: &str = "Failed to disable raw mode";
const SHUTDOWN_MESSAGE: &str = "Shutting down...\n";
const ERROR_READING_EVENT: &str = "Error reading event: {:?}";
const LOG_DIRECTORY: &str = "logs";
const LOG_FILENAME: &str = "tracing.log";

const TAGLINE: &str = "Rapidly scaffold and update Acton Reactive Framework applications";
const TITLE: &str = "Acton Console";

#[derive(Clone, Debug)]
enum PrinterMessage {
    Repaint,
}

struct RawModeGuard;

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        disable_raw_mode().expect(FAILED_TO_DISABLE_RAW_MODE);
    }
}

const PADLEFT: u16 = 4;
const PADTOP: u16 = 2;

#[tokio::main]
async fn main() -> Result<()> {
    initialize_tracing();
    info!("** App startup **");

    enable_raw_mode().expect("Failed to enable raw mode");
    let _raw_mode_guard = RawModeGuard;
    let mut stdout = stdout();
    execute!(stdout, cursor::Hide, Clear(ClearType::All), cursor::MoveTo(PADLEFT, PADTOP))?;

    let mut app = ActonApp::launch();
    let view_manager = ViewManager::new(&mut app).await?;
    let scaffold_agent = ScaffoldAgent::new(&mut app).await;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    static PROJECT_NAME: &str = "test_project";
    // let shutdown_tx_clone = shutdown_tx.clone();

    // Shutdown signal handler
    tokio::spawn(async move {
        let mut reader = event::EventStream::new();
        while let Some(event_result) = reader.next().await {
            match event_result {
                Ok(Event::Key(key_event)) => match key_event.code {
                    KeyCode::Char('c') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                        info!("CTRL+C pressed. Sending shutdown signal...");
                        let _ = shutdown_tx.send(());
                        break;
                    }
                    KeyCode::Char('q') => {
                        info!("'q' pressed. Sending shutdown signal...");
                        let _ = shutdown_tx.send(());
                        break;
                    }
                    KeyCode::Char('s') => {
                        info!("'s' pressed. Starting project scaffold...");
                        scaffold_agent.send(InitProject { project_name: PROJECT_NAME }).await;
                    }
                    KeyCode::Char('j') => {
                        view_manager.send(MenuMoveDown).await;
                    }
                    KeyCode::Enter => {
                        view_manager.send(MenuSelect).await;
                    }
                    KeyCode::Char('k') => {
                        view_manager.send(MenuMoveUp).await;
                    }
                    KeyCode::Char('s') => {
                        info!("'s' pressed. Starting project scaffold...");
                        scaffold_agent.send(InitProject { project_name: PROJECT_NAME }).await;
                    }
                    _ => {}
                },
                Err(e) => {
                    error!("Error reading event: {:?}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Await the shutdown signal
    match shutdown_rx.await {
        Ok(_) => info!("Shutdown signal received."),
        Err(e) => error!("Failed to receive shutdown signal: {:?}", e),
    };

    // Clean up terminal
    queue!(stdout, cursor::MoveTo(0, 0))?;
    queue!(stdout, Clear(ClearType::FromCursorDown))?;
    stdout.write_all(SHUTDOWN_MESSAGE.as_bytes())?;
    queue!(stdout, cursor::Show)?;
    stdout.flush()?;
    queue!(stdout, cursor::MoveTo(0, 1))?;

    // Shutdown agents
    if let Err(e) = app.shutdown_all().await {
        error!("Error during agent shutdown: {:?}", e);
    } else {
        info!("All agents shut down successfully.");
    }

    info!("** App shutdown complete **");
    Ok(())
}

static INIT: Once = Once::new();

pub fn initialize_tracing() {
    INIT.call_once(|| {
        let filter = EnvFilter::new("")
            .add_directive("acton_cli=trace".parse().unwrap());

        let file_appender = RollingFileAppender::new(Rotation::DAILY, LOG_DIRECTORY, LOG_FILENAME);

        let subscriber = FmtSubscriber::builder()
            .with_span_events(FmtSpan::NONE)
            .with_max_level(Level::TRACE)
            .compact()
            .with_line_number(false)
            .with_target(true)
            .without_time()
            .with_env_filter(filter)
            .with_writer(file_appender)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("Setting default subscriber failed");
    });
}
