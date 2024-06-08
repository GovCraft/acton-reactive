/*
 *
 *  *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  *  Licensed under the Business Source License, Version 1.1 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      https://github.com/GovCraft/akton-framework/tree/main/LICENSES
 *  *
 *  *  Change Date: Three years from the release date of this version of the Licensed Work.
 *  *  Change License: Apache License, Version 2.0
 *  *
 *  *  Usage Limitations:
 *  *    - You may use the Licensed Work for non-production purposes only, such as internal testing, development, and experimentation.
 *  *    - You may not use the Licensed Work for any production or commercial purpose, including, but not limited to, the provision of any service to third parties, without a commercial use license from the Licensor, except as stated in the Exemptions section of the License.
 *  *
 *  *  Exemptions:
 *  *    - Open Source Projects licensed under an OSI-approved open source license.
 *  *    - Non-Profit Organizations using the Licensed Work for non-commercial purposes.
 *  *    - Small For-Profit Companies with annual gross revenues not exceeding $2,000,000 USD.
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::time::Duration;

use akton_arn::Arn;
use dashmap::DashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::task::TaskTracker;
use tracing::{event, info, instrument, trace, warn, Level};

use crate::common::{Envelope, StopSignal, SystemSignal};
use crate::pool::PoolItem;
use crate::traits::SupervisorContext;

/// Represents a supervisor in the actor system, responsible for managing subordinates and handling their messages.
pub(crate) struct Supervisor {
    /// The unique identifier (ARN) for the supervisor.
    pub(crate) key: Arn,
    /// The signal used to halt the supervisor.
    pub(crate) halt_signal: StopSignal,
    /// A map of subordinates managed by the supervisor.
    pub(crate) subordinates: DashMap<String, PoolItem>,
    /// The task tracker for managing the supervisor's tasks.
    pub(crate) task_tracker: TaskTracker,
    /// The outbound channel for sending messages.
    pub(crate) outbox: Sender<Envelope>,
    /// The mailbox for receiving messages.
    pub(crate) mailbox: Receiver<Envelope>,
}

/// Custom implementation of the `Debug` trait for the `Supervisor` struct.
///
/// This implementation provides a formatted output for the `Supervisor` struct.
impl Debug for Supervisor {
    /// Formats the `Supervisor` struct using the given formatter.
    ///
    /// # Parameters
    /// - `f`: The formatter used for writing formatted output.
    ///
    /// # Returns
    /// A result indicating whether the formatting was successful.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key.value)
    }
}

impl Supervisor {
    /// Wakes the supervisor and processes incoming messages.
    ///
    /// # Returns
    /// An `anyhow::Result` indicating success or failure.
    #[instrument(skip(self))]
    pub(crate) async fn wake_supervisor(&mut self) -> anyhow::Result<()> {
        while let Some(envelope) = self.mailbox.recv().await {
            event!(
                Level::TRACE,
                "Supervisor mailbox received {:?}",
                &envelope.message
            );
            if let Some(concrete_msg) = &envelope.message.as_any().downcast_ref::<SystemSignal>() {
                match concrete_msg {
                    SystemSignal::Terminate => {
                        // Event: Termination Signal Received
                        // Description: Received a termination signal.
                        // Context: None
                        info!("Received termination signal.");
                        self.terminate().await?;
                    }
                }
            }
            if let Some(ref pool_id) = &envelope.pool_id {
                // Event: Processing Envelope
                // Description: Processing an envelope for a specific pool.
                // Context: Pool ID and envelope details.
                // trace!(pool_id = ?pool_id, envelope = ?envelope, "Processing envelope for pool.");

                if let Some(mut pool_def) = self.subordinates.get_mut(pool_id) {
                    // First, clone or copy the data needed for the immutable borrow.
                    // NOTE: Cloning the whole pool may be expensive, so consider alternatives if performance is a concern.
                    let pool_clone = pool_def.pool.clone();

                    // Now perform the selection outside the mutable borrowed variable's scope.
                    if let Some(index) = pool_def.strategy.select_context(&pool_clone) {
                        // Access the original data using the index now that we're outside the conflicting borrow.
                        let context = &pool_def.pool[index];
                        trace!(index = index, "Emitting to pool item");
                        context.emit_envelope(envelope).await?;
                    }
                }
            }

            let should_stop =
                { self.halt_signal.load(Ordering::SeqCst) && self.mailbox.is_empty() };

            if should_stop {
                // Event: Stopping Supervisor
                // Description: Stopping the supervisor as halt signal is set and mailbox is empty.
                // Context: None
                info!("Stopping the supervisor as halt signal is set and mailbox is empty.");
                break;
            } else {
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }
        }

        // Check stop condition.
        Ok(())
    }

    /// Terminates the supervisor and its subordinates.
    ///
    /// # Returns
    /// An `anyhow::Result` indicating success or failure.
    #[instrument(skip(self))]
    pub(crate) async fn terminate(&self) -> anyhow::Result<()> {
        let subordinates = &self.subordinates;

        // Event: Terminating Supervisor
        // Description: Initiating termination of the supervisor and its subordinates.
        // Context: Number of subordinates.
        warn!(
            subordinate_count = subordinates.len(),
            "Initiating termination of the supervisor and its subordinates."
        );

        let halt_signal = self.halt_signal.load(Ordering::SeqCst);
        if !halt_signal {
            for item in subordinates {
                for context in &item.value().pool {
                    // Event: Terminating Subordinate
                    // Description: Terminating a subordinate actor.
                    // Context: Subordinate context details.
                    warn!(subordinate=context.key.value,context = ?context, "Terminating subordinate actor.");
                    context.terminate().await?;
                }
            }
            self.halt_signal.store(true, Ordering::SeqCst);
        }

        // Event: Supervisor Terminated
        // Description: The supervisor and its subordinates have been terminated.
        // Context: None
        info!("The supervisor and its subordinates have been terminated.");

        Ok(())
    }
}
