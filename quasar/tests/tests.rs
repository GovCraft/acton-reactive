/*
 *
 *  * Copyright (c) 2024 Govcraft.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */


use quasar_core::prelude::*;
use quasar::prelude::*;
use std::sync::{Arc, Mutex};
use tracing::{error, info, instrument, Level, trace, warn};
use tracing_subscriber::FmtSubscriber;

#[derive(Default, Debug)]
pub struct RicksMemory {
    pub current_thought: String,
    pub adventure_count: usize,
}

impl RicksMemory {
    #[instrument(skip(self))]
    pub fn say_current_thought(&self) {
        info!("{}", self.current_thought);
    }
}

#[tokio::test]
async fn test_actor_mutation() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .compact()
        .with_line_number(true)
        .without_time()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let rick_sanchez_state = RicksMemory {
        current_thought: "Wubba Lubba Dub Dub".to_string(),
        adventure_count: 0,
    };

    // Creates a new root actor (system GalacticCore) and begins processing internal system (non-actor) messages
    let universe = System::spawn().await;
    assert_eq!(universe.context.key().value, "qrn:quasar:system:framework:root");

    // Creating a dormant quasar-core, which will soon embark on wild adventures
    let mut rick_dormant = universe.context.new_actor::<RicksMemory>(rick_sanchez_state, "RickSanchez");
    assert_eq!(rick_dormant.state.key.value, "qrn:quasar:system:framework:root/RickSanchez");

    // Setup to track the final outcome of our adventures
    let final_memory = Arc::new(Mutex::new(String::new()));
    let memory_clone = final_memory.clone();  // Clone for use in the closure

    // Set up behaviors before Rick's portal gun starts
    rick_dormant.state.on_before_wake(|_rick| {
        trace!("Getting Schwifty before starting Rick");
    })
        .act_on::<PortalGunAction>(move |rick, action|
            {
                trace!("MUTATING: Rick was {}", rick.state.current_thought);
                match action {
                    PortalGunAction::PickleRick => {
                        rick.state.current_thought = "I'm Pickle Rick!".to_string();
                    }
                    PortalGunAction::SzechuanSauce => {
                        rick.state.current_thought = "Need more Szechuan Sauce!".to_string();
                    }
                }

                trace!("Rick now {}", rick.state.current_thought.clone());
                let mut memory_lock = memory_clone.lock().unwrap();
                *memory_lock = rick.state.current_thought.clone();

                rick.state.say_current_thought();
            });

    // Awaken Rick with his portal gun ready
    let mut portal_gun = Actor::spawn(rick_dormant).await;

    // Firing up some crazy actions through Rick's portal gun
    portal_gun.emit(PortalGunAction::SzechuanSauce).await?;
    portal_gun.emit(PortalGunAction::PickleRick).await?;

    // Stop the adventures and check on Rick's last known state
    let _ = universe.context.terminate().await;

    // let final_rick_memory = final_memory.lock().unwrap(); // Lock to access data safely
    // assert_eq!(*final_rick_memory, "I'm Pickle Rick!");
    Ok(())
}

#[tokio::test]
async fn test_multiple_actor_mutation() -> anyhow::Result<()> {
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .compact()
        .with_line_number(true)
        .without_time()
        .finish();

    let my_state = RicksMemory {
        current_thought: "Initial State".to_string(),
        adventure_count: 0,

    };
    let second_my_state = RicksMemory {
        current_thought: "I'm number two!".to_string(),
        adventure_count: 0,

    };

    //creates a new root actor (system singularity) and begins processing system messages
    let system = System::spawn().await;

    let mut dormant_quanta = system.context.new_actor::<RicksMemory>(my_state, "my_state");
    let mut second_dormant_quanta = system.context.new_actor::<RicksMemory>(second_my_state, "second_my_state");
    assert_eq!(dormant_quanta.state.key.value, "qrn:quasar:system:framework:root/my_state");
    assert_eq!(second_dormant_quanta.state.key.value, "qrn:quasar:system:framework:root/second_my_state");

    let final_state = Arc::new(Mutex::new(String::new()));
    let final_state_clone = final_state.clone();  // Clone for use in the closure


    let second_final_state = Arc::new(Mutex::new(String::new()));
    let second_final_state_clone = second_final_state.clone();  // Clone for use in the closure

    dormant_quanta.state.on_before_wake(|_actor| {
        trace!("before starting actor");
    })
        .act_on::<FunnyMessage>(move |actor, msg|
            {
                // warn!("MUTATING: actor was {}",actor.state.current_thought);
                // debug!("Actor {}",actor.key.value);
                match msg {
                    FunnyMessage::Haha => {
                        actor.state.current_thought = "Haha".to_string();
                    }
                    FunnyMessage::Lol => {
                        actor.state.current_thought = "Lol".to_string();
                    }
                    FunnyMessage::Giggle => {
                        info!("Other quanta giggled.");
                    }
                }
                // info!("actor now {}",actor.state.current_thought.clone());
                actor.state.adventure_count += 1;
                let mut state_lock = final_state_clone.lock().unwrap();
                *state_lock = actor.state.current_thought.clone();
                // info!("Actor mutation count {}", actor.state.adventure_count);
            });


    let mut context = Actor::spawn(dormant_quanta).await;
    let reply_to = context.return_address();

    second_dormant_quanta.state.on_stop(|_actor| {
        info!("after stopping actor");
    })
        .act_on::<Message>(move |actor, message|
            {
                match message {
                    Message::Hello => {
                        actor.state.current_thought = "Hello".to_string();

                        for _chuckle in 0..5 {
                            let reply_to = reply_to.clone();
                            tokio::spawn( async move {
                                if let Err(e) = reply_to.reply(FunnyMessage::Giggle).await {
                                    error!("Error sending FunnyMessage::Giggle: {:?}", e);
                                }

                            });
                        }
                    }
                    Message::Hola => {
                        actor.state.current_thought = "Hola".to_string();
                    }
                }

                actor.state.adventure_count += 1;
                let mut state_lock = second_final_state_clone.lock().unwrap();
                *state_lock = actor.state.current_thought.clone();

                // info!("Actor mutation count {}", actor.state.adventure_count);
            });
    let mut second_context = Actor::spawn(second_dormant_quanta).await;

    context.emit(FunnyMessage::Lol).await?;
    second_context.emit(Message::Hello).await?;
    context.emit(FunnyMessage::Haha).await?;
    second_context.emit(Message::Hola).await?;


    // let _ = context.terminate().await;
    let _ = second_context.terminate().await;
    let _ = system.context.terminate().await;

    let final_result = final_state.lock().unwrap(); // Lock to access data safely
    assert_eq!(*final_result, "Haha");

    // let second_final_result = second_final_state.lock().unwrap(); // Lock to access data safely
    // assert_eq!(*second_final_result, "Hola");

    Ok(())
}

#[tokio::test]
async fn test_on_before_start() -> anyhow::Result<()> {

// Spawning the central command center of the Council of Ricks
    let council_of_ricks = System::spawn().await;

// Setting up a generic Rick's memory template
    let rick_template = RicksMemory {
        current_thought: String::new(),  // Starts off with no specific thoughts
        adventure_count: 0,             // No adventures logged yet
    };

// Enrolling a new Rick into the Citadel's system
    let mut dormant_rick = council_of_ricks.context.new_actor::<RicksMemory>(rick_template, "Rick137");

// Prepare Rick before his first portal jump
    dormant_rick.state
        .on_before_wake(|_rick| {
            // Maybe load Rick with some initial thoughts or settings
            trace!("Preparing Rick137 for his first adventure");
        })
        .act_on::<InterdimensionalAdventure>(|ricks_memory, adventure| {
            match adventure {
                InterdimensionalAdventure::MortyRescue => {
                    info!("{}", ricks_memory.state.current_thought)
                }
            }
            ricks_memory.state.adventure_count += 1;
        });

// Activate Rick, ready for multiverse chaos
    let mut active_rick = Actor::spawn(dormant_rick).await;

// Rick encounters a crazy scenario
    active_rick.emit(InterdimensionalAdventure::MortyRescue).await?;

// After the chaos, shutting down the system
    let _ = council_of_ricks.context.terminate().await;

    Ok(())
}


impl std::fmt::Debug for InterdimensionalAdventure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, stringify!(#name))
    }
}

impl QuasarMessage for InterdimensionalAdventure {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// #[photon_packet]
pub enum InterdimensionalAdventure {
    MortyRescue,
}

#[quasar_message]
pub enum PortalGunAction {
    PickleRick,
    SzechuanSauce,
}

#[quasar_message]
pub enum Message {
    Hello,
    Hola,
}

#[quasar_message]
pub enum DifferentMessage {
    Sup,
    Suuuup,
}

#[quasar_message]
pub enum FunnyMessage {
    Haha,
    Lol,
    Giggle,
}

#[quasar_message]
pub struct Ping;
