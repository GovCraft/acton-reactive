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
#![allow(unused)]

use acton_reactive::prelude::*;

#[derive(Clone, Debug)]
pub struct Pong;

// #[acton_message]
#[derive(Clone, Debug)]
pub struct Ping;

#[acton_message]
pub enum FunnyJoke {
    ChickenCrossesRoad,
    Pun,
}

#[acton_message]
pub enum FunnyJokeFor {
    ChickenCrossesRoad(Ern),
    Pun(String),
}

#[acton_message]
pub enum AudienceReactionMsg {
    Chuckle,
    Groan,
}

// the joke told by the comedian
#[acton_message]
pub struct Joke;

#[acton_message]
pub enum Tally {
    AddCount,
}

#[acton_message]
pub enum StatusReport {
    Complete(usize),
}
