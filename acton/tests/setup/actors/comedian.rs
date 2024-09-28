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

// A simple actor for testing purposes
// tracks the overall number of jokes told
// the number of jokes audience members found funny
// and the number of jokes which bombed with the audience
#[derive(Default, Debug, Clone)]
pub struct Comedian {
    pub jokes_told: usize,
    pub funny: usize,
    pub bombers: usize,
}
