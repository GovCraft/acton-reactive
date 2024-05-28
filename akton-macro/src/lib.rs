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
#![forbid(unsafe_code)]

// extern crate proc_macro;
//! Akton Macro Library
//!
//! This library provides procedural macros for the Akton actor framework.
//! It includes macros to derive common traits and boilerplate code for Akton messages.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// A procedural macro to derive the necessary traits for an Akton message.
///
/// This macro will automatically implement `Clone`, `Debug`, `AktonMessage`, and `Sync`
/// for the annotated type.
///
/// # Example
///
/// ```rust,ignore
/// #[akton_message]
/// struct MyMessage {
///     // fields...
/// }
/// ```

#[proc_macro_attribute]
pub fn akton_message(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(item as DeriveInput);

    // Get the name of the struct.
    let name = &input.ident;

    // Generate the expanded code.
    let expanded = quote! {

        // Derive the Clone trait.
        #[derive(Clone)]
        #input

        // Implement the Debug trait.
        impl std::fmt::Debug for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, stringify!(#name))
            }
        }

        // Implement the AktonMessage trait.
        impl AktonMessage for #name {
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        // Implement the Sync trait.
        unsafe impl Sync for #name {}
    };

    // Return the generated tokens.
    TokenStream::from(expanded)
}
