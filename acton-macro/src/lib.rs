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
#![forbid(unsafe_code)]
// extern crate proc_macro;
//! Acton Macro Library
//!
//! This library provides procedural macros for the Acton actor framework.
//! It includes macros to derive common traits and boilerplate code for Acton messages.

use proc_macro::TokenStream;

use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// A procedural macro to derive the necessary traits for an Acton message.
///
/// This macro will automatically implement `Clone`, `Debug`, `ActonMessage`, and `Sync`
/// for the annotated type.
#[proc_macro_attribute]
pub fn acton_message(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(item as DeriveInput);

    // Get the name and generics of the struct.
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Generate the expanded code.
    let expanded = quote! {
        // Derive the Clone trait.
        #[derive(Clone)]
        #input

        // Implement the Debug trait.
        impl #impl_generics std::fmt::Debug for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, stringify!(#name))
            }
        }

        // Implement the Sync trait.
        unsafe impl #impl_generics Sync for #name #ty_generics #where_clause {}
    };

    // Return the generated tokens.
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn acton_actor(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(item as DeriveInput);

    // Get the name and generics of the struct.
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Generate the expanded code.
    let expanded = quote! {
        // Derive the Clone trait.
        #[derive(Default, Clone)]
        #input

        // Implement the Debug trait.
        impl #impl_generics std::fmt::Debug for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, stringify!(#name))
            }
        }
    };

    // Return the generated tokens.
    TokenStream::from(expanded)
}
