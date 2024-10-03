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

use proc_macro::TokenStream;

use quote::quote;
use syn::{ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn acton_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;
    let attrs = &input.attrs;
    let name = &sig.ident;
    let inputs = &sig.inputs;
    let output = &sig.output;

    let async_name = syn::Ident::new(&format!("__{}_async", name), name.span());

    let output = quote! {
            #[test]
            #(#attrs)*
            #vis fn #name() {
                use std::sync::atomic::{AtomicBool, Ordering};
                use std::sync::Arc;
                use std::panic;
                use tracing::{info, error, Span};

                #[derive(Clone, Default)]
                struct PanicInfo {
                    occurred: Arc<AtomicBool>,
                    message: Arc<parking_lot::Mutex<Option<String>>>,
                    location: Arc<parking_lot::Mutex<Option<String>>>,
                }

                let panic_info = Arc::new(PanicInfo::default());
                let panic_info_clone = Arc::clone(&panic_info);

                let orig_hook = panic::take_hook();
                panic::set_hook(Box::new(move |panic_info| {
                    panic_info_clone.occurred.store(true, Ordering::SeqCst);
                    *panic_info_clone.message.lock() = panic_info.payload().downcast_ref::<&str>().map(|s| s.to_string());
                    *panic_info_clone.location.lock() = panic_info.location().map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()));

                let message = panic_info_clone.message.lock().clone();
                let cleaned_message = message
                    .unwrap_or_else(|| "No error message".to_string())
                    .trim()
                    .replace('\n', " ");
                    error!("Panic: {}",cleaned_message);
                    orig_hook(panic_info);
                }));

                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                let result = runtime.block_on(async {
                    panic_info.occurred.store(false, Ordering::SeqCst);
                    *panic_info.message.lock() = None;
                    *panic_info.location.lock() = None;

                    let test_span = tracing::info_span!("acton_test", name = stringify!(#name));
                    let _enter = test_span.enter();

                    #async_name().await
                });

         if panic_info.occurred.load(Ordering::SeqCst) {
        let message = panic_info.message.lock().clone();
        let location = panic_info.location.lock().clone();
        let location_str = location.unwrap_or_else(|| "unknown location".to_string());
        let cleaned_message = message
            .unwrap_or_else(|| "No error message".to_string())
            .trim()
            .replace('\n', " ");
        let panic_message = format!("Panic at {}: {}", location_str, cleaned_message);
        panic!("{}", panic_message);
    }

                result.unwrap()
            }

            async fn #async_name(#inputs) #output #body
        };

    output.into()
}
