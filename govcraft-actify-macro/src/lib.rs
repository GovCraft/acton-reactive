extern crate proc_macro;

use proc_macro::TokenStream;
use std::collections::HashSet;
use quote::quote;
use syn::{parse_macro_input, ItemStruct, Fields, Lifetime, Type, Lit, Meta, MetaNameValue, Path, LitStr, token};
use syn::parse::Parser;
use syn::punctuated::Punctuated;

#[proc_macro_attribute]
pub fn govcraft_actor(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
// Extract the type from the attribute
    let type_path = extract_type_from_attr(attr);
    let name = &input.ident;
    let internal_name = syn::Ident::new(&format!("{}Internal", name), name.span()); // Name for the internal struct
    let context_name = syn::Ident::new(&format!("{}ActorContext", name), name.span()); // Name for the internal struct

    // Collect unique lifetimes from all fields
    let mut lifetimes_set = HashSet::new();
    if let Fields::Named(fields) = &input.fields {
        for field in &fields.named {
            let field_lifetimes = extract_lifetimes(&field.ty);
            for lifetime in field_lifetimes {
                lifetimes_set.insert(lifetime);
            }
        }
    }
    let lifetimes: Vec<_> = lifetimes_set.into_iter().collect();
    let lifetime_declarations = {
        if lifetimes.is_empty() {
            quote! {}
        } else {
            quote! {<#(#lifetimes),*>}
        }
    };
    // Check if there are any fields at all
    let struct_body = match &input.fields {
        // Handle named fields
        Fields::Named(fields) => {
            if fields.named.is_empty() {
                // For structs with no fields, generate an empty struct body
                quote! {
                    {
                        __internal: std::option::Option<#internal_name >
                    }
                }
            } else {
                // For structs with named fields, generate the field declarations
                let field_names = fields.named.iter().map(|f| &f.ident);
                let field_types = fields.named.iter().map(|f| &f.ty);
                quote! {
                    {
                        __internal: std::option::Option<#internal_name >,
                        #( #field_names: #field_types),*
                    }
                }
            }
        }
        // Explicitly handle empty structs without fields
        Fields::Unit => {
            quote! {
                {
                    __internal: std::option::Option<#internal_name >
                }
            }
        }
        // Optionally, handle unnamed fields (tuple structs) or panic if you want to exclude them
        Fields::Unnamed(_) => panic!("govcraft_actor does not support tuple structs."),
    };

    let public_func_args = match &input.fields {
        // Handle named fields
        Fields::Named(fields) => {
            if fields.named.is_empty() {
                // For structs with no fields, generate an empty struct body
                quote!()
            } else {
                // For structs with named fields, generate the field declarations
                let field_names = fields.named.iter().map(|f| &f.ident);
                let field_types = fields.named.iter().map(|f| &f.ty);
                quote! {

                        // The internal, private struct containing macro-generated fields
                        #( #field_names: #field_types),*

                }
            }
        }
        // Explicitly handle empty structs without fields
        Fields::Unit => {
            quote! {
            }
        }
        // Optionally, handle unnamed fields (tuple structs) or panic if you want to exclude them
        Fields::Unnamed(_) => panic!("govcraft_actor does not support tuple structs."),
    };
    let new_args_defaults = match &input.fields {
        // Handle named fields
        Fields::Named(fields) => {
            if fields.named.is_empty() {
                // For structs with no fields, generate an empty struct body
                quote!()
            } else {
                // For structs with named fields, generate the field declarations
                let field_names = fields.named.iter().map(|f| &f.ident);
                quote! {

                        // The internal, private struct containing macro-generated fields
                        #( #field_names),*

                }
            }
        }
        // Explicitly handle empty structs without fields
        Fields::Unit => {
            quote! {
            }
        }
        // Optionally, handle unnamed fields (tuple structs) or panic if you want to exclude them
        Fields::Unnamed(_) => panic!("govcraft_actor does not support tuple structs."),
    };

    let gen = quote! {
            struct #name #lifetime_declarations #struct_body

            impl #lifetime_declarations #name #lifetime_declarations {
                // Public constructor
                pub fn new(#public_func_args) -> Self {
                Self {
                     __internal:None,
                    #new_args_defaults
                    }
                }
                // Other public methods that interact with the internal fields
                // ...
            }


            struct #internal_name {
                broadcast_receiver: govcraft_actify_core::prelude::Receiver<#type_path>
                // Add other internal fields here
            }

            //Actor Context Object
            #[derive(Debug)]
            struct #context_name {
                sender: govcraft_actify_core::prelude::Sender<#type_path>,
        }


    };

    gen.into()
}

fn extract_lifetimes(ty: &Type) -> Vec<Lifetime> {
    use syn::PathArguments::AngleBracketed;
    let mut lifetimes = Vec::new();

    match ty {
        Type::Reference(type_ref) => {
            if let Some(lifetime) = &type_ref.lifetime {
                lifetimes.push(lifetime.clone());
            }
        }
        Type::Path(type_path) => {
            for segment in &type_path.path.segments {
                if let AngleBracketed(args) = &segment.arguments {
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(ty) = arg {
                            lifetimes.extend(extract_lifetimes(ty));
                        }
                    }
                }
            }
        }
        _ => {}
    }

    lifetimes
}
fn extract_type_from_attr(attr: TokenStream) -> Path {
    // Define a parser for the attribute input
    let parser = Punctuated::<LitStr, token::Comma>::parse_terminated;
    // Try parsing the attribute input; it might be empty
    let parsed_attrs = parser.parse(attr);

    match parsed_attrs {
        Ok(attrs) => {
            // If attributes are provided, attempt to use the first one
            if let Some(attr) = attrs.first() {
                attr.parse().expect("Expected a valid type")
            } else {
                // No attributes provided, default to ActorMessage
                syn::parse_str("govcraft_actify_core::ActorMessage").unwrap()
            }
        },
        Err(_) => {
            // If parsing fails, default to ActorMessage
            syn::parse_str("govcraft_actify_core::ActorMessage").unwrap()
        }
    }
}