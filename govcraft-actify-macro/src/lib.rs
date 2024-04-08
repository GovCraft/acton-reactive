extern crate proc_macro;

use proc_macro::TokenStream;
use std::collections::HashSet;
use quote::quote;
use syn::{parse_macro_input, ItemStruct, Fields, Lifetime, Type, Path, LitStr, token};
use syn::parse::Parser;
use syn::punctuated::Punctuated;

#[proc_macro_attribute]
pub fn govcraft_actor(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
// Extract the type from the attribute
    let type_path = extract_type_from_attr(attr);
    let name = &input.ident;
    let internal_name = syn::Ident::new(&format!("{}Internal", name), name.span()); // Name for the internal struct
    let context_name = syn::Ident::new(&format!("{}Context", name), name.span()); // Name for the internal struct

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

    let args_sans_lifetimes = match &input.fields {
        Fields::Named(fields) => {
            if fields.named.is_empty() {
                quote!()
            } else {
                // Generate the field declarations without lifetimes
                let fields_sans_lifetimes = fields.named.iter().map(|f| {
                    let mut field_sans_lifetime = f.clone();
                    field_sans_lifetime.ty = type_sans_lifetimes(&f.ty);
                    field_sans_lifetime
                });

                quote! {
                    #( #fields_sans_lifetimes ),*
                }
            }
        },
        Fields::Unit => quote! {},
        Fields::Unnamed(_) => panic!("govcraft_actor does not support tuple structs."),
    };
    let gen = quote! {
            pub struct #name #lifetime_declarations #struct_body

            impl #lifetime_declarations #name #lifetime_declarations {
                // Public constructor
                pub fn new(internal: Option<#internal_name>, #public_func_args) -> Self {
                Self {
                     __internal:internal,
                    #new_args_defaults
                    }
                }
                async fn run(&mut self) {
                    loop {
                        if let Some(internal) = self.__internal.as_mut() {
                                    tokio::select! {
                                        Some(msg) = internal.receiver.recv() => {
                                            // Handle personal messages
                                            self.handle_message(msg).await;
                                        },
                                        Ok(msg) = internal.broadcast_receiver.recv() => {
                                            // Handle broadcasted messages
                                            self.handle_message(msg).await;
                                        },
                                        else => {
                                            // tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                // println!("nope");
                                        break;
                                    },
                                    }
                                } else {
                                eprintln!("internal was none");
                            }
                    }
                }
            }




            struct #internal_name {
                broadcast_receiver: tokio::sync::broadcast::Receiver<#type_path> ,
                receiver: tokio::sync::mpsc::Receiver<#type_path>
                // Add other internal fields here
            }

            //Actor Context Object
            #[derive(Debug)]
            pub struct #context_name {
                sender: tokio::sync::mpsc::Sender<#type_path>,
            }
            impl #context_name {
                pub fn new(broadcast_receiver: tokio::sync::broadcast::Receiver<#type_path>, #args_sans_lifetimes) -> Self {
                    let (sender, receiver) = tokio::sync::mpsc::channel(255);
                    let mut actor = #name ::new(Some(#internal_name {receiver, broadcast_receiver}), #new_args_defaults );
                    tokio::spawn(async move {
                    actor.pre_run().await;
                    actor.run().await;
                });
                    Self {sender}
                }
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
        }
        Err(_) => {
            // If parsing fails, default to ActorMessage
            syn::parse_str("govcraft_actify_core::ActorMessage").unwrap()
        }
    }
}

// Helper function to remove lifetimes from a type
fn type_sans_lifetimes(ty: &Type) -> Type {
    match ty {
        // Handle reference types specifically
        Type::Reference(type_reference) => {
            let elem = Box::new(type_sans_lifetimes(&type_reference.elem));
            Type::Reference(syn::TypeReference {
                and_token: type_reference.and_token,
                lifetime: None, // Remove the lifetime
                mutability: type_reference.mutability,
                elem,
            })
        },
        // Add other type cases as needed
        _ => ty.clone(),
    }
}
