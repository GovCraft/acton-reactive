use acton_reactive::prelude::*;

#[acton_message]
pub struct InitProject<'a>{
    pub project_name: &'a str,
}