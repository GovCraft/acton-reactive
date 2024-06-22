use tokio::runtime::Runtime;

#[derive(Default, Debug)]
pub struct AktonLaunch {
    pub(crate) runtime: Option<Runtime>,
}

impl AktonLaunch {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn with_runtime(runtime: Runtime) -> Self {
        Self { runtime: Some(runtime) }
    }
}