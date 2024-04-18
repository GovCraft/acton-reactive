use quasar_qrn::{Category, Company, Domain, Part, Parts, QrnBuilder, QrnParser};
use crate::common::Singularity;
use crate::common::{Quasar, QuasarContext, QuasarDormant};

#[derive(Debug)]
pub struct QuasarSystem {
    pub singularity: QuasarContext,
}

impl QuasarSystem {
    pub async fn new() -> Self {
        let system: Quasar<QuasarDormant<Singularity, Self>> = Quasar::new(Default::default(), Singularity);
        QuasarSystem { singularity: Quasar::spawn(system).await }
    }
}