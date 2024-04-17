use quasar_qrn::{Category, Company, Domain, Part, QrnBuilder};
use crate::common::Singularity;
use crate::common::{Quasar, QuasarContext, QuasarDormant};

pub struct QuasarSystem {
    pub singularity: QuasarContext,
}
impl QuasarSystem {
    pub async fn new() -> Self {
        let qrn = QrnBuilder::new()
            .add::<Domain>("quasar")
            .add::<Category>("system")
            .add::<Company>("framework")
            .add::<Part>("root")
            .build();
        let system: Quasar<QuasarDormant<Singularity, Self>>= Quasar::new(qrn);
        QuasarSystem { singularity: Quasar::spawn(system).await }
    }
}