use crate::common::GalacticCore;
use crate::common::{Quasar, EntanglementLink, QuasarDormant};

#[derive(Debug)]
pub struct QuasarCore {
    pub entanglement_link: EntanglementLink,
}

impl QuasarCore {
    pub async fn spawn() -> Self {
        let system: Quasar<QuasarDormant<GalacticCore, Self>> = Quasar::new(Default::default(), GalacticCore);
        QuasarCore { entanglement_link: Quasar::spawn(system).await }
    }
}