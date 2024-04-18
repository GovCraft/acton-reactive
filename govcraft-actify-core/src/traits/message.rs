use std::any::{Any, TypeId};
use std::fmt::Debug;

//TODO: deprecate
pub trait GovcraftMessage: 'static + Send + Sized {}

trait UserMessage: Any + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;
    fn type_id(&self) -> TypeId { TypeId::of::<Self>() }
}

trait SystemMessage: Any + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;
}
