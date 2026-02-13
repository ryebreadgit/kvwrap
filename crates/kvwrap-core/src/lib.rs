mod backend;
mod config;
mod error;
mod traits;
mod watch;

pub use config::LocalConfig;
pub use error::{Error, Result};
pub use traits::KvStore;
pub use watch::{WatchEvent, WatchRegistry};

#[cfg(feature = "fjall")]
pub use backend::FjallStore;
#[cfg(feature = "sled")]
pub use backend::SledStore;

#[cfg(feature = "fjall")]
pub type LocalStore = FjallStore;

#[cfg(all(feature = "sled", not(feature = "fjall")))]
pub type LocalStore = SledStore;
