//! Process-wide environment serialization for the kit test binary.

use std::sync::{Mutex, MutexGuard};

pub(crate) static ENV_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn lock_env() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) struct RestoreEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl RestoreEnv {
    /// Snapshot every name, clear it, and restore the prior process values on drop.
    pub(crate) fn clear(keys: &[&'static str]) -> Self {
        let restore = Self(
            keys.iter()
                .map(|key| (*key, std::env::var_os(key)))
                .collect(),
        );
        for key in keys {
            unsafe { std::env::remove_var(key) };
        }
        restore
    }
}

impl Drop for RestoreEnv {
    fn drop(&mut self) {
        for (key, prior) in self.0.drain(..) {
            unsafe {
                if let Some(value) = prior {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }
}
