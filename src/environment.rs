//! Automatic `.env` loading for the Rust entry points.
//!
//! TypeScript origin: `packages/underload/harness/src/server/environment.ts`.
//!
//! SCOPE (`environment.ts:3-9`): only the server/CLI entry imports this, never the library
//! graph, because importing the harness must not read a user's files. The Rust bin target is
//! the equivalent boundary, and the primitives below stay side-effect-free until a caller asks
//! for them.
//!
//! LOCATION (`environment.ts:11-21`): the default file is PACKAGE-relative
//! (`<package root>/.env`), never cwd-relative, so a workspace-root launch and a
//! package-directory launch read the same file.
//!
//! PRECEDENCE (`environment.ts:23-27`): dotenv's default `override: false` leaves an existing
//! process value untouched, INCLUDING an explicitly empty one, so shell/exported values and CLI
//! flags remain authoritative and the file only fills gaps. A missing file is harmless: every
//! setting has a fallback and the search keys are optional.
//!
//! LOADED VALUES ARE NEVER RETURNED in the status struct (`environment.ts:52-57`): a caller
//! cannot accidentally log a secret alongside the result. The parsed map lives in the private
//! cache and reaches a caller only through [`harness_runtime_env`], which the config loader
//! consumes.
//!
//! DEVIATION (deliberate, recorded in knowledge.md): `HARNESS_ENV_DEBUG=1` prints the resolved
//! path and the load result. TS `environment.ts:61-64` is deliberately SILENT (dotenv's
//! `quiet: true`), so this opt-in exists only for a deployment diagnosing "which file did it
//! read", and it is read from the PROCESS environment, never from the file being loaded.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::package_root;

/// The result of one `.env` load: where it looked, whether it read, and why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessEnvironment {
    /// The file that was read (or the file that was missing).
    pub path: PathBuf,
    /// True only when the file was read and parsed.
    pub loaded: bool,
    /// The load failure, reduced to text so a missing or unreadable file cannot crash a cold
    /// start (`environment.ts:63-64`). Carries no values, so it can never hold a secret.
    pub error: Option<String>,
}

/// One cached load plus the parsed values behind it.
struct CachedEnvironment {
    status: HarnessEnvironment,
    values: BTreeMap<String, String>,
}

/// The load cache. `OnceLock<Mutex<Option<..>>>` rather than a plain `OnceLock`, because the
/// whole point of [`clear_harness_environment_cache`] is that a test can re-load after pointing
/// at a fixture.
fn cache() -> &'static Mutex<Option<CachedEnvironment>> {
    static CACHE: OnceLock<Mutex<Option<CachedEnvironment>>> = OnceLock::new();

    CACHE.get_or_init(|| Mutex::new(None))
}

/// The path that is currently authoritative: a test override, or the package-root `.env`
/// (`environment.ts:40` `ENVIRONMENT_PATH`).
fn effective_path() -> PathBuf {
    path_override_slot()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
        .unwrap_or_else(|| package_root::package_path(&[".env"]))
}

/// Test/tooling override of the `.env` location (`environment.ts:43` `EnvironmentFileOptions.path`).
static PATH_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

fn path_override_slot() -> &'static Mutex<Option<PathBuf>> {
    PATH_OVERRIDE.get_or_init(|| Mutex::new(None))
}

/// Points the package-root loader at another file, e.g. a fixture under the repo `Temp/`
/// directory. `None` restores the package-root default. Clears the cache, because a stale cache
/// would silently keep serving the previous file.
pub fn set_harness_environment_path(path: Option<PathBuf>) {
    *path_override_slot()
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = path;

    clear_harness_environment_cache();
}

/// Reads and parses ONE `.env` file, filling (never overriding) the process environment.
///
/// Mirrors `loadEnvironmentFile({ path })` (`environment.ts:59-73`). The parsed pairs are not
/// returned: precedence is observable through the process environment and through
/// [`harness_runtime_env`].
#[must_use]
pub fn load_environment_file(path: &Path) -> HarnessEnvironment {
    // `from_path` uses dotenvy's default precedence, which is the non-override behaviour TS
    // relies on: an existing process value - including an explicitly empty one - wins.
    match dotenvy::from_path(path) {
        Ok(()) => HarnessEnvironment {
            path: path.to_path_buf(),
            loaded: true,
            error: None,
        },
        Err(error) => HarnessEnvironment {
            path: path.to_path_buf(),
            loaded: false,
            // A missing file is the normal case, and its message is kept for the debug print
            // only: nothing on this path is fatal.
            error: Some(error.to_string()),
        },
    }
}

/// Parses the pairs one `.env` file holds, without touching the process environment.
///
/// `None` when the file cannot be read at all; an empty map when it reads but holds nothing.
fn parse_environment_file(path: &Path) -> Option<BTreeMap<String, String>> {
    let entries = dotenvy::from_path_iter(path).ok()?;
    let mut values = BTreeMap::new();

    for entry in entries.flatten() {
        values.insert(entry.0, entry.1);
    }

    Some(values)
}

/// Loads the package-root `.env` (or the [`set_harness_environment_path`] override) exactly
/// once per process, and reports what happened.
///
/// Failure is recorded, never fatal. Calling this twice is free: the second call returns the
/// cached status, which is what makes "loading must not print counts, paths, or values"
/// (`environment.ts:61-64`) true for a normal start.
#[must_use]
pub fn load_harness_environment() -> HarnessEnvironment {
    if let Some(cached) = cache()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .as_ref()
    {
        return cached.status.clone();
    }

    let path = effective_path();
    let status = load_environment_file(&path);
    let values = parse_environment_file(&path).unwrap_or_default();

    // The escape hatch for tooling: `HARNESS_ENV_DEBUG=1` prints the resolved path and the
    // result, and nothing prints values.
    if process_flag("HARNESS_ENV_DEBUG") {
        match &status.error {
            None => println!("harness env: loaded {}", path.display()),
            Some(error) => println!("harness env: {} not loaded ({error})", path.display()),
        }
    }

    *cache().lock().unwrap_or_else(|error| error.into_inner()) = Some(CachedEnvironment {
        status: status.clone(),
        values,
    });

    status
}

/// The environment a run should read: the loaded `.env` values with the PROCESS environment
/// taking precedence (`environment.ts:23-27` stated as a merge rather than a mutation, so a
/// caller can read the effective configuration without the file having been injected).
#[must_use]
pub fn harness_runtime_env() -> BTreeMap<String, String> {
    let values = {
        // Ensures the one-time load has happened before the merge. The status itself is not the
        // answer here - the merged map is - so the returned value is deliberately dropped.
        let _status = load_harness_environment();

        cache()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(|cached| cached.values.clone())
            .unwrap_or_default()
    };

    let mut effective = process_environment();

    for (key, value) in values {
        // `entry().or_insert()` states the no-override rule the TS comment makes contractual:
        // an existing process value - including an explicitly empty one - is never replaced.
        effective.entry(key).or_insert(value);
    }

    effective
}

/// A snapshot of the process environment as a map. Non-UTF-8 pairs are skipped, because every
/// consumer here is a text setting.
#[must_use]
pub fn process_environment() -> BTreeMap<String, String> {
    std::env::vars_os()
        .filter_map(|(key, value)| {
            let key = key.into_string().ok()?;
            let value = value.into_string().ok()?;

            Some((key, value))
        })
        .collect()
}

/// Drops the cached `.env` load so the next read re-opens the file. For tests only: production
/// loads exactly once, which is what keeps a cold start side-effect free.
pub fn clear_harness_environment_cache() {
    *cache().lock().unwrap_or_else(|error| error.into_inner()) = None;
}

/// True when a process variable is set to `1` (trimmed), i.e. an opt-in a deployment turns on
/// explicitly rather than by leaving a stray value behind.
fn process_flag(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .is_some_and(|value| value.trim() == "1")
}
