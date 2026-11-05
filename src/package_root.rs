//! The package root and the paths derived from it.
//!
//! TypeScript origin: `packages/underload/harness/src/server/package-root.ts`, plus the dump
//! root of `packages/underload/service/server.ts:43`.
//!
//! WHY the root is discovered rather than computed from a module-relative depth
//! (`package-root.ts:3-20`): Vite's asset plugin statically rewrites
//! `new URL('<literal neighbouring file>', import.meta.url)` at build time, and when the
//! neighbouring path is the package `.env` that rewrite INLINES the file - with its keys - into
//! the published bundle. A module-relative depth is also wrong after bundling, because the same
//! module evaluates from `src/server/` in the source tree but from a shared chunk in `dist/`.
//! TS therefore walks up to the nearest `package.json`.
//!
//! The Rust port has a stronger answer for the same requirement: `CARGO_MANIFEST_DIR` is baked
//! in at compile time as an ABSOLUTE path to the directory holding `Cargo.toml`, which is the
//! same directory that holds `package.json`. There is no bundle layout to disagree with, so the
//! walk is unnecessary - and it must NOT be replaced by a cwd lookup, because the contract
//! requires the same `.env` for a workspace-root launch and for a package-directory launch.

use std::path::{Component, Path, PathBuf};

/// The package root: the directory holding this crate's `Cargo.toml` (and its `package.json`).
///
/// The TS equivalent is `PACKAGE_ROOT` (`package-root.ts:50`).
#[must_use]
pub fn package_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// Join a package-relative path, e.g. `.env` or `src/frontend/dist`
/// (`package-root.ts:53` `packagePath`).
#[must_use]
pub fn package_path(segments: &[&str]) -> PathBuf {
    let mut path = package_root().to_path_buf();

    for segment in segments {
        path.push(segment);
    }

    path
}

/// The monorepo root: the nearest ancestor whose `package.json` declares `"workspaces"`.
///
/// The package root itself has a `package.json` (that is what makes it the package root), so
/// the walk looks for the ONE marker the TS root has: this repo's
/// `package.json` carries a `workspaces` array (`package.json:9-26`), while a workspace member's
/// does not. Falling back to four parents keeps the answer stable when the tree is copied
/// without the root manifest - the same "bounded, cannot loop or throw" policy as
/// `package-root.ts:32-44`.
#[must_use]
pub fn repo_root() -> PathBuf {
    let manifest = package_root();
    let mut directory = manifest;

    while let Some(candidate) = directory.parent() {
        if declares_workspaces(&candidate.join("package.json")) {
            return candidate.to_path_buf();
        }

        directory = candidate;
    }

    // Four levels up == the repo root of `packages/<scope>/<name>`; kept as the documented
    // fallback so a partial checkout still resolves a usable path instead of the filesystem root.
    let fallback = manifest.join("../../../../");

    normalize_lexically(&fallback)
}

/// True when the file parses as JSON with a `workspaces` key.
///
/// Deliberately tolerant: an unreadable or malformed manifest is "not the root", never an
/// error, because a wrong answer here only moves a default path.
fn declares_workspaces(manifest: &Path) -> bool {
    let Ok(text) = std::fs::read_to_string(manifest) else {
        return false;
    };
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) else {
        return false;
    };

    parsed.get("workspaces").is_some()
}

/// The default writable data root: `<repo-root>/../temporary/dump`, lexically normalized.
///
/// `packages/underload/service/server.ts:43` resolves `path.join(<repo-root>, '..',
/// 'temporary', 'dump')`, i.e. a sibling OF the repo root, so on this machine the answer is
/// `D:\Projects\typescript\temporary\dump`. Both runtimes therefore read and write the SAME
/// dump directory, which is the point of porting the rule rather than approximating it.
///
/// The directory is NOT created here: the root is a boundary, and whoever writes first creates
/// its own folder below it (the same policy as the TS `variables: { root }`).
#[must_use]
pub fn default_database_root() -> PathBuf {
    let root = repo_root();
    let candidate = root.join("..").join("temporary").join("dump");

    normalize_lexically(&candidate)
}

/// Lexical `.`/`..` resolution with NO filesystem access.
///
/// `Path::canonicalize` would require the target to exist (the dump directory is created
/// lazily by its first writer) and on Windows it also strips the verbatim prefix, which makes
/// logged paths confusing. So paths are cleaned by component: `.` is dropped, `..` pops a real
/// segment (at the top of an absolute path a further `..` is dropped, while a leading `..` on a
/// relative path survives because there is nothing to resolve it against yet), and everything
/// else is copied through. The same rules `packages/typecrate/services/src/server/mod.rs:119`
/// applies to its own default root.
#[must_use]
pub fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();

    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                // Only a real segment can be popped; `Prefix`/`RootDir` mean we are already at
                // the top, and an empty `out` means the `..` is relative and must survive.
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                Some(Component::Prefix(_)) | Some(Component::RootDir) => {}
                _ => out.push(component.as_os_str()),
            },
            other => out.push(other.as_os_str()),
        }
    }

    out
}
