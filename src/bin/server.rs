//! `typecrate-harness-server` - the standalone `/v1/harness` listener.
//!
//! The native form of `startHarnessServer` (`packages/underload/harness/src/server/start.ts:160-161`
//! for the bind defaults, `:191-507` for the construction and the close sequence). The library owns
//! the router and the per-root runtime (`src/server.rs`, `src/runtime.rs`); this file is the thinnest
//! possible process wrapper: flags, the runtime-support gate that refuses an execution mode this build
//! does not provide, the bind (ONE bind, as `start.ts:421-440` performs one `server.listen`), the
//! banner, the shutdown signal, and the final durable checkpoint.
//!
//! THE THREE SUBCOMMANDS
//! ---------------------
//! * `serve` (the default, and the form that has always worked: bare flags) - the human listener: it
//!   prints the banner, serves, and on Ctrl+C/SIGTERM drains and checkpoints.
//! * `parity` - the SAME listener for an integration probe: the only thing it writes to stdout is one
//!   `PARITY-LISTEN <json>` line carrying the port it actually bound, the runtime id every
//!   `X-Harness-Epoch` header quotes, and the URLs a client dials. It asks for `auto` by default, so
//!   a probe never races another test for a port, and it exits 0 on SIGTERM, which is what lets the
//!   probe reap it. Diagnostics go to stderr, where they cannot corrupt the line.
//! * `parity-manifest` - no listener at all: writes the machine-readable surface document
//!   (`src/manifest.rs`) to `--out <path>` or to stdout, and exits.
//!
//! WHY THE BIN DOES NOT DEPEND ON `typecrate_services`: the gateway mount is a SEPARATE transport
//! (`endpoint/harness/harness-route.rs`, shipped by T6) that serves the same library dispatcher inside
//! the `typecrate-services` listener, and an embedder that runs that bridge never starts this
//! executable. Keeping the bin standalone means `cargo run --bin typecrate-harness-server` works on
//! its own, and a gateway build cannot pull a CLI parse into the library.
//!
//! EXIT CODES are the reference launchers' convention (`dev.ts:82-105`, `bin/index.ts:370-380`):
//! `process.exitCode` is 0 for a clean shutdown and 1 for EVERY failure - a flag parse error, a bad
//! port, a refused bind - and the value 2 never appears. The sibling crate's `2 = bad usage` was a
//! crate-side invention and is gone: a supervisor that restarted a mistyped invocation was restarting
//! a process that would fail again. A deployment that asked for `HARNESS_*_EXECUTION=worker` is
//! therefore the same code as a typo, and the message is what separates them. `main` builds the tokio
//! runtime by hand rather than with `#[tokio::main]`, because `[profile.release] panic = "abort"`
//! turns a `main -> Result` into an abort status instead of the promised code.

use std::ffi::OsString;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpListener;
use typecrate_harness::config;
use typecrate_harness::contract;
use typecrate_harness::environment;
use typecrate_harness::error::ApiError;
use typecrate_harness::events;
use typecrate_harness::listen::{self, ListenConfig, ListenFlags, ListenPort};
use typecrate_harness::manifest;
use typecrate_harness::package_root;
use typecrate_harness::runtime;
use typecrate_harness::server;

/// Exit code for a clean Ctrl+C/SIGTERM shutdown.
const EXIT_OK: i32 = 0;
/// Exit code for everything the process could not do: bad usage, a refused port, a refused bind,
/// a runtime mode this build does not provide. The reference's launchers use 1 for all four
/// (`dev.ts:88`/`dev.ts:104`, `bin/index.ts:376-380`), so this binary does too.
const EXIT_FAILURE: i32 = 1;

/// The subcommand name that starts the listener explicitly.
const SERVE: &str = "serve";
/// The subcommand a probe spawns.
const PARITY: &str = "parity";
/// The subcommand that writes the surface manifest and exits.
const PARITY_MANIFEST: &str = "parity-manifest";

const HELP: &str = "\
typecrate-harness-server - the native /v1/harness control plane

USAGE:
    cargo run --bin typecrate-harness-server -- [COMMAND] [OPTIONS]
    typecrate-harness-server [COMMAND] [OPTIONS]

COMMANDS:
    (none) | serve        Bind, print the banner, and serve until Ctrl+C or SIGTERM.
    parity               Serve for an integration probe. Asks for a free port
                         (--port auto is the default), prints ONE parseable
                         `PARITY-LISTEN {json}` line on stdout with the bound
                         port and the runtime id, and exits 0 on SIGTERM. Every
                         diagnostic goes to stderr so the line stays parseable.
    parity-manifest      Write the machine-readable surface manifest (the mounted
                         route table, the delivery surface, the listen defaults)
                         and exit. --out <path> writes a file; --out - or no
                         --out writes stdout. No listener, no .env requirement.

OPTIONS:
    -h, --host <host>        Bind host: an IPv4 or IPv6 address ([::1] accepted,
                             and an inline [<ipv6>]:<port> wins over --port) or a
                             hostname such as localhost, which resolves like the
                             reference's `server.listen(port, host)` does.
                             Default 127.0.0.1 - loopback only, because this server
                             has no authentication and its Origin policy is a policy
                             gate rather than a boundary.
                             Also read from HARNESS_HOST.
        --port <port|auto>   Port, 0..=65535, or `auto` for any free one (which is
                             the reference's `--port 0`: the OS picks and the
                             banner prints what it got). Default 5181 for serve,
                             `auto` for parity. A value outside the range is
                             refused with `invalid port: <what you typed>`, the
                             launcher's own sentence.
                             Also read from HARNESS_PORT.
                             A taken port is a refusal, not a search: the
                             reference performs one `server.listen` and rejects on
                             EADDRINUSE (start.ts:421-440), so this bind does too -
                             nothing is killed and the process exits 1.
        --database-root <dir>
                             Durable state root. <dir>/harness/state.json is the
                             session and definition document and
                             <dir>/harness/sessions/<id>/events.jsonl is one
                             session's journal.
                             Default <repo-parent>/temporary/dump, the same dump
                             boundary the TypeScript service uses.
        --static-root <dir>  Serve a built UI beside the API, on the paths the API
                             does NOT own (`/`, `/assets/...`, any extensionless
                             SPA route). Absolute path required. Files under an
                             `assets` directory are served immutable, everything
                             else revalidates. OFF by default - the TypeScript
                             server carries no default either, so an API-only
                             listener is unchanged.
                             Also read from HARNESS_STATIC_ROOT.
        --help               Print this help and exit.

RUNTIME EXECUTION:
    This Rust build serves the inline lane only. A deployment that asks for
    more - HARNESS_THREAD_EXECUTION=worker or HARNESS_IO_EXECUTION=worker - is
    refused HERE, before this process binds: the message is printed and the
    exit code is 1. The same refusal answers the first request of the gateway
    mount with 400. Nothing degrades silently: the TypeScript server treats a
    worker lane as a promise and aborts when it cannot be kept
    (workers/capability.ts, workers/io-client.ts, sessions.ts), and this build
    keeps that promise by refusing rather than by running inline anyway.

WHERE IT ANSWERS:
    GET  /v1/harness           the bootstrap document (identity, limits,
                               capabilities, epoch, links)
    GET  /v1/harness/health    liveness plus the live session count
    GET  /v1/harness/stream    the live update stream (WebSocket upgrade,
                               subprotocol harness.v1)
    GET  /<mirror path>        a mirrored UI file, when --static-root was given
    ... plus every route the bootstrap document's `links` map and the contract's
    operation table declare; 404 and 405 bodies inside the mount name the
    canonical root, and a miss outside it is the reference's plain `Not found`.

Every JSON response carries the contract's X-Harness-Epoch header, and an
identified read or a write carries X-Harness-Revision.

Exit codes: 0 for a clean shutdown, 1 for anything else - bad usage, a bind that
was taken, or a deployment that asked for a runtime mode this build does not
provide (fixing the environment or the flag clears those, restarting does not). The
reference's own launchers set process.exitCode = 1 for all three and never use 2.

The process stops on Ctrl+C (and SIGTERM on unix) after draining in-flight
requests and writing its final durable document.";

/// The resolved configuration the process runs on.
#[derive(Debug)]
struct Config {
    /// What to do: serve, serve for a probe, or write the manifest.
    command: Command,
    /// Host, port and mirror root, resolved through ONE function so the precedence and the
    /// refusals are the library's (`src/listen.rs`) rather than the CLI's.
    listen: ListenConfig,
    /// The delivery half of the router: the static mirror, which is all the reference has.
    delivery: server::Delivery,
    database_root: PathBuf,
    help: bool,
}

/// What the process was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Serve,
    Parity,
    /// `--out` is `None` for stdout.
    ParityManifest {
        out: Option<PathBuf>,
    },
}

fn main() {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();

    let config = match resolve(&args) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("typecrate-harness-server: {message}\n\n{HELP}");
            std::process::exit(EXIT_FAILURE);
        }
    };

    if config.help {
        println!("{HELP}");
        std::process::exit(EXIT_OK);
    }

    // The manifest is a read of the compiled-in table: it needs no provider, no lane and no socket,
    // so it runs BEFORE the support gate. A probe that wants the table must get it even in a
    // deployment whose environment this build would refuse.
    if let Command::ParityManifest { out } = &config.command {
        std::process::exit(write_manifest(out, config.delivery.static_root.as_deref()));
    }

    // THE RUNTIME-SUPPORT GATE, before anything is bound. The TypeScript standalone start aborts on
    // an unusable runtime setting (`start.ts` -> the config loader throws), and this build has no
    // worker backend at all, so a `worker` deployment has to fail HERE rather than quietly serve
    // every turn inline behind the operator's back.
    //
    // Exit 1, not 2: the CLI usage was valid - the DEPLOYMENT asked for something this executable
    // cannot provide - so a supervisor restart does not help and the environment edit does.
    if let Err(error) = startup_support_check() {
        eprintln!("typecrate-harness-server: {}", error.message);

        std::process::exit(EXIT_FAILURE);
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("typecrate-harness-server: could not start the tokio runtime: {error}");
            std::process::exit(EXIT_FAILURE);
        }
    };

    std::process::exit(runtime.block_on(run(config)));
}

/// Writes the surface manifest to a file or to stdout. 0 on success, 1 when the file could not be
/// written - the same "could not do the thing" class as a refused bind.
fn write_manifest(out: &Option<PathBuf>, static_root: Option<&std::path::Path>) -> i32 {
    let text = manifest::manifest_text(static_root);

    match out.as_deref() {
        Some(path) => match std::fs::write(path, &text) {
            Ok(()) => {
                eprintln!(
                    "typecrate-harness-server: wrote the surface manifest to {}",
                    path.display()
                );

                EXIT_OK
            }
            Err(error) => {
                eprintln!(
                    "typecrate-harness-server: could not write `{}`: {error}",
                    path.display()
                );

                EXIT_FAILURE
            }
        },
        None => {
            // stdout carries the document and nothing else, so `parity-manifest > table.json` works.
            print!("{text}");

            EXIT_OK
        }
    }
}

/// Reads the deployment's configuration the same way the library runtime does (`environment`'s
/// `.env` merge, then `config::load_server_config`) and refuses an execution mode this build cannot
/// provide ([`runtime::validate_supported_execution`]).
///
/// The provider refusals ride along for free: a bad `HARNESS_BASE_URL` is a startup abort in
/// TypeScript too, and printing it before the bind is what stops the banner from advertising a
/// server whose first request would fail.
fn startup_support_check() -> Result<(), ApiError> {
    let loaded = config::load_server_config(&environment::harness_runtime_env())?;

    runtime::validate_supported_execution(&loaded)
}

/// The raw flags, before any value is validated. Parsing and validation are separate so `--help` can
/// short-circuit the second half: printing usage must not depend on a port or a host parsing.
#[derive(Debug, Default)]
struct Flags {
    host: Option<String>,
    port: Option<String>,
    static_root: Option<PathBuf>,
    database_root: Option<PathBuf>,
    help: bool,
    out: Option<Option<PathBuf>>,
}

/// Parses the flags, applies the built-in defaults, and resolves the listen configuration through
/// `listen::resolve` (flag > environment > default, with the reference's port refusal).
///
/// The environment is READ HERE and passed down, so `resolve_in` is pure and its unit tests are not
/// affected by whatever `HARNESS_*` value this machine happens to export.
fn resolve(args: &[OsString]) -> Result<Config, String> {
    resolve_in(&environment::harness_runtime_env(), args)
}

/// The pure half: one environment map in, one configuration out.
fn resolve_in(
    env: &std::collections::BTreeMap<String, String>,
    args: &[OsString],
) -> Result<Config, String> {
    let (command_name, rest) = split_subcommand(args);
    let command_label = command_name.unwrap_or(SERVE);
    let flags = parse_flags(rest)?;

    let command = match command_name {
        // No subcommand is the form this CLI has always had: bare flags mean `serve`.
        None => Command::Serve,
        Some(SERVE) => Command::Serve,
        Some(PARITY) => Command::Parity,
        Some(PARITY_MANIFEST) => Command::ParityManifest {
            out: flags.out.clone().flatten(),
        },
        Some(other) => return Err(format!("unknown command `{other}`")),
    };

    if flags.help {
        return Ok(Config {
            command,
            listen: help_defaults(),
            delivery: server::Delivery::default(),
            database_root: package_root::default_database_root(),
            help: true,
        });
    }

    if flags.out.is_some() && command_name != Some(PARITY_MANIFEST) {
        return Err(format!(
            "`--out` belongs to `{PARITY_MANIFEST}`, not to `{command_label}`"
        ));
    }

    // A probe wants a port it does not have to negotiate for; an operator's listener keeps the
    // reference's documented port.
    let default_port = if command == Command::Parity {
        ListenPort::Auto
    } else {
        ListenPort::Fixed(listen::DEFAULT_PORT)
    };

    let listen = listen::resolve(
        env,
        &ListenFlags {
            port: flags.port.clone(),
            host: flags.host.clone(),
            static_root: flags.static_root.clone(),
        },
        default_port,
    )?;

    // The mirror is OFF unless the operator named a directory (`start.ts:396-402`, and `dev.ts:123`
    // passes `staticRoot: null`). That is the whole delivery surface: the reference answers no HTML
    // document of its own, so this launcher has no docs flag either and `Delivery` carries exactly the
    // one member the reference's `staticRoot` has.
    let delivery = server::Delivery {
        static_root: listen.static_root.clone(),
    };

    Ok(Config {
        command,
        listen,
        delivery,
        database_root: flags
            .database_root
            .clone()
            .unwrap_or_else(package_root::default_database_root),
        help: false,
    })
}

/// The `Config` `--help` returns when nothing else parses: the defaults, with no claim about a bind.
fn help_defaults() -> ListenConfig {
    ListenConfig {
        port: ListenPort::Fixed(listen::DEFAULT_PORT),
        host: listen::DEFAULT_HOST.to_owned(),
        static_root: None,
    }
}

/// A leading subcommand, if there is one.
///
/// Only position 0 can be a command, and any non-flag token there IS one: this CLI takes no
/// positional arguments, so `typecrate-harness-server stat` is a mistyped command rather than a
/// mistyped option, and saying so is the shorter path to the fix. A flag value can never be mistaken
/// for a command, because a flag's value is consumed by the flag.
fn split_subcommand(args: &[OsString]) -> (Option<&str>, &[OsString]) {
    let Some(first) = args.first().and_then(|value| value.to_str()) else {
        return (None, args);
    };

    if first.starts_with('-') {
        return (None, args);
    }

    (Some(first), &args[1..])
}

/// Reads the flag spellings, without validating any value.
fn parse_flags(args: &[OsString]) -> Result<Flags, String> {
    let mut flags = Flags::default();
    let mut index = 0;

    while index < args.len() {
        let flag = args[index]
            .to_str()
            .ok_or_else(|| format!("argument {} is not valid text", index + 1))?;

        index += 1;

        match flag {
            "--help" => flags.help = true,
            // `-h` is the HOST flag, so the help short form is deliberately absent: a CLI that
            // reuses `-h` for two things cannot be scripted.
            "-h" | "--host" => flags.host = Some(take_text(args, &mut index, flag)?),
            "--port" => flags.port = Some(take_text(args, &mut index, flag)?),
            "--static-root" => flags.static_root = Some(take_path(args, &mut index, flag)?),
            "--database-root" => flags.database_root = Some(take_path(args, &mut index, flag)?),
            "--out" => {
                // `--out` alone (or `--out -`) is stdout; `--out <path>` is that file.
                flags.out = Some(next_path(args, &mut index));
            }
            other => return Err(format!("unknown option `{other}`")),
        }
    }

    Ok(flags)
}

/// One text flag value, advancing `index`; the name only points the error at the right flag.
fn take_text(args: &[OsString], index: &mut usize, name: &str) -> Result<String, String> {
    let raw = args
        .get(*index)
        .ok_or_else(|| format!("{name} needs a value"))?;

    *index += 1;

    raw.to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("{name} value is not valid text"))
}

/// One path flag value, losslessly: a Windows path can hold bytes that are not valid UTF-8, and the
/// data root has to survive them intact.
fn take_path(args: &[OsString], index: &mut usize, name: &str) -> Result<PathBuf, String> {
    let raw = args
        .get(*index)
        .ok_or_else(|| format!("{name} needs a value"))?;

    *index += 1;

    Ok(PathBuf::from(raw.as_os_str()))
}

/// The OPTIONAL path value of `--out`: absent, or `-` for stdout, both mean `None`.
fn next_path(args: &[OsString], index: &mut usize) -> Option<PathBuf> {
    let next = args.get(*index)?;

    *index += 1;

    if next.as_os_str() == "-" {
        return None;
    }

    Some(PathBuf::from(next.as_os_str()))
}

/// Binds, prints the banner, serves, then checkpoints the durable document.
async fn run(config: Config) -> i32 {
    let address = match config.listen.address() {
        Ok(address) => address,
        Err(message) => {
            eprintln!("typecrate-harness-server: {message}");

            return EXIT_FAILURE;
        }
    };

    // ONE bind, for the address the library resolved. `start.ts:421-440` awaits a single
    // `server.listen(requestedPort, host)` and its `error` event REJECTS the start, so a taken port
    // is a refusal here too: nothing is killed, nothing walks upward, and the code is 1.
    let listener = match TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) if error.kind() == ErrorKind::AddrInUse => {
            eprintln!(
                "typecrate-harness-server: cannot bind {address} - address already in use. \
                 Nothing was killed; pass --host/--port or free the port first."
            );

            return EXIT_FAILURE;
        }
        Err(error) => {
            eprintln!("typecrate-harness-server: cannot bind {address}: {error}");

            return EXIT_FAILURE;
        }
    };

    // The bound address is READ back rather than assumed, because `--port 0` / `auto` asks for any
    // free port (`start.ts:439-440` does the same with `server.address()`).
    let local = match listener.local_addr() {
        Ok(local) => local,
        Err(error) => {
            eprintln!(
                "typecrate-harness-server: bound {address} but cannot read its address: {error}"
            );

            return EXIT_FAILURE;
        }
    };

    if config.command == Command::Parity {
        // ONE line on stdout, parseable, and nothing else on stdout ever. The runtime id is the
        // process epoch, i.e. the exact value every `X-Harness-Epoch` header of this process carries
        // (`src/events.rs`), so a probe can pin a header against the id it was handed.
        let line = listen::listening_line(
            local,
            events::process_epoch(),
            &banner_api(local),
            config.delivery.static_root.as_deref(),
        );

        let mut stdout = tokio::io::stdout();

        // The terminator is written explicitly: `listening_line` returns a bare line, and a probe
        // reading stdout by line would otherwise wait for a newline that never lands.
        if let Err(error) = stdout.write_all(format!("{line}\n").as_bytes()).await {
            eprintln!("typecrate-harness-server: could not print the listen line: {error}");

            return EXIT_FAILURE;
        }

        // And the flush is not decoration: a probe connects after ONE line, so the write has to
        // reach the pipe now rather than whenever this long-lived process next writes.
        if let Err(error) = stdout.flush().await {
            eprintln!("typecrate-harness-server: could not print the listen line: {error}");

            return EXIT_FAILURE;
        }
    } else {
        println!(
            "{} v{} listening on {local}",
            contract::SERVICE_NAME,
            typecrate_harness::version()
        );
        println!("  api:            {}", banner_api(local));
        println!("  stream:         {}", banner_stream(local));

        if let Some(root) = &config.delivery.static_root {
            println!("  static mirror:  {}", root.display());
        }

        println!("  database root:  {}", config.database_root.display());
    }

    let outcome = server::serve_on_with(
        listener,
        config.database_root.clone(),
        config.delivery.clone(),
        shutdown_signal(),
    )
    .await;

    // The close order is the TS one (`start.ts:319-340`): stop the listener first, then make the
    // durable state consistent. `flush` is this crate's `dispose()` + `drainIo()`: the store writes
    // every document synchronously inside the mutation that produced it, so what is left at exit is
    // one final checkpoint of the sessions, the registry rows and the saved provider pair.
    if let Err(error) = server::flush(&config.database_root) {
        eprintln!(
            "typecrate-harness-server: the final state write failed: {}",
            error.message
        );
    }

    match outcome {
        Ok(()) => {
            // Parity keeps stdout clean for the probe, so the shutdown notice is a stderr line.
            if config.command == Command::Parity {
                eprintln!("typecrate-harness-server: shutdown complete");
            } else {
                println!("typecrate-harness-server: shutdown complete");
            }

            EXIT_OK
        }
        Err(error) => {
            eprintln!("typecrate-harness-server: server error: {error}");

            EXIT_FAILURE
        }
    }
}

/// `http://<reachable host>:<port>/v1/harness`. `0.0.0.0` / `::` is a set of interfaces and no
/// client can navigate to it, so a wildcard bind is reported on loopback; an explicit bind is
/// reported as itself, because `http://127.0.0.1:5181/` is a refused connection when the process
/// bound `--host 192.168.1.20`. `SocketAddr::Display` brackets an IPv6 host, which is what makes the
/// IPv6 URL correct without a special case.
fn banner_url(scheme: &str, path: &str, local: SocketAddr) -> String {
    let reported: IpAddr = match local.ip() {
        IpAddr::V4(v4) if v4.is_unspecified() => IpAddr::from([127, 0, 0, 1]),
        IpAddr::V6(v6) if v6.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        other => other,
    };

    format!(
        "{scheme}://{}{path}",
        SocketAddr::new(reported, local.port())
    )
}

fn banner_api(local: SocketAddr) -> String {
    banner_url("http", contract::HARNESS_API_ROOT, local)
}

fn banner_stream(local: SocketAddr) -> String {
    banner_url("ws", contract::HARNESS_WS_STREAM_PATH, local)
}

/// Resolves when the process should stop: Ctrl+C / Ctrl+Break (windows and unix) or SIGTERM, which
/// is what containers send and what a plain `kill <pid>` produces. A signal backend that is
/// unavailable logs why and then waits forever, so one missing signal source cannot stop the server
/// from starting.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("typecrate-harness-server: Ctrl+C unavailable ({error})");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                eprintln!("typecrate-harness-server: SIGTERM unavailable ({error})");
                std::future::pending::<()>().await;
            }
        }
    };

    // Windows has no SIGTERM to subscribe to; the console Ctrl+C/Ctrl+Break branch above is the
    // shutdown path there, so this one never fires.
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }

    eprintln!("typecrate-harness-server: shutdown signal received, draining");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `&[&str]` -> `Vec<OsString>`, i.e. what `std::env::args_os` hands `main`.
    fn owned(args: &[&str]) -> Vec<OsString> {
        args.iter().map(|arg| OsString::from(*arg)).collect()
    }

    /// An EMPTY environment: the CLI tests assert the built-in defaults and the flag rules, and an
    /// ambient `HARNESS_PORT` on the test machine must not move any of them. `resolve_in` is the pure
    /// half of `resolve` precisely so this is possible.
    fn empty_env() -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::new()
    }

    /// The test-side `resolve`, shadowing the process one: same rules, no ambient environment.
    fn resolve(args: &[OsString]) -> Result<Config, String> {
        resolve_in(&empty_env(), args)
    }

    fn config(args: &[&str]) -> Config {
        resolve(&owned(args)).unwrap_or_else(|error| panic!("`{args:?}` resolves: {error}"))
    }

    /// The address a resolved config would bind, asserted as one string.
    fn address(args: &[&str]) -> String {
        config(args).listen.address().unwrap().to_string()
    }

    #[test]
    fn defaults_match_the_typescript_standalone_start() {
        // `start.ts:160-161`: 127.0.0.1 and 5181, with the library's dump root.
        let resolved = config(&[]);

        assert_eq!(address(&[]), "127.0.0.1:5181");
        assert_eq!(resolved.listen.port, ListenPort::Fixed(5181));
        assert_eq!(
            resolved.database_root,
            package_root::default_database_root()
        );
        assert!(!resolved.help);
        // A bare listener serves no mirror (`start.ts:396-402`) and defaults to `serve`; the
        // delivery surface has no other member, because the reference answers no `/docs`.
        assert_eq!(resolved.delivery.static_root, None);
        assert!(!resolved.delivery.serves_outside_mount());
        assert_eq!(resolved.command, Command::Serve);
    }

    /// The port refusal is the launchers sentence (`dev.ts:104`, `cli/index.ts:379`):
    /// `invalid port: ` plus what was typed.
    #[test]
    fn a_non_numeric_port_is_refused_with_the_launchers_sentence() {
        assert_eq!(
            resolve(&owned(&["--port", "5181a"])).unwrap_err(),
            "invalid port: 5181a"
        );
        assert_eq!(
            resolve(&owned(&["--port", "70000"])).unwrap_err(),
            "invalid port: 70000"
        );
        assert_eq!(
            resolve(&owned(&["--port"])).unwrap_err(),
            "--port needs a value"
        );
        assert_eq!(
            resolve(&owned(&["--nope"])).unwrap_err(),
            "unknown option `--nope`"
        );
        assert_eq!(
            resolve(&owned(&["--database-root"])).unwrap_err(),
            "--database-root needs a value"
        );
        // The range boundary and `auto` both parse, and 0 is the same question as `auto`
        // (`dev.ts:98-100`).
        assert_eq!(address(&["--port", "65535"]), "127.0.0.1:65535");
        assert_eq!(address(&["--port", "auto"]), "127.0.0.1:0");
        assert_eq!(address(&["--port", "0"]), "127.0.0.1:0");
    }

    #[test]
    fn an_ipv6_host_and_an_inline_port_are_both_accepted() {
        assert_eq!(address(&["--host", "[::1]"]), "[::1]:5181");
        assert_eq!(
            address(&["--host", "[::1]", "--port", "4321"]),
            "[::1]:4321"
        );
        // The full address wins when it already names a port.
        assert_eq!(
            address(&["--host", "0.0.0.0:1", "--port", "4321"]),
            "0.0.0.0:1"
        );
        // Wave W2: an unbracketed `::1:5199` is a legal IPv6 literal (hextet `5199`), and the host is
        // resolved where the reference resolves it - inside the bind, which is `address()` here. So the
        // CLI accepts it and binds that literal on the default port.
        assert_eq!(address(&["--host", "::1:5199"]), "[::1:5199]:5181");

        // A name that no resolver knows is still a refusal, and it surfaces at the bind rather than at
        // the parse, because a hostname is legal (`server.listen(port, host)` takes one too).
        assert!(
            config(&["--host", "no-such-host.invalid"])
                .listen
                .address()
                .unwrap_err()
                .starts_with("`no-such-host.invalid` --host is not a valid address or hostname")
        );
    }

    #[test]
    fn help_short_circuits_value_validation_but_not_unknown_options() {
        let resolved = config(&["--help", "--host", "not an address"]);

        assert!(resolved.help);
        assert_eq!(
            resolved.listen.address().unwrap().to_string(),
            "127.0.0.1:5181"
        );

        assert_eq!(
            resolve(&owned(&["--help", "--nope"])).unwrap_err(),
            "unknown option `--nope`"
        );
    }

    /// The subcommand surface a probe drives.
    #[test]
    fn the_three_subcommands_resolve_and_only_parity_asks_for_a_free_port() {
        assert_eq!(config(&[]).command, Command::Serve);
        assert_eq!(config(&["serve"]).command, Command::Serve);
        assert_eq!(config(&["parity"]).command, Command::Parity);
        assert_eq!(
            config(&["parity-manifest"]).command,
            Command::ParityManifest { out: None }
        );
        assert_eq!(
            config(&["parity-manifest", "--out", "/tmp/table.json"]).command,
            Command::ParityManifest {
                out: Some(PathBuf::from("/tmp/table.json"))
            }
        );
        // `--out -` is the stdout spelling, and no --out is the same answer.
        assert_eq!(
            config(&["parity-manifest", "--out", "-"]).command,
            Command::ParityManifest { out: None }
        );

        // `serve` keeps 5181; `parity` defaults to `auto` so a probe never negotiates for a port.
        assert_eq!(config(&["parity"]).listen.port, ListenPort::Auto);
        assert_eq!(
            config(&["parity", "--port", "4321"]).listen.port,
            ListenPort::Fixed(4321)
        );

        assert_eq!(
            resolve(&owned(&["nope"])).unwrap_err(),
            "unknown command `nope`"
        );
        // `--out` outside its subcommand is a usage error rather than a silently ignored flag.
        assert_eq!(
            resolve(&owned(&["serve", "--out", "/tmp/x.json"])).unwrap_err(),
            "`--out` belongs to `parity-manifest`, not to `serve`"
        );
    }

    /// The mirror is opt-in, absolute, and reported back through `--help`'s own words.
    #[test]
    fn a_static_root_is_opt_in_and_must_be_absolute() {
        assert_eq!(config(&[]).delivery.static_root, None);

        let mirrored = config(&["--static-root", "/srv/ui/dist"]);

        assert_eq!(
            mirrored.delivery.static_root,
            Some(PathBuf::from("/srv/ui/dist"))
        );
        assert!(mirrored.delivery.serves_outside_mount());

        assert_eq!(
            resolve(&owned(&["--static-root", "relative/dist"])).unwrap_err(),
            format!("{}: `relative/dist`", listen::STATIC_ROOT_REFUSAL)
        );
    }

    /// Wave W2: the free-port walk and the public-origin override had no reference counterpart, so
    /// both flags are refused as unknown options rather than silently ignored.
    #[test]
    fn the_invented_flags_are_refused_as_unknown_options() {
        for flag in ["--port-retry", "--public-base-url", "--no-docs"] {
            assert_eq!(
                resolve(&owned(&[flag])).unwrap_err(),
                format!("unknown option `{flag}`")
            );
        }
    }

    /// The refusal every unusable host spells, kept as one literal so the message the operator SEES
    /// is the message the test asserts.
    const THREAD_WORKER_REFUSAL: &str = concat!(
        "Invalid runtime settings: thread_execution \"worker\" is not supported in this Rust ",
        "build; only \"inline\" is available. Unset HARNESS_THREAD_EXECUTION or set it to ",
        "\"inline\" - this build never runs a \"worker\" deployment inline silently.",
    );

    /// The same for the durable-IO knob.
    const IO_WORKER_REFUSAL: &str = concat!(
        "Invalid runtime settings: io_execution \"worker\" is not supported in this Rust build; ",
        "only \"inline\" is available. Unset HARNESS_IO_EXECUTION or set it to \"inline\" - this ",
        "build never runs a \"worker\" deployment inline silently.",
    );

    /// Both knobs set: one pass, one refusal naming BOTH variables, so the operator fixes them in
    /// one edit (the same aggregation `load_server_config` uses).
    const BOTH_WORKER_REFUSAL: &str = concat!(
        "Invalid runtime settings: thread_execution \"worker\" is not supported in this Rust ",
        "build; only \"inline\" is available. Unset HARNESS_THREAD_EXECUTION or set it to ",
        "\"inline\" - this build never runs a \"worker\" deployment inline silently.",
        "; io_execution \"worker\" is not supported in this Rust build; only \"inline\" is ",
        "available. Unset HARNESS_IO_EXECUTION or set it to \"inline\" - this build never runs a ",
        "\"worker\" deployment inline silently.",
    );

    /// The loader + gate pair `startup_support_check` runs, driven from an EXPLICIT environment map:
    /// writing the process environment is `unsafe` in edition 2024 and this crate forbids unsafe, so
    /// the composition is pinned this way instead of through `set_var`.
    fn support(env: &[(&str, &str)]) -> Result<(), ApiError> {
        let values: std::collections::BTreeMap<String, String> = env
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();

        let loaded = config::load_server_config(&values)?;

        runtime::validate_supported_execution(&loaded)
    }

    #[test]
    fn the_default_deployment_passes_the_startup_support_gate() {
        // An empty environment is `inline`/`inline`, i.e. exactly what this build serves, so the
        // standalone start still binds.
        assert_eq!(support(&[]), Ok(()));

        // The spelled-out `inline` values pass as well - the gate is about the MODE, not about
        // whether the variable happens to be set.
        assert_eq!(
            support(&[
                ("HARNESS_THREAD_EXECUTION", "inline"),
                ("HARNESS_IO_EXECUTION", "inline"),
            ]),
            Ok(())
        );
    }

    #[test]
    fn a_worker_deployment_is_refused_before_the_bind() {
        // `HARNESS_THREAD_EXECUTION=worker`: the PARSE succeeds (`config.rs` ports the TS loader
        // verbatim, so `/config` can still report the value) and the SUPPORT gate refuses it as a 400.
        assert_eq!(
            support(&[("HARNESS_THREAD_EXECUTION", "worker")]),
            Err(ApiError::bad_request(THREAD_WORKER_REFUSAL))
        );
        assert_eq!(
            support(&[("HARNESS_IO_EXECUTION", "worker")]),
            Err(ApiError::bad_request(IO_WORKER_REFUSAL))
        );
        assert_eq!(
            support(&[
                ("HARNESS_THREAD_EXECUTION", "worker"),
                ("HARNESS_IO_EXECUTION", "worker"),
            ]),
            Err(ApiError::bad_request(BOTH_WORKER_REFUSAL))
        );
    }

    #[test]
    fn help_states_that_a_worker_deployment_is_refused_before_the_bind() {
        // `--help` is the offline documentation, so the honesty promise is asserted rather than
        // eyeballed: the mode this build serves, the two variables, and the exit code. HELP is
        // hand-wrapped, so every fragment below sits on ONE printed line.
        assert!(HELP.contains("serves the inline lane only"));
        assert!(HELP.contains("HARNESS_THREAD_EXECUTION=worker"));
        assert!(HELP.contains("HARNESS_IO_EXECUTION=worker"));
        assert!(HELP.contains("before this process binds"));
        assert!(HELP.contains("exit code is 1"));
        assert!(HELP.contains("Nothing degrades silently"));
        // Wave W2: one failure code, like the reference's launchers, so `--help` says exactly that.
        assert!(HELP.contains("1 for anything else - bad usage, a bind that"));
        assert!(!HELP.contains("2 bad usage"));
    }

    /// `--help` also has to document the delivery surface and the probe surface it now offers.
    #[test]
    fn help_documents_the_delivery_surface_and_the_probe_commands() {
        for fact in [
            "parity-manifest",
            "PARITY-LISTEN",
            "--static-root",
            "--port <port|auto>",
            "HARNESS_STATIC_ROOT",
        ] {
            assert!(HELP.contains(fact), "`--help` never mentions `{fact}`");
        }
    }

    #[test]
    fn the_banner_reports_urls_a_client_can_open() {
        let wildcard: SocketAddr = "0.0.0.0:5181".parse().unwrap();

        assert_eq!(
            banner_api(wildcard),
            format!("http://127.0.0.1:5181{}", contract::HARNESS_API_ROOT)
        );
        assert_eq!(
            banner_stream(wildcard),
            format!("ws://127.0.0.1:5181{}", contract::HARNESS_WS_STREAM_PATH)
        );

        // An explicit bind is reported as itself, and an IPv6 bind keeps its brackets.
        let explicit: SocketAddr = "192.168.1.20:5181".parse().unwrap();

        assert_eq!(banner_api(explicit), "http://192.168.1.20:5181/v1/harness");

        let six: SocketAddr = "[::1]:5181".parse().unwrap();

        assert_eq!(banner_api(six), "http://[::1]:5181/v1/harness");
    }

    /// The single bind is proven on a REAL socket: a listener holds the requested port, and the
    /// bind refuses rather than moving - `start.ts:421-440` rejects on EADDRINUSE and kills nothing.
    #[tokio::test]
    async fn a_taken_port_refuses_without_moving() {
        // Ask the OS for a port, then hold it, so the test never collides with another process.
        let held = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral port");
        let taken: SocketAddr = held
            .local_addr()
            .expect("the held listener reports its port");

        let refused = TcpListener::bind(taken).await;
        let error = match refused {
            Ok(_) => panic!("a port that is held cannot bind"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            ErrorKind::AddrInUse,
            "the OS reports the collision the operator has to free"
        );

        drop(held);

        // Freed, the same address binds at once: the refusal was the collision, not the address.
        let free = TcpListener::bind(taken)
            .await
            .expect("the released port binds");

        assert_eq!(free.local_addr().expect("the bind reports its port"), taken);
    }

    /// The manifest document the `parity-manifest` subcommand writes is the library's own, so the
    /// bin cannot quietly print a different table than the router mounts.
    #[test]
    fn the_manifest_the_bin_writes_is_the_library_document() {
        let text = manifest::manifest_text(Some(std::path::Path::new("/srv/ui/dist")));
        let parsed: serde_json::Value =
            serde_json::from_str(&text).expect("the manifest text is JSON");

        assert_eq!(parsed["counts"]["templates"], serde_json::json!(52));
        assert_eq!(
            parsed["delivery_surface"]["static_mirror"]["root"],
            serde_json::json!("/srv/ui/dist")
        );
    }
}
