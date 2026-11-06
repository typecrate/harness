//! `typecrate-harness-server` - the standalone `/v1/harness` listener.
//!
//! The native form of `startHarnessServer` (`packages/underload/harness/src/server/start.ts:102-103`
//! for the bind defaults, `:133-340` for the construction and the close sequence). The library owns
//! the router and the per-root runtime (`src/server.rs`, `src/runtime.rs`); this file is the
//! thinnest possible process wrapper: flags, one bind, the banner, the shutdown signal, and the
//! final durable checkpoint.
//!
//! WHY THE BIN DOES NOT DEPEND ON `typecrate_services`: the gateway integration (T6's `*-route.rs`
//! discovery wiring) is a later stage, and an embedder that serves this mount inside the service
//! gateway never runs this executable. Keeping the bin standalone means `cargo run --bin
//! typecrate-harness-server` works on its own, and a gateway build cannot pull a CLI parse into the
//! library.
//!
//! EXIT CODES are the sibling crate's convention (`packages/typecrate/services/src/bin/server.rs`):
//! 0 clean shutdown, 1 could not serve, 2 bad usage - a supervisor restarts the second and fixes the
//! first. `main` builds the tokio runtime by hand rather than with `#[tokio::main]`, because
//! `[profile.release] panic = "abort"` turns a `main -> Result` into an abort status instead of the
//! promised code.

use std::ffi::OsString;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

use tokio::net::TcpListener;
use typecrate_harness::contract;
use typecrate_harness::package_root;
use typecrate_harness::server;

/// The native bind of the standalone listener (`start.ts:103`, `DEFAULT_HOST`). Loopback on
/// purpose: this server has no auth, and its Origin policy is a policy gate, not a boundary.
const DEFAULT_HOST: &str = "127.0.0.1";

/// The native port (`start.ts:102`, `DEFAULT_PORT`).
const DEFAULT_PORT: u16 = 5181;

/// Exit code for a clean Ctrl+C/SIGTERM shutdown.
const EXIT_OK: i32 = 0;
/// Exit code for anything the process could not do (bind refused, runtime down).
const EXIT_RUNTIME: i32 = 1;
/// Exit code for bad usage.
const EXIT_USAGE: i32 = 2;

const HELP: &str = "\
typecrate-harness-server - the native /v1/harness control plane

USAGE:
    cargo run --bin typecrate-harness-server -- [OPTIONS]
    typecrate-harness-server [OPTIONS]

OPTIONS:
    -h, --host <ip>          Bind address, numeric IPv4 or IPv6 ([::1] accepted,
                             and an inline [<ipv6>]:<port> wins over --port).
                             Default 127.0.0.1 - loopback only, because this server
                             has no authentication and its Origin policy is a policy
                             gate rather than a boundary.
        --port <port>        Port, 0..=65535. Default 5181, the port the standalone
                             listener documents. Pass 0 to take any free port; the
                             banner prints the port actually bound.
        --database-root <dir>
                             Durable state root. <dir>/harness/state.json is the
                             session and definition document and
                             <dir>/harness/sessions/<id>/events.jsonl is one
                             session's journal.
                             Default <repo-parent>/temporary/dump, the same dump
                             boundary the TypeScript service uses.
        --help               Print this help and exit.

WHERE IT ANSWERS:
    GET  /v1/harness           the bootstrap document (identity, limits,
                               capabilities, epoch, links)
    GET  /v1/harness/health    liveness plus the live session count
    GET  /v1/harness/stream    the live update stream (WebSocket upgrade,
                               subprotocol harness.v1)
    ... plus every route the bootstrap document's `links` map and the contract's
    operation table declare; 404 and 405 bodies name the canonical root.

Every JSON response carries the contract's X-Harness-Epoch header, and an
identified read or a write carries X-Harness-Revision.

Exit codes: 0 clean shutdown, 1 could not serve, 2 bad usage.

The process stops on Ctrl+C (and SIGTERM on unix) after draining in-flight
requests and writing its final durable document.";

/// The resolved configuration the process runs on.
#[derive(Debug)]
struct Config {
    /// The bind address, already resolved from `--host` + `--port`.
    address: SocketAddr,
    database_root: PathBuf,
    help: bool,
}

fn main() {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();

    let config = match resolve(&args) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("typecrate-harness-server: {message}\n\n{HELP}");
            std::process::exit(EXIT_USAGE);
        }
    };

    if config.help {
        println!("{HELP}");
        std::process::exit(EXIT_OK);
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("typecrate-harness-server: could not start the tokio runtime: {error}");
            std::process::exit(EXIT_RUNTIME);
        }
    };

    std::process::exit(runtime.block_on(run(config)));
}

/// Parses the flags and applies the built-in defaults. Everything resolves through ONE pure function
/// so the rules are unit-testable without touching the process environment.
fn resolve(args: &[OsString]) -> Result<Config, String> {
    let mut host = DEFAULT_HOST.to_owned();
    let mut port = DEFAULT_PORT;
    let mut database_root: Option<PathBuf> = None;
    let mut help = false;

    let mut index = 0;

    while index < args.len() {
        let flag = args[index]
            .to_str()
            .ok_or_else(|| format!("argument {} is not valid text", index + 1))?;

        index += 1;

        match flag {
            "--help" => help = true,
            // `-h` is the HOST flag, so the help short form is deliberately absent: a CLI that
            // reuses `-h` for two things cannot be scripted.
            "-h" | "--host" => host = take_text(args, &mut index, flag)?,
            "--port" => {
                let raw = take_text(args, &mut index, flag)?;
                port = raw
                    .parse::<u16>()
                    .map_err(|_| format!("--port expects a number in 0..=65535, got `{raw}`"))?;
            }
            "--database-root" => database_root = Some(take_path(args, &mut index, flag)?),
            other => return Err(format!("unknown option `{other}`")),
        }
    }

    // The bind address has to be numeric: `TcpListener::bind` takes a resolved `SocketAddr`, and
    // inventing a DNS step here would make the start depend on a nameserver rather than on what the
    // operator typed.
    let address = match parse_host(&host, port) {
        Ok(address) => address,
        // `--help` short-circuits the VALUE validation: printing usage must not depend on a
        // hostname parsing, which is the same rule the sibling CLI states.
        Err(_) if help => SocketAddr::from(([127, 0, 0, 1], DEFAULT_PORT)),
        Err(message) => return Err(message),
    };

    if help {
        return Ok(Config {
            address,
            database_root: package_root::default_database_root(),
            help: true,
        });
    }

    Ok(Config {
        address,
        database_root: database_root.unwrap_or_else(package_root::default_database_root),
        help: false,
    })
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

/// The refusal every unusable host spells, naming the accepted forms.
const HOST_REFUSAL: &str =
    "--host is not a valid address (expected <ip>, <ip>:<port>, [<ipv6>], or [<ipv6>]:<port>)";

/// Accepts `ip`, `ip:port`, and the bracketed IPv6 spellings `[::1]` / `[::1]:5181`. An inline port
/// wins over `--port`, so the two flags cannot disagree about which one is authoritative (the more
/// specific value - the full address - does). A bare `::1` stays a refusal: without the brackets the
/// value is ambiguous with `host:port`, and this CLI never guesses.
fn parse_host(raw: &str, port: u16) -> Result<SocketAddr, String> {
    if let Ok(address) = raw.parse::<SocketAddr>() {
        return Ok(address);
    }

    if let Some(stripped) = raw
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        && let Ok(ip) = stripped.parse::<IpAddr>()
    {
        return Ok(SocketAddr::new(ip, port));
    }

    if raw.contains(':') {
        return Err(format!("`{raw}` {HOST_REFUSAL}"));
    }

    format!("{raw}:{port}")
        .parse()
        .map_err(|_| format!("`{raw}` {HOST_REFUSAL}"))
}

/// Binds, prints the banner, serves, then checkpoints the durable document.
async fn run(config: Config) -> i32 {
    let listener = match TcpListener::bind(config.address).await {
        Ok(listener) => listener,
        Err(error) if error.kind() == ErrorKind::AddrInUse => {
            eprintln!(
                "typecrate-harness-server: cannot bind {} - address already in use. \
                 Nothing was killed; pass --host/--port or free the port first.",
                config.address
            );
            return EXIT_RUNTIME;
        }
        Err(error) => {
            eprintln!(
                "typecrate-harness-server: cannot bind {}: {error}",
                config.address
            );
            return EXIT_RUNTIME;
        }
    };

    // The bound address is READ back rather than assumed, because `--port 0` asks for any free port.
    let local = match listener.local_addr() {
        Ok(local) => local,
        Err(error) => {
            eprintln!(
                "typecrate-harness-server: bound {} but cannot read its address: {error}",
                config.address
            );
            return EXIT_RUNTIME;
        }
    };

    println!(
        "{} v{} listening on {local}",
        contract::SERVICE_NAME,
        typecrate_harness::version()
    );
    println!("  api:            {}", banner_api(local));
    println!("  stream:         {}", banner_stream(local));
    println!("  database root:  {}", config.database_root.display());

    let outcome = server::serve_on(listener, config.database_root.clone(), shutdown_signal()).await;

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
            println!("typecrate-harness-server: shutdown complete");
            EXIT_OK
        }
        Err(error) => {
            eprintln!("typecrate-harness-server: server error: {error}");
            EXIT_RUNTIME
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

    fn config(args: &[&str]) -> Config {
        resolve(&owned(args)).unwrap_or_else(|error| panic!("`{args:?}` resolves: {error}"))
    }

    #[test]
    fn defaults_match_the_typescript_standalone_start() {
        // `start.ts:102-103`: 127.0.0.1 and 5181, with the library's dump root.
        let resolved = config(&[]);

        assert_eq!(resolved.address.to_string(), "127.0.0.1:5181");
        assert_eq!(
            resolved.database_root,
            package_root::default_database_root()
        );
        assert!(!resolved.help);
    }

    #[test]
    fn a_non_numeric_port_is_bad_usage_not_a_default() {
        assert_eq!(
            resolve(&owned(&["--port", "5181a"])).unwrap_err(),
            "--port expects a number in 0..=65535, got `5181a`"
        );
        assert!(resolve(&owned(&["--port", "70000"])).is_err());
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
    }

    #[test]
    fn an_ipv6_host_and_an_inline_port_are_both_accepted() {
        assert_eq!(
            config(&["--host", "[::1]"]).address.to_string(),
            "[::1]:5181"
        );
        assert_eq!(
            config(&["--host", "[::1]", "--port", "4321"])
                .address
                .to_string(),
            "[::1]:4321"
        );
        // The full address wins when it already names a port.
        assert_eq!(
            config(&["--host", "0.0.0.0:1", "--port", "4321"])
                .address
                .to_string(),
            "0.0.0.0:1"
        );
        assert_eq!(
            resolve(&owned(&["--host", "::1:5199"])).unwrap_err(),
            "`::1:5199` --host is not a valid address (expected <ip>, <ip>:<port>, [<ipv6>], or [<ipv6>]:<port>)"
        );
    }

    #[test]
    fn help_short_circuits_value_validation_but_not_unknown_options() {
        let resolved = config(&["--help", "--host", "not an address"]);

        assert!(resolved.help);
        assert_eq!(resolved.address.to_string(), "127.0.0.1:5181");

        assert_eq!(
            resolve(&owned(&["--help", "--nope"])).unwrap_err(),
            "unknown option `--nope`"
        );
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
}
