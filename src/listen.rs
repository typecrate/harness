//! Listen configuration: one host, one port, one bind — the reference's shape.
//!
//! Reference: `packages/underload/harness/src/server/start.ts:160-161` (`DEFAULT_PORT = 5181`,
//! `DEFAULT_HOST = '127.0.0.1'`), `start.ts:421-440` (ONE `server.listen(requestedPort, host)`,
//! whose `error` event rejects the start), `start.ts:444` (`url` reports `localhost` for a
//! `0.0.0.0` bind); the port refusal sentence is `dev.ts:96-105` (`invalid port: <what you typed>`)
//! and `bin/index.ts:370-380` (the same sentence from the packaged CLI).
//!
//! WHAT THIS MODULE OWNS
//! ---------------------
//! * precedence — flag > environment > built-in default — so the executable and an embedder cannot
//!   disagree about what `--port`/`HARNESS_PORT` meant;
//! * the refusals, kept as one literal each so the message the operator reads is the message the
//!   tests assert (`invalid port: ` and `STATIC_ROOT_REFUSAL`);
//! * `auto`, which is this crate's spelling of the reference's `--port 0` (`dev.ts:98-100`: "0 is
//!   valid and means let the OS pick a free port"); the bound port is always read back off the
//!   socket rather than assumed.
//!
//! ## Parity note (delivery surface, wave W2)
//!
//! The module previously carried a `public_base_url` value (flag `--public-base-url`, environment
//! `HARNESS_PUBLIC_BASE_URL`) and the executable carried a free-port walk (`--port-retry`). Neither
//! has a reference counterpart: `grep -rn publicBaseUrl packages/underload/harness/src` is empty, and
//! `start.ts:421-440` performs exactly one `listen` whose `EADDRINUSE` rejects the start — there is
//! no retry loop in the TypeScript standalone. Both are gone, so the resolved configuration is the
//! pair the reference resolves, plus the mirror root the router needs.
//!
//! The HOST is passed through as the reference passes it: `server.listen(port, host)` accepts a
//! hostname, so `localhost` is legal here too. A name resolves through the platform resolver and the
//! first IPv4 candidate wins, which is what a loopback hostname (`localhost`, or an `/etc/hosts`
//! entry) yields.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};

/// Default port of the standalone listener: the reference's `DEFAULT_PORT` (`start.ts:160`).
pub const DEFAULT_PORT: u16 = 5181;

/// Default bind host of the standalone listener: the reference's `DEFAULT_HOST` (`start.ts:161`).
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// Highest port the CLI accepts: the top of a TCP port number.
pub const MAX_PORT: u16 = 65_535;

/// The port value that means "let the OS pick", i.e. the reference's `--port 0` (`dev.ts:98-100`).
pub const AUTO_PORT: u16 = 0;

/// The word a human types for [`AUTO_PORT`]; both spellings mean the same bind.
pub const AUTO: &str = "auto";

/// Environment override for the port (`harnessEnvironmentSchema`).
pub const PORT_ENV: &str = "HARNESS_PORT";

/// Environment override for the host.
pub const HOST_ENV: &str = "HARNESS_HOST";

/// Environment override for the built-UI mirror root.
pub const STATIC_ROOT_ENV: &str = "HARNESS_STATIC_ROOT";

/// The one marker a probe waits for on stdout. It is a literal rather than an inline string because
/// the probe test asserts equality against it, so a rename cannot silently break the contract.
pub const LISTENING: &str = "PARITY-LISTEN";

/// The refusal a mirror root that is not absolute gets. Kept as one literal for the same reason.
pub const STATIC_ROOT_REFUSAL: &str = "static root must be an absolute path";

/// What port to bind: an explicit one, or "any free one".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenPort {
    /// An explicit port, 1..=[`MAX_PORT`].
    Fixed(u16),
    /// The OS picks (`--port 0`, spelled `auto` for humans).
    Auto,
}

impl ListenPort {
    /// The port number to hand the bind: `0` for [`Self::Auto`].
    #[must_use]
    pub const fn value(self) -> u16 {
        match self {
            Self::Fixed(port) => port,
            Self::Auto => AUTO_PORT,
        }
    }
}

/// The raw listen flags, before precedence and validation.
#[derive(Clone, Debug, Default)]
pub struct ListenFlags {
    /// `--port <port|auto>`, as typed.
    pub port: Option<String>,
    /// `--host <host>`, as typed.
    pub host: Option<String>,
    /// `--static-root <dir>`.
    pub static_root: Option<PathBuf>,
}

/// The resolved listen configuration: the pair the reference resolves, plus the mirror root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenConfig {
    /// Port to bind, or `Auto`.
    pub port: ListenPort,
    /// Host as configured: an address (`"127.0.0.1"`, `"[::1]"`) or a hostname (`"localhost"`).
    pub host: String,
    /// The built-UI mirror root, `None` when the mirror is OFF (`start.ts:396-402`).
    pub static_root: Option<PathBuf>,
}

impl ListenConfig {
    /// The socket address to bind.
    ///
    /// Resolution is lazy on purpose: `--help` must print without a port parsing or a DNS lookup, and
    /// an unparseable host is a USAGE refusal the caller reports before binding. A hostname goes
    /// through the platform resolver here, which is the moment the lookup can actually fail.
    ///
    /// An inline `[ipv6]:port` or `ipv4:port` in the host wins over `--port`, because a full address
    /// is the more specific statement an operator made.
    pub fn address(&self) -> Result<SocketAddr, String> {
        resolve_host_port(&self.host, self.port.value())
    }
}

/// Resolves one host string and one default port into a bind address.
fn resolve_host_port(host: &str, default_port: u16) -> Result<SocketAddr, String> {
    let refusal = || {
        format!(
            "`{host}` --host is not a valid address or hostname \
             (expected <host>, <host>:<port>, [<ipv6>], or [<ipv6>]:<port>)"
        )
    };

    if host.is_empty() {
        return Err(refusal());
    }

    // 1. Split an inline port. Brackets are the IPv6 literal marker and are never part of the host,
    //    so `[::1]:4321` splits into `::1` and 4321 while `::1:5199` (unbracketed, two colons after
    //    the last one) is the address the operator has to write as `[::1]:5199`.
    let (name, inline_port) = split_inline_port(host);

    // 2. A numeric address, bracketed or not.
    if let Ok(address) = name.parse::<IpAddr>() {
        return Ok(SocketAddr::new(
            address,
            inline_port.unwrap_or(default_port),
        ));
    }

    if inline_port.is_some() && name.contains(':') {
        // `::1:5199`: it is address-shaped but parses as neither, so the inline-port branch has
        // nothing left to try and DNS will not save it either.
        return Err(refusal());
    }

    // 3. A hostname — the reference hands the string to Node, which resolves it. Prefer the first
    //    IPv4 candidate so a dual-stack `localhost` binds v4, as the documented default does.
    let port = inline_port.unwrap_or(default_port);
    let candidates = (name, port)
        .to_socket_addrs()
        .map_err(|_| refusal())?
        .collect::<Vec<_>>();

    candidates
        .iter()
        .find(|candidate| candidate.ip().is_ipv4())
        .or_else(|| candidates.first())
        .copied()
        .ok_or_else(refusal)
}

/// Splits `host[:port]`, honouring a bracketed IPv6 literal. Returns `(name, port)`; the port is
/// `None` when the host did not name one.
fn split_inline_port(host: &str) -> (&str, Option<u16>) {
    if let Some(rest) = host.strip_prefix('[') {
        // Bracketed: the port may only follow the closing bracket.
        let Some((literal, after)) = rest.split_once(']') else {
            return (host, None);
        };

        let port = after
            .strip_prefix(':')
            .and_then(|value| value.parse::<u16>().ok());

        return (literal, port);
    }

    // Unbracketed: an IPv6 literal holds colons and cannot carry a port, so a host with more than
    // one colon is taken whole; a single colon splits a v4/hostname port.
    if host.matches(':').count() > 1 {
        return (host, None);
    }

    match host.split_once(':') {
        Some((name, raw)) => (name, raw.parse::<u16>().ok()),
        None => (host, None),
    }
}

/// Resolves flags + environment + default into the configuration the process runs on.
///
/// The environment is READ by the caller and passed in, so this stays pure and its tests are not
/// moved by whatever `HARNESS_*` value the machine happens to export.
pub fn resolve(
    env: &BTreeMap<String, String>,
    flags: &ListenFlags,
    default_port: ListenPort,
) -> Result<ListenConfig, String> {
    let port = match flags
        .port
        .as_deref()
        .or_else(|| env.get(PORT_ENV).map(String::as_str))
    {
        Some(raw) => parse_port(raw)?,
        None => default_port,
    };

    let host = flags
        .host
        .as_deref()
        .or_else(|| env.get(HOST_ENV).map(String::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_HOST)
        .to_owned();

    let static_root = flags
        .static_root
        .clone()
        .or_else(|| env.get(STATIC_ROOT_ENV).map(PathBuf::from))
        .filter(|root| !root.as_os_str().is_empty());

    if let Some(root) = &static_root
        && !root.is_absolute()
    {
        // The mirror root is a directory the router serves at `/`; a relative one resolves against
        // the listener's working directory, which a supervisor does not control, so it is refused.
        return Err(format!("{STATIC_ROOT_REFUSAL}: `{}`", root.display()));
    }

    Ok(ListenConfig {
        port,
        host,
        static_root,
    })
}

/// Parses one port token: `auto` / `0` mean "any free one", everything else must be a real TCP port.
///
/// The refusal is the launcher's own sentence (`dev.ts:104`): `invalid port: ` plus the value that
/// was typed, because "5181a" and "70000" need different fixes and the operator has to see which one
/// they typed.
fn parse_port(raw: &str) -> Result<ListenPort, String> {
    let raw = raw.trim();

    if raw.eq_ignore_ascii_case(AUTO) {
        return Ok(ListenPort::Auto);
    }

    let port = raw
        .parse::<u16>()
        .map_err(|_| format!("invalid port: {raw}"))?;

    // `dev.ts:98-100` treats 0 as "let the OS pick", which is this crate's `auto`; the range check is
    // implicit because `u16` already refuses 65536 and above.
    if port == AUTO_PORT {
        return Ok(ListenPort::Auto);
    }

    Ok(ListenPort::Fixed(port))
}

/// The single parseable line a probe waits for: `PARITY-LISTEN <json>`.
///
/// It carries the port the socket ACTUALLY bound (`--port 0`/`auto` never reports the sentinel), the
/// host it bound, the two URLs a client dials, and `runtime_id`, the process epoch that every
/// `X-Harness-Epoch` header quotes (`src/events.rs`). `static_root` appears only when the mirror is
/// on, so an absent key means the listener serves the contract surface alone.
pub fn listening_line(
    local: SocketAddr,
    runtime_id: &str,
    api: &str,
    static_root: Option<&Path>,
) -> String {
    let stream = format!(
        "ws://{}{}",
        // `SocketAddr::Display` brackets an IPv6 host, which is what makes the URL correct without
        // a special case.
        local,
        crate::contract::HARNESS_WS_STREAM_PATH
    );

    let document = match static_root {
        Some(root) => serde_json::json!({
            "port": local.port(),
            "host": local.ip().to_string(),
            "api": api,
            "stream": stream,
            "runtime_id": runtime_id,
            "static_root": root.display().to_string(),
        }),
        None => serde_json::json!({
            "port": local.port(),
            "host": local.ip().to_string(),
            "api": api,
            "stream": stream,
            "runtime_id": runtime_id,
        }),
    };

    format!("{LISTENING} {document}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn address(host: Option<&str>, port: Option<&str>) -> String {
        let flags = ListenFlags {
            port: port.map(str::to_owned),
            host: host.map(str::to_owned),
            static_root: None,
        };

        resolve(&env(&[]), &flags, ListenPort::Fixed(DEFAULT_PORT))
            .expect("it resolves")
            .address()
            .expect("it binds")
            .to_string()
    }

    #[test]
    fn the_defaults_are_the_references_port_and_host() {
        // `start.ts:160-161`.
        let config = resolve(
            &env(&[]),
            &ListenFlags::default(),
            ListenPort::Fixed(DEFAULT_PORT),
        )
        .expect("the defaults resolve");

        assert_eq!(config.port, ListenPort::Fixed(5181));
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(
            config.address().expect("a bind address").to_string(),
            "127.0.0.1:5181"
        );
        assert_eq!(config.static_root, None);
    }

    #[test]
    fn a_flag_beats_the_environment_and_both_beat_the_default() {
        let flags = ListenFlags {
            port: Some("4321".to_owned()),
            ..ListenFlags::default()
        };
        let values = env(&[
            (PORT_ENV, "9999"),
            (HOST_ENV, "0.0.0.0"),
            (STATIC_ROOT_ENV, "/from/env"),
        ]);
        let config = resolve(&values, &flags, ListenPort::Auto).expect("it resolves");

        assert_eq!(config.port, ListenPort::Fixed(4321));
        assert_eq!(config.address().expect("binds").port(), 4321);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.static_root, Some(PathBuf::from("/from/env")));

        // The same environment without the flag: the environment answers.
        let config =
            resolve(&values, &ListenFlags::default(), ListenPort::Auto).expect("it resolves");

        assert_eq!(config.port, ListenPort::Fixed(9999));

        // And an empty environment falls back to the default the caller passed (a probe's `Auto`).
        let config = resolve(&BTreeMap::new(), &ListenFlags::default(), ListenPort::Auto)
            .expect("it resolves");

        assert_eq!(config.port, ListenPort::Auto);
        assert_eq!(config.address().expect("binds").port(), 0);
    }

    /// The launcher's own refusal (`dev.ts:104`), and both spellings of "any free port".
    #[test]
    fn a_port_is_parsed_or_refused_with_the_launchers_sentence() {
        assert_eq!(parse_port("auto"), Ok(ListenPort::Auto));
        assert_eq!(parse_port("0"), Ok(ListenPort::Auto));
        assert_eq!(parse_port("65535"), Ok(ListenPort::Fixed(65535)));
        assert_eq!(parse_port("5181a"), Err("invalid port: 5181a".to_owned()));
        assert_eq!(parse_port("70000"), Err("invalid port: 70000".to_owned()));
        assert_eq!(parse_port("-1"), Err("invalid port: -1".to_owned()));
    }

    #[test]
    fn an_ipv6_host_and_an_inline_port_are_both_accepted() {
        assert_eq!(address(Some("[::1]"), None), "[::1]:5181");
        assert_eq!(address(Some("[::1]"), Some("4321")), "[::1]:4321");
        // A full address already names a port, so it wins over `--port`.
        assert_eq!(address(Some("0.0.0.0:1"), Some("4321")), "0.0.0.0:1");
        assert_eq!(address(Some("::"), None), "[::]:5181");
        // An unbracketed `::1:5199` is a legal IPv6 literal (hextet `5199`), not an address plus a
        // port: the reference hands the string to `getaddrinfo`, which resolves the same way, so the
        // bind is that literal on the default port rather than a refusal.
        assert_eq!(address(Some("::1:5199"), Some("4321")), "[::1:5199]:4321");
        // A bracket whose port does not parse is the host alone, again as Node reads it.
        assert_eq!(address(Some("[::1]:x"), None), "[::1]:5181");
    }

    fn address_err(host: &str) -> String {
        let config = ListenConfig {
            port: ListenPort::Fixed(DEFAULT_PORT),
            host: host.to_owned(),
            static_root: None,
        };

        config.address().unwrap_err()
    }

    #[test]
    fn a_hostname_resolves_rather_than_being_rejected() {
        // `server.listen(port, host)` accepts a hostname; demanding a numeric address would refuse a
        // deployment the reference serves.
        let config = ListenConfig {
            port: ListenPort::Fixed(5181),
            host: "localhost".to_owned(),
            static_root: None,
        };

        assert_eq!(
            config.address().expect("localhost resolves").to_string(),
            "127.0.0.1:5181"
        );
    }

    #[test]
    fn an_unresolvable_host_is_a_usage_refusal() {
        // `.invalid` is RFC 2606 reserved, so the resolver always fails.
        assert!(
            address_err("no-such-host.invalid")
                .contains("`no-such-host.invalid` --host is not a valid address or hostname")
        );
    }

    #[test]
    fn a_relative_static_root_is_refused_with_the_literal_sentence() {
        let flags = ListenFlags {
            static_root: Some(PathBuf::from("relative/dist")),
            ..ListenFlags::default()
        };

        assert_eq!(
            resolve(&env(&[]), &flags, ListenPort::Fixed(DEFAULT_PORT)).unwrap_err(),
            format!("{STATIC_ROOT_REFUSAL}: `relative/dist`")
        );
    }

    #[test]
    fn the_listen_line_reports_the_port_that_was_actually_bound() {
        let local: SocketAddr = "127.0.0.1:4321".parse().expect("an address");
        let line = listening_line(local, "epoch-1", "http://127.0.0.1:4321/v1/harness", None);
        let (marker, payload) = line.split_once(' ').expect("marker + payload");

        assert_eq!(marker, LISTENING);

        let report: serde_json::Value = serde_json::from_str(payload).expect("the payload is JSON");

        assert_eq!(report["port"], 4321);
        assert_eq!(report["host"], "127.0.0.1");
        assert_eq!(report["runtime_id"], "epoch-1");
        assert_eq!(report["stream"], "ws://127.0.0.1:4321/v1/harness/stream");
        // A bare listener serves no mirror, and says so by omitting the key.
        assert!(report.get("static_root").is_none());
        // Wave W2: no `public_base_url`, because the reference has no public-origin override.
        assert!(report.get("public_base_url").is_none());
    }

    #[test]
    fn the_listen_line_names_the_mirror_it_serves() {
        let local: SocketAddr = "[::1]:5181".parse().expect("an address");
        let line = listening_line(
            local,
            "e",
            "http://[::1]:5181/v1/harness",
            Some(Path::new("/srv/ui")),
        );
        let report: serde_json::Value =
            serde_json::from_str(line.split_once(' ').expect("payload").1).expect("json");

        assert_eq!(report["host"], "::1");
        assert_eq!(report["stream"], "ws://[::1]:5181/v1/harness/stream");
        assert_eq!(report["static_root"], "/srv/ui");
    }
}
