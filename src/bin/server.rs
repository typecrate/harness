//! The server's HTTP entry point.
//!
//! T1 ships a placeholder so the `[[bin]]` target in `Cargo.toml` compiles from the first
//! commit; the router, the store, the agent runtime and the `*-route.rs` discovery wiring land
//! in the later stages of this port. The note is honest about that (exit 0, no listener), and
//! everything it CAN state truthfully - identity, version, the API root, and the configuration
//! the primitives resolve - is stated, because that is the part a deployment operator checks
//! first.

use typecrate_harness::{config, contract, environment, events, package_root, version};

fn main() {
    // The runtime is resolved exactly the way the server will resolve it: package-root `.env`
    // first (no override of exported values), then the validated settings.
    let runtime_env = environment::harness_runtime_env();
    let boot = config::load_server_config(&runtime_env);

    println!(
        "{} v{} ({})",
        contract::SERVICE_NAME,
        version(),
        env!("CARGO_PKG_NAME")
    );
    println!("  status:        foundation only - the HTTP layer is not wired yet");
    println!("  api root:      {}", contract::HARNESS_API_ROOT);
    println!("  stream:        {}", contract::HARNESS_WS_STREAM_PATH);
    println!("  epoch:         {}", events::process_epoch());
    println!(
        "  package root:  {}",
        package_root::package_root().display()
    );
    println!(
        "  database root: {}",
        package_root::default_database_root().display()
    );

    match boot {
        Ok(settings) => {
            println!("  provider:      {}", settings.base_url);
            println!("  model:         {}", settings.model);
            println!(
                "  api key:       {}",
                if settings.api_key.trim().is_empty() {
                    "not configured"
                } else {
                    "configured (not printed)"
                }
            );
            println!(
                "  execution:     thread={} io={} idle_ms={} lane_workers={} handshake_ms={}",
                settings.thread_execution,
                settings.io_execution,
                settings.worker_idle_ms,
                settings.max_lane_workers,
                settings.worker_handshake_ms
            );
        }
        Err(error) => {
            // A deployment misconfiguration is reported and then exits 0 for now: a later stage
            // turns it into exit 2 (bad usage), the same code `packages/typecrate/services`
            // reserves for a refused start. The credential itself is never printed.
            println!("  settings:      {error}");
        }
    }
}
