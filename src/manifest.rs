//! The machine-readable parity manifest: the mounted surface, as one JSON document.
//!
//! WHY THIS EXISTS: the next wave drives this crate over a real socket and compares it against the
//! TypeScript reference route by route. Doing that from `--help` output or from a scraped HTML page
//! means parsing prose, so the crate emits the facts instead: the templates, the verbs, the operation
//! names, the counts, and the delivery-surface paths that are NOT operations. `parity-manifest` in
//! `src/bin/server.rs` writes this document to a file or to stdout.
//!
//! IT IS RENDERED, NOT MAINTAINED. Every value below comes from one of three places the dispatcher
//! itself reads: `api::ROUTES` (the 52 templates x 74 operations `src/api/mod.rs` owns), `contract`
//! (the mount, the stream path, the service name), and `listen`/`static_files` (the
//! delivery surface). There is no second copy of any list, so the manifest cannot claim an operation
//! the router does not mount - which is the same argument `api.ts:623-625` gives for compiling its own
//! table from the generated templates.
//!
//! THE OPERATION NAMES. Each row carries `Operation`'s variant name via `Debug`, which for a
//! fieldless enum IS the variant identifier. The variants are named after the contract's operation
//! ids (`harness-api.ts`'s `GetSessionPlugins` and friends, see `api::Operation`), so the name printed
//! here is the contract id - and if that ever stops being true, `tests/delivery_surface.rs` pins the
//! whole list and says so loudly.
//!
//! EXTENSION POINTS, for the wave that fills this in rather than inventing it:
//!   * `routes[].request` / `routes[].response` - the swagger schemas, once this crate carries the
//!     document instead of the TS package owning it;
//!   * `records[]` - the 48-table durable schema and its migrations, which `persistence.rs` owns;
//!   * `ws_messages[]` - the `harness.v1` frame vocabulary, which the stream state-machine wave owns
//!     (`stream/protocol.ts`), deliberately absent here because this build wires only the upgrade.

use std::path::Path;

use serde_json::{Value, json};

use crate::api;
use crate::contract;
use crate::listen;
use crate::static_files;

/// The verbs the contract declares, in the split order `tests/routes.rs` pins (GET, POST, PATCH,
/// DELETE, PUT). A verb this build does not use still appears, with 0, so a comparison against the
/// reference sees the same key set on both sides.
pub const METHODS: [&str; 5] = ["GET", "POST", "PATCH", "DELETE", "PUT"];

/// The manifest for one listener.
///
/// `static_root` is the mirror this process actually serves, or `None` for an API-only listener - the
/// manifest reports the truth of the process that emits it, because a probe that started the server
/// with a mirror wants the path back so it can write fixtures into the right place.
#[must_use]
pub fn manifest_document(static_root: Option<&Path>) -> Value {
    let operation_count = api::ROUTES
        .iter()
        .map(|route| route.operations.len())
        .sum::<usize>();

    let methods: Vec<Value> = METHODS
        .iter()
        .map(|method| {
            let count = api::ROUTES
                .iter()
                .flat_map(|route| route.operations.iter())
                .filter(|(declared, _)| declared == method)
                .count();

            json!({ "method": method, "operations": count })
        })
        .collect();

    let routes: Vec<Value> = api::ROUTES
        .iter()
        .map(|route| {
            let operations: Vec<Value> = route
                .operations
                .iter()
                .map(|(method, operation)| {
                    json!({
                        "method": method,
                        // `Debug` on a fieldless enum is the variant name, i.e. the contract id.
                        "operation": format!("{operation:?}"),
                    })
                })
                .collect();

            json!({
                "id": route.id,
                "template": route.template,
                "operations": operations,
            })
        })
        .collect();

    json!({
        "service": contract::SERVICE_NAME,
        "version": crate::version(),
        "api_root": contract::HARNESS_API_ROOT,
        "stream_path": contract::HARNESS_WS_STREAM_PATH,
        "counts": {
            "templates": api::ROUTES.len(),
            "operations": operation_count,
            "methods": methods,
        },
        "routes": routes,
        // The delivery surface, separated from the contract on purpose: none of these paths is an
        // operation, and a parity probe that compared them against the swagger would find nothing to
        // compare (`harness-server.swagger.yaml` declares /v1/harness paths only).
        "delivery_surface": {
            "static_mirror": {
                "served": static_root.is_some(),
                "root": static_root.map(|root| root.display().to_string()),
                "immutable_directory": static_files::ASSETS_DIRECTORY,
                "cache_immutable": static_files::CACHE_IMMUTABLE,
                "cache_default": static_files::CACHE_NO_CACHE,
                "media_types": static_files::MIME_TYPES
                    .iter()
                    .map(|(extension, media_type)| json!({ "extension": extension, "media_type": media_type }))
                    .collect::<Vec<Value>>(),
            },
            "not_found": {
                "content_type": static_files::NOT_FOUND_TYPE,
                "body": static_files::NOT_FOUND_BODY,
                "note": "the terminal answer of a listener that serves a mirror; a mount-relative 404 is the dispatcher's JSON sentence that names /v1/harness",
            },
        },
        "listen": {
            "default_host": listen::DEFAULT_HOST,
            "default_port": listen::DEFAULT_PORT,
            "auto_port": listen::AUTO_PORT,
            "max_port": listen::MAX_PORT,
            "port_env": listen::PORT_ENV,
            "host_env": listen::HOST_ENV,
            "static_root_env": listen::STATIC_ROOT_ENV,
            "listen_marker": listen::LISTENING,
        },
    })
}

/// The manifest as pretty-printed text, which is what `parity-manifest --out <path>` writes: a file a
/// human diffs against the TypeScript reference's own table, and `serde_json::from_str` reads back.
#[must_use]
pub fn manifest_text(static_root: Option<&Path>) -> String {
    let pretty = serde_json::to_string_pretty(&manifest_document(static_root))
        .expect("the manifest document is serializable by construction");

    format!("{pretty}\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The counts are the same pins `tests/routes.rs` holds for the dispatcher, restated here so a
    /// manifest that disagrees with the router fails in the crate rather than in the next wave.
    #[test]
    fn the_manifest_counts_the_mounted_surface() {
        let document = manifest_document(None);

        assert_eq!(document["service"], json!("noobscript-harness"));
        assert_eq!(document["api_root"], json!("/v1/harness"));
        assert_eq!(document["stream_path"], json!("/v1/harness/stream"));
        assert_eq!(document["version"], json!(crate::version()));
        assert_eq!(document["counts"]["templates"], json!(52));
        assert_eq!(document["counts"]["operations"], json!(74));

        // The verb split, in the contract's split order.
        assert_eq!(
            document["counts"]["methods"]
                .as_array()
                .expect("a method split")
                .iter()
                .map(|row| format!(
                    "{}={}",
                    row["method"].as_str().expect("a method"),
                    row["operations"].as_u64().expect("a count")
                ))
                .collect::<Vec<String>>(),
            vec!["GET=42", "POST=14", "PATCH=11", "DELETE=6", "PUT=1"]
        );
    }

    /// The first and last rows, spelled out: the table order is load-bearing (it is what `matchRoute`
    /// resolves first), so a manifest that re-sorted it would silently break a diff.
    #[test]
    fn the_routes_are_in_the_contracts_declaration_order() {
        let document = manifest_document(None);
        let routes = document["routes"].as_array().expect("a route list");

        assert_eq!(routes.len(), 52);
        assert_eq!(
            routes.first().expect("the first row"),
            &json!({
                "id": "root",
                "template": "/v1/harness",
                "operations": [{ "method": "GET", "operation": "GetRoot" }],
            })
        );
        assert_eq!(
            routes.get(1).expect("the health row"),
            &json!({
                "id": "health",
                "template": "/v1/harness/health",
                "operations": [{ "method": "GET", "operation": "GetHealth" }],
            })
        );
        assert_eq!(
            routes.last().expect("the last row")["template"],
            json!("/v1/harness/stream")
        );
        // And every row's ids are the enum's own names, i.e. the contract's operation ids.
        assert_eq!(
            routes.get(2).expect("the config row")["operations"]
                .as_array()
                .expect("config operations")
                .iter()
                .map(|row| format!(
                    "{} {}",
                    row["method"].as_str().expect("a method"),
                    row["operation"].as_str().expect("an operation id")
                ))
                .collect::<Vec<String>>(),
            vec!["GET GetConfig", "PATCH PatchConfig"]
        );
    }

    #[test]
    fn the_manifest_reports_the_delivery_surface_as_a_separate_plane() {
        let with_mirror = manifest_document(Some(Path::new("/srv/ui/dist")));

        // Wave W2: there is no `docs_page` entry, because the reference answers no `/docs`
        // (`api.ts:6149-6320` publishes no HTML route and `start.ts:404-414` answers the rest with the
        // mirror or `sendNotFound`). Sorted so the claim does not depend on the map's ordering.
        let mut surface_keys: Vec<String> = with_mirror["delivery_surface"]
            .as_object()
            .expect("the delivery surface")
            .keys()
            .cloned()
            .collect();

        surface_keys.sort();

        assert_eq!(surface_keys, vec!["not_found", "static_mirror"]);
        assert_eq!(
            with_mirror["delivery_surface"]["static_mirror"]["served"],
            json!(true)
        );
        assert_eq!(
            with_mirror["delivery_surface"]["static_mirror"]["root"],
            json!("/srv/ui/dist")
        );
        assert_eq!(
            with_mirror["delivery_surface"]["static_mirror"]["cache_immutable"],
            json!("public, max-age=31536000, immutable")
        );
        assert_eq!(
            with_mirror["delivery_surface"]["not_found"]["body"],
            json!("Not found")
        );
        // The media-type table travels with the manifest, so a probe can check the headers against the
        // same list the handler uses.
        assert_eq!(
            with_mirror["delivery_surface"]["static_mirror"]["media_types"]
                .as_array()
                .expect("a media type table")
                .len(),
            11
        );

        // An API-only listener says so instead of printing a null root it claims to serve.
        let without = manifest_document(None);

        assert_eq!(
            without["delivery_surface"]["static_mirror"]["served"],
            json!(false)
        );
        assert_eq!(
            without["delivery_surface"]["static_mirror"]["root"],
            json!(null)
        );
    }

    #[test]
    fn the_text_form_parses_back_and_ends_with_one_newline() {
        let text = manifest_text(Some(Path::new("/srv/ui/dist")));

        assert!(text.ends_with("}\n"));
        assert!(
            text.lines().count() > 200,
            "a pretty document is line-per-key"
        );

        let parsed: Value = serde_json::from_str(&text).expect("the written text is JSON");

        assert_eq!(parsed, manifest_document(Some(Path::new("/srv/ui/dist"))));
    }
}
