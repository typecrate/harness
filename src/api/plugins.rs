//! `{root}/plugins` - the built-in plugin catalogue and the one write the Plugins pane makes.
//!
//! TypeScript origin: `src/server/api.ts` - `pluginSettingOf` / `pluginRowOf` (`:1925-1965`),
//! `getPlugins` (`:1968-1993`) and `patchPlugin` (`:1996-2024`), with the settings store behind
//! them in `src/server/plugins.ts` (ported to [`crate::plugins`]) and the durable commit in
//! `src/server/sessions.ts` `updatePluginSettings` (`:1853-1862`).
//!
//! WHAT THIS ROUTE SURFACES, AND WHAT IT DOES NOT
//! ----------------------------------------------
//! The contract's plugin surface is a SETTINGS surface: one read that answers the catalogue with
//! the values in force, and one patch that edits those values. There is no enable/disable verb
//! (loading is auto-scanned), no `plugins` stream update kind (the pane rereads on open and on
//! save), and no per-plugin subresource beyond the row itself. This build additionally executes no
//! plugin, so a row is metadata plus values rather than a live hook registration - the whole
//! deviation is recorded in `knowledge.md` "T10 plugin uplift".
//!
//! WHY THE REVISION IS NOT A HUB COUNTER
//! -------------------------------------
//! `api.ts:1985-1991` states it: the collection revision is the plugin SETTINGS store's own counter
//! (one bump per accepted patch), not a hub counter, because `plugins` is not a
//! `StreamResourceKind` and adding one would move the stream's bounded per-scope baseline for a
//! pane that rereads anyway. The store lives inside the session store's guarded state
//! ([`crate::store::Store::plugin_revision`]), so the figure is read under the same lock that
//! writes it and a page can never pair a value with a stale counter.
//!
//! WHY THE LIST STAMPS NO `X-Harness-Revision`
//! -------------------------------------------
//! `getPlugins` calls `json(200, pageMeta(...))` with TWO arguments (`api.ts:1988-1991`), so the
//! collection revision rides the page BODY only - the identical rule `getHarnesses` follows
//! (`:1503-1509`), and pinned against this port by `tests/plugins.rs`. `patchPlugin` DOES pass the
//! third argument (`:2022`), because there the figure is the result of the write.

use serde_json::{Map, Value};

use crate::api::{self, ApiResult, Ctx};
use crate::contract;
use crate::error::ApiError;
use crate::events;
use crate::paging;

/// `GET {root}/plugins` (`api.ts:1968-1993`): the loaded catalogue plus what each plugin is set to.
///
/// Answerable with no harness and no model, in the manifest's stable order. The `q` filter is the
/// contract's own haystack - `"{name} {title} {description} {tags}"`, case-insensitively - and NOT
/// the hooks or the settings keys. Probe-verified against the TypeScript reference server:
/// `?q=drained` and `?q=dispose` (words that live only inside async-ping's HOOK list) and
/// `?q=contextlimit` (a declared SETTING key) are all empty pages, while `?q=lifecycle` (a tag both
/// built-ins carry) answers both. `?q=turn` is deliberately NOT one of the empty cases - the word
/// sits inside the compaction plugin's DESCRIPTION, which IS in the haystack - so the hook-only
/// spelling `?q=turn.before` is the probe that separates "description matched" from "hook matched".
pub async fn get_plugins(ctx: &mut Ctx) -> ApiResult {
    // TS reads `limit`, then `cursor`, then `q`, in that order (`api.ts:1969-1971`), so an invalid
    // `limit` is refused before a filter is ever considered.
    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;
    let search = api::param_of(&ctx.query, "q").map(str::to_lowercase);

    let revision = ctx.runtime.store.plugin_revision();

    let mut rows = ctx.runtime.store.plugin_rows();

    if let Some(needle) = search {
        rows.retain(|row| haystack_of(row).contains(&needle));
    }

    let total = rows.len();
    let slice = paging::slice_page(rows, cursor, limit, Some(total));

    // TWO arguments, like `getHarnesses`: the revision lives in the page body only.
    let body = paging::page_slice_body(
        ctx.runtime.hub.epoch(),
        slice,
        revision,
        events::now_millis(),
    );

    Ok(api::json(200, body))
}

/// `PATCH {root}/plugins/{plugin_id}` (`api.ts:1996-2024`): edit one plugin's settings.
///
/// IMMEDIATE and durable: the store applies the value to the live map, then the document carrying
/// it is written BEFORE the answer - this crate's rule for every structural mutation. There is no
/// stream publish, because the contract has no `plugins` update kind.
///
/// The split between here and [`crate::plugins`] is TypeScript's own: every REFUSAL (unknown plugin
/// => 404, unknown key / wrong type / out-of-range value => 400) is decided by the store, before it
/// mutates anything, so this handler only moves bytes.
pub async fn patch_plugin(ctx: &mut Ctx) -> ApiResult {
    // Owned, because `ctx.param(..)` borrows `ctx` for the caller's lifetime and the body read needs
    // `&mut ctx`.
    let name = ctx.param("plugin_id").to_owned();
    let body = ctx.body_object().await?;

    // `settings` is the ONLY field the contract names: there is no enable/disable member, and an
    // unknown one is refused rather than ignored (`api.ts:2003`).
    api::require_body_fields(&body, &["settings"])?;

    // A MISSING `settings` is the route's own 400, checked before the store is asked about the id
    // (`api.ts:2005-2007`), so `PATCH /plugins/nope` with an empty body is a 400, not a 404. An
    // explicit `null` is NOT absent: TypeScript spreads it to an empty patch, which is the OTHER 400.
    let Some(submitted) = body.get("settings") else {
        return Err(ApiError::bad_request(
            "A plugin settings patch requires a \"settings\" object.",
        ));
    };

    // TypeScript hands the store `{ ...body.settings }`, so a non-object arrives as what its spread
    // produces: `null` / a number / a boolean / an empty array become an EMPTY patch (400 "must
    // change at least one setting"), while a string or a non-empty array becomes index-keyed entries
    // (400 "Unknown setting \"0\" ..."). Reproduced here rather than collapsed into one sentence,
    // because the message is the contract.
    let patch = settings_patch(submitted);

    // Validate + apply + bump, under the store lock, in one step (`sessions.ts updatePluginSettings`
    // minus the commit). A refusal leaves the store exactly as it was.
    ctx.runtime.store.update_plugin_settings(&name, &patch)?;

    // DURABLE before the answer. A failed write leaves the applied value in place - the running
    // sessions have already been given it, so a rollback would report a failure the process does not
    // actually have (`sessions.ts:1848-1851`).
    if let Err(error) = ctx.runtime.save() {
        return Err(ApiError::internal(error.message));
    }

    let metadata = ctx
        .runtime
        .store
        .plugin_row(&name)
        .ok_or_else(|| api::missing("plugin", &name))?;

    let response = contract::PluginDetailResponse { plugin: metadata };

    // The figure the header quotes is the SETTINGS store's counter after this write, which is also
    // the figure the next `GET /plugins` prints in its body (`api.ts:2020-2022`).
    let revision = ctx.runtime.store.plugin_revision();

    Ok(api::with_revision(
        200,
        serde_json::to_value(response).unwrap_or_default(),
        revision,
    ))
}

/// The `{ ...body.settings }` the TypeScript route passes to the store (`api.ts:2019`).
///
/// An absent `settings` never reaches here - the route refuses it first - but a `null`, a boolean, a
/// number and an empty array all spread to an EMPTY object (400 "must change at least one setting"),
/// while a non-empty array spreads to its indices (`{"0": ...}`) and a string to one index entry per
/// character (400 "Unknown setting \"0\" ..."). Every one of those is a 400 either way; the point of
/// reproducing the spread is that each shape produces the SENTENCE the contract produces rather than
/// one invented here.
fn settings_patch(submitted: &Value) -> Map<String, Value> {
    match submitted {
        Value::Object(fields) => fields.clone(),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value.clone()))
            .collect(),
        Value::String(text) => text
            .chars()
            .enumerate()
            .map(|(index, ch)| (index.to_string(), Value::String(ch.to_string())))
            .collect(),
        // `null`, a number and a boolean have no own enumerable properties.
        _ => Map::new(),
    }
}

/// The text one row's `q` filter runs against: `"{name} {title} {description} {tags}"` joined with
/// single spaces and lower-cased (`api.ts:1978-1983`).
///
/// The absent description contributes the empty string and the absent tags do too, so the
/// separators stay - a filter is a substring test, and a shifted space changes which queries hit.
#[must_use]
fn haystack_of(row: &contract::PluginSummary) -> String {
    let tags = row.tags.clone().unwrap_or_default().join(" ");

    format!(
        "{} {} {} {}",
        row.name,
        row.title,
        row.description.clone().unwrap_or_default(),
        tags
    )
    .to_lowercase()
}
