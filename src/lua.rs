//! Lua scripting support, backed by a pinned worker-thread pool.
//!
//! Attach a [`LuaScript`] component to an entity to bind it to a Lua file
//! on disk. Two optional globals in the script are called by convention:
//!
//! - `init()`      — called once, right after the script is loaded/reloaded.
//! - `update(dt)`  — called every frame, `dt` is seconds since last frame.
//!
//! ## Architecture
//! A fixed pool of OS threads is spawned once, at plugin build time, and
//! lives for the app's whole lifetime ([`LuaWorkerPool`]). Each scripted
//! entity is assigned to exactly one worker (round-robin) the first time
//! its [`LuaScript`] is seen, and that worker owns the entity's `Lua` VM
//! for as long as the entity exists — the VM is created on that thread and
//! never leaves it. That's what makes this sound without needing `Lua` to
//! be `Send`/`Sync` at all, and without any `unsafe impl`: nothing
//! mlua-related ever crosses a thread boundary. Only plain data does, over
//! `std::sync::mpsc` channels:
//!
//! - Main thread → worker: [`WorkerRequest`] (load a script, tick with a
//!   `dt`, unload, shut down).
//! - Worker → main thread: [`WorkerEvent`] (loaded / load failed /
//!   reloaded / runtime error), all workers sharing one `Sender` cloned
//!   into a single `Receiver` the main thread drains each frame.
//!
//! `dispatch_ticks` fires a `Tick` at every worker once per Bevy frame;
//! each worker then runs every `update()` it owns on its own thread,
//! genuinely in parallel with the other workers and with the rest of the
//! Bevy schedule. `collect_worker_events` is a non-blocking drain, so
//! load/error status on `LuaScriptReady`/`LuaScriptError` can lag by a
//! frame or two — fine for status reflection, and it's what keeps this
//! from re-serializing everything with a wait-for-results barrier.
//!
//! `LuaWorkerPool` itself only ever holds `Sender`/`Receiver`/`JoinHandle`/
//! `HashMap<Entity, usize>` — no `Lua` field — so it's registered as a
//! `NonSend` resource purely because `std::sync::mpsc::Sender` isn't
//! `Sync`, not because anything here is thread-unsafe. The systems that
//! touch it (`load_pending_scripts`, `dispatch_ticks`,
//! `collect_worker_events`, `cleanup_removed_scripts`) get pinned to the
//! main thread by Bevy as a result, but they only ever send/receive small
//! messages — the actual Lua execution happens entirely off that thread,
//! which is the whole point.
//!
//! ## What this gives up
//! Scripts can't reach into the live `World` (no querying other entities,
//! no spawning) because they're not running anywhere near the ECS when
//! they execute. If a script needs to read something, snapshot it into
//! `WorkerRequest::Tick` (extend it with per-entity input data) before
//! dispatch; if it needs to change something, have it return values a new
//! `WorkerEvent` variant carries back for `collect_worker_events` to apply
//! via `Commands`. Calling a custom Lua function from Rust ad hoc (the old
//! `LuaVms::get(entity)` pattern) isn't possible anymore for the same
//! reason — that would need its own `WorkerRequest::Invoke { entity, .. }`
//! / `WorkerEvent::InvokeResult { .. }` round trip.
//!
//! Requires in `Cargo.toml` — no new crates beyond mlua, and no `send`
//! feature needed, since no `Lua` value ever needs to be `Send`:
//! ```toml
//! mlua = { version = "0.12", features = ["lua54", "vendored"] }
//! ```

use std::collections::HashMap;
use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

use bevy::prelude::*;
use mlua::{Function, Lua};

pub struct LuaScriptPlugin {
    /// How often, in seconds, each worker checks its own scripts on disk
    /// for changes and hot-reloads them. `None` disables hot-reloading.
    pub hot_reload_interval: Option<f32>,
    /// Number of persistent worker threads to spawn. Defaults to the
    /// number of available cores.
    pub worker_count: usize,
}

impl Default for LuaScriptPlugin {
    fn default() -> Self {
        Self {
            hot_reload_interval: Some(0.5),
            worker_count: thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        }
    }
}

impl Plugin for LuaScriptPlugin {
    fn build(&self, app: &mut App) {
        let hot_reload_interval = self.hot_reload_interval.map(Duration::from_secs_f32);

        app.insert_non_send_resource(LuaWorkerPool::spawn(self.worker_count, hot_reload_interval))
            .add_systems(
                Update,
                (
                    load_pending_scripts,
                    dispatch_ticks,
                    collect_worker_events,
                    cleanup_removed_scripts,
                )
                    .chain(),
            );
    }
}

/// Attach to an entity to bind it to a Lua script loaded from disk.
#[derive(Component, Clone, Debug)]
pub struct LuaScript {
    pub path: PathBuf,
}

impl LuaScript {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

/// Internal marker: this entity's script has already been sent to a
/// worker, so `load_pending_scripts` shouldn't dispatch it again.
#[derive(Component)]
struct LuaScriptDispatched;

/// Present once a script has successfully loaded (or reloaded) and is
/// actively running on its worker.
#[derive(Component)]
pub struct LuaScriptReady;

/// Present when a script's most recent load, reload, or `update()` call
/// failed. Doesn't stop future hot-reload attempts — those live entirely
/// on the worker and keep retrying on their own schedule.
#[derive(Component)]
pub struct LuaScriptError(pub String);

/// Sent from the main thread to a specific worker.
enum WorkerRequest {
    LoadScript { entity: Entity, path: PathBuf },
    Unload { entity: Entity },
    Tick { dt: f32 },
    Shutdown,
}

/// Sent from a worker back to the main thread.
enum WorkerEvent {
    Loaded { entity: Entity },
    Reloaded { entity: Entity },
    LoadFailed { entity: Entity, error: String },
    RuntimeError { entity: Entity, error: String },
}

/// `NonSend` resource owning the worker-thread pool. Holds only plain,
/// thread-safe handles — see module docs for why `Lua` itself never
/// appears here.
pub struct LuaWorkerPool {
    senders: Vec<Sender<WorkerRequest>>,
    events_rx: Receiver<WorkerEvent>,
    assignments: HashMap<Entity, usize>,
    next_worker: usize,
    handles: Vec<JoinHandle<()>>,
}

impl LuaWorkerPool {
    fn spawn(worker_count: usize, hot_reload_interval: Option<Duration>) -> Self {
        let worker_count = worker_count.max(1);
        let (events_tx, events_rx) = mpsc::channel();
        let mut senders = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);

        for id in 0..worker_count {
            let (req_tx, req_rx) = mpsc::channel();
            let events_tx = events_tx.clone();

            let handle = thread::Builder::new()
                .name(format!("lua-script-worker-{id}"))
                .spawn(move || worker_loop(req_rx, events_tx, hot_reload_interval))
                .expect("failed to spawn lua script worker thread");

            senders.push(req_tx);
            handles.push(handle);
        }

        Self {
            senders,
            events_rx,
            assignments: HashMap::new(),
            next_worker: 0,
            handles,
        }
    }

    fn dispatch_new_script(&mut self, entity: Entity, path: PathBuf) {
        let worker = self.next_worker;
        self.next_worker = (self.next_worker + 1) % self.senders.len();
        self.assignments.insert(entity, worker);
        let _ = self.senders[worker].send(WorkerRequest::LoadScript { entity, path });
    }

    fn unload(&mut self, entity: Entity) {
        if let Some(worker) = self.assignments.remove(&entity) {
            let _ = self.senders[worker].send(WorkerRequest::Unload { entity });
        }
    }

    fn tick_all(&self, dt: f32) {
        for sender in &self.senders {
            let _ = sender.send(WorkerRequest::Tick { dt });
        }
    }

    fn drain_events(&self) -> impl Iterator<Item = WorkerEvent> + '_ {
        self.events_rx.try_iter()
    }
}

impl Drop for LuaWorkerPool {
    fn drop(&mut self) {
        for sender in &self.senders {
            let _ = sender.send(WorkerRequest::Shutdown);
        }
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn load_pending_scripts(
    mut commands: Commands,
    mut pool: NonSendMut<LuaWorkerPool>,
    query: Query<(Entity, &LuaScript), Without<LuaScriptDispatched>>,
) {
    for (entity, script) in &query {
        pool.dispatch_new_script(entity, script.path.clone());
        commands.entity(entity).insert(LuaScriptDispatched);
    }
}

fn dispatch_ticks(time: Res<Time>, pool: NonSend<LuaWorkerPool>) {
    pool.tick_all(time.delta_secs());
}

fn collect_worker_events(mut commands: Commands, pool: NonSend<LuaWorkerPool>) {
    for event in pool.drain_events() {
        match event {
            WorkerEvent::Loaded { entity } | WorkerEvent::Reloaded { entity } => {
                commands.entity(entity).insert(LuaScriptReady);
                commands.entity(entity).remove::<LuaScriptError>();
            }
            WorkerEvent::LoadFailed { entity, error } => {
                error!("lua script on entity {entity:?} failed to load: {error}");
                commands.entity(entity).remove::<LuaScriptReady>();
                commands.entity(entity).insert(LuaScriptError(error));
            }
            WorkerEvent::RuntimeError { entity, error } => {
                error!("lua runtime error on entity {entity:?}: {error}");
                commands.entity(entity).insert(LuaScriptError(error));
            }
        }
    }
}

/// Routes an `Unload` to the owning worker whenever a `LuaScript` is
/// removed or its entity is despawned. Reads the owning worker out of
/// `pool.assignments` rather than any component, since a despawned
/// entity's other components are already gone by the time this runs.
fn cleanup_removed_scripts(
    mut pool: NonSendMut<LuaWorkerPool>,
    mut removed: RemovedComponents<LuaScript>,
) {
    for entity in removed.read() {
        pool.unload(entity);
    }
}

// ---- Worker thread body — everything below this line runs on a worker
// thread, never on the main thread. `Lua` values never leave this file's
// worker-side functions. ----

struct WorkerScript {
    lua: Lua,
    path: PathBuf,
    last_modified: Option<SystemTime>,
    has_update: bool,
}

fn worker_loop(
    rx: Receiver<WorkerRequest>,
    tx: Sender<WorkerEvent>,
    hot_reload_interval: Option<Duration>,
) {
    let mut scripts: HashMap<Entity, WorkerScript> = HashMap::new();
    let mut since_last_check = Duration::ZERO;

    while let Ok(msg) = rx.recv() {
        match msg {
            WorkerRequest::LoadScript { entity, path } => match load_and_init(&path, entity) {
                Ok(script) => {
                    scripts.insert(entity, script);
                    let _ = tx.send(WorkerEvent::Loaded { entity });
                }
                Err(err) => {
                    let _ = tx.send(WorkerEvent::LoadFailed {
                        entity,
                        error: err.to_string(),
                    });
                }
            },

            WorkerRequest::Unload { entity } => {
                scripts.remove(&entity);
            }

            WorkerRequest::Tick { dt } => {
                if let Some(interval) = hot_reload_interval {
                    since_last_check += Duration::from_secs_f32(dt.max(0.0));
                    if since_last_check >= interval {
                        since_last_check = Duration::ZERO;
                        hot_reload_pass(&mut scripts, &tx);
                    }
                }

                run_updates(&mut scripts, dt, &tx);
            }

            WorkerRequest::Shutdown => break,
        }
    }
}

fn run_updates(scripts: &mut HashMap<Entity, WorkerScript>, dt: f32, tx: &Sender<WorkerEvent>) {
    for (&entity, script) in scripts.iter() {
        if !script.has_update {
            continue;
        }

        // Catch panics per-entity so one buggy script can't take down the
        // rest of this worker's shard (and every other entity assigned to
        // it) along with it.
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            let update_fn: mlua::Result<Function> = script.lua.globals().get("update");
            let Ok(update_fn) = update_fn else {
                return Ok(());
            };
            update_fn.call::<()>(dt)
        }));

        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let _ = tx.send(WorkerEvent::RuntimeError {
                    entity,
                    error: err.to_string(),
                });
            }
            Err(_panic) => {
                let _ = tx.send(WorkerEvent::RuntimeError {
                    entity,
                    error: format!("script for {entity:?} panicked in update()"),
                });
            }
        }
    }
}

fn hot_reload_pass(scripts: &mut HashMap<Entity, WorkerScript>, tx: &Sender<WorkerEvent>) {
    for (&entity, script) in scripts.iter_mut() {
        let Ok(modified) = fs::metadata(&script.path).and_then(|m| m.modified()) else {
            continue;
        };

        if script.last_modified == Some(modified) {
            continue;
        }

        match load_and_init(&script.path, entity) {
            Ok(new_script) => {
                *script = new_script;
                let _ = tx.send(WorkerEvent::Reloaded { entity });
            }
            Err(err) => {
                let _ = tx.send(WorkerEvent::LoadFailed {
                    entity,
                    error: err.to_string(),
                });
            }
        }
    }
}

fn load_and_init(path: &Path, entity: Entity) -> mlua::Result<WorkerScript> {
    let source = fs::read_to_string(path)
        .map_err(|err| mlua::Error::RuntimeError(format!("could not read {path:?}: {err}")))?;

    let lua = Lua::new();
    install_api(&lua, entity)?;

    lua.load(&source)
        .set_name(path.to_string_lossy())
        .exec()?;

    let init_fn: mlua::Result<Function> = lua.globals().get("init");
    if let Ok(init_fn) = init_fn {
        let result: mlua::Result<()> = init_fn.call(());
        result?;
    }

    let update_check: mlua::Result<Function> = lua.globals().get("update");
    let has_update = update_check.is_ok();

    let last_modified = fs::metadata(path).and_then(|m| m.modified()).ok();

    Ok(WorkerScript {
        lua,
        path: path.to_path_buf(),
        last_modified,
        has_update,
    })
}

fn install_api(lua: &Lua, entity: Entity) -> mlua::Result<()> {
    let globals = lua.globals();

    globals.set("entity_id", entity.to_bits() as i64)?;

    let log_info = lua.create_function(|_, msg: String| {
        info!("[lua] {msg}");
        Ok(())
    })?;
    globals.set("log_info", log_info)?;

    let log_warn = lua.create_function(|_, msg: String| {
        warn!("[lua] {msg}");
        Ok(())
    })?;
    globals.set("log_warn", log_warn)?;

    let log_error = lua.create_function(|_, msg: String| {
        error!("[lua] {msg}");
        Ok(())
    })?;
    globals.set("log_error", log_error)?;

    Ok(())
}