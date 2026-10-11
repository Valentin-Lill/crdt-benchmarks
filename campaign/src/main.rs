use automerge::{
    transaction::Transactable, ActorId, AutoCommit, ObjId, ObjType, ReadDoc as AmReadDoc,
    ScalarValue, Value as AmValue, ROOT,
};
use diamond_types::list::{encoding::{ENCODE_FULL, ENCODE_PATCH}, ListCRDT};
use loro::{ExportMode, LoroDoc, LoroValue};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeMap, fs, time::Instant};
use yrs::{
    updates::decoder::Decode, Doc, GetString, Map, Out, ReadTxn, StateVector, Text, Transact,
    Update,
};
mod night;
mod upstream;

// Enabled ONLY in the separate memory executable. Timing executable uses System directly.
#[cfg(feature = "allocation-metrics")]
mod allocation {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    pub static LIVE: AtomicUsize = AtomicUsize::new(0);
    pub static PEAK: AtomicUsize = AtomicUsize::new(0);
    pub struct Counting;
    fn add(n: usize) {
        let v = LIVE.fetch_add(n, Relaxed) + n;
        PEAK.fetch_max(v, Relaxed);
    }
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = System.alloc(l);
            if !p.is_null() {
                add(l.size());
            }
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = System.alloc_zeroed(l);
            if !p.is_null() {
                add(l.size());
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            System.dealloc(p, l);
            LIVE.fetch_sub(l.size(), Relaxed);
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
            let q = System.realloc(p, l, n);
            if !q.is_null() {
                if n >= l.size() {
                    add(n - l.size());
                } else {
                    LIVE.fetch_sub(l.size() - n, Relaxed);
                }
            }
            q
        }
    }
}
#[cfg(feature = "allocation-metrics")]
#[global_allocator]
static ALLOCATOR: allocation::Counting = allocation::Counting;
fn live() -> usize {
    #[cfg(feature = "allocation-metrics")]
    {
        allocation::LIVE.load(std::sync::atomic::Ordering::Relaxed)
    }
    #[cfg(not(feature = "allocation-metrics"))]
    {
        0
    }
}
fn rss() -> usize {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|l| {
            l.strip_prefix("VmRSS:").map(|s| {
                s.split_whitespace()
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
                    * 1024
            })
        })
        .unwrap()
}
fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}
fn trace_memory(stage: &str) {
    if std::env::var_os("CRDT_TRACE_MEMORY").is_some() {
        let status = fs::read_to_string("/proc/self/status").unwrap();
        let value = |name: &str| {
            status
                .lines()
                .find_map(|l| {
                    l.strip_prefix(name).map(|s| {
                        s.split_whitespace()
                            .next()
                            .unwrap()
                            .parse::<usize>()
                            .unwrap()
                            * 1024
                    })
                })
                .unwrap()
        };
        eprintln!(
            "{}",
            json!({"stage":stage,"rss_bytes":value("VmRSS:"),"peak_rss_bytes":value("VmHWM:")})
        );
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum Scalar {
    Str(String),
    Int(i64),
    Bool(bool),
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct State {
    text: String,
    map: BTreeMap<String, Scalar>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Op {
    Text {
        pos: usize,
        delete: usize,
        insert: String,
    },
    Set {
        key: String,
        value: Scalar,
    },
    Delete {
        key: String,
    },
}
#[derive(Deserialize)]
struct Fixture {
    name: String,
    batch: usize,
    initial: State,
    server: Vec<Op>,
    offline: Vec<Op>,
    server_expected: State,
    offline_expected: State,
}
enum Version {
    Y(StateVector),
    L(loro::VersionVector),
    A(Vec<automerge::ChangeHash>),
    D(Vec<usize>),
}
enum Backend {
    Y { doc: Doc, codec_v1: bool },
    P { inner: Box<Backend>, base: Option<Vec<u8>>, base_version: Option<Version> },
    L(LoroDoc),
    LU(LoroDoc),
    A {
        doc: AutoCommit,
        text: ObjId,
        map: ObjId,
        chunk_initial: bool,
        bounded_sync: bool,
    },
    D {
        doc: ListCRDT,
        agent: u32,
    },
}

impl Backend {
    fn empty(lib: &str, actor: u64) -> Self {
        if let Some(base_lib)=lib.strip_suffix("-tail") { return Self::P { inner:Box::new(Self::empty(base_lib,actor)),base:None,base_version:None }; }
        match lib {
            "yrs" | "yrs-v1" | "yrs-no-gc" | "yrs-v1-no-gc" => Self::Y { doc: Doc::with_options(yrs::Options {
                client_id: yrs::ClientID::new(actor),
                skip_gc: lib.contains("no-gc"),
                offset_kind: yrs::OffsetKind::Utf16,
                ..Default::default()
            }), codec_v1: lib.contains("v1") },
            "loro" | "loro-updates" => {
                let d = LoroDoc::new();
                d.set_peer_id(actor).unwrap();
                if lib == "loro" {
                    Self::L(d)
                } else {
                    Self::LU(d)
                }
            }
            "automerge" | "automerge-chunked" | "automerge-bounded" => {
                let mut doc =
                    AutoCommit::new().with_actor(ActorId::from(actor.to_le_bytes().to_vec()));
                let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
                let map = doc.put_object(ROOT, "map", ObjType::Map).unwrap();
                doc.commit();
                Self::A {
                    doc,
                    text,
                    map,
                    chunk_initial: lib != "automerge",
                    bounded_sync: lib == "automerge-bounded",
                }
            }
            "diamond" => {
                let mut doc = ListCRDT::new();
                let agent = doc.get_or_create_agent_id(&format!("actor-{actor}"));
                Self::D { doc, agent }
            }
            _ => panic!("unknown library: {lib}"),
        }
    }
    fn load(lib: &str, bytes: &[u8], actor: u64) -> Self {
        if let Some(base_lib)=lib.strip_suffix("-tail") {
            assert_eq!(&bytes[..8],b"CRDTAIL1"); let n=u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
            let base=bytes[16..16+n].to_vec();let mut inner=Self::load(base_lib,&base,actor);let v=inner.version();inner.import(&bytes[16+n..]);
            return Self::P {inner:Box::new(inner),base:Some(base),base_version:Some(v)};
        }
        match lib {
            "yrs" | "yrs-v1" | "yrs-no-gc" | "yrs-v1-no-gc" => {
                let d = Self::empty(lib, actor);
                if let Self::Y{doc,codec_v1} = &d {
                    doc.transact_mut()
                        .apply_update(if *codec_v1 {Update::decode_v1(bytes).unwrap()} else {Update::decode_v2(bytes).unwrap()})
                        .unwrap();
                }
                d
            }
            "loro" | "loro-updates" => {
                let d = Self::empty(lib, actor);
                if let Self::L(doc) | Self::LU(doc) = &d {
                    doc.import(bytes).unwrap();
                }
                d
            }
            "automerge" | "automerge-chunked" | "automerge-bounded" => {
                let mut doc = AutoCommit::load(bytes).unwrap();
                doc.set_actor(ActorId::from(actor.to_le_bytes().to_vec()));
                let text = doc.get(ROOT, "text").unwrap().unwrap().1;
                let map = doc.get(ROOT, "map").unwrap().unwrap().1;
                Self::A {
                    doc,
                    text,
                    map,
                    chunk_initial: lib != "automerge",
                    bounded_sync: lib == "automerge-bounded",
                }
            }
            "diamond" => {
                let mut doc = ListCRDT::load_from(bytes).unwrap();
                let agent = doc.get_or_create_agent_id(&format!("actor-{actor}"));
                Self::D { doc, agent }
            }
            _ => panic!("unknown library"),
        }
    }
    fn init(&mut self, s: &State, batch: usize) {
        if let Self::P {inner,..}=self {inner.init(s,batch);return;}
        if matches!(
            self,
            Self::A {
                chunk_initial: true,
                ..
            }
        ) {
            assert!(
                s.text.is_ascii(),
                "chunked bulk fixture currently requires ASCII"
            );
            // Bound temporary transaction buffers when importing/pasting a huge
            // document. Each 64 KiB segment is committed; all history is retained.
            let mut pos = 0;
            for chunk in s.text.as_bytes().chunks(64 * 1024) {
                self.apply(&[Op::Text {
                    pos,
                    delete: 0,
                    insert: std::str::from_utf8(chunk).unwrap().to_string(),
                }]);
                pos += chunk.len();
            }
        } else {
            self.apply(&[Op::Text {
                pos: 0,
                delete: 0,
                insert: s.text.clone(),
            }]);
        }
        let ops: Vec<_> = s
            .map
            .iter()
            .map(|(key, value)| Op::Set {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        for chunk in ops.chunks(batch.max(1024)) {
            self.apply(chunk);
        }
    }
    fn apply(&mut self, ops: &[Op]) {
        match self {
            Self::P{inner,..} => inner.apply(ops),
            Self::Y{doc:d,..} => {
                let text = d.get_or_insert_text("text");
                let map = d.get_or_insert_map("map");
                let mut tx = d.transact_mut();
                // A forward edit run uses one traversal rather than restarting at
                // the beginning for every indexed insertion/deletion. On a jump
                // backwards, finish this delta and begin another. Map edits are
                // independent of the text, and remain in the same transaction.
                let mut delta: Vec<yrs::types::Delta<yrs::In>> = Vec::new();
                let mut cursor = 0usize;
                for op in ops {
                    match op {
                        Op::Text {
                            pos,
                            delete,
                            insert,
                        } => {
                            if *pos < cursor {
                                text.apply_delta(&mut tx, delta.drain(..));
                                cursor = 0;
                            }
                            if *pos > cursor {
                                delta.push(yrs::types::Delta::retain((*pos - cursor) as u32));
                            }
                            if *delete > 0 {
                                delta.push(yrs::types::Delta::delete(*delete as u32));
                            }
                            if !insert.is_empty() {
                                delta.push(yrs::types::Delta::insert(insert.as_str()));
                            }
                            cursor = *pos + insert.encode_utf16().count();
                        }
                        Op::Set { key, value } => match value {
                            Scalar::Str(v) => {
                                map.insert(&mut tx, key.as_str(), v.as_str());
                            }
                            Scalar::Int(v) => {
                                map.insert(&mut tx, key.as_str(), *v);
                            }
                            Scalar::Bool(v) => {
                                map.insert(&mut tx, key.as_str(), *v);
                            }
                        },
                        Op::Delete { key } => {
                            map.remove(&mut tx, key);
                        }
                    }
                }
                if !delta.is_empty() {
                    text.apply_delta(&mut tx, delta);
                }
            }
            Self::L(d) | Self::LU(d) => {
                let text = d.get_text("text");
                let map = d.get_map("map");
                for op in ops {
                    match op {
                        Op::Text {
                            pos,
                            delete,
                            insert,
                        } => {
                            if *delete > 0 {
                                text.delete_utf16(*pos, *delete).unwrap();
                            }
                            if !insert.is_empty() {
                                text.insert_utf16(*pos, insert).unwrap();
                            }
                        }
                        Op::Set { key, value } => {
                            let v = match value {
                                Scalar::Str(v) => LoroValue::from(v.as_str()),
                                Scalar::Int(v) => LoroValue::from(*v),
                                Scalar::Bool(v) => LoroValue::from(*v),
                            };
                            map.insert(key, v).unwrap();
                        }
                        Op::Delete { key } => {
                            map.delete(key).unwrap();
                        }
                    }
                }
                d.commit();
            }
            Self::A { doc, text, map, .. } => {
                for op in ops {
                    match op {
                        Op::Text {
                            pos,
                            delete,
                            insert,
                        } => {
                            doc.splice_text(&*text, *pos, *delete as isize, insert)
                                .unwrap();
                        }
                        Op::Set { key, value } => {
                            let v = match value {
                                Scalar::Str(v) => ScalarValue::Str(v.as_str().into()),
                                Scalar::Int(v) => ScalarValue::Int(*v),
                                Scalar::Bool(v) => ScalarValue::Boolean(*v),
                            };
                            doc.put(&*map, key.as_str(), v).unwrap();
                        }
                        Op::Delete { key } => {
                            doc.delete(&*map, key.as_str()).unwrap();
                        }
                    }
                }
                doc.commit();
            }
            Self::D { doc, agent } => {
                for op in ops {
                    match op {
                        Op::Text {
                            pos,
                            delete,
                            insert,
                        } => {
                            if *delete > 0 {
                                doc.delete_without_content(*agent, *pos..*pos + *delete);
                            }
                            if !insert.is_empty() {
                                doc.insert(*agent, *pos, insert);
                            }
                        }
                        _ => panic!("Diamond Types only supports text in this benchmark"),
                    }
                }
            }
        }
    }
    fn state(&self) -> State {
        match self {
            Self::P{inner,..} => inner.state(),
            Self::Y{doc:d,..} => {
                let text = d.get_or_insert_text("text");
                let map = d.get_or_insert_map("map");
                let tx = d.transact();
                State {
                    text: text.get_string(&tx),
                    map: map
                        .iter(&tx)
                        .map(|(k, v)| {
                            let v = match v {
                                Out::Any(yrs::Any::String(v)) => Scalar::Str(v.to_string()),
                                Out::Any(yrs::Any::Number(v)) => Scalar::Int(v.as_i64().unwrap()),
                                Out::Any(yrs::Any::Bool(v)) => Scalar::Bool(v),
                                _ => panic!("unexpected Yrs value"),
                            };
                            (k.to_string(), v)
                        })
                        .collect(),
                }
            }
            Self::L(d) | Self::LU(d) => {
                let v = d.get_map("map").get_value();
                let map = if let LoroValue::Map(m) = v {
                    m.iter()
                        .map(|(k, v)| {
                            let v = match v {
                                LoroValue::String(v) => Scalar::Str(v.to_string()),
                                LoroValue::I64(v) => Scalar::Int(*v),
                                LoroValue::Bool(v) => Scalar::Bool(*v),
                                _ => panic!("unexpected Loro value"),
                            };
                            (k.clone(), v)
                        })
                        .collect()
                } else {
                    panic!("expected map")
                };
                State {
                    text: d.get_text("text").to_string(),
                    map,
                }
            }
            Self::A { doc, text, map, .. } => {
                let map = doc
                    .map_range(map, ..)
                    .map(|item| {
                        let owned: AmValue = item.value.into();
                        let v = match owned {
                            AmValue::Scalar(v) => match v.as_ref() {
                                ScalarValue::Str(v) => Scalar::Str(v.to_string()),
                                ScalarValue::Int(v) => Scalar::Int(*v),
                                ScalarValue::Uint(v) => Scalar::Int(*v as i64),
                                ScalarValue::Boolean(v) => Scalar::Bool(*v),
                                _ => panic!("unexpected scalar"),
                            },
                            _ => panic!("unexpected object"),
                        };
                        (item.key.to_string(), v)
                    })
                    .collect();
                State {
                    text: doc.text(text).unwrap(),
                    map,
                }
            }
            Self::D { doc, .. } => State {
                text: doc.branch.content().to_string(),
                map: BTreeMap::new(),
            },
        }
    }
    fn save(&mut self) -> Vec<u8> {
        match self {
            Self::P{inner,base,base_version} => {
                if base.is_none(){*base=Some(inner.save());*base_version=Some(inner.version());}
                let bytes=base.as_ref().unwrap();let mut out=b"CRDTAIL1".to_vec();out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());out.extend_from_slice(bytes);out.extend_from_slice(&inner.delta(base_version.as_ref().unwrap()));out
            },
            Self::Y{doc:d,codec_v1} => if *codec_v1 {d.transact().encode_state_as_update_v1(&StateVector::default())} else {d.transact().encode_state_as_update_v2(&StateVector::default())},
            Self::L(d) => d.export(ExportMode::Snapshot).unwrap(),
            Self::LU(d) => d.export(ExportMode::all_updates()).unwrap(),
            Self::A { doc, .. } => doc.save(),
            Self::D { doc, .. } => doc.oplog.encode(ENCODE_FULL),
        }
    }
    fn version(&mut self) -> Version {
        match self {
            Self::P{inner,..} => inner.version(),
            Self::Y{doc:d,..} => Version::Y(d.transact().state_vector()),
            Self::L(d) | Self::LU(d) => Version::L(d.oplog_vv()),
            Self::A { doc, .. } => Version::A(doc.get_heads()),
            Self::D { doc, .. } => Version::D(doc.oplog.local_version_ref().to_vec()),
        }
    }
    fn delta(&mut self, from: &Version) -> Vec<u8> {
        match (self, from) {
            (Self::P{inner,..},v) => inner.delta(v),
            (Self::Y{doc:d,codec_v1}, Version::Y(v)) => if *codec_v1 {d.transact().encode_state_as_update_v1(v)} else {d.transact().encode_state_as_update_v2(v)},
            (Self::L(d) | Self::LU(d), Version::L(v)) => d.export(ExportMode::updates(v)).unwrap(),
            (
                Self::A {
                    doc, bounded_sync, ..
                },
                Version::A(v),
            ) => {
                if !*bounded_sync {
                    return doc.save_after(v);
                }
                // Explicit framing of compressed native changes permits bounded
                // ingestion without parsing or modifying Automerge's wire format.
                let mut out = Vec::new();
                for mut change in doc.get_changes(v) {
                    let bytes = change.bytes();
                    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                    out.extend_from_slice(&bytes);
                }
                out
            }
            (Self::D { doc, .. }, Version::D(v)) => {
                doc.oplog.encode_from(ENCODE_PATCH, v)
            }
            _ => panic!("version type mismatch"),
        }
    }
    fn import(&mut self, bytes: &[u8]) {
        match self {
            Self::P{inner,..} => inner.import(bytes),
            Self::Y{doc:d,codec_v1} => {
                d.transact_mut()
                    .apply_update(if *codec_v1 {Update::decode_v1(bytes).unwrap()} else {Update::decode_v2(bytes).unwrap()})
                    .unwrap();
            }
            Self::L(d) | Self::LU(d) => {
                d.import(bytes).unwrap();
            }
            Self::A {
                doc, bounded_sync, ..
            } => {
                if !*bounded_sync {
                    doc.load_incremental(bytes).unwrap();
                    return;
                }
                let mut offset = 0;
                let mut changes = Vec::with_capacity(32);
                while offset < bytes.len() {
                    let len =
                        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
                    offset += 8;
                    changes.push(
                        automerge::Change::from_bytes(bytes[offset..offset + len].to_vec())
                            .unwrap(),
                    );
                    offset += len;
                    if changes.len() == 32 {
                        doc.apply_changes(std::mem::take(&mut changes)).unwrap();
                    }
                }
                if !changes.is_empty() {
                    doc.apply_changes(changes).unwrap();
                }
            }
            Self::D { doc, .. } => {
                doc.merge_data_and_ff(bytes).unwrap();
            }
        }
    }
}

fn edit(doc: &mut Backend, ops: &[Op], batch: usize) -> (f64, f64, f64) {
    let mut latencies = Vec::with_capacity(ops.len().div_ceil(batch));
    let start = Instant::now();
    for chunk in ops.chunks(batch) {
        let t = Instant::now();
        doc.apply(chunk);
        latencies.push(ms(t));
    }
    let elapsed = ms(start);
    latencies.sort_by(f64::total_cmp);
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)];
    (elapsed, p50, p99)
}

fn run(lib: &str, fixture_path: &str, output: &str) {
    let fixture: Fixture = serde_json::from_slice(&fs::read(fixture_path).unwrap()).unwrap();
    let baseline_rss = rss();
    let mut d = Backend::empty(lib, 1);
    let t = Instant::now();
    d.init(&fixture.initial, fixture.batch);
    let init_ms = ms(t);
    trace_memory("initial document built");
    assert!(d.state() == fixture.initial, "initial state differs");
    let common = d.version();
    let base = d.save();
    drop(d);
    let mut server = Backend::load(lib, &base, 2);
    let mut offline = Backend::load(lib, &base, 3);
    trace_memory("two replicas loaded");
    let split = fixture.server.len() / 2;
    let (server_1_ms, p50_1, p99_1) = edit(&mut server, &fixture.server[..split], fixture.batch);
    trace_memory("first server edit phase complete");
    let midway = server.version();
    let delta1 = server.delta(&common);
    let t = Instant::now();
    let compact = server.save();
    let checkpoint_save_ms = ms(t);
    drop(server);
    let t = Instant::now();
    server = Backend::load(lib, &compact, 4);
    let checkpoint_load_ms = ms(t);
    drop(compact);
    trace_memory("midpoint server reload complete");
    let (server_2_ms, p50_2, p99_2) = edit(&mut server, &fixture.server[split..], fixture.batch);
    trace_memory("second server edit phase complete");
    assert!(
        server.state() == fixture.server_expected,
        "server edit trace differs from reference"
    );
    let delta2 = server.delta(&midway);
    let t = Instant::now();
    let compact = server.save();
    let compact_ms = ms(t);
    let pre_merge_bytes = compact.len();
    drop(server);
    let t = Instant::now();
    server = Backend::load(lib, &compact, 5);
    let reload_ms = ms(t);
    drop(compact);
    trace_memory("final server reload complete");
    let (offline_edit_ms, offline_p50, offline_p99) =
        edit(&mut offline, &fixture.offline, fixture.batch);
    trace_memory("offline edits complete");
    assert!(
        offline.state() == fixture.offline_expected,
        "offline edit trace differs from reference"
    );
    // Both replicas restart from durable CRDT state before reconnecting.
    let offline_saved = offline.save();
    drop(offline);
    offline = Backend::load(lib, &offline_saved, 6);
    drop(offline_saved);
    trace_memory("offline client reload complete");
    let t = Instant::now();
    let upload = offline.delta(&common);
    let upload_encode_ms = ms(t);
    let t = Instant::now();
    server.import(&upload);
    let reconnect_server_ms = ms(t);
    trace_memory("offline upload merged on server");
    let t = Instant::now();
    offline.import(&delta1);
    offline.import(&delta2);
    let reconnect_client_ms = ms(t);
    trace_memory("server changes merged on offline client");
    let merged = server.state();
    assert!(merged == offline.state(), "replicas did not converge");
    assert!(
        merged.text.contains("OFFLINE_SENTINEL_2026"),
        "offline insertion was lost"
    );
    assert!(
        merged.text.contains("SERVER_SENTINEL_2026"),
        "server insertion was lost"
    );
    if !fixture.initial.map.is_empty() {
        assert_eq!(
            merged.map.get("offline-only"),
            Some(&Scalar::Str("preserved".into()))
        );
        assert_eq!(
            merged.map.get("server-only"),
            Some(&Scalar::Str("preserved".into()))
        );
    }
    server.import(&upload);
    offline.import(&delta1);
    offline.import(&delta2);
    trace_memory("duplicate replay complete");
    assert!(server.state() == merged);
    assert!(offline.state() == merged);
    // Replay server deltas in reverse dependency order on a fresh common replica.
    let mut reordered = Backend::load(lib, &base, 20);
    drop(base);
    if lib == "diamond" || lib == "automerge-bounded" {
        reordered.import(&delta1);
        reordered.import(&delta2);
    } else {
        reordered.import(&delta2);
        reordered.import(&delta1);
    }
    reordered.import(&upload);
    trace_memory("dependency reorder replay complete");
    assert!(
        reordered.state() == merged,
        "reordered updates did not converge"
    );
    drop(reordered);
    let t = Instant::now();
    let snapshot = server.save();
    let save_ms = ms(t);
    let t = Instant::now();
    let compressed = zstd::stream::encode_all(snapshot.as_slice(), 3).unwrap();
    let zstd_ms = ms(t);
    fs::write(output, &snapshot).unwrap();
    fs::write(
        format!("{output}.state.json"),
        serde_json::to_vec(&merged).unwrap(),
    )
    .unwrap();
    println!(
        "{}",
        json!({"library":lib,"scenario":fixture.name,"batch":fixture.batch,"initial_text_bytes":fixture.initial.text.len(),"initial_map_keys":fixture.initial.map.len(),"server_edits":fixture.server.len(),"offline_edits":fixture.offline.len(),"init_ms":init_ms,"server_edit_ms":server_1_ms+server_2_ms,"server_batch_p50_ms":(p50_1+p50_2)/2.0,"server_batch_p99_ms":p99_1.max(p99_2),"offline_edit_ms":offline_edit_ms,"offline_batch_p50_ms":offline_p50,"offline_batch_p99_ms":offline_p99,"checkpoint_save_ms":checkpoint_save_ms,"checkpoint_load_ms":checkpoint_load_ms,"compact_ms":compact_ms,"reload_ms":reload_ms,"pre_merge_snapshot_bytes":pre_merge_bytes,"upload_encode_ms":upload_encode_ms,"reconnect_server_ms":reconnect_server_ms,"reconnect_client_ms":reconnect_client_ms,"upload_bytes":upload.len(),"download_bytes":delta1.len()+delta2.len(),"snapshot_bytes":snapshot.len(),"zstd3_bytes":compressed.len(),"zstd3_ms":zstd_ms,"save_ms":save_ms,"final_text_bytes":merged.text.len(),"final_map_keys":merged.map.len(),"converged":true,"offline_sentinel_preserved":true,"duplicate_updates_pass":true,"reordered_updates_pass":lib!="diamond" && lib!="automerge-bounded","baseline_rss_bytes":baseline_rss})
    );
}

fn memory(lib: &str, snapshot_path: &str) {
    let baseline_live = live();
    let baseline_rss = rss();
    let bytes = fs::read(snapshot_path).unwrap();
    let size = bytes.len();
    let t = Instant::now();
    let mut d = Backend::load(lib, &bytes, 90);
    let load_ms = ms(t);
    drop(bytes);
    let loaded_live = live().saturating_sub(baseline_live);
    let loaded_rss = rss();
    let t = Instant::now();
    let state = d.state();
    let materialize_ms = ms(t);
    let text_bytes = state.text.len();
    let map_keys = state.map.len();
    drop(state);
    let materialized_live = live().saturating_sub(baseline_live);
    let materialized_rss = rss();
    let t = Instant::now();
    d.apply(&[Op::Text {
        pos: text_bytes / 2,
        delete: 0,
        insert: "probe".into(),
    }]);
    let first_edit_ms = ms(t);
    let edited_live = live().saturating_sub(baseline_live);
    let edited_rss = rss();
    println!(
        "{}",
        json!({"library":lib,"snapshot_bytes":size,"load_ms":load_ms,"materialize_ms":materialize_ms,"first_edit_ms":first_edit_ms,"baseline_rss_bytes":baseline_rss,"loaded_rss_bytes":loaded_rss,"materialized_rss_bytes":materialized_rss,"edited_rss_bytes":edited_rss,"loaded_live_bytes":loaded_live,"materialized_live_bytes":materialized_live,"edited_live_bytes":edited_live,"text_bytes":text_bytes,"map_keys":map_keys,"allocation_metrics":cfg!(feature="allocation-metrics")})
    );
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "upstream-text" => upstream::run(&args[2], &args[3], &args[4]),
        "focus" => night::focus(&args[2], &args[3], &args[4], &args[5]),
        "night" => night::run(&args[2], &args[3], &args[4], &args[5]),
        "run" => run(&args[2], &args[3], &args[4]),
        "memory" => memory(&args[2], &args[3]),
        _ => panic!("usage: run LIB FIXTURE SNAPSHOT | memory LIB SNAPSHOT"),
    }
}
