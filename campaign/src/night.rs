//! Isolated overnight workers. Each measurement owns exactly one replica.
use super::*;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[derive(Clone, Deserialize)]
struct Recipe {
    text_path: String,
    text_bytes: usize,
    keys: usize,
    server_edits: usize,
    offline_edits: usize,
    batch: usize,
    pattern: String,
    #[serde(default)]
    churn_bytes: usize,
}
struct Trace {
    rng: u32,
    len: usize,
    pos: usize,
    count: usize,
    index: usize,
    keys: usize,
    pattern: String,
    churn: usize,
    branch: String,
}
impl Trace {
    fn new(r: &Recipe, branch: &str) -> Self {
        Self {
            rng: if branch == "server" { 123456 } else { 987654 },
            len: r.text_bytes,
            pos: 100,
            count: if branch == "server" {
                r.server_edits
            } else {
                r.offline_edits
            },
            index: 0,
            keys: r.keys,
            pattern: r.pattern.clone(),
            churn: r.churn_bytes,
            branch: branch.into(),
        }
    }
    fn rand(&mut self) -> u32 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 17;
        self.rng ^= self.rng << 5;
        self.rng
    }
    fn next(&mut self) -> Option<Op> {
        let i = self.index;
        self.index += 1;
        if i == self.count {
            return Some(Op::Text {
                pos: 0,
                delete: 0,
                insert: format!("{}_SENTINEL_2026\n", self.branch.to_uppercase()),
            });
        }
        if i == self.count + 1 && self.keys > 0 {
            return Some(Op::Set {
                key: format!("{}-only", self.branch),
                value: Scalar::Str("preserved".into()),
            });
        }
        if i >= self.count {
            return None;
        }
        if self.keys > 0 && i % 3 != 0 {
            let key = format!("field-{:06}", self.rand() as usize % self.keys);
            return Some(if i % 7 == 0 {
                Op::Delete { key }
            } else {
                Op::Set {
                    key,
                    value: if i % 5 == 0 {
                        Scalar::Str(format!("{} revision {i}", self.branch))
                    } else if i % 3 == 1 {
                        Scalar::Int(i as i64)
                    } else {
                        Scalar::Bool(i % 2 == 1)
                    },
                }
            });
        }
        if self.pattern == "random" || self.pattern == "churn" || i % 32 == 0 {
            self.pos = 64 + self.rand() as usize % (self.len - 128 - self.churn);
        }
        self.pos = self.pos.min(self.len - 32 - self.churn);
        let (delete, insert) = if self.pattern == "churn" {
            let insert: String = (0..self.churn)
                .map(|_| (b'a' + (self.rand() % 26) as u8) as char)
                .collect();
            (self.churn, insert)
        } else if i % 5 == 0 {
            (8, String::new())
        } else if i % 11 == 0 {
            (4, format!("word{:08x} ", self.rand()))
        } else if i % 4 == 0 {
            (0, format!("text{:08x} ", self.rand()))
        } else {
            (0, ((b'a' + (self.rand() % 26) as u8) as char).to_string())
        };
        let pos = self.pos;
        self.len = self.len - delete + insert.len();
        self.pos += insert.len();
        Some(Op::Text {
            pos,
            delete,
            insert,
        })
    }
}
fn initial_value(i: usize) -> Scalar {
    match i % 3 {
        0 => Scalar::Str(format!("Record {i}: persistent document")),
        1 => Scalar::Int(i as i64),
        _ => Scalar::Bool(i % 2 == 1),
    }
}
fn initial_map(keys: usize) -> BTreeMap<String, Scalar> {
    (0..keys)
        .map(|i| (format!("field-{i:06}"), initial_value(i)))
        .collect()
}
fn digest_state(s: &State) -> String {
    digest_parts(std::iter::once(s.text.as_str()), &s.map)
}
fn digest_parts<'a>(
    chunks: impl Iterator<Item = &'a str>,
    map: &BTreeMap<String, Scalar>,
) -> String {
    let mut h = Sha256::new();
    for c in chunks {
        h.update(c.as_bytes());
    }
    h.update([0]);
    for (k, v) in map {
        h.update(k.as_bytes());
        h.update([0]);
        h.update(serde_json::to_vec(v).unwrap());
        h.update([0]);
    }
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
fn high_rss() -> usize {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|l| {
            l.strip_prefix("VmHWM:").map(|v| {
                v.split_whitespace()
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
                    * 1024
            })
        })
        .unwrap()
}
fn reset_peak() -> bool {
    fs::write("/proc/self/clear_refs", b"5").is_ok()
}
fn marker(stage: &str) {
    eprintln!(
        "{}",
        json!({"stage":stage,"rss_bytes":rss(),"peak_rss_bytes":high_rss()})
    );
}
fn read_doc(lib: &str, path: &Path, actor: u64) -> (Backend, f64) {
    let bytes = fs::read(path).unwrap();
    let t = Instant::now();
    let d = Backend::load(lib, &bytes, actor);
    let load = ms(t);
    drop(bytes);
    (d, load)
}
fn save_doc(d: &mut Backend, path: &Path) -> (usize, f64) {
    let t = Instant::now();
    let b = d.save();
    let elapsed = ms(t);
    let n = b.len();
    fs::write(path, b).unwrap();
    (n, elapsed)
}
fn check_state(s: &State) {
    assert!(s.text.contains("SERVER_SENTINEL_2026") && s.text.contains("OFFLINE_SENTINEL_2026"));
    if !s.map.is_empty() {
        assert_eq!(
            s.map.get("server-only"),
            Some(&Scalar::Str("preserved".into()))
        );
        assert_eq!(
            s.map.get("offline-only"),
            Some(&Scalar::Str("preserved".into()))
        );
    }
}
fn reference(r: &Recipe, branch: &str) -> serde_json::Value {
    let mut rope = ropey::Rope::from_str(&fs::read_to_string(&r.text_path).unwrap());
    let mut map = initial_map(r.keys);
    let mut trace = Trace::new(r, branch);
    let mut inserted = 0usize;
    let mut deleted = 0usize;
    let mut count = 0usize;
    while let Some(op) = trace.next() {
        count += 1;
        match op {
            Op::Text {
                pos,
                delete,
                insert,
            } => {
                rope.remove(pos..pos + delete);
                rope.insert(pos, &insert);
                inserted += insert.len();
                deleted += delete;
            }
            Op::Set { key, value } => {
                map.insert(key, value);
            }
            Op::Delete { key } => {
                map.remove(&key);
            }
        }
    }
    json!({"hash":digest_parts(rope.chunks(),&map),"text_bytes":rope.len_bytes(),"map_keys":map.len(),"commands":count,"inserted_bytes":inserted,"deleted_bytes":deleted})
}
pub fn run(lib: &str, recipe_path: &str, action: &str, dir: &str) {
    let r: Recipe = serde_json::from_slice(&fs::read(recipe_path).unwrap()).unwrap();
    let dir = PathBuf::from(dir);
    fs::create_dir_all(&dir).unwrap();
    let baseline = rss();
    let result = match action {
        "reference-server" | "reference-offline" => reference(
            &r,
            if action.ends_with("server") {
                "server"
            } else {
                "offline"
            },
        ),
        "init" => {
            let mut d = Backend::empty(lib, 1);
            let state = State {
                text: fs::read_to_string(&r.text_path).unwrap(),
                map: BTreeMap::new(),
            };
            let t = Instant::now();
            d.init(&state, r.batch);
            let mut init = ms(t);
            drop(state);
            for start in (0..r.keys).step_by(1024) {
                let ops: Vec<_> = (start..(start + 1024).min(r.keys))
                    .map(|i| Op::Set {
                        key: format!("field-{i:06}"),
                        value: initial_value(i),
                    })
                    .collect();
                let t = Instant::now();
                d.apply(&ops);
                init += ms(t);
            }
            let init_peak = high_rss();
            marker("initial document built");
            let reset = reset_peak();
            let (size, save) = save_doc(&mut d, &dir.join("base.bin"));
            json!({"init_ms":init,"init_peak_rss_bytes":init_peak,"save_ms":save,"save_peak_rss_bytes":high_rss(),"snapshot_bytes":size,"peak_reset_supported":reset})
        }
        "server" | "offline" => {
            let (mut d, load) = read_doc(
                lib,
                &dir.join("base.bin"),
                if action == "server" { 2 } else { 3 },
            );
            let common = d.version();
            let load_peak = high_rss();
            let reset = reset_peak();
            let mut trace = Trace::new(&r, action);
            let split = r.server_edits / 2;
            let mut done = 0usize;
            let mut edit_ms = 0f64;
            let mut encode_ms = 0f64;
            let mut checkpoint_save = 0f64;
            let mut checkpoint_load = 0f64;
            let mut checkpointed = false;
            let mut edit_high = 0usize;
            let mut checkpoint_peak = 0usize;
            let mut since = common;
            let mut latencies = Vec::new();
            loop {
                let mut ops = Vec::with_capacity(r.batch);
                let take = if action == "server" && !checkpointed {
                    r.batch.min(split - done)
                } else {
                    r.batch
                };
                for _ in 0..take {
                    if let Some(op) = trace.next() {
                        ops.push(op)
                    } else {
                        break;
                    }
                }
                if ops.is_empty() {
                    break;
                }
                done += ops.len();
                let t = Instant::now();
                d.apply(&ops);
                let elapsed = ms(t);
                edit_ms += elapsed;
                latencies.push(elapsed);
                if action == "server" && !checkpointed && done == split {
                    edit_high = edit_high.max(high_rss());
                    let t = Instant::now();
                    let delta = d.delta(&since);
                    encode_ms += ms(t);
                    fs::write(dir.join("download-1.bin"), delta).unwrap();
                    since = d.version();
                    let (_, save) = save_doc(&mut d, &dir.join("checkpoint.bin"));
                    checkpoint_save = save;
                    drop(d);
                    let (next, load) = read_doc(lib, &dir.join("checkpoint.bin"), 4);
                    d = next;
                    checkpoint_load = load;
                    fs::remove_file(dir.join("checkpoint.bin")).unwrap();
                    checkpointed = true;
                    checkpoint_peak = high_rss();
                    reset_peak();
                    marker("server checkpoint reloaded");
                }
            }
            let edit_peak = edit_high.max(high_rss());
            marker("branch edits complete");
            let t = Instant::now();
            let delta = d.delta(&since);
            encode_ms += ms(t);
            let delta_size = delta.len();
            fs::write(
                dir.join(if action == "server" {
                    "download-2.bin"
                } else {
                    "upload.bin"
                }),
                delta,
            )
            .unwrap();
            let (_, save) = save_doc(&mut d, &dir.join(format!("{action}.bin")));
            latencies.sort_by(f64::total_cmp);
            json!({"load_ms":load,"load_peak_rss_bytes":load_peak,"edit_ms":edit_ms,"edit_peak_rss_bytes":edit_peak,"checkpoint_peak_rss_bytes":checkpoint_peak,"export_and_save_peak_rss_bytes":high_rss(),"encode_ms":encode_ms,"last_delta_bytes":delta_size,"save_ms":save,"checkpoint_save_ms":checkpoint_save,"checkpoint_load_ms":checkpoint_load,"commands":done,"batch_p99_ms":latencies[((latencies.len() as f64*0.99) as usize).min(latencies.len()-1)],"peak_reset_supported":reset})
        }
        "merge-server" | "merge-client" => {
            let server = action == "merge-server";
            let (mut d, load) = read_doc(
                lib,
                &dir.join(if server { "server.bin" } else { "offline.bin" }),
                if server { 5 } else { 6 },
            );
            let load_peak = high_rss();
            marker("merge input loaded");
            let reset = reset_peak();
            let mut elapsed = 0f64;
            let paths = if server {
                vec!["upload.bin"]
            } else {
                vec!["download-1.bin", "download-2.bin"]
            };
            for path in paths {
                let b = fs::read(dir.join(path)).unwrap();
                let t = Instant::now();
                d.import(&b);
                elapsed += ms(t);
                drop(b);
            }
            let merge_peak = high_rss();
            let merge_rss = rss();
            marker("merge complete");
            reset_peak();
            let (size, save) = save_doc(
                &mut d,
                &dir.join(if server {
                    "merged-server.bin"
                } else {
                    "merged-client.bin"
                }),
            );
            json!({"load_ms":load,"load_peak_rss_bytes":load_peak,"merge_ms":elapsed,"merge_peak_rss_bytes":merge_peak,"merge_rss_bytes":merge_rss,"save_ms":save,"save_peak_rss_bytes":high_rss(),"snapshot_bytes":size,"peak_reset_supported":reset})
        }
        "check-server" | "check-offline" | "check-merged-server" | "check-merged-client" => {
            let name = action.strip_prefix("check-").unwrap();
            let (mut d, _) = read_doc(lib, &dir.join(format!("{name}.bin")), 20);
            let s = d.state();
            let hash = digest_state(&s);
            let text = s.text.len();
            let keys = s.map.len();
            if name.starts_with("merged") {
                check_state(&s);
            }
            drop(s);
            if name.starts_with("merged") {
                for p in if name.ends_with("server") {
                    vec!["upload.bin"]
                } else {
                    vec!["download-1.bin", "download-2.bin"]
                } {
                    d.import(&fs::read(dir.join(p)).unwrap());
                }
                assert_eq!(
                    hash,
                    digest_state(&d.state()),
                    "duplicate update changed state"
                );
            }
            json!({"hash":hash,"text_bytes":text,"map_keys":keys,"duplicate_updates_pass":name.starts_with("merged")})
        }
        "cold" => {
            let baseline_live = live();
            let (mut d, load) = read_doc(lib, &dir.join("merged-server.bin"), 90);
            let load_peak = high_rss();
            let loaded_rss = rss();
            let reset = reset_peak();
            let t = Instant::now();
            let s = d.state();
            let read = ms(t);
            let pos = s.text.len() / 2;
            drop(s);
            let read_peak = high_rss();
            reset_peak();
            let t = Instant::now();
            d.apply(&[Op::Text {
                pos,
                delete: 0,
                insert: "probe".into(),
            }]);
            let first = ms(t);
            json!({"load_ms":load,"read_ms":read,"first_edit_ms":first,"load_peak_rss_bytes":load_peak,"read_peak_rss_bytes":read_peak,"first_edit_peak_rss_bytes":high_rss(),"edited_rss_bytes":rss(),"loaded_rss_bytes":loaded_rss,"edited_live_bytes":live().saturating_sub(baseline_live),"allocation_metrics":cfg!(feature="allocation-metrics"),"peak_reset_supported":reset})
        }
        "storage" => {
            let bytes = fs::read(dir.join("merged-server.bin")).unwrap();
            let t = Instant::now();
            let compressed = zstd::stream::encode_all(bytes.as_slice(), 3).unwrap();
            json!({"snapshot_bytes":bytes.len(),"zstd3_bytes":compressed.len(),"zstd3_ms":ms(t),"best_disk_bytes":bytes.len().min(compressed.len())})
        }
        _ => panic!("unknown night action {action}"),
    };
    println!(
        "{}",
        json!({"action":action,"library":lib,"baseline_rss_bytes":baseline,"result":result})
    );
}

mod focused;
pub fn focus(lib: &str, recipe: &str, action: &str, dir: &str) {focused::run(lib,recipe,action,dir)}
