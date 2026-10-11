//! Focused large-document probes, persistence and history growth.
use super::*;
#[derive(Deserialize)]
struct Focus {
    #[serde(flatten)] r: Recipe,
    history_rounds: Vec<usize>, history_chunk: usize,
    local_ops: usize, random_ops: usize,
}
fn replacement(seed: &mut u32, n: usize) -> String {
    (0..n).map(|_| { *seed^=*seed<<13;*seed^=*seed>>17;*seed^=*seed<<5;(b'a'+(*seed%26) as u8) as char }).collect()
}
fn rope_apply(rope: &mut ropey::Rope,map: &mut BTreeMap<String,Scalar>,op: &Op) {
    match op {
        Op::Text{pos,delete,insert} => {rope.remove(*pos..*pos+*delete);rope.insert(*pos,insert);},
        Op::Set{key,value} => {map.insert(key.clone(),value.clone());},
        Op::Delete{key} => {map.remove(key);},
    }
}
fn probe_ops(f: &Focus) -> Vec<(String,Op)> {
    let mid=f.r.text_bytes/2;
    let mut ops=vec![("first-edit-before-read".into(),Op::Text{pos:mid,delete:1,insert:"p".into()})];
    for p in [64,mid,f.r.text_bytes-128] {ops.push(("edges".into(),Op::Text{pos:p,delete:1,insert:"e".into()}));}
    for i in 0..f.local_ops {ops.push(("local-single".into(),Op::Text{pos:mid+64+i,delete:1,insert:((b'a'+(i%26) as u8) as char).to_string()}));}
    let mut seed=642123;
    for _ in 0..f.random_ops {
        let token=replacement(&mut seed,1);let p=128+seed as usize%(f.r.text_bytes-256);
        ops.push(("random-single".into(),Op::Text{pos:p,delete:1,insert:token}));
    }
    let sizes=if f.r.text_bytes>16*1024*1024 {vec![1024*1024,10*1024*1024]} else {vec![1024,2048]};
    for size in sizes {
        ops.push((format!("paste-{size}"),Op::Text{pos:mid,delete:0,insert:"x".repeat(size)}));
        ops.push((format!("delete-{size}"),Op::Text{pos:mid,delete:size,insert:String::new()}));
    }
    ops
}
fn quantiles(mut values: Vec<f64>) -> serde_json::Value {
    values.sort_by(f64::total_cmp);let pick=|p:f64|values[((values.len()-1) as f64*p).ceil() as usize];
    json!({"samples":values.len(),"median_ms":pick(0.5),"p95_ms":pick(0.95),"p99_ms":pick(0.99),"max_ms":values.last().unwrap(),"total_ms":values.iter().sum::<f64>()})
}
fn storage(d: &mut Backend, path: &Path) -> serde_json::Value {
    let resident=rss();reset_peak();let t=Instant::now();let bytes=d.save();let save_ms=ms(t);let save_peak=high_rss();
    reset_peak();let t=Instant::now();let compressed=zstd::stream::encode_all(bytes.as_slice(),3).unwrap();let compress_ms=ms(t);let compress_peak=high_rss();
    reset_peak();let t=Instant::now();let roundtrip=zstd::stream::decode_all(compressed.as_slice()).unwrap();let decompress_ms=ms(t);let decompress_peak=high_rss();assert_eq!(bytes,roundtrip);drop(roundtrip);
    let n=bytes.len();let z=compressed.len();let t=Instant::now();let temp=path.with_extension("tmp");fs::write(&temp,&bytes).unwrap();fs::File::open(&temp).unwrap().sync_all().unwrap();fs::rename(&temp,path).unwrap();let persist_ms=ms(t);
    fs::write(path.with_extension("zst"),&compressed).unwrap();
    json!({"raw_bytes":n,"zstd3_bytes":z,"minimum_payload_bytes":n.min(z),"resident_before_save_bytes":resident,"save_ms":save_ms,"save_peak_rss_bytes":save_peak,"compress_ms":compress_ms,"compress_peak_rss_bytes":compress_peak,"decompress_ms":decompress_ms,"decompress_peak_rss_bytes":decompress_peak,"persist_raw_fsync_ms":persist_ms,"compressed_roundtrip_pass":true})
}
pub(super) fn run(lib: &str,recipe: &str,action: &str,dir: &str) {
    let f:Focus=serde_json::from_slice(&fs::read(recipe).unwrap()).unwrap();let dir=PathBuf::from(dir);fs::create_dir_all(&dir).unwrap();
    let result=match action {
        "history-reference" | "probe-reference" => {
            let mut rope=ropey::Rope::from_str(&fs::read_to_string(&f.r.text_path).unwrap());let mut map=initial_map(f.r.keys);
            if action=="probe-reference" {
                for (_,op) in probe_ops(&f) {rope_apply(&mut rope,&mut map,&op);}
                json!({"hash":digest_parts(rope.chunks(),&map),"text_bytes":rope.len_bytes(),"map_keys":map.len()})
            } else {
                let mut trace=Trace::new(&f.r,"server");while let Some(op)=trace.next(){rope_apply(&mut rope,&mut map,&op);}
                let mut seed=35621;let mut points=vec![];
                for i in 1..=*f.history_rounds.last().unwrap() {
                    let op=Op::Text{pos:f.r.text_bytes/2,delete:f.history_chunk,insert:replacement(&mut seed,f.history_chunk)};rope_apply(&mut rope,&mut map,&op);
                    if f.history_rounds.contains(&i){points.push(json!({"round":i,"hash":digest_parts(rope.chunks(),&map),"text_bytes":rope.len_bytes(),"map_keys":map.len()}));}
                }
                json!({"points":points})
            }
        },
        "probe" => {
            let (mut d,load)=read_doc(lib,&dir.join("base.bin"),110);let load_peak=high_rss();let loaded=rss();let mut groups:BTreeMap<String,Vec<f64>>=BTreeMap::new();let mut peaks:BTreeMap<String,usize>=BTreeMap::new();
            for (name,op) in probe_ops(&f) {
                reset_peak();let t=Instant::now();d.apply(&[op]);let elapsed=ms(t);let peak=high_rss();
                groups.entry(name.clone()).or_default().push(elapsed);peaks.entry(name).and_modify(|p|*p=(*p).max(peak)).or_insert(peak);
            }
            let edited=rss();reset_peak();let t=Instant::now();let s=d.state();let read=ms(t);let read_peak=high_rss();let hash=digest_state(&s);let text_bytes=s.text.len();drop(s);
            json!({"load_ms":load,"load_peak_rss_bytes":load_peak,"loaded_rss_bytes":loaded,"edited_rss_bytes":edited,"latencies":groups.into_iter().map(|(k,v)|(k,quantiles(v))).collect::<BTreeMap<_,_>>(),"operation_peak_rss_bytes":peaks,"full_read_ms":read,"full_read_peak_rss_bytes":read_peak,"hash":hash,"text_bytes":text_bytes})
        },
        "history" => {
            let (mut base,_)=read_doc(lib,&dir.join("base.bin"),111);let common=base.version();drop(base);
            let (mut d,load)=read_doc(lib,&dir.join("server.bin"),112);let load_peak=high_rss();let mut seed=35621;let mut points=vec![];let mut elapsed=0.;let mut peak=0;let mut latencies=vec![];
            let baseline=storage(&mut d,&dir.join("server.bin"));
            for i in 1..=*f.history_rounds.last().unwrap() {
                let op=Op::Text{pos:f.r.text_bytes/2,delete:f.history_chunk,insert:replacement(&mut seed,f.history_chunk)};
                reset_peak();let t=Instant::now();d.apply(&[op]);let dt=ms(t);elapsed+=dt;latencies.push(dt);peak=peak.max(high_rss());
                if f.history_rounds.contains(&i) {
                    let stored=storage(&mut d,&dir.join("server.bin"));
                    let point=json!({"round":i,"inserted_bytes":i*f.history_chunk,"deleted_bytes":i*f.history_chunk,"edit_ms_cumulative":elapsed,"edit_peak_rss_bytes":peak,"latencies":quantiles(latencies.clone()),"storage":stored});
                    eprintln!("{}",json!({"history_checkpoint":point}));fs::write(dir.join(format!("history-point-{i}.json")),serde_json::to_vec_pretty(&point).unwrap()).unwrap();points.push(point);
                }
            }
            let t=Instant::now();let delta=d.delta(&common);let encode=ms(t);let n=delta.len();fs::write(dir.join("download-2.bin"),delta).unwrap();
            json!({"load_ms":load,"load_peak_rss_bytes":load_peak,"baseline_storage":baseline,"points":points,"encode_ms":encode,"server_delta_bytes":n,"history_bytes":f.history_rounds.last().unwrap()*f.history_chunk})
        },
        "reverse-check" => {
            let (mut d,load)=read_doc(lib,&dir.join("offline.bin"),114);
            for name in ["download-2.bin","download-1.bin"] {d.import(&fs::read(dir.join(name)).unwrap());}
            let state=d.state();check_state(&state);let hash=digest_state(&state);drop(state);
            for name in ["download-2.bin","download-1.bin"] {d.import(&fs::read(dir.join(name)).unwrap());}
            assert_eq!(hash,digest_state(&d.state()),"reverse duplicate changed state");
            json!({"hash":hash,"load_ms":load,"reverse_and_duplicate_delivery_pass":true,"peak_rss_bytes":high_rss()})
        },
        "storage" => {
            let (mut d,load)=read_doc(lib,&dir.join("merged-server.bin"),113);let stored=storage(&mut d,&dir.join("merged-server.bin"));json!({"load_ms":load,"storage":stored})
        },
        _ => panic!("unknown focus action {action}"),
    };
    println!("{}",json!({"library":lib,"action":action,"result":result}));
}
