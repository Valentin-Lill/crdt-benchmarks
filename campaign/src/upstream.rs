//! The exact upstream B4 trace on native Rust implementations.
use super::*;
#[derive(Deserialize)]
struct Trace { edits: Vec<(usize, usize, String)>, #[serde(rename="finalText")] final_text: String }
fn peak() -> usize {
    fs::read_to_string("/proc/self/status").unwrap().lines().find_map(|l|l.strip_prefix("VmHWM:").map(|s|s.split_whitespace().next().unwrap().parse::<usize>().unwrap()*1024)).unwrap()
}
fn reset(){fs::write("/proc/self/clear_refs",b"5").unwrap();}
pub fn run(lib: &str, input: &str, output: &str) {
    let trace:Trace=serde_json::from_slice(&fs::read(input).unwrap()).unwrap();
    assert!(trace.edits.iter().all(|e|e.2.is_ascii()),"Native B4 requires ASCII trace positions");
    let mut d=Backend::empty(lib,1); reset();let start=Instant::now();
    for (pos,delete,insert) in &trace.edits {
        // Match the two separate adapter calls in upstream B4.
        if *delete>0 {d.apply(&[Op::Text{pos:*pos,delete:*delete,insert:String::new()}]);}
        if !insert.is_empty(){d.apply(&[Op::Text{pos:*pos,delete:0,insert:insert.clone()}]);}
    }
    let edit_ms=ms(start);let edit_peak=peak();assert_eq!(d.state().text,trace.final_text);
    reset();let start=Instant::now();let bytes=d.save();let save_ms=ms(start);let save_peak=peak();
    let size=bytes.len();drop(d);reset();let start=Instant::now();let mut d=Backend::load(lib,&bytes,2);let load_ms=ms(start);let load_peak=peak();
    reset();let start=Instant::now();assert_eq!(d.state().text,trace.final_text);let read_ms=ms(start);let read_peak=peak();
    reset();let start=Instant::now();d.apply(&[Op::Text{pos:trace.final_text.len()/2,delete:0,insert:"probe".into()}]);let first_edit_ms=ms(start);let first_edit_peak=peak();
    fs::write(output,serde_json::to_vec_pretty(&json!({"library":lib,"test":"B4-native","edits":trace.edits.len(),"verified":true,"edit_ms":edit_ms,"edit_peak_rss_bytes":edit_peak,"save_ms":save_ms,"save_peak_rss_bytes":save_peak,"load_ms":load_ms,"load_peak_rss_bytes":load_peak,"read_ms":read_ms,"read_peak_rss_bytes":read_peak,"first_edit_ms":first_edit_ms,"first_edit_peak_rss_bytes":first_edit_peak,"snapshot_bytes":size,"rss_bytes":rss(),"native_timing_excludes_per_op_wire_encoding":true})).unwrap()).unwrap();
    println!("{}",json!({"verified":true,"result_file":output}));
}
