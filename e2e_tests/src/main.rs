
//! veclite end-to-end test client — single file.

mod proto {
    tonic::include_proto!("database");
}

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use proto::database_client::DatabaseClient;
use proto::value::Value as ProtoValueKind;
use proto::{
    DeleteRequest, GetRequest, InsertRequest, QueryParameters, QueryRequest,
    Record, UpdateRequest, Value, Vector,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;
use tonic::Request;

#[derive(Parser, Debug)]
#[command(about = "veclite end-to-end test suite")]
struct Args {
    #[arg(long, default_value = "..")]
    project_dir: PathBuf,
    #[arg(long, default_value = "veclite")]
    binary_name: String,
    #[arg(long, default_value_t = 25051)]
    port: u16,
    #[arg(long, default_value_t = 200)]
    stress_count: usize,
}

struct Results {
    items: Vec<(String, Result<()>)>,
}

impl Results {
    fn new() -> Self {
        Results { items: Vec::new() }
    }

    fn record(&mut self, name: &str, outcome: Result<()>) {
        let ok = outcome.is_ok();
        self.items.push((name.to_string(), outcome));
        let tag = if ok { "PASS" } else { "FAIL" };
        println!("[{tag}] {}", self.items.last().unwrap().0);
        if !ok {
            println!("       -> {:?}", self.items.last().unwrap().1);
        }
    }

    fn summary(&self) -> (usize, usize) {
        let passed = self.items.iter().filter(|(_, r)| r.is_ok()).count();
        (passed, self.items.len())
    }

    fn any_failed(&self) -> bool {
        self.items.iter().any(|(_, r)| r.is_err())
    }
}

struct ServerHandle {
    child: Child,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn binary_path(project_dir: &PathBuf, binary_name: &str) -> PathBuf {
    let mut p = project_dir.clone();
    p.push("target");
    p.push("release");
    p.push(format!("{binary_name}{}", std::env::consts::EXE_SUFFIX));
    p
}

fn spawn_configure(
    project_dir: &PathBuf,
    binary_name: &str,
    odb_dir: &PathBuf,
) -> Result<()> {
    let bin = binary_path(project_dir, binary_name);
    let status = Command::new(&bin)
        .args(["configure", "--dim", "3", "--metric", "cosine", "--density", "4"])
        .env("ODB_DIR", odb_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("failed to spawn {bin:?} configure"))?;

    if !status.success() {
        return Err(anyhow!("configure exited with status {status}"));
    }
    Ok(())
}

fn spawn_server(
    project_dir: &PathBuf,
    binary_name: &str,
    odb_dir: &PathBuf,
    port: u16,
) -> Result<ServerHandle> {
    let bin = binary_path(project_dir, binary_name);
    let child = Command::new(&bin)
        .args(["start", "--port", &port.to_string()])
        .env("ODB_DIR", odb_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to spawn {bin:?} start"))?;

    Ok(ServerHandle { child })
}

async fn connect_with_retry(port: u16) -> Result<DatabaseClient<Channel>> {
    let addr = format!("http://[::1]:{port}");
    let attempt = timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(mut client) = DatabaseClient::connect(addr.clone()).await {
                if client.heartbeat(Request::new(())).await.is_ok() {
                    return client;
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await;

    attempt.context("server did not become ready within 15s")
}

fn text_value(s: &str) -> Value {
    Value { value: Some(ProtoValueKind::Text(s.to_string())) }
}

fn number_value(n: f64) -> Value {
    Value { value: Some(ProtoValueKind::Number(n)) }
}

fn vector(data: Vec<f32>) -> Vector {
    Vector { data }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut results = Results::new();

    let bin = binary_path(&args.project_dir, &args.binary_name);
    if !bin.exists() {
        eprintln!(
            "Release binary not found at {bin:?}.\nRun `cargo build --release` in {:?} first.",
            args.project_dir
        );
        std::process::exit(2);
    }

    let odb_dir = std::env::temp_dir().join(format!("veclite-e2e-{}", uuid::Uuid::new_v4()));
    println!("Using disposable ODB_DIR: {odb_dir:?}");

    spawn_configure(&args.project_dir, &args.binary_name, &odb_dir)?;
    let mut server = spawn_server(&args.project_dir, &args.binary_name, &odb_dir, args.port)?;
    let mut client = connect_with_retry(args.port).await?;
    println!("Server is up on port {}\n", args.port);

    results.record(
        "heartbeat returns a version string",
        async {
            let resp = client.heartbeat(Request::new(())).await?;
            if resp.get_ref().version.is_empty() {
                return Err(anyhow!("version string was empty"));
            }
            Ok(())
        }
        .await,
    );

    let mut inserted_ids: Vec<String> = Vec::new();
    for i in 0..12u32 {
        let outcome: Result<()> = async {
            let mut metadata = HashMap::new();
            metadata.insert("name".to_string(), text_value(&format!("rec-{i}")));
            metadata.insert("age".to_string(), number_value((20 + i) as f64));

            let req = InsertRequest {
                record: Some(Record {
                    vector: Some(vector(vec![i as f32, (i * 2) as f32, (i * 3) as f32])),
                    metadata,
                }),
            };
            let resp = client.insert(Request::new(req)).await?;
            let id = resp.into_inner().id;
            uuid::Uuid::parse_str(&id)
                .map_err(|e| anyhow!("returned id was not a valid uuid: {e}"))?;
            inserted_ids.push(id);
            Ok(())
        }
        .await;
        results.record(&format!("insert record #{i}"), outcome);
    }

    results.record(
        "inserted enough records to trigger at least one cluster split",
        if inserted_ids.len() >= 12 {
            Ok(())
        } else {
            Err(anyhow!("expected 12 successful inserts, got {}", inserted_ids.len()))
        },
    );

    let first_id = inserted_ids
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("no inserted ids to continue testing with"))?;

    results.record(
        "get returns the record we just inserted",
        async {
            let resp = client.get(Request::new(GetRequest { id: first_id.clone() })).await?;
            let record = resp.into_inner().record.ok_or_else(|| anyhow!("response had no record"))?;
            if record.vector.as_ref().map(|v| v.data.len()) != Some(3) {
                return Err(anyhow!("vector dimension mismatch on returned record"));
            }
            Ok(())
        }
        .await,
    );

    results.record(
        "get on a malformed (non-uuid) id is invalid_argument, not a panic",
        async {
            let resp = client.get(Request::new(GetRequest { id: "not-a-uuid".to_string() })).await;
            match resp {
                Err(status) if status.code() == tonic::Code::InvalidArgument => Ok(()),
                Err(status) => Err(anyhow!("expected InvalidArgument, got {status:?}")),
                Ok(_) => Err(anyhow!("expected an error, got a successful response")),
            }
        }
        .await,
    );

    results.record(
        "get on a well-formed but nonexistent id is not_found",
        async {
            let fake_id = uuid::Uuid::new_v4().to_string();
            let resp = client.get(Request::new(GetRequest { id: fake_id })).await;
            match resp {
                Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
                Err(status) => Err(anyhow!("expected NotFound, got {status:?}")),
                Ok(_) => Err(anyhow!("expected an error, got a successful response")),
            }
        }
        .await,
    );

    results.record(
        "update replaces metadata without touching the vector",
        async {
            let before = client
                .get(Request::new(GetRequest { id: first_id.clone() }))
                .await?
                .into_inner()
                .record
                .ok_or_else(|| anyhow!("missing record before update"))?;

            let mut new_metadata = HashMap::new();
            new_metadata.insert("name".to_string(), text_value("rec-0-renamed"));
            client
                .update(Request::new(UpdateRequest { id: first_id.clone(), metadata: new_metadata }))
                .await?;

            let after = client
                .get(Request::new(GetRequest { id: first_id.clone() }))
                .await?
                .into_inner()
                .record
                .ok_or_else(|| anyhow!("missing record after update"))?;

            if after.vector != before.vector {
                return Err(anyhow!("vector changed after a metadata-only update"));
            }
            match after.metadata.get("name").and_then(|v| v.value.as_ref()) {
                Some(ProtoValueKind::Text(t)) if t == "rec-0-renamed" => Ok(()),
                other => Err(anyhow!("metadata was not updated as expected: {other:?}")),
            }
        }
        .await,
    );

    results.record(
        "query with no filter returns nearest-first results",
        async {
            let req = QueryRequest {
                vector: Some(vector(vec![0.0, 0.0, 0.0])),
                k: 5,
                filter: String::new(),
                params: None,
            };
            let resp = client.query(Request::new(req)).await?;
            let results = resp.into_inner().results;
            if results.is_empty() {
                return Err(anyhow!("expected at least one result"));
            }
            let distances: Vec<f32> = results.iter().map(|r| r.distance).collect();
            let mut sorted = distances.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            if distances != sorted {
                return Err(anyhow!("results were not sorted nearest-first: {distances:?}"));
            }
            Ok(())
        }
        .await,
    );

    results.record(
        "query with a metadata filter only returns matching records",
        async {
            let req = QueryRequest {
                vector: Some(vector(vec![0.0, 0.0, 0.0])),
                k: 20,
                filter: "age >= 30".to_string(),
                params: None,
            };
            let resp = client.query(Request::new(req)).await?;
            for r in resp.into_inner().results {
                match r.metadata.get("age").and_then(|v| v.value.as_ref()) {
                    Some(ProtoValueKind::Number(n)) if *n >= 30.0 => {}
                    other => return Err(anyhow!("filter leaked a non-matching record, age={other:?}")),
                }
            }
            Ok(())
        }
        .await,
    );

    results.record(
        "query with a tiny radius excludes everything without erroring",
        async {
            let req = QueryRequest {
                vector: Some(vector(vec![999.0, 999.0, 999.0])),
                k: 5,
                filter: String::new(),
                params: Some(QueryParameters { probes: 4, radius: 0.0001 }),
            };
            let resp = client.query(Request::new(req)).await?;
            if !resp.into_inner().results.is_empty() {
                return Err(anyhow!("expected empty results with a near-zero radius"));
            }
            Ok(())
        }
        .await,
    );

    results.record(
        "query rejects k = 0 with invalid_argument",
        async {
            let req = QueryRequest {
                vector: Some(vector(vec![0.0, 0.0, 0.0])),
                k: 0,
                filter: String::new(),
                params: None,
            };
            match client.query(Request::new(req)).await {
                Err(status) if status.code() == tonic::Code::InvalidArgument => Ok(()),
                Err(status) => Err(anyhow!("expected InvalidArgument, got {status:?}")),
                Ok(_) => Err(anyhow!("expected an error for k=0")),
            }
        }
        .await,
    );

    results.record(
        "insert rejects a vector with the wrong dimension",
        async {
            let req = InsertRequest {
                record: Some(Record { vector: Some(vector(vec![1.0, 2.0])), metadata: HashMap::new() }),
            };
            match client.insert(Request::new(req)).await {
                Err(status) if status.code() == tonic::Code::InvalidArgument => Ok(()),
                Err(status) => Err(anyhow!("expected InvalidArgument, got {status:?}")),
                Ok(_) => Err(anyhow!("expected an error for wrong-dimension vector")),
            }
        }
        .await,
    );

    results.record(
        "filter string mixing AND and OR is rejected",
        async {
            let req = QueryRequest {
                vector: Some(vector(vec![0.0, 0.0, 0.0])),
                k: 5,
                filter: "age >= 20 AND name = x OR age < 10".to_string(),
                params: None,
            };
            match client.query(Request::new(req)).await {
                Err(status) if status.code() == tonic::Code::InvalidArgument => Ok(()),
                Err(status) => Err(anyhow!("expected InvalidArgument, got {status:?}")),
                Ok(_) => Err(anyhow!("expected an error for mixed AND/OR filter")),
            }
        }
        .await,
    );

    let count_before_snapshot = inserted_ids.len();
    results.record(
        "manual snapshot reports the correct record count",
        async {
            let resp = client.snapshot(Request::new(())).await?;
            let count = resp.into_inner().count as usize;
            if count != count_before_snapshot {
                return Err(anyhow!("snapshot count {count} != expected {count_before_snapshot}"));
            }
            Ok(())
        }
        .await,
    );

    let id_to_delete = inserted_ids.pop().unwrap();
    results.record(
        "delete removes a record from both storage and index",
        async {
            client.delete(Request::new(DeleteRequest { id: id_to_delete.clone() })).await?;
            match client.get(Request::new(GetRequest { id: id_to_delete.clone() })).await {
                Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
                Err(status) => Err(anyhow!("expected NotFound after delete, got {status:?}")),
                Ok(_) => Err(anyhow!("record was still gettable after delete")),
            }
        }
        .await,
    );

    client.snapshot(Request::new(())).await?;

    println!("\nRestarting server to verify persistence...");
    drop(server);
    sleep(Duration::from_millis(500)).await;

    server = spawn_server(&args.project_dir, &args.binary_name, &odb_dir, args.port)?;
    client = connect_with_retry(args.port).await?;

    results.record(
        "a record inserted before restart is still gettable after restart",
        async {
            let still_alive_id = inserted_ids
                .first()
                .cloned()
                .ok_or_else(|| anyhow!("no surviving ids to check"))?;
            client.get(Request::new(GetRequest { id: still_alive_id })).await?;
            Ok(())
        }
        .await,
    );

    results.record(
        "the deleted record stays deleted after restart",
        async {
            match client.get(Request::new(GetRequest { id: id_to_delete.clone() })).await {
                Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
                other => Err(anyhow!("expected NotFound after restart, got {other:?}")),
            }
        }
        .await,
    );

    println!(
        "\nRunning concurrency stress test ({} concurrent inserts)...",
        args.stress_count
    );
    let stress_outcome: Result<()> = async {
        let addr = format!("http://[::1]:{}", args.port);
        let mut insert_handles = Vec::new();
        for i in 0..args.stress_count {
            let addr = addr.clone();
            insert_handles.push(tokio::spawn(async move {
                let mut c = DatabaseClient::connect(addr).await?;
                let req = InsertRequest {
                    record: Some(Record {
                        vector: Some(vector(vec![i as f32, 0.0, 0.0])),
                        metadata: HashMap::new(),
                    }),
                };
                let resp = c.insert(Request::new(req)).await?;
                Ok::<String, anyhow::Error>(resp.into_inner().id)
            }));
        }

        let insert_results = timeout(Duration::from_secs(30), futures_join_all(insert_handles))
            .await
            .map_err(|_| anyhow!("concurrent inserts hung — likely a lock-order deadlock"))?;

        let mut stress_ids = Vec::new();
        for r in insert_results {
            stress_ids.push(r??);
        }

        let mut delete_handles = Vec::new();
        for id in stress_ids {
            let addr = addr.clone();
            delete_handles.push(tokio::spawn(async move {
                let mut c = DatabaseClient::connect(addr).await?;
                c.delete(Request::new(DeleteRequest { id })).await?;
                Ok::<(), anyhow::Error>(())
            }));
        }

        let delete_results = timeout(Duration::from_secs(30), futures_join_all(delete_handles))
            .await
            .map_err(|_| anyhow!("concurrent deletes hung — likely a lock-order deadlock"))?;

        for r in delete_results {
            r??;
        }

        timeout(Duration::from_secs(5), async {
            let mut c = DatabaseClient::connect(addr).await?;
            c.heartbeat(Request::new(())).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow!("server unresponsive after concurrency stress test"))??;

        Ok(())
    }
    .await;
    results.record("server survives concurrent insert/delete load without deadlocking", stress_outcome);

    drop(server);
    let _ = std::fs::remove_dir_all(&odb_dir);

    let (passed, total) = results.summary();
    println!("\n========================================");
    println!("RESULT: {passed}/{total} checks passed");
    println!("========================================");

    if results.any_failed() {
        std::process::exit(1);
    }
    Ok(())
}

async fn futures_join_all<T>(
    handles: Vec<tokio::task::JoinHandle<T>>,
) -> Vec<Result<T, tokio::task::JoinError>> {
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await);
    }
    out
}
