use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type DynError = Box<dyn Error + Send + Sync>;
type Result<T> = std::result::Result<T, DynError>;

const USER_AGENT: &str = "db-qa-upstash/0.1";

#[derive(Serialize)]
struct Report {
    schema: u8,
    timestamp_unix_seconds: u64,
    environment: Environment,
    methodology: Methodology,
    integrity: Integrity,
    latency: Latencies,
    throughput: Throughput,
    generation_updates: GenerationUpdates,
}

#[derive(Serialize)]
struct Environment {
    fly_region: String,
    fly_machine_id: String,
    upstash_endpoint: String,
    page_bytes: usize,
    runtime: &'static str,
}

#[derive(Serialize)]
struct Methodology {
    latency_samples: usize,
    operations_per_throughput_case: usize,
    concurrency_levels: Vec<usize>,
    payload_mbit_excludes_protocol_overhead: bool,
    database: &'static str,
}

#[derive(Serialize, Default)]
struct Integrity {
    page_reads_verified: usize,
    atomic_commits_verified: usize,
    cleanup_keys_deleted: usize,
}

#[derive(Serialize)]
struct Latencies {
    first_ping_ms: f64,
    warm_ping: Latency,
    page_set: Latency,
    page_get: Latency,
}

#[derive(Clone, Serialize)]
struct Latency {
    samples: usize,
    min_ms: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Serialize)]
struct Throughput {
    writes: Vec<ConcurrentRate>,
    reads: Vec<ConcurrentRate>,
}

#[derive(Serialize)]
struct ConcurrentRate {
    concurrency: usize,
    #[serde(flatten)]
    rate: Rate,
}

#[derive(Clone, Serialize)]
struct Rate {
    operations: usize,
    elapsed_ms: f64,
    operations_per_second: f64,
    payload_mbit_per_second: f64,
}

#[derive(Serialize)]
struct GenerationUpdates {
    single_page: GenerationRate,
    concurrent_requests: Vec<BatchRate>,
    command_batches: Vec<CommandBatchRate>,
}

#[derive(Serialize)]
struct GenerationRate {
    latency: Latency,
    #[serde(flatten)]
    rate: Rate,
}

#[derive(Serialize)]
struct BatchRate {
    pages_per_generation: usize,
    generations: usize,
    pages: usize,
    generation_latency: Latency,
    generations_per_second: f64,
    #[serde(flatten)]
    page_rate: Rate,
}

#[derive(Serialize)]
struct CommandBatchRate {
    pages_per_request: usize,
    requests: usize,
    write_request_latency: Latency,
    read_request_latency: Latency,
    generations_per_second: f64,
    write_page_rate: Rate,
    read_page_rate: Rate,
}

struct Harness {
    client: Client,
    rest_url: String,
    token: String,
    prefix: String,
    keys: RwLock<BTreeSet<String>>,
}

impl Harness {
    fn command(&self, command: Value) -> Result<(Value, Duration)> {
        let started = Instant::now();
        let response = self
            .client
            .post(&self.rest_url)
            .bearer_auth(&self.token)
            .json(&command)
            .send()?;
        let elapsed = started.elapsed();
        let status = response.status();
        let body: Value = response.json()?;
        if !status.is_success() {
            return Err(format!("Upstash command failed: HTTP {status}").into());
        }
        if let Some(error) = body.get("error") {
            return Err(format!("Upstash command failed: {error}").into());
        }
        Ok((body.get("result").cloned().unwrap_or(Value::Null), elapsed))
    }

    fn set_page(&self, key: &str, page: &[u8]) -> Result<Duration> {
        self.track(key);
        let started = Instant::now();
        let response = self
            .client
            .post(format!("{}/set/{key}", self.rest_url))
            .bearer_auth(&self.token)
            .header("content-type", "application/octet-stream")
            .body(page.to_vec())
            .send()?;
        let elapsed = started.elapsed();
        if !response.status().is_success() {
            return Err(format!("SET failed: HTTP {}", response.status()).into());
        }
        Ok(elapsed)
    }

    fn get_page(&self, key: &str) -> Result<(Vec<u8>, Duration)> {
        let started = Instant::now();
        let response = self
            .client
            .get(format!("{}/get/{key}", self.rest_url))
            .bearer_auth(&self.token)
            .header("Upstash-Response-Format", "resp2")
            .send()?;
        let status = response.status();
        let body = response.bytes()?;
        let elapsed = started.elapsed();
        if !status.is_success() {
            return Err(format!("GET failed: HTTP {status}").into());
        }
        Ok((parse_resp_bulk(&body)?.to_vec(), elapsed))
    }

    fn mset_pages(&self, pages: &[(String, Vec<u8>)]) -> Result<Duration> {
        let mut command = Vec::with_capacity(1 + pages.len() * 2);
        command.push(Value::String("MSET".to_owned()));
        for (key, page) in pages {
            self.track(key);
            command.push(Value::String(key.clone()));
            command.push(Value::String(BASE64.encode(page)));
        }
        self.command(Value::Array(command))
            .map(|(_, elapsed)| elapsed)
    }

    fn mget_pages(&self, keys: &[String]) -> Result<(Vec<Vec<u8>>, Duration)> {
        let mut command = Vec::with_capacity(1 + keys.len());
        command.push(Value::String("MGET".to_owned()));
        command.extend(keys.iter().cloned().map(Value::String));
        let (result, elapsed) = self.command(Value::Array(command))?;
        let values = result
            .as_array()
            .ok_or("MGET returned a non-array result")?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| "MGET returned a non-string page".into())
                    .and_then(|encoded| BASE64.decode(encoded).map_err(Into::into))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((values, elapsed))
    }

    fn advance_head(&self, key: &str, expected: usize, next: usize) -> Result<Duration> {
        self.track(key);
        let script = "local current=redis.call('GET',KEYS[1]); \
            if current==ARGV[1] then redis.call('SET',KEYS[1],ARGV[2]); \
            return 1 else return 0 end";
        let (result, elapsed) = self.command(json!([
            "EVAL",
            script,
            1,
            key,
            expected.to_string(),
            next.to_string()
        ]))?;
        if result.as_i64() != Some(1) {
            return Err(format!("head compare-and-set failed at generation {expected}").into());
        }
        Ok(elapsed)
    }

    fn track(&self, key: &str) {
        self.keys.write().unwrap().insert(key.to_owned());
    }

    fn cleanup(&self) -> Result<usize> {
        let keys: Vec<_> = self.keys.read().unwrap().iter().cloned().collect();
        for chunk in keys.chunks(100) {
            let mut command = Vec::with_capacity(chunk.len() + 1);
            command.push(Value::String("DEL".to_owned()));
            command.extend(chunk.iter().cloned().map(Value::String));
            self.command(Value::Array(command))?;
        }
        Ok(keys.len())
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("db-qa-upstash failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let page_bytes = data_bucket::PAGE_SIZE;
    let latency_samples = env_usize("DB_QA_LATENCY_SAMPLES", 64);
    let throughput_operations = env_usize("DB_QA_THROUGHPUT_OPS", 128);
    let concurrency_levels = env_usizes("DB_QA_CONCURRENCY", &[1, 4, 16, 32]);
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .pool_max_idle_per_host(64)
        .timeout(Duration::from_secs(30))
        .build()?;
    let (rest_url, token) = create_temporary_database(&client)?;
    let endpoint = endpoint_host(&rest_url)?;
    let harness = Harness {
        client,
        rest_url,
        token,
        prefix: unique_prefix()?,
        keys: RwLock::new(BTreeSet::new()),
    };
    eprintln!(
        "phase=provisioned endpoint={endpoint} region={}",
        env_string("FLY_REGION", "unknown")
    );

    let measured = measure(
        &harness,
        page_bytes,
        latency_samples,
        throughput_operations,
        &concurrency_levels,
    );
    let cleanup = harness.cleanup();
    let mut report = measured?;
    report.integrity.cleanup_keys_deleted = cleanup?;
    report.environment.upstash_endpoint = endpoint;
    println!("DB_QA_UPSTASH_JSON={}", serde_json::to_string(&report)?);
    Ok(())
}

fn measure(
    harness: &Harness,
    page_bytes: usize,
    latency_samples: usize,
    throughput_operations: usize,
    concurrency_levels: &[usize],
) -> Result<Report> {
    let mut integrity = Integrity::default();
    let mut page = vec![0_u8; page_bytes];
    for (index, byte) in page.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }

    let (_, first_ping) = harness.command(json!(["PING"]))?;
    eprintln!("phase=latency");
    let warm_ping = sample(latency_samples, || {
        harness.command(json!(["PING"])).map(|(_, elapsed)| elapsed)
    })?;

    let latency_key = format!("{}:latency-page", harness.prefix);
    let mut set_index = 0;
    let page_sets = sample(latency_samples, || {
        page[0] = (set_index % 251) as u8;
        set_index += 1;
        harness.set_page(&latency_key, &page)
    })?;
    let page_gets = sample(latency_samples, || {
        let (read, elapsed) = harness.get_page(&latency_key)?;
        if read != page {
            return Err("page read integrity check failed".into());
        }
        integrity.page_reads_verified += 1;
        Ok(elapsed)
    })?;
    let warm_ping_latency = latency(&warm_ping);
    let page_set_latency = latency(&page_sets);
    let page_get_latency = latency(&page_gets);
    eprintln!(
        "latency_ms ping_p50={} ping_p95={} set_p50={} set_p95={} get_p50={} get_p95={}",
        warm_ping_latency.p50_ms,
        warm_ping_latency.p95_ms,
        page_set_latency.p50_ms,
        page_set_latency.p95_ms,
        page_get_latency.p50_ms,
        page_get_latency.p95_ms
    );

    let pool_keys: Vec<_> = (0..256)
        .map(|index| format!("{}:pool:{index}", harness.prefix))
        .collect();
    let mut writes = Vec::new();
    let mut reads = Vec::new();
    eprintln!("phase=throughput");
    for &concurrency in concurrency_levels {
        let elapsed = run_pool(throughput_operations, concurrency, |index| {
            let mut body = page.clone();
            body[0] = (index % 251) as u8;
            harness
                .set_page(&pool_keys[index % pool_keys.len()], &body)
                .map(|_| ())
        })?;
        let write_rate = rate(throughput_operations, elapsed, page_bytes);
        writes.push(ConcurrentRate {
            concurrency,
            rate: write_rate.clone(),
        });

        let verified = AtomicUsize::new(0);
        let elapsed = run_pool(throughput_operations, concurrency, |index| {
            let (read, _) = harness.get_page(&pool_keys[index % pool_keys.len()])?;
            if read.len() != page_bytes {
                return Err("throughput read returned the wrong page size".into());
            }
            verified.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })?;
        integrity.page_reads_verified += verified.load(Ordering::Relaxed);
        let read_rate = rate(throughput_operations, elapsed, page_bytes);
        reads.push(ConcurrentRate {
            concurrency,
            rate: read_rate.clone(),
        });
        eprintln!(
            "throughput concurrency={concurrency} writes_per_second={} reads_per_second={}",
            write_rate.operations_per_second, read_rate.operations_per_second
        );
    }

    eprintln!("phase=single-page-generations");
    let head = format!("{}:head:sequential", harness.prefix);
    harness.command(json!(["SET", head, "0"]))?;
    harness.track(&head);
    let generations_started = Instant::now();
    let mut generation = 0;
    let generation_samples = sample(latency_samples, || {
        let started = Instant::now();
        generation += 1;
        let page_key = format!("{}:generation:{generation}", harness.prefix);
        page[0] = (generation % 251) as u8;
        harness.set_page(&page_key, &page)?;
        harness.advance_head(&head, generation - 1, generation)?;
        integrity.atomic_commits_verified += 1;
        Ok(started.elapsed())
    })?;
    let single_page_elapsed = generations_started.elapsed();

    let mut concurrent_requests = Vec::new();
    for (pages_per_generation, generations) in [
        (8, env_usize("DB_QA_CONCURRENT_GENERATIONS_8", 40)),
        (32, env_usize("DB_QA_CONCURRENT_GENERATIONS_32", 20)),
    ] {
        eprintln!("phase=batch-generations pages={pages_per_generation}");
        let head = format!("{}:head:batch:{pages_per_generation}", harness.prefix);
        harness.command(json!(["SET", head, "0"]))?;
        harness.track(&head);
        let all_started = Instant::now();
        let mut generation = 0;
        let samples = sample(generations, || {
            let started = Instant::now();
            generation += 1;
            run_pool(pages_per_generation, pages_per_generation, |page_index| {
                let key = format!(
                    "{}:batch:{pages_per_generation}:{generation}:{page_index}",
                    harness.prefix
                );
                harness.set_page(&key, &page).map(|_| ())
            })?;
            harness.advance_head(&head, generation - 1, generation)?;
            integrity.atomic_commits_verified += 1;
            Ok(started.elapsed())
        })?;
        let elapsed = all_started.elapsed();
        let pages = pages_per_generation * generations;
        concurrent_requests.push(BatchRate {
            pages_per_generation,
            generations,
            pages,
            generation_latency: latency(&samples),
            generations_per_second: round(generations as f64 / elapsed.as_secs_f64()),
            page_rate: rate(pages, elapsed, page_bytes),
        });
    }

    let mut command_batches = Vec::new();
    for (pages_per_request, requests) in [
        (8, env_usize("DB_QA_COMMAND_BATCH_REQUESTS_8", 20)),
        (32, env_usize("DB_QA_COMMAND_BATCH_REQUESTS_32", 10)),
        (128, env_usize("DB_QA_COMMAND_BATCH_REQUESTS_128", 5)),
    ] {
        eprintln!("phase=command-batches pages={pages_per_request}");
        let head = format!("{}:head:command-batch:{pages_per_request}", harness.prefix);
        harness.command(json!(["SET", head, "0"]))?;
        harness.track(&head);
        let mut write_samples = Vec::with_capacity(requests);
        let mut read_samples = Vec::with_capacity(requests);
        let all_writes_started = Instant::now();
        for generation in 1..=requests {
            let pages: Vec<_> = (0..pages_per_request)
                .map(|page_index| {
                    let key = format!(
                        "{}:command-batch:{pages_per_request}:{generation}:{page_index}",
                        harness.prefix
                    );
                    let mut body = page.clone();
                    body[0] = (page_index % 251) as u8;
                    (key, body)
                })
                .collect();
            write_samples.push(harness.mset_pages(&pages)?);
            harness.advance_head(&head, generation - 1, generation)?;
            integrity.atomic_commits_verified += 1;
        }
        let writes_elapsed = all_writes_started.elapsed();

        let all_reads_started = Instant::now();
        for generation in 1..=requests {
            let keys: Vec<_> = (0..pages_per_request)
                .map(|page_index| {
                    format!(
                        "{}:command-batch:{pages_per_request}:{generation}:{page_index}",
                        harness.prefix
                    )
                })
                .collect();
            let (pages, elapsed) = harness.mget_pages(&keys)?;
            if pages.len() != pages_per_request || pages.iter().any(|page| page.len() != page_bytes)
            {
                return Err("MGET page integrity check failed".into());
            }
            integrity.page_reads_verified += pages.len();
            read_samples.push(elapsed);
        }
        let reads_elapsed = all_reads_started.elapsed();
        let page_count = pages_per_request * requests;
        let write_page_rate = rate(page_count, writes_elapsed, page_bytes);
        let read_page_rate = rate(page_count, reads_elapsed, page_bytes);
        eprintln!(
            "command_batch pages={pages_per_request} write_pages_per_second={} read_pages_per_second={}",
            write_page_rate.operations_per_second, read_page_rate.operations_per_second
        );
        command_batches.push(CommandBatchRate {
            pages_per_request,
            requests,
            write_request_latency: latency(&write_samples),
            read_request_latency: latency(&read_samples),
            generations_per_second: round(requests as f64 / writes_elapsed.as_secs_f64()),
            write_page_rate,
            read_page_rate,
        });
    }

    Ok(Report {
        schema: 1,
        timestamp_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        environment: Environment {
            fly_region: env_string("FLY_REGION", "unknown"),
            fly_machine_id: env_string("FLY_MACHINE_ID", "unknown"),
            upstash_endpoint: String::new(),
            page_bytes,
            runtime: "static Rust musl binary in a shell-free scratch image",
        },
        methodology: Methodology {
            latency_samples,
            operations_per_throughput_case: throughput_operations,
            concurrency_levels: concurrency_levels.to_vec(),
            payload_mbit_excludes_protocol_overhead: true,
            database: "temporary Upstash Redis database created inside the Fly Machine",
        },
        integrity,
        latency: Latencies {
            first_ping_ms: milliseconds(first_ping),
            warm_ping: warm_ping_latency,
            page_set: page_set_latency,
            page_get: page_get_latency,
        },
        throughput: Throughput { writes, reads },
        generation_updates: GenerationUpdates {
            single_page: GenerationRate {
                latency: latency(&generation_samples),
                rate: rate(latency_samples, single_page_elapsed, page_bytes),
            },
            concurrent_requests,
            command_batches,
        },
    })
}

fn create_temporary_database(client: &Client) -> Result<(String, String)> {
    let response = client.post("https://upstash.com/start-redis").send()?;
    let status = response.status();
    let body = response.text()?;
    if !status.is_success() {
        return Err(format!("temporary database creation failed: HTTP {status}").into());
    }
    let endpoint = markdown_value(&body, "Endpoint")?;
    let token = markdown_value(&body, "Token")?;
    Ok((endpoint.trim_end_matches('/').to_owned(), token.to_owned()))
}

fn markdown_value<'a>(body: &'a str, label: &str) -> Result<&'a str> {
    let prefix = format!("**{label}:**");
    body.lines()
        .find_map(|line| line.strip_prefix(&prefix).map(str::trim))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("temporary database response has no {label}").into())
}

fn parse_resp_bulk(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.first() != Some(&b'$') {
        return Err("expected RESP bulk string".into());
    }
    let header_end = bytes
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or("malformed RESP bulk string")?;
    let length: usize = std::str::from_utf8(&bytes[1..header_end])?.parse()?;
    let start = header_end + 2;
    let end = start + length;
    if bytes.len() < end + 2 || &bytes[end..end + 2] != b"\r\n" {
        return Err("truncated RESP bulk string".into());
    }
    Ok(&bytes[start..end])
}

fn run_pool<F>(operations: usize, concurrency: usize, operation: F) -> Result<Duration>
where
    F: Fn(usize) -> Result<()> + Sync,
{
    let cursor = AtomicUsize::new(0);
    let error = Mutex::new(None::<String>);
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..concurrency {
            scope.spawn(|| {
                loop {
                    if error.lock().unwrap().is_some() {
                        return;
                    }
                    let index = cursor.fetch_add(1, Ordering::Relaxed);
                    if index >= operations {
                        return;
                    }
                    if let Err(failure) = operation(index) {
                        *error.lock().unwrap() = Some(failure.to_string());
                        return;
                    }
                }
            });
        }
    });
    if let Some(error) = error.into_inner().unwrap() {
        return Err(error.into());
    }
    Ok(started.elapsed())
}

fn sample<F>(count: usize, mut operation: F) -> Result<Vec<Duration>>
where
    F: FnMut() -> Result<Duration>,
{
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(operation()?);
    }
    Ok(values)
}

fn latency(samples: &[Duration]) -> Latency {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum: Duration = sorted.iter().copied().sum();
    Latency {
        samples: sorted.len(),
        min_ms: milliseconds(sorted[0]),
        mean_ms: milliseconds(sum / sorted.len() as u32),
        p50_ms: milliseconds(percentile(&sorted, 50)),
        p95_ms: milliseconds(percentile(&sorted, 95)),
        p99_ms: milliseconds(percentile(&sorted, 99)),
        max_ms: milliseconds(sorted[sorted.len() - 1]),
    }
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
    sorted[index.min(sorted.len() - 1)]
}

fn rate(operations: usize, elapsed: Duration, bytes_per_operation: usize) -> Rate {
    let operations_per_second = operations as f64 / elapsed.as_secs_f64();
    Rate {
        operations,
        elapsed_ms: milliseconds(elapsed),
        operations_per_second: round(operations_per_second),
        payload_mbit_per_second: round(
            operations_per_second * bytes_per_operation as f64 * 8.0 / 1_000_000.0,
        ),
    }
}

fn milliseconds(duration: Duration) -> f64 {
    round(duration.as_secs_f64() * 1_000.0)
}

fn round(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn env_usizes(name: &str, fallback: &[usize]) -> Vec<usize> {
    let values: Vec<_> = std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .filter_map(|value| value.trim().parse().ok())
        .filter(|value| *value > 0)
        .collect();
    if values.is_empty() {
        fallback.to_vec()
    } else {
        values
    }
}

fn env_string(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn unique_prefix() -> Result<String> {
    Ok(format!(
        "wtbench:{:x}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ))
}

fn endpoint_host(rest_url: &str) -> Result<String> {
    Ok(reqwest::Url::parse(rest_url)?
        .host_str()
        .ok_or("Upstash REST URL has no host")?
        .to_owned())
}
