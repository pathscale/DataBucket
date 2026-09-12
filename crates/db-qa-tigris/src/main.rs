use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
    UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use reqwest::blocking::Client;
use reqwest::header::{ETAG, IF_MATCH, IF_NONE_MATCH, RANGE};
use reqwest::{Method, Url};
use rusty_s3::{Bucket, S3Action, UrlStyle};
use serde::Serialize;
use std::collections::BTreeSet;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type DynError = Box<dyn Error + Send + Sync>;
type Result<T> = std::result::Result<T, DynError>;

const USER_AGENT: &str = "db-qa-tigris/0.1";

#[derive(Serialize)]
struct Report {
    schema: u8,
    timestamp_unix_seconds: u64,
    environment: Environment,
    methodology: Methodology,
    integrity: Integrity,
    latency: Latencies,
    throughput: Throughput,
    packed_objects: Vec<ObjectRate>,
    conditional_put: ConditionalPut,
}

#[derive(Serialize)]
struct Environment {
    provider: String,
    fly_region: String,
    fly_machine_id: String,
    endpoint_host: String,
    bucket: String,
    region: String,
    url_style: &'static str,
    page_bytes: usize,
    runtime: &'static str,
}

#[derive(Serialize)]
struct Methodology {
    latency_samples: usize,
    operations_per_throughput_case: usize,
    concurrency_levels: Vec<usize>,
    payload_mbit_excludes_protocol_overhead: bool,
}

#[derive(Default, Serialize)]
struct Integrity {
    page_reads_verified: usize,
    conditional_results_verified: usize,
    cleanup_objects_deleted: usize,
}

#[derive(Serialize)]
struct Latencies {
    page_put: Latency,
    page_get: Latency,
    page_head: Latency,
    range_get_256k: Latency,
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
    bytes_per_operation: usize,
    elapsed_ms: f64,
    operations_per_second: f64,
    payload_mbit_per_second: f64,
}

#[derive(Serialize)]
struct ObjectRate {
    object_bytes: usize,
    iterations: usize,
    put_latency: Latency,
    get_latency: Latency,
    put_mbit_per_second: f64,
    get_mbit_per_second: f64,
}

#[derive(Serialize)]
struct ConditionalPut {
    if_none_match_create_status: u16,
    if_none_match_conflict_status: u16,
    if_match_wrong_status: u16,
    if_match_current_status: u16,
    safe_manifest_cas: bool,
}

struct Harness {
    client: Client,
    bucket: Bucket,
    credentials: Credentials,
    region: String,
    prefix: String,
    keys: RwLock<BTreeSet<String>>,
}

impl Harness {
    fn put(&self, key: &str, body: &[u8]) -> Result<Duration> {
        self.track(key);
        let url = self.bucket.put_object(None, key).sign(Duration::ZERO);
        let started = Instant::now();
        checked(self.send(Method::PUT, url, &[], body)?)?;
        Ok(started.elapsed())
    }

    fn get(&self, key: &str) -> Result<(Vec<u8>, Duration)> {
        let url = self.bucket.get_object(None, key).sign(Duration::ZERO);
        let started = Instant::now();
        let body = checked(self.send(Method::GET, url, &[], &[])?)?
            .bytes()
            .map_err(reqwest::Error::without_url)?;
        Ok((body.to_vec(), started.elapsed()))
    }

    fn range_get(&self, key: &str, first: usize, last: usize) -> Result<(Vec<u8>, Duration)> {
        let url = self.bucket.get_object(None, key).sign(Duration::ZERO);
        let range = format!("bytes={first}-{last}");
        let started = Instant::now();
        let body = checked(self.send(Method::GET, url, &[(RANGE, &range)], &[])?)?
            .bytes()
            .map_err(reqwest::Error::without_url)?;
        Ok((body.to_vec(), started.elapsed()))
    }

    fn head(&self, key: &str) -> Result<(String, Duration)> {
        let url = self.bucket.head_object(None, key).sign(Duration::ZERO);
        let started = Instant::now();
        let response = checked(self.send(Method::HEAD, url, &[], &[])?)?;
        let elapsed = started.elapsed();
        let etag = response
            .headers()
            .get(ETAG)
            .ok_or("HEAD response has no ETag")?
            .to_str()?
            .to_owned();
        Ok((etag, elapsed))
    }

    fn conditional_put(
        &self,
        key: &str,
        body: &[u8],
        header: reqwest::header::HeaderName,
        value: &str,
    ) -> Result<u16> {
        self.track(key);
        let url = self.bucket.put_object(None, key).sign(Duration::ZERO);
        Ok(self
            .send(Method::PUT, url, &[(header, value)], body)?
            .status()
            .as_u16())
    }

    fn track(&self, key: &str) {
        self.keys.write().unwrap().insert(key.to_owned());
    }

    fn cleanup(&self) -> usize {
        let keys: Vec<_> = self.keys.read().unwrap().iter().cloned().collect();
        let mut deleted = 0;
        for key in keys {
            let url = self.bucket.delete_object(None, &key).sign(Duration::ZERO);
            if self
                .send(Method::DELETE, url, &[], &[])
                .is_ok_and(|r| r.status().is_success())
            {
                deleted += 1;
            }
        }
        deleted
    }

    fn send(
        &self,
        method: Method,
        url: Url,
        headers: &[(reqwest::header::HeaderName, &str)],
        body: &[u8],
    ) -> Result<reqwest::blocking::Response> {
        let mut request = self
            .client
            .request(method, url)
            .headers(
                headers
                    .iter()
                    .map(|(name, value)| {
                        Ok((name.clone(), reqwest::header::HeaderValue::from_str(value)?))
                    })
                    .collect::<Result<_>>()?,
            )
            .body(body.to_vec())
            .build()
            .map_err(reqwest::Error::without_url)?;
        let identity = self.credentials.clone().into();
        let mut settings = SigningSettings::default();
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        let params: aws_sigv4::http_request::SigningParams<'_> = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("s3")
            .time(SystemTime::now())
            .settings(settings)
            .build()?
            .into();
        let header_values = request
            .headers()
            .iter()
            .map(|(name, value)| Ok((name.as_str(), value.to_str()?)))
            .collect::<Result<Vec<_>>>()?;
        let signable = SignableRequest::new(
            request.method().as_str(),
            request.url().as_str(),
            header_values.iter().copied(),
            SignableBody::Bytes(body),
        )?;
        let (instructions, _) = sign(signable, &params)?.into_parts();
        for (name, value) in instructions.headers() {
            request.headers_mut().insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
                reqwest::header::HeaderValue::from_str(value)?,
            );
        }
        self.client
            .execute(request)
            .map_err(reqwest::Error::without_url)
            .map_err(Into::into)
    }
}

fn checked(response: reqwest::blocking::Response) -> Result<reqwest::blocking::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().unwrap_or_default();
    let code = xml_tag(&body, "Code").unwrap_or("unknown");
    let message = xml_tag(&body, "Message").unwrap_or("no provider message");
    Err(format!("S3 request failed with HTTP {status} ({code}): {message}").into())
}

fn xml_tag<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = body.find(&start_tag)? + start_tag.len();
    let end = body[start..].find(&end_tag)? + start;
    Some(&body[start..end])
}

fn main() {
    if let Err(error) = run() {
        eprintln!("db-qa-tigris failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let provider = env("DB_QA_PROVIDER")?;
    let endpoint = env_any(&["S3_ENDPOINT", "AWS_ENDPOINT_URL_S3"])?;
    let region = env_any(&["S3_REGION", "AWS_REGION"])?;
    let bucket_name = env_any(&["S3_BUCKET", "BUCKET_NAME"])?;
    let access_key = env("AWS_ACCESS_KEY_ID")?;
    let secret_key = env("AWS_SECRET_ACCESS_KEY")?;
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
    let (url_style, url_style_name) = match std::env::var("S3_URL_STYLE").as_deref() {
        Ok("virtual") => (UrlStyle::VirtualHost, "virtual"),
        _ => (UrlStyle::Path, "path"),
    };
    let bucket = Bucket::new(
        endpoint.parse()?,
        url_style,
        bucket_name.clone(),
        region.clone(),
    )?;
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .pool_max_idle_per_host(64)
        .timeout(Duration::from_secs(120))
        .build()?;
    let harness = Harness {
        client,
        bucket,
        credentials: Credentials::new(
            access_key,
            secret_key,
            session_token,
            None,
            "db-qa-tigris environment",
        ),
        region: region.clone(),
        prefix: unique_prefix()?,
        keys: RwLock::new(BTreeSet::new()),
    };
    eprintln!(
        "phase=provisioned provider={provider} region={} fly_region={}",
        region,
        env_string("FLY_REGION", "unknown")
    );
    let measured = measure(
        &harness,
        provider,
        endpoint,
        region,
        bucket_name,
        url_style_name,
    );
    let deleted = harness.cleanup();
    let mut report = measured?;
    report.integrity.cleanup_objects_deleted = deleted;
    println!("DB_QA_S3_JSON={}", serde_json::to_string(&report)?);
    Ok(())
}

fn measure(
    harness: &Harness,
    provider: String,
    endpoint: String,
    region: String,
    bucket_name: String,
    url_style: &'static str,
) -> Result<Report> {
    let page_bytes = data_bucket::PAGE_SIZE;
    let latency_samples = env_usize("DB_QA_LATENCY_SAMPLES", 32);
    let throughput_operations = env_usize("DB_QA_THROUGHPUT_OPS", 128);
    let concurrency_levels = env_usizes("DB_QA_CONCURRENCY", &[1, 4, 16]);
    let packed_iterations = env_usize("DB_QA_PACKED_ITERS", 3);
    let mut integrity = Integrity::default();
    let page = patterned(page_bytes, 7);

    eprintln!("phase=latency");
    let latency_key = format!("{}/latency/page", harness.prefix);
    let page_put = sample(latency_samples, || harness.put(&latency_key, &page))?;
    let page_get = sample(latency_samples, || {
        let (body, elapsed) = harness.get(&latency_key)?;
        if body != page {
            return Err("page GET integrity check failed".into());
        }
        integrity.page_reads_verified += 1;
        Ok(elapsed)
    })?;
    let page_head = sample(latency_samples, || {
        harness.head(&latency_key).map(|(_, elapsed)| elapsed)
    })?;

    let range_key = format!("{}/range/segment", harness.prefix);
    let range_object = patterned(4 * 1024 * 1024, 23);
    harness.put(&range_key, &range_object)?;
    let range_bytes = 256 * 1024;
    let range_get = sample(latency_samples, || {
        let (body, elapsed) = harness.range_get(&range_key, 0, range_bytes - 1)?;
        if body != range_object[..range_bytes] {
            return Err("range GET integrity check failed".into());
        }
        integrity.page_reads_verified += range_bytes / page_bytes;
        Ok(elapsed)
    })?;
    let page_put_latency = latency(&page_put);
    let page_get_latency = latency(&page_get);
    let page_head_latency = latency(&page_head);
    let range_get_latency = latency(&range_get);
    eprintln!(
        "latency_ms put_p50={} get_p50={} head_p50={} range_256k_p50={}",
        page_put_latency.p50_ms,
        page_get_latency.p50_ms,
        page_head_latency.p50_ms,
        range_get_latency.p50_ms
    );

    eprintln!("phase=throughput");
    let pool_keys: Vec<_> = (0..throughput_operations.max(64))
        .map(|index| format!("{}/pool/{index}", harness.prefix))
        .collect();
    let mut writes = Vec::new();
    let mut reads = Vec::new();
    for &concurrency in &concurrency_levels {
        let elapsed = run_pool(throughput_operations, concurrency, |index| {
            harness.put(&pool_keys[index], &page).map(|_| ())
        })?;
        let write_rate = rate(throughput_operations, elapsed, page_bytes);
        writes.push(ConcurrentRate {
            concurrency,
            rate: write_rate.clone(),
        });

        let verified = AtomicUsize::new(0);
        let elapsed = run_pool(throughput_operations, concurrency, |index| {
            let (body, _) = harness.get(&pool_keys[index])?;
            if body != page {
                return Err("throughput GET integrity check failed".into());
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

    eprintln!("phase=packed-objects");
    let mut packed_objects = Vec::new();
    for object_bytes in [256 * 1024, 4 * 1024 * 1024, 32 * 1024 * 1024] {
        let body = patterned(object_bytes, object_bytes / 1024);
        let key = format!("{}/packed/{object_bytes}", harness.prefix);
        let puts = sample(packed_iterations, || harness.put(&key, &body))?;
        let gets = sample(packed_iterations, || {
            let (read, elapsed) = harness.get(&key)?;
            if read != body {
                return Err("packed-object GET integrity check failed".into());
            }
            integrity.page_reads_verified += object_bytes / page_bytes;
            Ok(elapsed)
        })?;
        let put_latency = latency(&puts);
        let get_latency = latency(&gets);
        let put_mbit_per_second = payload_mbit_per_second(object_bytes, put_latency.mean_ms);
        let get_mbit_per_second = payload_mbit_per_second(object_bytes, get_latency.mean_ms);
        eprintln!(
            "packed object_bytes={object_bytes} put_mbit_per_second={put_mbit_per_second} get_mbit_per_second={get_mbit_per_second}"
        );
        packed_objects.push(ObjectRate {
            object_bytes,
            iterations: packed_iterations,
            put_latency,
            get_latency,
            put_mbit_per_second,
            get_mbit_per_second,
        });
    }

    eprintln!("phase=conditional-put");
    let manifest = format!("{}/manifest/head", harness.prefix);
    let create = harness.conditional_put(&manifest, b"0", IF_NONE_MATCH, "*")?;
    let conflict = harness.conditional_put(&manifest, b"1", IF_NONE_MATCH, "*")?;
    let (etag, _) = harness.head(&manifest)?;
    let wrong = harness.conditional_put(&manifest, b"1", IF_MATCH, "\"not-current\"")?;
    let current = harness.conditional_put(&manifest, b"1", IF_MATCH, &etag)?;
    let safe_manifest_cas = is_success(create)
        && is_precondition(conflict)
        && is_precondition(wrong)
        && is_success(current);
    if safe_manifest_cas {
        integrity.conditional_results_verified = 4;
    }
    eprintln!(
        "conditional_put create={create} conflict={conflict} wrong={wrong} current={current} safe={safe_manifest_cas}"
    );

    Ok(Report {
        schema: 1,
        timestamp_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        environment: Environment {
            provider,
            fly_region: env_string("FLY_REGION", "unknown"),
            fly_machine_id: env_string("FLY_MACHINE_ID", "unknown"),
            endpoint_host: reqwest::Url::parse(&endpoint)?
                .host_str()
                .ok_or("S3 endpoint has no host")?
                .to_owned(),
            bucket: bucket_name,
            region,
            url_style,
            page_bytes,
            runtime: "static Rust musl binary in a shell-free scratch image",
        },
        methodology: Methodology {
            latency_samples,
            operations_per_throughput_case: throughput_operations,
            concurrency_levels,
            payload_mbit_excludes_protocol_overhead: true,
        },
        integrity,
        latency: Latencies {
            page_put: page_put_latency,
            page_get: page_get_latency,
            page_head: page_head_latency,
            range_get_256k: range_get_latency,
        },
        throughput: Throughput { writes, reads },
        packed_objects,
        conditional_put: ConditionalPut {
            if_none_match_create_status: create,
            if_none_match_conflict_status: conflict,
            if_match_wrong_status: wrong,
            if_match_current_status: current,
            safe_manifest_cas,
        },
    })
}

fn patterned(bytes: usize, seed: usize) -> Vec<u8> {
    (0..bytes)
        .map(|index| ((index.wrapping_mul(31).wrapping_add(seed)) % 251) as u8)
        .collect()
}

fn is_success(status: u16) -> bool {
    (200..300).contains(&status)
}

fn is_precondition(status: u16) -> bool {
    status == 409 || status == 412
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
        bytes_per_operation,
        elapsed_ms: milliseconds(elapsed),
        operations_per_second: round(operations_per_second),
        payload_mbit_per_second: round(
            operations_per_second * bytes_per_operation as f64 * 8.0 / 1_000_000.0,
        ),
    }
}

fn payload_mbit_per_second(bytes: usize, elapsed_ms: f64) -> f64 {
    round(bytes as f64 * 8.0 / 1_000.0 / elapsed_ms)
}

fn milliseconds(duration: Duration) -> f64 {
    round(duration.as_secs_f64() * 1_000.0)
}

fn round(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn env(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| format!("{name} must be set").into())
}

fn env_any(names: &[&str]) -> Result<String> {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .ok_or_else(|| format!("one of {} must be set", names.join(", ")).into())
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
        "db-qa/{:x}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ))
}
