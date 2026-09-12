use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

type DynError = Box<dyn Error + Send + Sync>;
type Result<T> = std::result::Result<T, DynError>;
type HmacSha256 = Hmac<Sha256>;

const DEFAULT_TTL_SECONDS: u64 = 3600;
const MAX_TTL_SECONDS: u64 = 86_400;

#[derive(Serialize)]
struct Header<'a> {
    alg: &'a str,
    typ: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Paths<'a> {
    prefix_paths: [&'a str; 1],
    object_paths: [&'a str; 0],
}

#[derive(Serialize)]
struct Claims<'a> {
    bucket: &'a str,
    scope: &'a str,
    paths: Paths<'a>,
    sub: &'a str,
    iss: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

fn main() -> Result<()> {
    let account_id = env_any(&["CLOUDFLARE_ACCOUNT_ID", "CAFE__R2__ACCOUNT_ID"])?;
    let endpoint = env_any(&["S3_ENDPOINT", "CAFE__R2__ENDPOINT"])?;
    let bucket = env_any(&["S3_BUCKET", "CAFE__R2__BUCKET_NAME"])?;
    let parent_access_key = secret_env_or_prompt(
        &["AWS_ACCESS_KEY_ID", "CAFE__R2__ACCESS_KEY_ID"],
        "Parent R2 Access Key ID: ",
    )?;
    let parent_secret = secret_env_or_prompt(
        &["AWS_SECRET_ACCESS_KEY", "CAFE__R2__SECRET_ACCESS_KEY"],
        "Parent R2 Secret Access Key: ",
    )?;
    let fly_app = env("FLY_APP_NAME")?;
    let ttl = std::env::var("R2_SESSION_TTL_SECONDS")
        .map_or(Ok(DEFAULT_TTL_SECONDS), |value| value.parse::<u64>())?;
    if ttl == 0 || ttl > MAX_TTL_SECONDS {
        return Err(
            format!("R2_SESSION_TTL_SECONDS must be between 1 and {MAX_TTL_SECONDS}").into(),
        );
    }

    let endpoint_url = reqwest::Url::parse(&endpoint)?;
    let audience = endpoint_url
        .host_str()
        .ok_or("S3_ENDPOINT must contain a host")?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let header = Header {
        alg: "HS256",
        typ: "JWT",
    };
    let claims = Claims {
        bucket: &bucket,
        scope: "object-read-write",
        paths: Paths {
            prefix_paths: ["db-qa/"],
            object_paths: [],
        },
        sub: &account_id,
        iss: &parent_access_key,
        aud: audience,
        iat: now,
        exp: now.checked_add(ttl).ok_or("credential expiry overflow")?,
    };

    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
    let signing_input = format!("{header}.{claims}");
    let mut signer = HmacSha256::new_from_slice(parent_secret.as_bytes())?;
    signer.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(signer.finalize().into_bytes());
    let jwt = format!("{signing_input}.{signature}");
    let child_secret = Sha256::digest(jwt.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let session_token = STANDARD.encode(format!("jwt/{jwt}"));

    let mut child = Command::new("fly")
        .args(["secrets", "import", "--app", &fly_app])
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    let input = format!(
        "DB_QA_PROVIDER=cloudflare-r2\nS3_ENDPOINT={endpoint}\nS3_REGION=auto\nS3_BUCKET={bucket}\nS3_URL_STYLE=path\nAWS_ACCESS_KEY_ID={parent_access_key}\nAWS_SECRET_ACCESS_KEY={child_secret}\nAWS_SESSION_TOKEN={session_token}\n"
    );
    child
        .stdin
        .take()
        .ok_or("failed to open fly secrets input")?
        .write_all(input.as_bytes())?;
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("fly secrets import exited with {status}").into());
    }
    eprintln!("Imported a {ttl}-second db-qa/ R2 session into Fly app {fly_app}");
    Ok(())
}

fn env(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

fn env_any(names: &[&str]) -> Result<String> {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .ok_or_else(|| {
            format!(
                "missing required environment variable: {}",
                names.join(" or ")
            )
            .into()
        })
}

fn secret_env_or_prompt(names: &[&str], prompt: &str) -> Result<String> {
    if let Some(value) = names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
    {
        return Ok(value);
    }
    let value = rpassword::prompt_password(prompt)?;
    if value.is_empty() {
        Err(format!("{} cannot be empty", names.join(" or ")).into())
    } else {
        Ok(value)
    }
}
