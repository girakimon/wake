//! Resolve source locations from Langfuse observations without depending on its
//! private ClickHouse schema. Telemetry supplies data, never launcher commands.
use crate::tunnel::{SourceConfig, TransportConfig, TunnelVisionConfig};
use anyhow::{anyhow, bail, Context, Result};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::path::Path;
use std::time::Duration;

const MAX_PAGES: usize = 100;
const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;

struct Langfuse {
    url: reqwest::Url,
    public_key: String,
    secret_key: String,
    trace_id: String,
    from_time: String,
    to_time: String,
    api_version: u8,
    triage_id: Option<String>,
}

fn required(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Langfuse discovery requires {name}"))
}

impl Langfuse {
    fn from_env() -> Result<Self> {
        let mut url = reqwest::Url::parse(&required("LANGFUSE_BASE_URL")?)
            .context("invalid LANGFUSE_BASE_URL")?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!("LANGFUSE_BASE_URL must be an HTTP(S) base URL without credentials, query, or fragment");
        }
        let api_version = match std::env::var("WAKE_TUNNEL_LANGFUSE_API_VERSION").as_deref() {
            Ok("1") => 1,
            Ok("2") | Err(_) => 2,
            _ => bail!("WAKE_TUNNEL_LANGFUSE_API_VERSION must be 1 or 2"),
        };
        let suffix = if api_version == 1 {
            "/api/public/observations"
        } else {
            "/api/public/v2/observations"
        };
        url.set_path(&format!("{}{suffix}", url.path().trim_end_matches('/')));
        let trace_id = required("WAKE_TUNNEL_TRACE_ID")?;
        if trace_id.len() != 32
            || !trace_id.bytes().all(|byte| byte.is_ascii_hexdigit())
            || trace_id.bytes().all(|byte| byte == b'0')
        {
            bail!("WAKE_TUNNEL_TRACE_ID must be a nonzero 32-digit OTel trace ID");
        }
        Ok(Self {
            url,
            public_key: required("LANGFUSE_PUBLIC_KEY")?,
            secret_key: required("LANGFUSE_SECRET_KEY")?,
            trace_id: trace_id.to_ascii_lowercase(),
            from_time: required("WAKE_TUNNEL_FROM_TIME")?,
            to_time: required("WAKE_TUNNEL_TO_TIME")?,
            api_version,
            triage_id: std::env::var("WAKE_TUNNEL_TRIAGE_ID")
                .ok()
                .filter(|value| !value.is_empty()),
        })
    }

    fn discover(&self) -> Result<TunnelVisionConfig> {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let mut observations = Vec::new();
        let mut cursor: Option<String> = None;
        let mut cursors = HashSet::new();
        for page in 1..=MAX_PAGES {
            let mut query = vec![
                ("traceId", self.trace_id.clone()),
                ("name", "wake.run".to_owned()),
                ("fromStartTime", self.from_time.clone()),
                ("toStartTime", self.to_time.clone()),
                ("limit", "100".to_owned()),
            ];
            if self.api_version == 1 {
                query.push(("page", page.to_string()));
            } else {
                query.push(("fields", "core,basic,metadata".to_owned()));
                if let Some(cursor) = &cursor {
                    query.push(("cursor", cursor.clone()));
                }
            }
            let response = client
                .get(self.url.clone())
                .basic_auth(&self.public_key, Some(&self.secret_key))
                .query(&query)
                .send()
                .context("querying Langfuse observations")?;
            if !response.status().is_success() {
                // Do not echo backend response bodies, which can contain private data.
                bail!("Langfuse discovery returned HTTP {} (check credentials, base URL, and API version)", response.status());
            }
            let mut bytes = Vec::new();
            response
                .take(MAX_RESPONSE_BYTES + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_RESPONSE_BYTES {
                bail!("Langfuse discovery response exceeds 8 MiB");
            }
            let response: ObservationPage =
                serde_json::from_slice(&bytes).context("decoding Langfuse observations")?;
            observations.extend(response.data);
            if self.api_version == 1 {
                let total_pages = response
                    .meta
                    .total_pages
                    .ok_or_else(|| anyhow!("Langfuse v1 response missing meta.totalPages"))?;
                if page >= total_pages {
                    return source_config(&observations, &self.trace_id, self.triage_id.as_deref());
                }
            } else {
                cursor = response.meta.cursor.filter(|value| !value.is_empty());
                let Some(next) = &cursor else {
                    return source_config(&observations, &self.trace_id, self.triage_id.as_deref());
                };
                if !cursors.insert(next.clone()) {
                    bail!("Langfuse repeated a pagination cursor");
                }
            }
        }
        bail!("Langfuse discovery exceeded {MAX_PAGES} pages; narrow the time range")
    }
}

#[derive(Deserialize)]
struct ObservationPage {
    data: Vec<Observation>,
    meta: PageMeta,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageMeta {
    cursor: Option<String>,
    total_pages: Option<usize>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Observation {
    trace_id: Option<String>,
    name: Option<String>,
    metadata: Value,
}

// Langfuse has represented dotted OTel keys both literally and as nested JSON.
fn attribute<'a>(metadata: &'a Value, key: &str) -> Option<&'a str> {
    let attributes = metadata.get("attributes")?;
    attributes.get(key).and_then(Value::as_str).or_else(|| {
        key.split('.')
            .try_fold(attributes, |value, part| value.get(part))?
            .as_str()
    })
}

fn source_config(
    observations: &[Observation],
    trace_id: &str,
    selected_triage: Option<&str>,
) -> Result<TunnelVisionConfig> {
    let mut sources = BTreeMap::new();
    let mut triage_id = selected_triage.map(str::to_owned);
    for observation in observations {
        if observation.trace_id.as_deref() != Some(trace_id)
            || observation.name.as_deref() != Some("wake.run")
        {
            continue;
        }
        let get = |key| attribute(&observation.metadata, key).filter(|value| !value.is_empty());
        let Some(triage) = get("wake.triage.id") else {
            continue;
        };
        if selected_triage.is_some_and(|selected| selected != triage) {
            continue;
        }
        if triage_id
            .as_deref()
            .is_some_and(|selected| selected != triage)
        {
            bail!("trace contains multiple triages; set WAKE_TUNNEL_TRIAGE_ID");
        }
        triage_id = Some(triage.to_owned());
        let field = |key| {
            get(key).ok_or_else(|| anyhow!("wake.run is missing {key}; publish the Tunnel Vision endpoint attributes on each worker"))
        };
        let id = field("wake.source.id")?;
        let host = get("wake.mcp.host")
            .or_else(|| get("wake.runner.host"))
            .ok_or_else(|| anyhow!("source {id} is missing wake.mcp.host / wake.runner.host"))?;
        if host.starts_with('-')
            || !host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-@:[]".contains(&byte))
        {
            bail!("source {id} has an invalid SSH host");
        }
        let database = field("wake.mcp.database")?;
        let artifact_root = field("wake.mcp.artifact_root")?;
        for path in [database, artifact_root] {
            if !Path::new(path).is_absolute() || path.contains('\0') {
                bail!("source {id} must advertise absolute database and artifact-root paths");
            }
        }
        let source = SourceConfig {
            id: id.to_owned(),
            label: id.to_owned(),
            runner: get("wake.runner.kind").unwrap_or("remote").to_owned(),
            execution_host: get("wake.runner.host").unwrap_or(host).to_owned(),
            database: database.to_owned(),
            artifact_root: artifact_root.to_owned(),
            timeout_seconds: 15,
            transport: TransportConfig::Ssh {
                host: host.to_owned(),
                ssh_args: vec![
                    "-o".to_owned(),
                    "BatchMode=yes".to_owned(),
                    "-o".to_owned(),
                    "ConnectTimeout=5".to_owned(),
                ],
                executable: "wake-mcp".to_owned(),
            },
        };
        if let Some(previous) = sources.insert(id.to_owned(), source.clone()) {
            if previous != source {
                bail!("conflicting endpoint mappings for source {id}; use a unique source ID for each workspace/worker");
            }
        }
    }
    if sources.is_empty() {
        bail!("no Wake sources found in Langfuse; check the trace/time range and triage attributes (Wake exports after completion and ingestion may be delayed)");
    }
    Ok(TunnelVisionConfig {
        version: 1,
        triage_id: triage_id.unwrap(),
        sources: sources.into_values().collect(),
    })
}

pub(crate) fn discover_langfuse() -> Result<TunnelVisionConfig> {
    Langfuse::from_env()?.discover()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

    fn observation(id: &str, host: &str) -> Value {
        json!({
            "name": "wake.run", "traceId": TRACE,
            "metadata": { "attributes": {
                "wake.triage.id": "triage-1", "wake.source.id": id,
                "wake.runner.kind": "slurm", "wake.runner.host": "compute-1",
                "wake.mcp.host": host, "wake.mcp.database": "/scratch/wake.db",
                "wake.mcp.artifact_root": "/scratch"
            }}
        })
    }

    fn config(values: Vec<Value>) -> Result<TunnelVisionConfig> {
        source_config(
            &serde_json::from_value::<Vec<Observation>>(json!(values))?,
            TRACE,
            None,
        )
    }

    #[test]
    fn deduplicates_runs_and_preserves_separate_execution_and_access_hosts() {
        let run = observation("worker-1", "user@gateway");
        let config = config(vec![run.clone(), run, observation("worker-2", "node-2")]).unwrap();
        assert_eq!(config.sources.len(), 2);
        assert_eq!(config.sources[0].execution_host, "compute-1");
        assert!(
            matches!(&config.sources[0].transport, TransportConfig::Ssh {host, ..} if host == "user@gateway")
        );
    }

    #[test]
    fn accepts_nested_attributes_and_runner_host_fallback() {
        let config = config(vec![json!({
            "name": "wake.run", "traceId": TRACE,
            "metadata": {"attributes": {"wake": {
                "triage": {"id": "triage-1"}, "source": {"id": "worker"},
                "runner": {"host": "node-1"},
                "mcp": {"database": "/work/wake.db", "artifact_root": "/work"}
            }}}
        })])
        .unwrap();
        assert!(
            matches!(&config.sources[0].transport, TransportConfig::Ssh {host, ..} if host == "node-1")
        );
    }

    #[test]
    fn rejects_conflicts_missing_locations_and_unsafe_hosts() {
        assert!(config(vec![
            observation("worker", "one"),
            observation("worker", "two")
        ])
        .is_err());
        for host in ["-oProxyCommand=bad", "node;touch", "node\nother", "$(bad)"] {
            assert!(config(vec![observation("worker", host)]).is_err());
        }
        for path in [Value::Null, json!("relative/wake.db"), json!("/work\0/db")] {
            let mut run = observation("worker", "node");
            run["metadata"]["attributes"]["wake.mcp.database"] = path;
            assert!(config(vec![run]).is_err());
        }
    }

    #[test]
    fn scopes_discovery_to_wake_run_trace_and_selected_triage() {
        let one = observation("one", "one");
        let mut other_trace = observation("other-trace", "two");
        other_trace["traceId"] = json!("other");
        let mut job = observation("job", "three");
        job["name"] = json!("compile");
        assert_eq!(
            config(vec![one.clone(), other_trace, job])
                .unwrap()
                .sources
                .len(),
            1
        );
        let mut two = observation("two", "two");
        two["metadata"]["attributes"]["wake.triage.id"] = json!("triage-2");
        assert!(config(vec![one.clone(), two.clone()]).is_err());
        let observations = serde_json::from_value::<Vec<Observation>>(json!([one, two])).unwrap();
        let selected = source_config(&observations, TRACE, Some("triage-2")).unwrap();
        assert_eq!(selected.sources[0].id, "two");
        assert!(source_config(&observations, TRACE, Some("absent")).is_err());
    }

    fn server(
        responses: Vec<(u16, Value)>,
        version: u8,
    ) -> (Langfuse, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "http://{}/api/public/{}observations",
            listener.local_addr().unwrap(),
            if version == 1 { "" } else { "v2/" }
        );
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    request.push_str(&line);
                }
                requests.push(request);
                let body = body.to_string();
                write!(stream, "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
            requests
        });
        (
            Langfuse {
                url: reqwest::Url::parse(&url).unwrap(),
                public_key: "pk".to_owned(),
                secret_key: "sk".to_owned(),
                trace_id: TRACE.to_owned(),
                from_time: "2026-09-01T00:00:00Z".to_owned(),
                to_time: "2026-09-07T00:00:00Z".to_owned(),
                api_version: version,
                triage_id: None,
            },
            server,
        )
    }

    #[test]
    fn fetches_all_v2_pages_with_auth_and_bounded_trace_filter() {
        let (client, server) = server(
            vec![
                (
                    200,
                    json!({"data": [observation("one", "one")], "meta": {"cursor": "next+/="}}),
                ),
                (
                    200,
                    json!({"data": [observation("two", "two")], "meta": {"cursor": null}}),
                ),
            ],
            2,
        );
        assert_eq!(client.discover().unwrap().sources.len(), 2);
        let requests = server.join().unwrap();
        for request in &requests {
            assert!(request
                .to_ascii_lowercase()
                .contains("authorization: basic cgs6c2s="));
            let path = request
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap();
            let url = reqwest::Url::parse(&format!("http://localhost{path}")).unwrap();
            let query = url.query_pairs().collect::<BTreeMap<_, _>>();
            assert_eq!(query["traceId"], TRACE);
            assert_eq!(query["name"], "wake.run");
            assert_eq!(query["fields"], "core,basic,metadata");
            assert_eq!(query["fromStartTime"], client.from_time);
            assert_eq!(query["toStartTime"], client.to_time);
        }
        assert!(requests[1].contains("cursor=next%2B%2F%3D"));
    }

    #[test]
    fn supports_v1_pagination_for_self_hosted_langfuse_v3() {
        let (client, server) = server(
            vec![
                (
                    200,
                    json!({"data": [observation("one", "one")], "meta": {"totalPages": 2}}),
                ),
                (
                    200,
                    json!({"data": [observation("two", "two")], "meta": {"totalPages": 2}}),
                ),
            ],
            1,
        );
        assert_eq!(client.discover().unwrap().sources.len(), 2);
        let requests = server.join().unwrap();
        assert!(requests[0].starts_with("GET /api/public/observations?"));
        assert!(requests[1].contains("page=2"));
        assert!(!requests[0].contains("fields="));
    }

    #[test]
    fn fails_on_backend_errors_and_pagination_loops() {
        let (client, worker) = server(vec![(401, json!({"message": "private backend data"}))], 2);
        let error = client.discover().unwrap_err().to_string();
        assert!(error.contains("401"));
        assert!(!error.contains("private backend data"));
        worker.join().unwrap();
        let page = json!({"data": [], "meta": {"cursor": "same"}});
        let (client, worker) = server(vec![(200, page.clone()), (200, page)], 2);
        assert!(client
            .discover()
            .unwrap_err()
            .to_string()
            .contains("repeated"));
        worker.join().unwrap();
    }
}
