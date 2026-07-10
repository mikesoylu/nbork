use std::{
    collections::HashMap,
    convert::Infallible,
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Empty, StreamBody};
use hyper::{
    body::Frame,
    header::{HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL},
    Method, Request,
};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use serde_json::{json, Map, Value};
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{self, Instant, MissedTickBehavior},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        protocol::{frame::coding::CloseCode, CloseFrame},
        Message,
    },
};
use url::Url;

const DEFAULT_BASE_URL: &str = "https://nbork.com";
const OUTBOUND_QUEUE_CAPACITY: usize = 1_024;
const HTTP_CHUNK_SIZE: usize = 64 * 1_024;

type Outbound = mpsc::Sender<Message>;
type RequestBodies = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Bytes>>>>;
type LocalSockets = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<LocalWsCommand>>>>;
type LocalHttpBody = UnsyncBoxBody<Bytes, Infallible>;
type LocalHttpClient = Client<HttpConnector, LocalHttpBody>;

#[derive(Debug)]
struct Config {
    channel: String,
    base_url: String,
    secret: Option<String>,
}

#[derive(Debug)]
enum LocalWsCommand {
    Data { bytes: Bytes, binary: bool },
    Close { code: u16, reason: String },
}

#[derive(Debug)]
struct SessionExit {
    connected: bool,
    error: anyhow::Error,
}

#[tokio::main]
async fn main() {
    let config = match parse_args() {
        Ok(Some(config)) => config,
        Ok(None) => return,
        Err(error) => {
            eprintln!("error: {error}\n");
            print_usage();
            std::process::exit(2);
        }
    };

    let control_url = match control_url(&config) {
        Ok(url) => url,
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(2);
        }
    };

    let mut failed_attempts = 0_u32;

    loop {
        let session = tokio::select! {
            session = run_session(&config.channel, &control_url) => session,
            _ = shutdown_signal() => {
                eprintln!("[tunnel] shutting down");
                return;
            }
        };

        if session.connected {
            failed_attempts = 0;
        } else {
            failed_attempts = failed_attempts.saturating_add(1);
        }

        let exponent = failed_attempts.min(5);
        let delay_ms = (500_u64.saturating_mul(1_u64 << exponent)).min(10_000);
        eprintln!(
            "[tunnel] disconnected: {:#}; reconnecting in {}ms",
            session.error, delay_ms
        );

        tokio::select! {
            _ = time::sleep(Duration::from_millis(delay_ms)) => {}
            _ = shutdown_signal() => {
                eprintln!("[tunnel] shutting down");
                return;
            }
        }
    }
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut channel = None;
    let mut base_url = env::var("TUNNEL_BASE").unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());
    let mut secret = env::var("TUNNEL_SECRET").ok();
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                return Ok(None);
            }
            "-V" | "--version" => {
                println!("nbork {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }
            "--base-url" => {
                base_url = args
                    .next()
                    .ok_or_else(|| "--base-url requires a value".to_owned())?;
            }
            "--secret" => {
                secret = Some(
                    args.next()
                        .ok_or_else(|| "--secret requires a value".to_owned())?,
                );
            }
            _ if arg.starts_with('-') => return Err(format!("unknown option: {arg}")),
            _ if channel.is_some() => return Err("only one channel may be specified".to_owned()),
            _ => channel = Some(arg),
        }
    }

    let channel = channel
        .or_else(|| env::var("CHANNEL").ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "missing channel name".to_owned())?;

    Ok(Some(Config {
        channel,
        base_url,
        secret,
    }))
}

fn print_usage() {
    eprintln!(
        "nbork {}\n\nUsage: nbork [OPTIONS] <CHANNEL>\n\nOptions:\n  --base-url <URL>  Broker URL [env: TUNNEL_BASE] [default: {DEFAULT_BASE_URL}]\n  --secret <VALUE>  Channel secret [env: TUNNEL_SECRET]\n  -h, --help        Print help\n  -V, --version     Print version\n\nThe channel can also be supplied with the CHANNEL environment variable.",
        env!("CARGO_PKG_VERSION")
    );
}

fn control_url(config: &Config) -> Result<Url> {
    let mut url = Url::parse(&config.base_url)
        .with_context(|| format!("invalid broker URL: {}", config.base_url))?;
    let websocket_scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" => "ws",
        "wss" => "wss",
        scheme => bail!("unsupported broker URL scheme: {scheme}"),
    };
    url.set_scheme(websocket_scheme)
        .map_err(|_| anyhow!("could not convert broker URL to WebSocket URL"))?;
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    url.path_segments_mut()
        .map_err(|_| anyhow!("broker URL cannot be used as a base URL"))?
        .push("_connect")
        .push(&config.channel);
    if let Some(secret) = &config.secret {
        url.query_pairs_mut().append_pair("auth", secret);
    }
    Ok(url)
}

async fn run_session(channel: &str, control_url: &Url) -> SessionExit {
    match connect_and_proxy(channel, control_url).await {
        Ok(()) => SessionExit {
            connected: true,
            error: anyhow!("control connection closed"),
        },
        Err((connected, error)) => SessionExit { connected, error },
    }
}

async fn connect_and_proxy(channel: &str, control_url: &Url) -> Result<(), (bool, anyhow::Error)> {
    let mut request = control_url
        .as_str()
        .into_client_request()
        .map_err(|error| {
            (
                false,
                anyhow!(error).context("invalid control WebSocket request"),
            )
        })?;
    request.headers_mut().insert(
        "user-agent",
        HeaderValue::from_static(concat!("nbork/", env!("CARGO_PKG_VERSION"))),
    );

    let (control, _) = connect_async(request)
        .await
        .map_err(|error| (false, anyhow!(error).context("could not connect to broker")))?;
    eprintln!("[tunnel] connected: {channel}");

    proxy_session(control).await.map_err(|error| (true, error))
}

async fn proxy_session<S>(control: tokio_tungstenite::WebSocketStream<S>) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut control_sink, mut control_stream) = control.split();
    let (outbound, mut outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
    let request_bodies: RequestBodies = Arc::new(Mutex::new(HashMap::new()));
    let local_sockets: LocalSockets = Arc::new(Mutex::new(HashMap::new()));
    let http_client = local_http_client();
    let mut jobs = JoinSet::new();
    let mut app_heartbeat = time::interval(Duration::from_secs(10));
    let mut transport_heartbeat = time::interval(Duration::from_secs(15));
    app_heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    transport_heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    app_heartbeat.tick().await;
    transport_heartbeat.tick().await;
    let mut last_pong = Instant::now();

    let result = loop {
        tokio::select! {
            outbound_message = outbound_rx.recv() => {
                match outbound_message {
                    Some(message) => {
                        if let Err(error) = control_sink.send(message).await {
                            break Err(anyhow!(error).context("failed to send to broker"));
                        }
                    }
                    None => break Err(anyhow!("all proxy senders stopped")),
                }
            }
            incoming = control_stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(error) = handle_control_message(
                            text.as_str(),
                            &outbound,
                            &request_bodies,
                            &local_sockets,
                            &http_client,
                            &mut jobs,
                            &mut last_pong,
                        ) {
                            eprintln!("[tunnel] ignored invalid control message: {error:#}");
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        match std::str::from_utf8(&bytes) {
                            Ok(text) => {
                                if let Err(error) = handle_control_message(
                                    text,
                                    &outbound,
                                    &request_bodies,
                                    &local_sockets,
                                    &http_client,
                                    &mut jobs,
                                    &mut last_pong,
                                ) {
                                    eprintln!("[tunnel] ignored invalid control message: {error:#}");
                                }
                            }
                            Err(error) => eprintln!("[tunnel] ignored non-UTF-8 control message: {error}"),
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if let Err(error) = control_sink.send(Message::Pong(payload)).await {
                            break Err(anyhow!(error).context("failed to answer broker ping"));
                        }
                    }
                    Some(Ok(Message::Pong(_))) => last_pong = Instant::now(),
                    Some(Ok(Message::Close(frame))) => {
                        let reason = frame
                            .map(|frame| format!("{} {}", u16::from(frame.code), frame.reason))
                            .unwrap_or_else(|| "no close frame".to_owned());
                        break Err(anyhow!("broker closed the connection ({reason})"));
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => break Err(anyhow!(error).context("control WebSocket error")),
                    None => break Err(anyhow!("broker ended the control stream")),
                }
            }
            _ = app_heartbeat.tick() => {
                if last_pong.elapsed() > Duration::from_secs(30) {
                    break Err(anyhow!("broker heartbeat timed out"));
                }
                if let Err(error) = send_json(&outbound, json!({
                    "type": "ping",
                    "ts": unix_millis(),
                })).await {
                    break Err(error);
                }
            }
            _ = transport_heartbeat.tick() => {
                if let Err(error) = control_sink.send(Message::Ping(Bytes::new())).await {
                    break Err(anyhow!(error).context("failed to send transport heartbeat"));
                }
            }
            completed = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Err(error)) = completed {
                    if error.is_panic() {
                        eprintln!("[tunnel] proxy task panicked: {error}");
                    }
                }
            }
        }
    };

    jobs.abort_all();
    request_bodies
        .lock()
        .expect("request body map poisoned")
        .clear();
    local_sockets
        .lock()
        .expect("local socket map poisoned")
        .clear();
    result
}

#[allow(clippy::too_many_arguments)]
fn handle_control_message(
    text: &str,
    outbound: &Outbound,
    request_bodies: &RequestBodies,
    local_sockets: &LocalSockets,
    http_client: &LocalHttpClient,
    jobs: &mut JoinSet<()>,
    last_pong: &mut Instant,
) -> Result<()> {
    let message: Value = serde_json::from_str(text).context("malformed JSON")?;
    let message_type = string_field(&message, "type")?;

    match message_type {
        "ping" => {
            let outbound = outbound.clone();
            jobs.spawn(async move {
                let _ = send_json(&outbound, json!({ "type": "pong", "ts": unix_millis() })).await;
            });
        }
        "pong" => *last_pong = Instant::now(),
        "http_open" => {
            let id = string_field(&message, "id")?.to_owned();
            let method = message
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("GET")
                .to_ascii_uppercase();
            let port = valid_port(string_field(&message, "port")?)?;
            let path = valid_local_path(string_field(&message, "url")?)?.to_owned();
            let headers = string_map(message.get("headers"));
            let can_have_body = method != "GET" && method != "HEAD";
            let body_rx = if can_have_body {
                let (body_tx, body_rx) = mpsc::unbounded_channel();
                request_bodies
                    .lock()
                    .expect("request body map poisoned")
                    .insert(id.clone(), body_tx);
                Some(body_rx)
            } else {
                None
            };

            let outbound = outbound.clone();
            let request_bodies = Arc::clone(request_bodies);
            let http_client = http_client.clone();
            jobs.spawn(async move {
                if let Err(error) = proxy_http_request(
                    &id,
                    &method,
                    port,
                    &path,
                    headers,
                    body_rx,
                    &outbound,
                    &http_client,
                )
                .await
                {
                    let _ = send_json(
                        &outbound,
                        json!({
                            "type": "http_error",
                            "id": id,
                            "message": format!("{error:#}"),
                        }),
                    )
                    .await;
                }
                request_bodies
                    .lock()
                    .expect("request body map poisoned")
                    .remove(&id);
            });
        }
        "http_body" => {
            let id = string_field(&message, "id")?;
            let chunk = BASE64
                .decode(string_field(&message, "chunkBase64")?)
                .context("invalid HTTP request body base64")?;
            if let Some(sender) = request_bodies
                .lock()
                .expect("request body map poisoned")
                .get(id)
            {
                let _ = sender.send(Bytes::from(chunk));
            }
        }
        "http_body_end" => {
            let id = string_field(&message, "id")?;
            request_bodies
                .lock()
                .expect("request body map poisoned")
                .remove(id);
        }
        "ws_open" => {
            let id = string_field(&message, "id")?.to_owned();
            let port = valid_port(string_field(&message, "port")?)?;
            let path = valid_local_path(string_field(&message, "url")?)?.to_owned();
            let protocol = message
                .get("protocol")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            local_sockets
                .lock()
                .expect("local socket map poisoned")
                .insert(id.clone(), command_tx);

            let outbound = outbound.clone();
            let local_sockets = Arc::clone(local_sockets);
            jobs.spawn(async move {
                if let Err(error) = proxy_local_websocket(
                    &id,
                    port,
                    &path,
                    protocol.as_deref(),
                    command_rx,
                    &outbound,
                )
                .await
                {
                    let _ = send_json(
                        &outbound,
                        json!({
                            "type": "ws_close",
                            "id": id,
                            "code": 1011,
                            "reason": truncate_close_reason(&format!("{error:#}")),
                        }),
                    )
                    .await;
                }
                local_sockets
                    .lock()
                    .expect("local socket map poisoned")
                    .remove(&id);
            });
        }
        "ws_from_edge" => {
            let id = string_field(&message, "id")?;
            let bytes = BASE64
                .decode(string_field(&message, "dataBase64")?)
                .context("invalid WebSocket message base64")?;
            let binary = message
                .get("binary")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(sender) = local_sockets
                .lock()
                .expect("local socket map poisoned")
                .get(id)
            {
                let _ = sender.send(LocalWsCommand::Data {
                    bytes: Bytes::from(bytes),
                    binary,
                });
            }
        }
        "ws_close" => {
            let id = string_field(&message, "id")?;
            let code = message
                .get("code")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(1000);
            let reason = message
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            if let Some(sender) = local_sockets
                .lock()
                .expect("local socket map poisoned")
                .remove(id)
            {
                let _ = sender.send(LocalWsCommand::Close {
                    code: wire_close_code(code),
                    reason,
                });
            }
        }
        _ => {}
    }

    Ok(())
}

fn local_http_client() -> LocalHttpClient {
    let mut connector = HttpConnector::new();
    connector.enforce_http(true);
    Client::builder(TokioExecutor::new()).build(connector)
}

#[allow(clippy::too_many_arguments)]
async fn proxy_http_request(
    id: &str,
    method: &str,
    port: u16,
    path: &str,
    headers: HashMap<String, String>,
    body_rx: Option<mpsc::UnboundedReceiver<Bytes>>,
    outbound: &Outbound,
    client: &LocalHttpClient,
) -> Result<()> {
    let uri = format!("http://127.0.0.1:{port}{path}");
    let method = Method::from_bytes(method.as_bytes()).context("invalid HTTP method")?;
    let is_head = method == Method::HEAD;
    let mut request = Request::builder().method(method).uri(&uri);

    for (name, value) in headers {
        if is_hop_by_hop_header(&name) {
            continue;
        }
        let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_str(&value) else {
            continue;
        };
        request = request.header(name, value);
    }

    let body = match body_rx {
        Some(body_rx) => {
            let stream = UnboundedReceiverStream::new(body_rx)
                .map(|chunk| Ok::<Frame<Bytes>, Infallible>(Frame::data(chunk)));
            BodyExt::boxed_unsync(StreamBody::new(stream))
        }
        None => BodyExt::boxed_unsync(Empty::<Bytes>::new()),
    };
    let request = request.body(body).context("invalid local HTTP request")?;
    let mut response = client
        .request(request)
        .await
        .with_context(|| format!("local HTTP request failed: {uri}"))?;

    let response_headers = header_map_to_json(response.headers());
    send_json(
        outbound,
        json!({
            "type": "http_head",
            "id": id,
            "status": response.status().as_u16(),
            "headers": response_headers,
        }),
    )
    .await?;

    if !is_head {
        while let Some(frame) = response.body_mut().frame().await {
            let frame = frame.context("failed while reading local HTTP response")?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            for chunk in data.chunks(HTTP_CHUNK_SIZE) {
                send_json(
                    outbound,
                    json!({
                        "type": "http_chunk",
                        "id": id,
                        "chunkBase64": BASE64.encode(chunk),
                    }),
                )
                .await?;
            }
        }
    }

    send_json(outbound, json!({ "type": "http_end", "id": id })).await
}

async fn proxy_local_websocket(
    id: &str,
    port: u16,
    path: &str,
    protocol: Option<&str>,
    mut commands: mpsc::UnboundedReceiver<LocalWsCommand>,
    outbound: &Outbound,
) -> Result<()> {
    let local_url = format!("ws://127.0.0.1:{port}{path}");
    let mut request = local_url
        .as_str()
        .into_client_request()
        .context("invalid local WebSocket URL")?;
    if let Some(protocol) = protocol {
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(protocol).context("invalid WebSocket subprotocol")?,
        );
    }

    let (local, _) = connect_async(request)
        .await
        .with_context(|| format!("local WebSocket connection failed: {local_url}"))?;
    send_json(outbound, json!({ "type": "ws_ready", "id": id })).await?;
    let (mut local_sink, mut local_stream) = local.split();
    let mut remote_requested_close = false;
    let mut close_code = 1000_u16;
    let mut close_reason = String::new();

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(LocalWsCommand::Data { bytes, binary }) => {
                        let message = if binary {
                            Message::Binary(bytes)
                        } else {
                            Message::Text(String::from_utf8_lossy(&bytes).into_owned().into())
                        };
                        local_sink.send(message).await.context("failed to write local WebSocket")?;
                    }
                    Some(LocalWsCommand::Close { code, reason }) => {
                        remote_requested_close = true;
                        let frame = CloseFrame {
                            code: CloseCode::from(code),
                            reason: truncate_close_reason(&reason).into(),
                        };
                        let _ = local_sink.send(Message::Close(Some(frame))).await;
                        break;
                    }
                    None => break,
                }
            }
            message = local_stream.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        send_json(outbound, json!({
                            "type": "ws_to_edge",
                            "id": id,
                            "dataBase64": BASE64.encode(text.as_bytes()),
                            "binary": false,
                        })).await?;
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        send_json(outbound, json!({
                            "type": "ws_to_edge",
                            "id": id,
                            "dataBase64": BASE64.encode(&bytes),
                            "binary": true,
                        })).await?;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        local_sink.send(Message::Pong(payload)).await.context("failed to answer local WebSocket ping")?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        if let Some(frame) = frame {
                            close_code = frame.code.into();
                            close_reason = frame.reason.to_string();
                        }
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => return Err(anyhow!(error).context("local WebSocket error")),
                    None => break,
                }
            }
        }
    }

    if !remote_requested_close {
        send_json(
            outbound,
            json!({
                "type": "ws_close",
                "id": id,
                "code": close_code,
                "reason": truncate_close_reason(&close_reason),
            }),
        )
        .await?;
    }
    Ok(())
}

async fn send_json(outbound: &Outbound, value: Value) -> Result<()> {
    let text = serde_json::to_string(&value).context("could not serialize control message")?;
    outbound
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| anyhow!("control connection is no longer available"))
}

fn string_field<'a>(message: &'a Value, field: &str) -> Result<&'a str> {
    message
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing or invalid {field} field"))
}

fn string_map(value: Option<&Value>) -> HashMap<String, String> {
    value
        .and_then(Value::as_object)
        .map(|object| {
            object
                .iter()
                .filter_map(|(key, value)| {
                    value.as_str().map(|value| (key.clone(), value.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn valid_port(value: &str) -> Result<u16> {
    let port = value
        .parse::<u16>()
        .with_context(|| format!("invalid local port: {value}"))?;
    if port == 0 {
        bail!("invalid local port: 0");
    }
    Ok(port)
}

fn valid_local_path(value: &str) -> Result<&str> {
    if !value.starts_with('/') || value.starts_with("//") {
        bail!("invalid local request path");
    }
    Ok(value)
}

fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn header_map_to_json(headers: &hyper::HeaderMap) -> Map<String, Value> {
    let mut result = Map::new();
    for (name, value) in headers {
        if is_hop_by_hop_header(name.as_str()) {
            continue;
        }
        if let Ok(value) = value.to_str() {
            result.insert(name.as_str().to_owned(), Value::String(value.to_owned()));
        }
    }
    result
}

fn truncate_close_reason(reason: &str) -> String {
    const MAX_REASON_BYTES: usize = 120;
    if reason.len() <= MAX_REASON_BYTES {
        return reason.to_owned();
    }

    let mut end = MAX_REASON_BYTES;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_owned()
}

fn wire_close_code(code: u16) -> u16 {
    match code {
        1000..=1003 | 1007..=1013 | 3000..=4999 => code,
        _ => 1000,
    }
}

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate()).expect("could not install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(base_url: &str, channel: &str) -> Config {
        Config {
            channel: channel.to_owned(),
            base_url: base_url.to_owned(),
            secret: None,
        }
    }

    #[test]
    fn builds_encoded_control_url() {
        let url = control_url(&config("https://nbork.com/something", "my channel")).unwrap();
        assert_eq!(url.as_str(), "wss://nbork.com/_connect/my%20channel");
    }

    #[test]
    fn includes_secret_in_control_url() {
        let mut config = config("http://localhost:8787", "demo");
        config.secret = Some("a secret&more".to_owned());
        let url = control_url(&config).unwrap();
        assert_eq!(
            url.as_str(),
            "ws://localhost:8787/_connect/demo?auth=a+secret%26more"
        );
    }

    #[test]
    fn rejects_unsafe_local_paths_and_ports() {
        assert!(valid_local_path("//example.com/path").is_err());
        assert!(valid_local_path("https://example.com").is_err());
        assert!(valid_local_path("/safe?yes").is_ok());
        assert!(valid_port("0").is_err());
        assert!(valid_port("65535").is_ok());
        assert!(valid_port("65536").is_err());
    }

    #[test]
    fn strips_hop_by_hop_request_headers() {
        assert!(is_hop_by_hop_header("Host"));
        assert!(is_hop_by_hop_header("transfer-encoding"));
        assert!(!is_hop_by_hop_header("content-type"));
    }

    #[test]
    fn replaces_reserved_websocket_close_codes() {
        assert_eq!(wire_close_code(1000), 1000);
        assert_eq!(wire_close_code(1011), 1011);
        assert_eq!(wire_close_code(4000), 4000);
        assert_eq!(wire_close_code(1005), 1000);
        assert_eq!(wire_close_code(1006), 1000);
        assert_eq!(wire_close_code(1014), 1000);
        assert_eq!(wire_close_code(1015), 1000);
    }

    #[test]
    fn truncates_websocket_close_reasons_by_encoded_size() {
        let reason = "🚀".repeat(100);
        let truncated = truncate_close_reason(&reason);
        assert!(truncated.len() <= 120);
        assert!(truncated.is_char_boundary(truncated.len()));
    }
}
