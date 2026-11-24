mod terminal;

use std::{
    collections::HashMap,
    fs,
    io::{self, ErrorKind, IsTerminal, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use russh::{
    keys::{ssh_key::LineEnding, Algorithm, PrivateKey},
    server, MethodKind, MethodSet,
};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
    task::{AbortHandle, JoinSet},
};

use crate::{send_json, string_field, Outbound, BASE64};

#[derive(Clone)]
pub struct ShellServer {
    config: Arc<server::Config>,
    secret: Option<Arc<str>>,
}

impl ShellServer {
    pub fn new(secret: Option<&str>) -> Result<Self> {
        if secret == Some("") {
            bail!("SSH secret must not be empty");
        }
        let key = host_key(&state_directory()?).context("could not load nbork SSH host key")?;
        let method = if secret.is_some() {
            MethodKind::Password
        } else {
            MethodKind::None
        };
        let config = server::Config {
            keys: vec![key],
            methods: MethodSet::from(&[method][..]),
            auth_rejection_time_initial: Some(Duration::ZERO),
            inactivity_timeout: Some(Duration::from_secs(3600)),
            ..Default::default()
        };
        Ok(Self {
            config: Arc::new(config),
            secret: secret.map(Arc::from),
        })
    }
}

fn state_directory() -> Result<PathBuf> {
    std::env::var_os("NBORK_SSH_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::home_dir().map(|home| home.join(".nbork")))
        .context("could not locate nbork SSH directory")
}

pub fn confirm_passwordless(
    channel: &str,
    control_url: &url::Url,
    accept_without_prompt: bool,
) -> Result<bool> {
    let directory = state_directory()?.join("passwordless-ssh");
    let approval = directory.join(format!("channel-{channel}"));
    let origin = control_url.origin().ascii_serialization();
    match fs::read_to_string(&approval) {
        Ok(origins) if origins.lines().any(|saved| saved == origin) => return Ok(true),
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e).context("could not read passwordless SSH preference"),
    }
    if !accept_without_prompt {
        if !io::stdin().is_terminal() || !io::stderr().is_terminal() {
            eprintln!("[ssh] web tunnel only. To approve passwordless SSH for {channel}, run interactively or pass --accept-passwordless-ssh.");
            return Ok(false);
        }
        eprint!("[ssh] Anyone reaching channel {channel} on {origin} can open a shell as your account. Allow passwordless SSH for this and future runs? [y/N] ");
        io::stderr().flush()?;
        let mut answer = String::new();
        io::stdin().read_line(&mut answer)?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            return Ok(false);
        }
    }
    fs::create_dir_all(&directory)?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(approval)?;
    file.write_all(format!("{origin}\n").as_bytes())?;
    Ok(true)
}

fn host_key(directory: &Path) -> Result<PrivateKey> {
    let path = directory.join("ssh_host_ed25519_key");
    match fs::read_to_string(&path) {
        Ok(text) => return Ok(PrivateKey::from_openssh(text)?),
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    fs::create_dir_all(directory)?;
    let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
    let mut tmp = tempfile::NamedTempFile::new_in(directory)?;
    key.write_openssh(tmp.as_file_mut(), LineEnding::LF)?;
    match tmp.persist_noclobber(&path) {
        Ok(_) => Ok(key),
        Err(e) if e.error.kind() == ErrorKind::AlreadyExists => {
            Ok(PrivateKey::read_openssh_file(path)?)
        }
        Err(e) => Err(e.into()),
    }
}

struct Peer {
    input: mpsc::Sender<Option<Vec<u8>>>,
    ack: mpsc::Sender<()>,
    task: AbortHandle,
}

pub struct Sessions {
    server: Option<ShellServer>,
    peers: HashMap<String, Peer>,
}

impl Sessions {
    pub fn new(server: Option<ShellServer>) -> Self {
        Self {
            server,
            peers: HashMap::new(),
        }
    }

    pub fn handle(
        &mut self,
        kind: &str,
        msg: &Value,
        outbound: &Outbound,
        jobs: &mut JoinSet<()>,
    ) -> Result<()> {
        let id = string_field(msg, "id")?;
        if kind == "ssh_open" {
            let Some(server) = self.server.clone() else {
                return Ok(());
            };
            if self.peers.contains_key(id) || self.peers.len() >= 32 {
                bail!("unexpected SSH session");
            }
            let (input, rx) = mpsc::channel(2);
            let (ack, ack_rx) = mpsc::channel(1);
            let session_id = id.to_owned();
            let outbound = outbound.clone();
            let task = jobs.spawn(async move {
                if let Err(e) = serve(&session_id, server, rx, ack_rx, &outbound).await {
                    eprintln!("[ssh] session ended: {e:#}");
                }
                let _ =
                    send_json(&outbound, json!({ "type": "ssh_close", "id": session_id })).await;
            });
            self.peers.insert(id.to_owned(), Peer { input, ack, task });
            return Ok(());
        }
        let Some(peer) = self.peers.get(id) else {
            return Ok(());
        };
        match kind {
            "ssh_data" => {
                let encoded = string_field(msg, "data")?;
                if encoded.len() > 43_692 {
                    bail!("oversize SSH frame");
                }
                peer.input.try_send(Some(BASE64.decode(encoded)?))?;
            }
            "ssh_eof" => peer.input.try_send(None)?,
            "ssh_ack" => peer.ack.try_send(())?,
            "ssh_close" => {
                peer.task.abort();
                self.peers.remove(id);
            }
            _ => {}
        }
        Ok(())
    }
}

async fn serve(
    id: &str,
    server: ShellServer,
    mut input: mpsc::Receiver<Option<Vec<u8>>>,
    mut ack: mpsc::Receiver<()>,
    outbound: &Outbound,
) -> Result<()> {
    // russh owns one end. The two independent pumps preserve SSH's byte stream
    // and propagate half-closes over the multiplexed control connection.
    let (stream, transport) = tokio::io::duplex(65536);
    let (mut reader, mut writer) = tokio::io::split(transport);
    let receive = async {
        while let Some(Some(bytes)) = input.recv().await {
            writer.write_all(&bytes).await?;
            send_json(outbound, json!({ "type": "ssh_ack", "id": id })).await?;
        }
        writer.shutdown().await?;
        Ok(())
    };
    let send = async {
        let mut buf = vec![0; 32768];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            send_json(
                outbound,
                json!({ "type": "ssh_data", "id": id, "data": BASE64.encode(&buf[..n]) }),
            )
            .await?;
            ack.recv().await.ok_or_else(|| anyhow!("tunnel closed"))?;
        }
        send_json(outbound, json!({ "type": "ssh_eof", "id": id })).await
    };
    let ssh = async {
        let handler = terminal::Handler::new(server.secret);
        let session = server::run_stream(server.config, stream, handler).await?;
        session.await
    };
    tokio::try_join!(receive, send, ssh)?;
    Ok(())
}
