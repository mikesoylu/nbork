use std::{
    io::{Read, Write},
    sync::Arc,
};

use anyhow::{Context, Result};
use portable_pty::{native_pty_system, Child, CommandBuilder, PtyPair, PtySize};
use russh::{
    server::{self, Auth, Msg, Session},
    Channel, ChannelId, ChannelMsg, Pty,
};
use subtle::ConstantTimeEq;
use tokio::{
    sync::mpsc,
    task::{self, JoinSet},
};

pub(super) struct Handler {
    secret: Option<Arc<str>>,
    channel: Option<Channel<Msg>>,
    size: PtySize,
    term: String,
    shell: JoinSet<()>,
}

impl Handler {
    pub fn new(secret: Option<Arc<str>>) -> Self {
        Self {
            secret,
            channel: None,
            size: PtySize::default(),
            term: "xterm".into(),
            shell: JoinSet::new(),
        }
    }
}

impl server::Handler for Handler {
    type Error = anyhow::Error;

    async fn auth_none(&mut self, _: &str) -> Result<Auth> {
        Ok(if self.secret.is_none() {
            Auth::Accept
        } else {
            Auth::reject()
        })
    }

    async fn auth_password(&mut self, _: &str, password: &str) -> Result<Auth> {
        let Some(secret) = &self.secret else {
            return Ok(Auth::reject());
        };
        if bool::from(secret.as_bytes().ct_eq(password.as_bytes())) {
            Ok(Auth::Accept)
        } else {
            Ok(Auth::Reject {
                proceed_with_methods: Some(russh::MethodSet::from(
                    &[russh::MethodKind::Password][..],
                )),
                partial_success: false,
            })
        }
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: server::ChannelOpenHandle,
        _: &mut Session,
    ) -> Result<()> {
        if self.channel.is_some() || !self.shell.is_empty() {
            reply
                .reject(russh::ChannelOpenFailure::ResourceShortage)
                .await;
        } else {
            self.channel = Some(channel);
            reply.accept().await;
        }
        Ok(())
    }

    async fn pty_request(
        &mut self,
        id: ChannelId,
        term: &str,
        cols: u32,
        rows: u32,
        width: u32,
        height: u32,
        _: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<()> {
        if self.channel.as_ref().is_some_and(|ch| ch.id() == id) {
            self.size = size(cols, rows, width, height);
            self.term = term.to_owned();
            session.channel_success(id)?;
        } else {
            session.channel_failure(id)?;
        }
        Ok(())
    }

    async fn shell_request(&mut self, id: ChannelId, session: &mut Session) -> Result<()> {
        let Some(channel) = self.channel.take() else {
            session.channel_failure(id)?;
            return Ok(());
        };
        let pair = native_pty_system().openpty(self.size)?;
        let mut command = CommandBuilder::new_default_prog();
        command.env("TERM", &self.term);
        command.env_remove("TUNNEL_SECRET");
        let child = ShellChild(Some(pair.slave.spawn_command(command)?));
        session.channel_success(id)?;
        self.shell.spawn(async move {
            if let Err(e) = terminal(channel, pair, child).await {
                eprintln!("[ssh] shell ended: {e:#}");
            }
        });
        Ok(())
    }

    async fn exec_request(&mut self, id: ChannelId, _: &[u8], session: &mut Session) -> Result<()> {
        session.channel_failure(id)?;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        id: ChannelId,
        _: &str,
        session: &mut Session,
    ) -> Result<()> {
        session.channel_failure(id)?;
        Ok(())
    }
}

fn size(cols: u32, rows: u32, width: u32, height: u32) -> PtySize {
    let dimension = |n: u32| n.min(u16::MAX.into()) as u16;
    PtySize {
        cols: dimension(cols).max(1),
        rows: dimension(rows).max(1),
        pixel_width: dimension(width),
        pixel_height: dimension(height),
    }
}

// Keep ownership until the process has been reaped, including on disconnect.
struct ShellChild(Option<Box<dyn Child + Send + Sync>>);

impl ShellChild {
    async fn wait(&mut self) -> Result<u32> {
        let child = self.0.as_mut().expect("shell already reaped");
        loop {
            if let Some(status) = child.try_wait()? {
                self.0.take();
                return Ok(status.exit_code());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
}

impl Drop for ShellChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            task::spawn_blocking(move || {
                if matches!(child.try_wait(), Ok(None)) {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            });
        }
    }
}

async fn terminal(channel: Channel<Msg>, pair: PtyPair, mut child: ShellChild) -> Result<()> {
    drop(pair.slave);
    let mut reader = pair.master.try_clone_reader()?;
    let mut writer = pair.master.take_writer()?;
    let (output, mut rx) = mpsc::channel(4);
    task::spawn_blocking(move || {
        let mut buf = [0; 8192];
        while let Ok(n) = reader.read(&mut buf) {
            if n == 0 || output.blocking_send(buf[..n].to_vec()).is_err() {
                break;
            }
        }
    });
    let (input, mut tx) = mpsc::channel::<bytes::Bytes>(4);
    task::spawn_blocking(move || {
        while let Some(bytes) = tx.blocking_recv() {
            if writer.write_all(&bytes).is_err() {
                break;
            }
        }
    });
    let (mut incoming, outgoing) = channel.split();
    let read = async {
        while let Some(bytes) = rx.recv().await {
            outgoing.data_bytes(bytes).await?;
        }
        outgoing.exit_status(child.wait().await?).await?;
        outgoing.eof().await?;
        outgoing.close().await?;
        Ok::<_, anyhow::Error>(())
    };
    let write = async move {
        let mut input = Some(input);
        while let Some(msg) = incoming.wait().await {
            match msg {
                ChannelMsg::Data { data } => {
                    if let Some(input) = &input {
                        input.send(data).await.context("terminal input closed")?;
                    }
                }
                ChannelMsg::WindowChange {
                    col_width,
                    row_height,
                    pix_width,
                    pix_height,
                } => pair
                    .master
                    .resize(size(col_width, row_height, pix_width, pix_height))?,
                ChannelMsg::Eof => {
                    input.take();
                }
                ChannelMsg::Close => break,
                _ => {}
            }
        }
        Ok(())
    };
    tokio::select! { r = read => r, r = write => r }
}
