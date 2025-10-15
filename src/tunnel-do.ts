// worker/src/tunnel-do.ts
type Json = Record<string, any>;

function b64(buf: ArrayBuffer | Uint8Array) {
  const u8 = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  let s = '';
  for (let i = 0; i < u8.length; i++) s += String.fromCharCode(u8[i]);
  return btoa(s);
}
function decodeB64(s: string): Uint8Array {
  const bin = atob(s);
  const u8 = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i);
  return u8;
}

export class TunnelDO {
  private state: DurableObjectState;
  constructor(state: DurableObjectState) {
    this.state = state;
  }

  private control?: WebSocket;
  private httpStreams = new Map<
    string,
    {
      controller: ReadableStreamDefaultController<Uint8Array>;
      resolveHead: (value: ResponseInit) => void;
      rejectHead: (reason?: any) => void;
    }
  >();
  private wsEdge = new Map<string, WebSocket>();

  // app heartbeat
  private hbTimer: number | undefined;
  private lastClientSeen = Date.now();

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const upgrade = req.headers.get('upgrade');

    // Control WS
    if (url.pathname.startsWith('/_connect/')) {
      if (upgrade !== 'websocket')
        return new Response('Upgrade required', { status: 426 });
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();
      this.control = server;
      this.lastClientSeen = Date.now();
      this.startHeartbeat();

      server.addEventListener('message', (ev: MessageEvent) => {
        this.lastClientSeen = Date.now();
        let msg: Json | undefined;
        try {
          const s =
            typeof ev.data === 'string'
              ? ev.data
              : new TextDecoder().decode(ev.data as ArrayBuffer);
          msg = JSON.parse(s);
        } catch {
          /* ignore */
        }

        if (!msg) return;

        // app-level heartbeat
        if (msg.type === 'ping') {
          try {
            server.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
          } catch {}
          return;
        }
        if (msg.type === 'pong') {
          return;
        }

        this.onControlMessage(server, msg);
      });

      server.addEventListener('close', () => this.onControlGone(server));
      server.addEventListener('error', () => this.onControlGone(server));

      return new Response(null, { status: 101, webSocket: client });
    }

    // Edge WS
    if (upgrade === 'websocket') {
      if (!this.control)
        return new Response('Tunnel not connected', { status: 502 });
      const port = req.headers.get('x-tunnel-port') || '';
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();

      const id = crypto.randomUUID();
      this.wsEdge.set(id, server);

      try {
        this.control!.send(
          JSON.stringify({
            type: 'ws_open',
            id,
            port,
            url: url.pathname + (url.search || ''),
            headers: Object.fromEntries(req.headers.entries()),
          } as Json),
        );
      } catch {}

      server.addEventListener('message', (ev: MessageEvent) => {
        const data = ev.data;
        if (typeof data === 'string') {
          const textB64 = btoa(unescape(encodeURIComponent(data)));
          try {
            this.control!.send(
              JSON.stringify({
                type: 'ws_from_edge',
                id,
                binary: false,
                dataBase64: textB64,
              }),
            );
          } catch {}
        } else if (data instanceof ArrayBuffer) {
          try {
            this.control!.send(
              JSON.stringify({
                type: 'ws_from_edge',
                id,
                binary: true,
                dataBase64: b64(data),
              }),
            );
          } catch {}
        }
      });
      server.addEventListener('close', (ev: CloseEvent) => {
        try {
          this.control!.send(
            JSON.stringify({
              type: 'ws_close',
              id,
              code: ev.code,
              reason: ev.reason || '',
            }),
          );
        } catch {}
        this.wsEdge.delete(id);
      });
      server.addEventListener('error', () => {
        try {
          this.control!.send(
            JSON.stringify({
              type: 'ws_close',
              id,
              code: 1011,
              reason: 'edge error',
            }),
          );
        } catch {}
        this.wsEdge.delete(id);
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    // HTTP
    if (!this.control)
      return new Response('Tunnel not connected', { status: 502 });

    const method = req.method.toUpperCase();
    const port = req.headers.get('x-tunnel-port') || '';
    const id = crypto.randomUUID();

    let resolveHead!: (ri: ResponseInit) => void;
    let rejectHead!: (err?: any) => void;
    const headP = new Promise<ResponseInit>((res, rej) => {
      resolveHead = res;
      rejectHead = rej;
    });

    const bodyStream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.httpStreams.set(id, { controller, resolveHead, rejectHead });
      },
      cancel: () => {
        this.httpStreams.delete(id);
      },
    });

    try {
      this.control!.send(
        JSON.stringify({
          type: 'http_open',
          id,
          port,
          method,
          url: url.pathname + (url.search || ''),
          headers: Object.fromEntries(req.headers.entries()),
        } as Json),
      );
    } catch {
      return new Response('Tunnel not connected', { status: 502 });
    }

    (async () => {
      if (req.body) {
        const reader = req.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          try {
            this.control!.send(
              JSON.stringify({
                type: 'http_body',
                id,
                chunkBase64: b64(value!),
              }),
            );
          } catch {
            break;
          }
        }
      }
      try {
        this.control!.send(JSON.stringify({ type: 'http_body_end', id }));
      } catch {}
    })().catch(() => {});

    const head = await headP.catch(
      () => ({ status: 502, statusText: 'Upstream error' }) as ResponseInit,
    );
    const status = (head as any).status ?? 200;
    const nullBodyStatuses =
      status === 101 || status === 204 || status === 205 || status === 304;
    const isHead = method === 'HEAD';
    if (nullBodyStatuses || isHead) return new Response(null, head);
    return new Response(bodyStream, head);
  }

  private startHeartbeat() {
    if (this.hbTimer) this.state.storage.deleteAlarm().catch(() => {});
    // Use alarms to schedule checks ~every 10s without busy loops
    this.state.storage.setAlarm(Date.now() + 10_000).catch(() => {});
    this.hbTimer = 1 as any;
  }

  // Called by the platform when an alarm fires
  async alarm() {
    // send ping if control alive
    if (this.control) {
      try {
        this.control.send(JSON.stringify({ type: 'ping', ts: Date.now() }));
      } catch {}
    }
    // if no activity from client in >30s, drop control so client reconnects
    if (Date.now() - this.lastClientSeen > 30_000) {
      try {
        this.control?.close(1011, 'heartbeat timeout');
      } catch {}
      this.onControlGone(this.control as any);
    }
    // schedule next
    this.state.storage.setAlarm(Date.now() + 10_000).catch(() => {});
  }

  private onControlGone(server?: WebSocket) {
    if (server && this.control === server) this.control = undefined;

    // close all edge sockets
    for (const [, edge] of this.wsEdge) {
      try {
        edge.close(1011, 'control lost');
      } catch {}
    }
    this.wsEdge.clear();

    // cancel HTTP streams
    for (const [, s] of this.httpStreams) {
      try {
        s.rejectHead(new Error('control lost'));
      } catch {}
      try {
        s.controller.error(new Error('control lost'));
      } catch {}
    }
    this.httpStreams.clear();
  }

  private onControlMessage(_ws: WebSocket, msg: Json) {
    switch (msg.type) {
      case 'http_head': {
        const s = this.httpStreams.get(msg.id);
        if (!s) break;
        const headers = new Headers();
        for (const [k, v] of Object.entries(msg.headers || {}))
          headers.set(k, String(v));
        s.resolveHead({ status: msg.status || 200, headers });
        break;
      }
      case 'http_chunk': {
        const s = this.httpStreams.get(msg.id);
        if (!s) break;
        s.controller.enqueue(decodeB64(msg.chunkBase64));
        break;
      }
      case 'http_end': {
        const s = this.httpStreams.get(msg.id);
        if (!s) break;
        s.controller.close();
        this.httpStreams.delete(msg.id);
        break;
      }
      case 'http_error': {
        const s = this.httpStreams.get(msg.id);
        if (!s) break;
        s.rejectHead(new Error(msg.message || 'upstream error'));
        try {
          s.controller.error(new Error(msg.message || 'upstream error'));
        } catch {}
        this.httpStreams.delete(msg.id);
        break;
      }
      case 'ws_to_edge': {
        const edge = this.wsEdge.get(msg.id);
        if (!edge) break;
        const payload = msg.binary
          ? decodeB64(msg.dataBase64)
          : decodeURIComponent(escape(atob(msg.dataBase64)));
        try {
          msg.binary ? edge.send(payload) : edge.send(String(payload));
        } catch {}
        break;
      }
      case 'ws_close': {
        const edge = this.wsEdge.get(msg.id);
        if (edge) {
          try {
            edge.close(msg.code || 1000, msg.reason || '');
          } catch {}
          this.wsEdge.delete(msg.id);
        }
        break;
      }
      // ping/pong handled earlier
    }
  }
}
