// client/src/index.ts
import WebSocket from 'ws';

export interface RunTunnelOptions {
  channel: string;
  baseUrl: string; // e.g. "https://nbork.com"
  secret?: string;
  log?: (...args: any[]) => void;
}

interface ControlMsg {
  type: string;
  [k: string]: any;
}

const SYM_CTRL = (id: string) => Symbol.for(`ctrl:${id}`);
const SYM_LWS = (id: string) => Symbol.for(`lws:${id}`);

const enc64 = (u8: Uint8Array | ArrayBuffer) =>
  Buffer.from(u8 instanceof Uint8Array ? u8 : new Uint8Array(u8)).toString(
    'base64',
  );
const dec64 = (s: string) => Buffer.from(s, 'base64');

function toWsUrl(httpOrWs: string) {
  const u = new URL(httpOrWs);
  u.protocol = u.protocol.replace('http', 'ws');
  return u.toString().replace(/\/$/, '');
}

type Conn = {
  ws: WebSocket;
  send: (obj: any) => void;
  close: () => void;
  isOpen: () => boolean;
  flush: () => void;
  onPong: () => void;
  lastPong: number;
};

export function runTunnel({
  channel,
  baseUrl,
  secret,
  log = console.log,
}: RunTunnelOptions) {
  const url = new URL(`/_connect/${encodeURIComponent(channel)}`, baseUrl);
  if (secret) url.searchParams.set('auth', secret);

  let closed = false;
  let attempt = 0;
  let reconnectTimer: NodeJS.Timeout | undefined;

  // Keep *only* minimal global refs
  let current: Conn | null = null;
  let transportHeartbeat: NodeJS.Timeout | undefined;
  let appHeartbeat: NodeJS.Timeout | undefined;

  function stopHeartbeats() {
    if (transportHeartbeat)
      (clearInterval(transportHeartbeat), (transportHeartbeat = undefined));
    if (appHeartbeat) (clearInterval(appHeartbeat), (appHeartbeat = undefined));
  }

  function startHeartbeats(conn: Conn) {
    stopHeartbeats();

    // ws-level ping (some hops honor this)
    transportHeartbeat = setInterval(() => {
      if (conn.isOpen()) {
        try {
          (conn.ws as any).ping?.();
        } catch {}
      }
    }, 15_000);

    // app-level ping/pong (data frames; every hop sees it)
    appHeartbeat = setInterval(() => {
      if (!conn.isOpen()) return;
      if (Date.now() - conn.lastPong > 30_000) {
        // stale: hard-drop; reconnect will spin
        try {
          conn.ws.terminate();
        } catch {}
        return;
      }
      conn.send({ type: 'ping', ts: Date.now() });
    }, 10_000);
  }

  function scheduleReconnect(reason: string) {
    if (closed) return;
    if (reconnectTimer) return;
    attempt++;
    const delay = Math.min(10_000, 500 * Math.pow(2, attempt)); // 0.5s..10s
    log(`[tunnel] reconnecting in ${delay}ms (${reason})`);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined;
      connect();
    }, delay);
  }

  function createConnection(): Conn {
    const ws = new WebSocket(toWsUrl(url.toString()));

    // Per-connection queue and helpers
    const queue: string[] = [];
    const MAX_QUEUE = 1000;

    const isOpen = () => ws.readyState === WebSocket.OPEN;

    const send = (obj: any) => {
      const payload = typeof obj === 'string' ? obj : JSON.stringify(obj);
      if (isOpen()) {
        try {
          ws.send(payload);
        } catch {
          if (queue.length >= MAX_QUEUE) queue.shift();
          queue.push(payload);
        }
      } else {
        if (queue.length >= MAX_QUEUE) queue.shift();
        queue.push(payload);
      }
    };

    const flush = () => {
      if (!isOpen()) return;
      while (queue.length) {
        const next = queue.shift()!;
        try {
          ws.send(next);
        } catch {
          queue.unshift(next);
          break;
        }
      }
    };

    const onPong = () => {
      conn.lastPong = Date.now();
    };

    const conn: Conn = {
      ws,
      send,
      close: () => {
        try {
          ws.terminate();
        } catch {}
      },
      isOpen,
      flush,
      onPong,
      lastPong: Date.now(),
    };
    return conn;
  }

  function attachHandlers(conn: Conn) {
    const { ws } = conn;

    ws.on('open', () => {
      attempt = 0;
      current = conn; // promote to current *only when open*
      log(`[tunnel] connected: ${channel}`);
      conn.flush();
      startHeartbeats(conn);
    });

    ws.on('close', (code, reason) => {
      if (current === conn) {
        current = null;
        stopHeartbeats();
      }
      log(`[tunnel] closed (${code}${reason ? ` ${reason}` : ''})`);
      scheduleReconnect('close');
    });

    ws.on('error', (err) => {
      if (current === conn) {
        current = null;
        stopHeartbeats();
      }
      log(`[tunnel] error`, err);
      scheduleReconnect('error');
    });

    ws.on('pong', conn.onPong);

    ws.on('message', async (raw) => {
      // IMPORTANT: reply using *this connection's* send(), never globals
      const send = conn.send.bind(conn);

      const msg: ControlMsg =
        typeof raw === 'string' ? JSON.parse(raw) : JSON.parse(raw.toString());

      // app heartbeat
      if (msg.type === 'ping') {
        send({ type: 'pong', ts: Date.now() });
        return;
      }
      if (msg.type === 'pong') {
        conn.onPong();
        return;
      }

      switch (msg.type) {
        case 'http_open': {
          const id = msg.id as string;
          const headers: Record<string, string> = msg.headers || {};
          const method = String(msg.method || 'GET').toUpperCase();
          const localUrl = `http://127.0.0.1:${msg.port}${msg.url}`;

          const canHaveBody = method !== 'GET' && method !== 'HEAD';

          let ctrl: ReadableStreamDefaultController<Uint8Array> | undefined;
          let reqBody: ReadableStream<Uint8Array> | undefined;

          if (canHaveBody) {
            reqBody = new ReadableStream<Uint8Array>({
              start(c) {
                ctrl = c;
                (globalThis as any)[SYM_CTRL(id)] = { ctrl };
              },
            });
          }

          const init: RequestInit = {
            method,
            headers,
            ...(canHaveBody && reqBody ? { body: reqBody as any } : {}),
          };

          const resP = fetch(localUrl, init).catch((e) => ({ error: e }));

          (async () => {
            const res: any = await resP;
            if ((res as any).error) {
              send({
                type: 'http_error',
                id,
                message: String((res as any).error),
              });
              return;
            }

            send({
              type: 'http_head',
              id,
              status: res.status,
              headers: Object.fromEntries(res.headers.entries()),
            });

            if (method !== 'HEAD' && res.body) {
              const reader = res.body.getReader();
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                send({ type: 'http_chunk', id, chunkBase64: enc64(value!) });
              }
            }
            send({ type: 'http_end', id });
          })();
          break;
        }

        case 'http_body': {
          const id = msg.id as string;
          const holder = (globalThis as any)[SYM_CTRL(id)];
          const controller:
            | ReadableStreamDefaultController<Uint8Array>
            | undefined = holder?.ctrl;
          if (controller) controller.enqueue(dec64(msg.chunkBase64));
          break;
        }

        case 'http_body_end': {
          const id = msg.id as string;
          const holder = (globalThis as any)[SYM_CTRL(id)];
          if (holder?.ctrl) holder.ctrl.close();
          delete (globalThis as any)[SYM_CTRL(id)];
          break;
        }

        case 'ws_open': {
          const id = msg.id as string;
          const localUrl = `ws://127.0.0.1:${msg.port}${msg.url}`;
          const protocol: string | undefined = msg.protocol; // <—— from DO

          // Pass protocol if provided (Vite expects "vite-hmr")
          const lws = protocol
            ? new WebSocket(localUrl, protocol) // <—— important
            : new WebSocket(localUrl);

          lws.on('open', () => {
            send({ type: 'ws_ready', id });
          });
          lws.on('message', (data, isBinary) => {
            if (isBinary) {
              const base64 = Buffer.isBuffer(data)
                ? data.toString('base64')
                : Buffer.from(data as any).toString('base64');
              send({
                type: 'ws_to_edge',
                id,
                dataBase64: base64,
                binary: true,
              });
            } else {
              const text =
                typeof data === 'string'
                  ? data
                  : Buffer.from(data as any).toString('utf8');
              const base64 = Buffer.from(text, 'utf8').toString('base64');
              send({
                type: 'ws_to_edge',
                id,
                dataBase64: base64,
                binary: false,
              });
            }
          });
          lws.on('close', (code, reason) => {
            send({ type: 'ws_close', id, code, reason: reason.toString() });
          });
          lws.on('error', (err) => {
            send({ type: 'ws_close', id, code: 1011, reason: String(err) });
          });

          (globalThis as any)[SYM_LWS(id)] = lws;
          break;
        }

        case 'ws_from_edge': {
          const id = msg.id as string;
          const lws: WebSocket | undefined = (globalThis as any)[SYM_LWS(id)];
          if (!lws) break;
          try {
            const payload = dec64(msg.dataBase64);
            if (msg.binary) lws.send(payload);
            else lws.send(payload.toString());
          } catch (_e) {
            // nothing
          }
          break;
        }

        case 'ws_close': {
          const id = msg.id as string;
          const lws: WebSocket | undefined = (globalThis as any)[SYM_LWS(id)];
          if (lws)
            try {
              lws.close(msg.code || 1000, msg.reason || '');
            } catch {}
          delete (globalThis as any)[SYM_LWS(id)];
          break;
        }

        default:
          // ignore
          break;
      }
    });
  }

  function connect() {
    // do NOT reuse handlers or a shared ws variable in handlers
    const conn = createConnection();
    attachHandlers(conn);
  }

  connect();

  return {
    close: () => {
      closed = true;
      stopHeartbeats();
      try {
        current?.close();
      } catch {}
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = undefined;
      }
    },
  };
}
