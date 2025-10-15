import WebSocket from "ws";

export interface RunTunnelOptions {
  channel: string;
  baseUrl: string;  // e.g. "https://tunnel.app"
  secret?: string;  // optional shared secret if you add auth
  log?: (...args: any[]) => void;
}

interface ControlMsg { type: string; [k: string]: any }

function toWsUrl(httpOrWs: string) {
  const u = new URL(httpOrWs);
  u.protocol = u.protocol.replace("http", "ws");
  return u.toString().replace(/\/$/, "");
}

const SYM_CTRL = (id: string) => Symbol.for(`ctrl:${id}`);
const SYM_LWS  = (id: string) => Symbol.for(`lws:${id}`);

const enc64 = (u8: Uint8Array | ArrayBuffer) =>
  Buffer.from(u8 instanceof Uint8Array ? u8 : new Uint8Array(u8)).toString("base64");
const dec64 = (s: string) => Buffer.from(s, "base64");

export function runTunnel({ channel, baseUrl, secret, log = console.log }: RunTunnelOptions) {
  const url = new URL(`/_connect/${encodeURIComponent(channel)}`, baseUrl);
  if (secret) url.searchParams.set("auth", secret);

  const ws = new WebSocket(toWsUrl(url.toString()));

  ws.on("open", () => log(`[tunnel] connected: ${channel}`));
  ws.on("close", () => log(`[tunnel] disconnected`));
  ws.on("error", (err) => log(`[tunnel] error`, err));

  ws.on("message", async (raw) => {
    const msg: ControlMsg = typeof raw === "string" ? JSON.parse(raw) : JSON.parse(raw.toString());

    switch (msg.type) {
      case "http_open": {
        const id = msg.id as string;
        const headers: Record<string, string> = msg.headers || {};
        const localUrl = `http://127.0.0.1:${msg.port}${msg.url}`;

        let ctrl: ReadableStreamDefaultController<Uint8Array> | undefined;
        const reqBody = new ReadableStream<Uint8Array>({
          start(c) { ctrl = c; (globalThis as any)[SYM_CTRL(id)] = { ctrl }; }
        });

        // Kick off local fetch immediately; body will be fed by later http_body frames
        const resP = fetch(localUrl, {
          method: msg.method,
          headers,
          body: reqBody as any
        }).catch((e) => ({ error: e }));

        // When response arrives, send head & stream body back
        (async () => {
          const res: any = await resP;
          if ((res as any).error) {
            ws.send(JSON.stringify({ type: "http_error", id, message: String((res as any).error) }));
            return;
          }
          ws.send(JSON.stringify({
            type: "http_head",
            id,
            status: res.status,
            headers: Object.fromEntries(res.headers.entries())
          }));

          if (res.body) {
            const reader = res.body.getReader();
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              ws.send(JSON.stringify({ type: "http_chunk", id, chunkBase64: enc64(value!) }));
            }
          }
          ws.send(JSON.stringify({ type: "http_end", id }));
        })();

        break;
      }

      case "http_body": {
        const id = msg.id as string;
        const holder = (globalThis as any)[SYM_CTRL(id)];
        const controller: ReadableStreamDefaultController<Uint8Array> | undefined = holder?.ctrl;
        if (controller) controller.enqueue(dec64(msg.chunkBase64));
        break;
      }

      case "http_body_end": {
        const id = msg.id as string;
        const holder = (globalThis as any)[SYM_CTRL(id)];
        const controller: ReadableStreamDefaultController<Uint8Array> | undefined = holder?.ctrl;
        if (controller) controller.close();
        delete (globalThis as any)[SYM_CTRL(id)];
        break;
      }

      case "ws_open": {
        const id = msg.id as string;
        const localUrl = `ws://127.0.0.1:${msg.port}${msg.url}`;
        const lws = new WebSocket(localUrl);

        lws.on("open", () => {
          ws.send(JSON.stringify({ type: "ws_ready", id }));
        });
        lws.on("message", (data, isBinary) => {
          const base64 = Buffer.isBuffer(data) ? data.toString("base64") : Buffer.from(data as any).toString("base64");
          ws.send(JSON.stringify({ type: "ws_to_edge", id, dataBase64: base64, binary: !!isBinary }));
        });
        lws.on("close", (code, reason) => {
          ws.send(JSON.stringify({ type: "ws_close", id, code, reason: reason.toString() }));
        });
        lws.on("error", (err) => {
          ws.send(JSON.stringify({ type: "ws_close", id, code: 1011, reason: String(err) }));
        });

        (globalThis as any)[SYM_LWS(id)] = lws;
        break;
      }

      case "ws_from_edge": {
        const id = msg.id as string;
        const lws: WebSocket | undefined = (globalThis as any)[SYM_LWS(id)];
        if (!lws) break;
        const payload = dec64(msg.dataBase64);
        if (msg.binary) lws.send(payload);
        else lws.send(payload.toString());
        break;
      }

      case "ws_close": {
        const id = msg.id as string;
        const lws: WebSocket | undefined = (globalThis as any)[SYM_LWS(id)];
        if (lws) try { lws.close(msg.code || 1000, msg.reason || ""); } catch {}
        delete (globalThis as any)[SYM_LWS(id)];
        break;
      }

      default:
        // ignore unknown
        break;
    }
  });

  return {
    close: () => ws.close(),
  };
}
