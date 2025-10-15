import { DurableObject } from "cloudflare:workers";

type Json = Record<string, any>;

function b64(buf: ArrayBuffer | Uint8Array) {
  const u8 = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  let s = "";
  for (let i = 0; i < u8.length; i++) s += String.fromCharCode(u8[i]);
  return btoa(s);
}

function decodeB64(s: string): Uint8Array {
  const bin = atob(s);
  const u8 = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i);
  return u8;
}

export class TunnelDO extends DurableObject {
  private control?: WebSocket; // control websocket to client/CLI

  // In-flight HTTP streaming state
  private httpStreams = new Map<string, {
    controller: ReadableStreamDefaultController<Uint8Array>;
    resolveHead: (value: ResponseInit) => void;
    rejectHead: (reason?: any) => void;
  }>();

  // Active user-facing WS sockets (edge)
  private wsEdge = new Map<string, WebSocket>();

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const upgrade = req.headers.get("upgrade");

    // Control WebSocket registration: /_connect/<channel>
    if (url.pathname.startsWith("/_connect/")) {
      if (upgrade !== "websocket") return new Response("Upgrade required", { status: 426 });
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      // Accept on DO side
      this.ctx.acceptWebSocket(server);
      this.control = server;
      return new Response(null, { status: 101, webSocket: client });
    }

    // Proxy WebSocket requests from end users
    if (upgrade === "websocket") {
      if (!this.control) return new Response("Tunnel not connected", { status: 502 });

      const port = req.headers.get("x-tunnel-port") || "";
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);

      const id = crypto.randomUUID();
      this.wsEdge.set(id, server);

      const openMsg = {
        type: "ws_open",
        id,
        port,
        url: new URL(req.url).pathname + (new URL(req.url).search || ""),
        headers: Object.fromEntries(req.headers.entries()),
      } satisfies Json;

      this.control!.send(JSON.stringify(openMsg));

      server.addEventListener("message", (ev: MessageEvent) => {
        const data = ev.data;
        if (typeof data === "string") {
          // Text frame
          const textB64 = btoa(unescape(encodeURIComponent(data)));
          this.control!.send(JSON.stringify({ type: "ws_from_edge", id, binary: false, dataBase64: textB64 }));
        } else if (data instanceof ArrayBuffer) {
          // Binary frame
          this.control!.send(JSON.stringify({ type: "ws_from_edge", id, binary: true, dataBase64: b64(data) }));
        }
      });

      server.addEventListener("close", (ev: CloseEvent) => {
        try { this.control!.send(JSON.stringify({ type: "ws_close", id, code: ev.code, reason: ev.reason || "" })); } catch {}
        this.wsEdge.delete(id);
      });

      server.addEventListener("error", () => {
        try { this.control!.send(JSON.stringify({ type: "ws_close", id, code: 1011, reason: "edge error" })); } catch {}
        this.wsEdge.delete(id);
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    // Regular HTTP request: stream to client via control WS
    if (!this.control) return new Response("Tunnel not connected", { status: 502 });

    const port = req.headers.get("x-tunnel-port") || "";
    const id = crypto.randomUUID();
    const pathAndQuery = url.pathname + (url.search || "");

    // Promise for response head; body is a ReadableStream controlled by client chunks
    let resolveHead!: (ri: ResponseInit) => void;
    let rejectHead!: (err?: any) => void;
    const headP = new Promise<ResponseInit>((res, rej) => { resolveHead = res; rejectHead = rej; });

    const bodyStream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.httpStreams.set(id, { controller, resolveHead, rejectHead });
      },
      cancel: () => {
        this.httpStreams.delete(id);
      },
    });

    // Notify client about new HTTP request
    this.control!.send(JSON.stringify({
      type: "http_open",
      id,
      port,
      method: req.method,
      url: pathAndQuery,
      headers: Object.fromEntries(req.headers.entries()),
    } as Json));

    // Stream request body to client
    (async () => {
      if (req.body) {
        const reader = req.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          this.control!.send(JSON.stringify({ type: "http_body", id, chunkBase64: b64(value!) }));
        }
      }
      this.control!.send(JSON.stringify({ type: "http_body_end", id }));
    })().catch(() => {});

    // Wait for client to reply with the response head
    const head = await headP.catch(() => ({ status: 502, statusText: "Upstream error" } as ResponseInit));
    return new Response(bodyStream, head);
  }

  // Handle control WS messages (from client/CLI)
  async webSocketMessage(ws: WebSocket, data: string | ArrayBuffer) {
    if (ws !== this.control) return;

    const msg: Json = typeof data === "string" ? JSON.parse(data) : JSON.parse(new TextDecoder().decode(data as ArrayBuffer));

    switch (msg.type) {
      case "http_head": {
        const s = this.httpStreams.get(msg.id);
        if (!s) break;
        const headers = new Headers();
        for (const [k, v] of Object.entries(msg.headers || {})) headers.set(k, String(v));
        s.resolveHead({ status: msg.status || 200, headers });
        break;
      }
      case "http_chunk": {
        const s = this.httpStreams.get(msg.id);
        if (!s) break;
        s.controller.enqueue(decodeB64(msg.chunkBase64));
        break;
      }
      case "http_end": {
        const s = this.httpStreams.get(msg.id);
        if (!s) break;
        s.controller.close();
        this.httpStreams.delete(msg.id);
        break;
      }
      case "http_error": {
        const s = this.httpStreams.get(msg.id);
        if (!s) break;
        s.rejectHead(new Error(msg.message || "upstream error"));
        try { s.controller.error(new Error(msg.message || "upstream error")); } catch {}
        this.httpStreams.delete(msg.id);
        break;
      }
      case "ws_to_edge": {
        const edge = this.wsEdge.get(msg.id);
        if (!edge) break;
        const payload = msg.binary ? decodeB64(msg.dataBase64) : decodeURIComponent(escape(atob(msg.dataBase64)));
        if (msg.binary) edge.send(payload);
        else edge.send(String(payload));
        break;
      }
      case "ws_close": {
        const edge = this.wsEdge.get(msg.id);
        if (edge) {
          try { edge.close(msg.code || 1000, msg.reason || ""); } catch {}
          this.wsEdge.delete(msg.id);
        }
        break;
      }
      default:
        // ignore
        break;
    }
  }

  webSocketClose(ws: WebSocket) {
    if (ws === this.control) this.control = undefined;
  }

  webSocketError(ws: WebSocket) {
    if (ws === this.control) this.control = undefined;
  }
}
