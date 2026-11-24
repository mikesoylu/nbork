import { DurableObject } from 'cloudflare:workers';
import '../.wrangler/ssh-wasm/wasm_exec.js';
import module from '../.wrangler/ssh-wasm/gateway.wasm';
import type { Env } from './worker';
import { closeSocket } from './ssh-relay';

type Callback = (error: string | null, value?: unknown) => void;
declare global {
  var Go: new () => {
    argv: string[];
    exit(code: number): void;
    importObject: WebAssembly.Imports;
    run(instance: WebAssembly.Instance): Promise<void>;
  };
  var nborkSessions: Map<string, unknown>;
}
globalThis.nborkSessions = new Map();

// Each invocation owns its Wasm instance and all socket callbacks. Suppress
// callbacks after Go exits, including reads interrupted by client disconnects.
export class SshGateway extends DurableObject<Env> {
  async connect(client: Socket) {
    const key = this.env.SSH_HOST_KEY;
    if (!key) return closeSocket(client);
    let active = true;
    const sockets = new Set<() => void>();
    function complete(promise: Promise<unknown>, cb: Callback) {
      promise.then(
        (value) => {
          if (active) cb(null, value);
        },
        () => {
          if (active) cb('socket or storage operation failed');
        },
      );
    }

    function wrap(socket: Socket) {
      const reader = socket.readable.getReader();
      const writer = socket.writable.getWriter();
      const timers = new Map<string, ReturnType<typeof setTimeout>>();
      const close = () => {
        if (!sockets.delete(close)) return;
        for (const timer of timers.values()) clearTimeout(timer);
        void closeSocket(socket);
      };
      sockets.add(close);
      return {
        close,
        deadline(direction: string, ms: number) {
          clearTimeout(timers.get(direction) ?? null);
          if (ms >= 0) timers.set(direction, setTimeout(close, ms));
        },
        read(cb: Callback) {
          complete(
            reader.read().then((r) => (r.done ? null : r.value)),
            cb,
          );
        },
        write(bytes: Uint8Array, cb: Callback) {
          complete(writer.write(bytes), cb);
        },
      };
    }

    const session = {
      key,
      client: wrap(client),
      dial: (name: string, timeout: number, cb: Callback) => {
        const peer = this.env.TUNNEL.getByName(name).connect('ssh:22', {
          allowHalfOpen: true,
        });
        const timer = setTimeout(
          () => void closeSocket(peer),
          Math.max(0, timeout),
        );
        complete(
          peer.opened
            .then(() => {
              if (!active) {
                void closeSocket(peer);
                throw new Error('session closed');
              }
              return wrap(peer);
            })
            .finally(() => clearTimeout(timer)),
          cb,
        );
      },
      pin: (name: string, fingerprint: string, cb: Callback) => {
        complete(this.env.TUNNEL.getByName(name).pinSshHost(fingerprint), cb);
      },
    };
    const id = crypto.randomUUID();
    nborkSessions.set(id, session);
    try {
      const runtime = new Go();
      runtime.argv = ['nbork-ssh', id];
      runtime.exit = (code) => {
        active = false;
        if (code !== 0) console.error('SSH gateway exited with code', code);
      };
      await runtime.run(new WebAssembly.Instance(module, runtime.importObject));
    } finally {
      active = false;
      nborkSessions.delete(id);
      for (const close of sockets) close();
    }
  }
}
