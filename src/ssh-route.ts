// Shared Spectrum endpoints use a netcat HTTP CONNECT preface. On a dedicated
// Spectrum IP the Worker can select the channel before reading any SSH bytes.
import { closeSocket } from './ssh-relay';
import type { Env } from './worker';

const encoder = new TextEncoder();
const channelName = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

function channelFromConnect(header: string, domain: string) {
  const line = header.split('\r\n', 1)[0];
  const match = /^CONNECT ([a-zA-Z0-9.-]+):[0-9]{1,5} HTTP\/1\.[01]$/.exec(
    line,
  );
  const suffix = '.' + domain.toLowerCase();
  const host = match?.[1].toLowerCase();
  if (!host?.endsWith(suffix)) return;
  const channel = host.slice(0, -suffix.length);
  if (channelName.test(channel)) return channel;
}

async function readConnect(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  domain: string,
) {
  // Latin-1 keeps byte offsets intact if the first SSH packet shares this read.
  const decoder = new TextDecoder('latin1');
  let header = '';
  while (header.length < 4096) {
    const { done, value } = await reader.read();
    if (done) break;
    const previous = header.length;
    header += decoder.decode(value.subarray(0, 4096 - previous));
    if (header.startsWith('SSH-')) {
      const rest = new Uint8Array(previous + value.length);
      rest.set(encoder.encode(header.slice(0, previous)));
      rest.set(value, previous);
      return { channel: undefined, rest };
    }
    const end = header.indexOf('\r\n\r\n', Math.max(0, previous - 3));
    if (end === -1) continue;
    const channel = channelFromConnect(header, domain);
    if (!channel) throw new Error('Unknown SSH destination');
    return { channel, rest: value.subarray(end + 4 - previous) };
  }
  throw new Error('Incomplete SSH routing header');
}

async function write(stream: WritableStream<Uint8Array>, data: Uint8Array) {
  const writer = stream.getWriter();
  try {
    await writer.write(data);
  } finally {
    writer.releaseLock();
  }
}

export async function routeSsh(socket: Socket, env: Env) {
  const { localAddress } = await socket.opened;
  const ip = localAddress?.replace(/:[0-9]+$/, '').replace(/^\[|\]$/g, '');
  const dedicated = (ip && env.SSH_ROUTES?.[ip]) || env.SSH_ROUTES?.['*'];
  let channel = dedicated;
  let rest: Uint8Array | undefined;
  let upstream: Socket | undefined;
  try {
    if (!channel) {
      const reader = socket.readable.getReader();
      const timer = setTimeout(() => {
        void reader.cancel().catch(() => {});
      }, 10000);
      try {
        ({ channel, rest } = await readConnect(reader, env.ROOT_DOMAIN));
      } catch {
        await write(
          socket.writable,
          encoder.encode('HTTP/1.0 400 Bad Request\r\n\r\n'),
        );
        return;
      } finally {
        clearTimeout(timer);
        reader.releaseLock();
      }
    }
    const viaProxy = !!channel && !dedicated;
    // Previously documented wildcard ProxyCommands also match ssh.<domain>.
    // Unwrap CONNECT for the gateway itself before processing its SSH username.
    const gateway = env.SSH_GATEWAY;
    const destination =
      channel && !(viaProxy && gateway && channel === 'ssh')
        ? env.TUNNEL.getByName(channel)
        : gateway?.get(gateway.newUniqueId());
    if (!destination) return;
    upstream = destination.connect('ssh:22', { allowHalfOpen: true });
    await upstream.opened;
    if (viaProxy) {
      await write(
        socket.writable,
        encoder.encode('HTTP/1.0 200 Connection established\r\n\r\n'),
      );
    }
    const peer = upstream;
    await Promise.all([
      (async () => {
        if (rest?.length) await write(peer.writable, rest);
        await socket.readable.pipeTo(peer.writable);
      })(),
      upstream.readable.pipeTo(socket.writable),
    ]);
  } catch (error) {
    console.debug('SSH connection ended', String(error));
  } finally {
    await Promise.all([closeSocket(socket), upstream && closeSocket(upstream)]);
  }
}
