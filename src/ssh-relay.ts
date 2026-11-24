import { b64, decodeB64 } from './base64';

// workerd can throw synchronously when the peer has already closed the socket.
export async function closeSocket(socket: Socket) {
  try {
    await socket.close();
  } catch {}
}

// One chunk in flight per direction keeps slow terminals from accumulating
// unbounded data on the shared control WebSocket.
export class SshRelay {
  private readonly writer: WritableStreamDefaultWriter<Uint8Array>;
  private acknowledge?: () => void;
  private stopped = false;

  constructor(
    private socket: Socket,
    private control: WebSocket,
    readonly id: string,
  ) {
    this.writer = socket.writable.getWriter();
  }

  private send(type: string, data?: string) {
    this.control.send(JSON.stringify({ type, id: this.id, data }));
  }

  async run() {
    try {
      this.send('ssh_open');
      await Promise.all([
        this.socket.readable.pipeTo(
          new WritableStream<Uint8Array>({
            write: async (bytes) => {
              for (let pos = 0; pos < bytes.length; pos += 32768) {
                if (this.stopped) throw new Error('SSH session closed');
                const ack = new Promise<void>((resolve) => {
                  this.acknowledge = resolve;
                });
                this.send('ssh_data', b64(bytes.subarray(pos, pos + 32768)));
                await ack;
              }
            },
            close: () => this.send('ssh_eof'),
          }),
        ),
        this.writer.closed,
      ]);
    } catch (error) {
      if (!this.stopped) console.debug('SSH relay ended', String(error));
    } finally {
      this.stop();
      try {
        this.send('ssh_close');
      } catch {}
    }
  }

  async receive(type: string, data?: string) {
    switch (type) {
      case 'ssh_ack':
        this.acknowledge?.();
        break;
      case 'ssh_data':
        if (!data || data.length > 43692) throw new Error('Invalid SSH frame');
        await this.writer.write(decodeB64(data));
        this.send('ssh_ack');
        break;
      case 'ssh_eof':
        await this.writer.close();
        break;
      case 'ssh_close':
        this.stop();
        break;
    }
  }

  stop() {
    if (this.stopped) return;
    this.stopped = true;
    this.acknowledge?.();
    void closeSocket(this.socket);
  }
}
