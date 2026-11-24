import { routeSsh } from './ssh-route';

export interface Env {
  TUNNEL: DurableObjectNamespace;
  SSH_ROUTES?: Record<string, string>;
  ROOT_DOMAIN: string; // e.g. "tunnel.app"
}

const SUBDOMAIN_RE = /^(?<sub>[^.]+)\.(?<root>.+)$/;
const CH_PORT_RE = /^(?<channel>[a-z0-9-]+)-(\*)?(?<port>\d{1,5})$/i;

export default {
  connect: routeSsh,
  async fetch(
    req: Request,
    env: Env,
    _ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(req.url);

    if (url.hostname === env.ROOT_DOMAIN && url.pathname === '/setup.sh') {
      if (req.method !== 'GET' && req.method !== 'HEAD') {
        return new Response('Method not allowed', {
          status: 405,
          headers: { allow: 'GET, HEAD' },
        });
      }

      return new Response(null, {
        status: 302,
        headers: {
          'cache-control': 'public, max-age=300',
          location:
            'https://raw.githubusercontent.com/mikesoylu/nbork/main/setup.sh',
        },
      });
    }

    // Control channel: ws connect from the client CLI
    if (url.pathname.startsWith('/_connect/')) {
      const channel = decodeURIComponent(
        url.pathname.split('/_connect/')[1] || '',
      ).trim();
      if (!channel) return new Response('Missing channel', { status: 400 });
      const id = env.TUNNEL.idFromName(channel);
      const stub = env.TUNNEL.get(id);
      return stub.fetch(req);
    }

    // Everything else is proxied based on subdomain <channel>-<port>.<root_domain>
    const host = req.headers.get('host') || '';
    const m = host.match(SUBDOMAIN_RE);
    if (!m) return new Response('Host header not recognized', { status: 400 });

    const { sub, root } = m.groups as any;
    if (env.ROOT_DOMAIN && root !== env.ROOT_DOMAIN) {
      return new Response('Wrong domain', { status: 421 });
    }

    const m2 = sub.match(CH_PORT_RE);
    if (!m2)
      return new Response('Subdomain must be <channel>-<port>', {
        status: 400,
      });

    const { channel, port } = m2.groups as any;

    console.log('Connecting:', { channel, port });

    const id = env.TUNNEL.idFromName(channel);
    const stub = env.TUNNEL.get(id);

    // Forward to the DO with metadata headers (keeps original method/URL/body).
    const fwdHeaders = new Headers(req.headers);
    fwdHeaders.set('x-tunnel-port', String(port));
    fwdHeaders.set('x-tunnel-host', host);

    const fwd = new Request(req, { headers: fwdHeaders });
    return stub.fetch(fwd);
  },
} satisfies ExportedHandler<Env>;

export { TunnelDO } from './tunnel-do';
