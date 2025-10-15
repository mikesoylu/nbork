#!/usr/bin/env node
import { runTunnel } from '../src/client';

const channel = process.argv[2] || process.env.CHANNEL;
const baseUrl = process.env.TUNNEL_BASE || 'https://nbork.com';

if (!channel) {
  console.error('Usage: nbork <channel>   # or set CHANNEL env var');
  process.exit(1);
}

runTunnel({ channel, baseUrl, log: (...a) => console.log(...a) });
