#!/usr/bin/env node
import { runTunnel } from "../src/index.js";

const channel = process.argv[2] || process.env.CHANNEL;
const baseUrl = process.env.TUNNEL_BASE || "https://tunnel.app";
const secret  = process.env.TUNNEL_SECRET;

if (!channel) {
  console.error("Usage: cftunnel <channel>   # or set CHANNEL env var");
  process.exit(1);
}

runTunnel({ channel, baseUrl, secret, log: (...a) => console.log(...a) });
