# nbork

CLI and TypeScript client for tunneling over the nbork service.

## Usage

```sh
npx nbork <channel>
```

- Pass the channel as the first positional argument, or set the `CHANNEL` environment variable.
- Override the broker base URL with `TUNNEL_BASE` if you host your own instance.

## Development

- `npm install` installs dependencies and generates the build output in `dist/`.
- `npm run build` compiles the CLI and library to CommonJS in `dist/`.
- Publishing runs `npm run build` automatically via the `prepare` script; use `npm publish --access public` when ready.
