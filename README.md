# nbork

CLI for exposing all your ports to the internet like an absolute madlad.

## Usage

```sh
npx nbork <channel-name>
```

Access any port on your machine via:

```sh
curl <channel-name>-<port>.nbork.com
```

## Configuration

- Pass the channel as the first positional argument, or set the `CHANNEL` environment variable.
- Override the broker base URL with `TUNNEL_BASE` if you host your own instance.
