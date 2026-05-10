# queued-build-hook

Async queue daemon for Nix's `post-build-hook`. Builds finish instantly; uploads run on a separate worker at idle priority.

## Usage

```nix
{
  imports = [ inputs.queued-build-hook.nixosModules.default ];

  services.queued-build-hook = {
    enable = true;

    workerScript = ''
      export AWS_SHARED_CREDENTIALS_FILE="$CREDENTIALS_DIRECTORY/aws-credentials"
      ${pkgs.nix}/bin/nix store sign \
        --key-file "$CREDENTIALS_DIRECTORY/signing-key" $OUT_PATHS
      exec ${pkgs.nix}/bin/nix copy --to "s3://my-bucket?profile=cache" $OUT_PATHS
    '';

    credentials = {
      aws-credentials = "/run/secrets/aws-credentials";
      signing-key     = "/run/secrets/nix-cache-signing-key";
    };
  };
}
```

Credentials arrive via systemd's `LoadCredential=`, so the daemon's `DynamicUser` never needs filesystem access to `/run/secrets`. Lowercase names appear at `$CREDENTIALS_DIRECTORY/<name>`; `UPPER_SNAKE_CASE` names are auto-exported as env vars (handy for `cachix`, `attic`, etc).

## Features

- **Instant ack** — hooks return as soon as the message is queued.
- **Idle-priority workers** — `Nice=19`, `CPUSchedulingPolicy=idle`, `IOSchedulingClass=idle` by default. Uploads never preempt interactive work.
- **Pause on metered** — with `pauseOnMetered = true`, subscribes to NetworkManager's D-Bus and parks workers while the connection is metered. Queue keeps accepting, drains the moment you're back on Wi-Fi.
- **Real status** — `queued-build-hook status` returns JSON with queue depth, in-flight, totals, rolling avg batch duration, current batch, and last completed batch.

## Status

```
queued-build-hook status --socket /var/lib/nix/queued-build-hook.sock
```

Output is JSON, suitable for `jq` or a watch loop:

```jsonc
{
  "queue_depth": 12,
  "in_flight": 1,
  "processed_total": 4711,
  "failed_total": 3,
  "uptime_seconds": 85322,
  "avg_batch_seconds": 7.4,
  "current_batch":   { "drv_path": "...", "paths_count": 1, "closure_bytes": 12345678, "started_seconds_ago": 240 },
  "last_completed":  { "drv_path": "...", "paths_count": 1, "closure_bytes": 4096000,  "duration_seconds": 12, "completed_seconds_ago": 870 },
  "network":         { "nm_available": true, "metered": false }
}
```

Fields are omitted when their value isn't known yet.

## CLI

```
queued-build-hook daemon  --hook <path> [--socket <path>] [--concurrency N] [--retries N]
                          [--retry-interval-secs N] [--pause-on-metered]
queued-build-hook enqueue --socket <path>     # reads DRV_PATH and OUT_PATHS env vars
queued-build-hook status  --socket <path>
```

## Development

```
nix build               # crane builds the binary, deps cached as a separate layer
nix flake check         # build + clippy + rustfmt + alejandra
nix develop             # devshell with rust + rust-analyzer
```

## License

MIT.
