# NixOS module for queued-build-hook.
#
# `inputs` is baked in so the default `package` resolves without requiring
# callers to add this flake's overlay to their nixpkgs.
#
# Minimal usage:
#   imports = [ inputs.queued-build-hook.nixosModules.default ];
#   services.queued-build-hook = {
#     enable = true;
#     workerScript = ''
#       nix copy --to "s3://my-bucket?profile=cache" $OUT_PATHS
#     '';
#   };
inputs: {
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.queued-build-hook;
in {
  options.services.queued-build-hook = {
    enable = lib.mkEnableOption "queued-build-hook (async post-build-hook queue)";

    package = lib.mkOption {
      type = lib.types.package;
      default = inputs.self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "inputs.queued-build-hook.packages.\${pkgs.stdenv.hostPlatform.system}.default";
      description = "The queued-build-hook package to use.";
    };

    socketPath = lib.mkOption {
      type = lib.types.str;
      default = "/var/lib/nix/queued-build-hook.sock";
      description = ''
        Filesystem path of the Unix socket the daemon listens on. The
        post-build-hook wrapper and the `queued-build-hook` CLI both connect
        here. Type is `str` (not `path`) on purpose — this is a runtime
        location, not a flake-time source path.
      '';
    };

    socketGroup = lib.mkOption {
      type = lib.types.str;
      default = "nixbld";
      description = ''
        Group with read/write access to the socket. The default `nixbld`
        works because nix-daemon runs the post-build-hook as root (full
        access regardless of group), and trusted-users / build wrappers
        invoking the CLI manually are typically in `nixbld`. Override if
        your CLI users live in a different group.
      '';
    };

    concurrency = lib.mkOption {
      type = lib.types.ints.positive;
      default = 1;
      description = ''
        Number of worker threads that run workerScript in parallel.
        Default 1 keeps `nix copy`'s xz compression from saturating every
        core during builds; bump if your network and CPU can keep up.
      '';
    };

    retries = lib.mkOption {
      type = lib.types.ints.positive;
      default = 3;
      description = "Max attempts per job before dropping it and incrementing failed_total.";
    };

    retryIntervalSecs = lib.mkOption {
      type = lib.types.ints.positive;
      default = 30;
      description = "Backoff between retry attempts, in seconds.";
    };

    pauseOnMetered = lib.mkEnableOption ''
      pausing uploads while NetworkManager reports a metered connection.

      The daemon subscribes to NM's `Metered` property over D-Bus on the
      system bus — transitions are instant, no polling. While metered,
      workers that have pulled a job park; nothing is dropped, the queue
      keeps accepting enqueues, and processing resumes the moment the
      connection becomes unmetered.

      No-op (logs a warning) on hosts where NetworkManager isn't on the
      bus (systemd-networkd, headless, NM not yet started). The daemon
      will not need any extra capabilities — only a system-bus
      connection, which DynamicUser already permits
    '';

    workerScript = lib.mkOption {
      type = lib.types.either lib.types.path lib.types.str;
      example = lib.literalExpression ''
        '''
          nix copy --to "s3://my-bucket?profile=cache" $OUT_PATHS
        '''
      '';
      description = ''
        Bash run by the daemon's worker once per batch pulled off the queue.
        This is where the actual upload action lives. Required.

        Available in the environment:
          - `DRV_PATH`             the .drv that produced this batch (may be empty)
          - `OUT_PATHS`            space-separated list of new store paths
          - `CREDENTIALS_DIRECTORY` per `credentials` below: a directory with
                                   one file per credential
          - any UPPER_SNAKE-named credential is auto-exported as an env var

        Pass either a path (e.g. `pkgs.writeShellScript "..." "..."`) or a
        bash string; strings are wrapped in `pkgs.writeShellScript`
        automatically.
      '';
    };

    enqueueScript = lib.mkOption {
      type = lib.types.either lib.types.path lib.types.str;
      default = ''
        exec ${cfg.package}/bin/queued-build-hook enqueue --socket ${cfg.socketPath}
      '';
      defaultText = lib.literalExpression ''
        "exec ''${cfg.package}/bin/queued-build-hook enqueue --socket ''${cfg.socketPath}"
      '';
      description = ''
        Bash that becomes the `nix.settings.post-build-hook` invoked by
        nix-daemon after every successful build. The default just forwards
        $OUT_PATHS to the queue daemon. Override to inject pre-flight
        checks (signing, marker files, network state) before the final
        `queued-build-hook enqueue` call.

        Receives the same env as `workerScript` minus
        `CREDENTIALS_DIRECTORY` — credentials live with the daemon
        (DynamicUser), not with this build-side hook (which runs as root).

        Pass either a path or a bash string.
      '';
    };

    extraDaemonArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [];
      example = ["--retries" "5"];
      description = ''
        Additional CLI arguments appended to the `queued-build-hook daemon`
        invocation. Forward-compat escape hatch for flags this module
        doesn't expose yet.
      '';
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
      example = "debug";
      description = ''
        Value of `RUST_LOG` for the daemon process. Standard tracing
        filter syntax: e.g. `info`, `debug`,
        `queued_build_hook::daemon=trace`.
      '';
    };

    credentials = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = {};
      example = lib.literalExpression ''
        {
          # File-mode (lowercase): exposed at $CREDENTIALS_DIRECTORY/signing-key
          signing-key = "/run/secrets/nix-cache-signing-key";

          # Env-var-mode (UPPER_SNAKE): file content auto-exported as a var.
          # Handy for any tool that reads tokens from the environment —
          # cachix, attic, GitHub releases, custom HTTP servers, etc.
          CACHIX_AUTH_TOKEN = "/run/secrets/cachix-token";
        }
      '';
      description = ''
        Mapping passed through to systemd's LoadCredential=. The daemon is
        agnostic about what these are for — they're whatever your
        workerScript needs. Two naming conventions:

          - Lowercase names (e.g. `signing-key`, `aws-credentials`,
            `client.crt`) appear as files at
            $CREDENTIALS_DIRECTORY/<name>. Use these for keys, certs, and
            profile files that you want to reference by path.

          - UPPER_SNAKE_CASE names (e.g. `GITHUB_TOKEN`,
            `CACHIX_AUTH_TOKEN`) are additionally auto-exported as env
            vars whose value is the file's contents (with one trailing
            newline trimmed) — useful for tools that read secrets from
            the environment.
      '';
    };

    nice = lib.mkOption {
      type = lib.types.ints.between (-20) 19;
      default = 19;
      description = ''
        Niceness of the daemon process. -20 is highest priority,
        19 (default) is lowest. Combined with `cpuSchedulingPolicy =
        "idle"` the daemon yields all CPU to anything else competing.
      '';
    };

    cpuSchedulingPolicy = lib.mkOption {
      type = lib.types.enum ["other" "batch" "idle" "fifo" "rr"];
      default = "idle";
      description = ''
        systemd CPUSchedulingPolicy. `idle` (default) means the kernel
        only schedules the daemon when no other process wants CPU.
      '';
    };

    ioSchedulingClass = lib.mkOption {
      type = lib.types.enum ["none" "realtime" "best-effort" "idle"];
      default = "idle";
      description = ''
        systemd IOSchedulingClass. `idle` (default) means the disk yields
        to anything else competing for I/O.

        Note: the kernel's `none` scheduler (default for NVMe) ignores
        this — it dispatches FIFO regardless of ionice class. See
        `ioWeight` for a cgroup v2 control that works scheduler-agnostically.
      '';
    };

    cpuWeight = lib.mkOption {
      type = lib.types.ints.between 1 10000;
      default = 10;
      description = ''
        systemd CPUWeight (cgroup v2). Default 100; this module defaults
        to 10 so the service gets ~1/10 the CPU share of normal-weight
        cgroups when anything else competes. Complementary to
        `cpuSchedulingPolicy = "idle"` — the policy is honored by the
        kernel scheduler within a cgroup, the weight is honored by the
        cgroup v2 cpu controller across cgroups.
      '';
    };

    ioWeight = lib.mkOption {
      type = lib.types.ints.between 1 10000;
      default = 10;
      description = ''
        systemd IOWeight (cgroup v2). Default 100; this module defaults
        to 10 so the service gets ~1/10 the I/O share when anything else
        competes. Honored by the block layer directly, so it works on
        the `none` and `mq-deadline` schedulers where `ioSchedulingClass`
        is a no-op.
      '';
    };

    extraEnvironment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = {};
      example = lib.literalExpression ''
        {
          AWS_REGION = "us-west-2";
          HTTPS_PROXY = "http://corp-proxy:3128";
        }
      '';
      description = ''
        Extra env vars set on the daemon process. Merged with the
        module's own (`HOME`, `RUST_LOG`); these take precedence on
        conflict. Use this for tool-specific config the
        `workerScript` needs (region pins, proxies, feature flags).
      '';
    };

    extraServiceConfig = lib.mkOption {
      type = lib.types.attrs;
      default = {};
      example = lib.literalExpression ''
        {
          MemoryMax = "2G";
          TimeoutStopSec = 60;
          Restart = "always";
        }
      '';
      description = ''
        Extra fields merged into the daemon unit's `serviceConfig`.
        Escape hatch for systemd knobs this module doesn't expose
        directly (resource limits, restart policy, security
        hardening, etc.). Module-managed keys (`DynamicUser`,
        `StateDirectory`, `Nice`, `CPUSchedulingPolicy`,
        `IOSchedulingClass`, `CPUWeight`, `IOWeight`,
        `LoadCredential`) win on conflict.
      '';
    };
  };

  config = lib.mkIf cfg.enable (let
    asPath = name: value:
      if builtins.isString value
      then pkgs.writeShellScript name value
      else value;
    hook = asPath "queued-build-hook-worker" cfg.workerScript;
    enqueueWrapper = asPath "queued-build-hook-enqueue" cfg.enqueueScript;
  in {
    nix.settings.post-build-hook = "${enqueueWrapper}";

    systemd.sockets.queued-build-hook = {
      description = "queued-build-hook socket";
      wantedBy = ["sockets.target"];
      socketConfig = {
        ListenStream = cfg.socketPath;
        SocketMode = "0660";
        SocketUser = "root";
        SocketGroup = cfg.socketGroup;
        Service = "queued-build-hook.service";
      };
    };

    systemd.services.queued-build-hook = {
      description = "queued-build-hook async post-build-hook queue";
      requires = ["queued-build-hook.socket"];
      after = ["network.target"];
      # Make `nix` discoverable on PATH for the daemon's metrics path
      # (`nix path-info` is invoked per batch). The user-supplied
      # workerScript usually references `${pkgs.nix}/bin/nix` directly,
      # but the daemon's own metrics call doesn't have that luxury.
      path = [pkgs.nix];
      script = ''
        set -euo pipefail
        # Auto-export UPPER_SNAKE-named credentials as env vars (file content,
        # not path). Strip a trailing newline so a one-line secret saved with
        # `echo > file` doesn't carry an unintended \n through to consumers.
        if [[ -n "''${CREDENTIALS_DIRECTORY:-}" ]]; then
          for f in "$CREDENTIALS_DIRECTORY"/*; do
            key=$(basename "$f")
            if [[ $key =~ ^[A-Z0-9_]+$ ]]; then
              export "$key=$(<"$f" tr -d '\n')"
            fi
          done
        fi
        exec ${cfg.package}/bin/queued-build-hook daemon \
          --hook ${hook} \
          --concurrency ${toString cfg.concurrency} \
          --retries ${toString cfg.retries} \
          --retry-interval-secs ${toString cfg.retryIntervalSecs} \
          ${lib.optionalString cfg.pauseOnMetered "--pause-on-metered"} \
          ${lib.escapeShellArgs cfg.extraDaemonArgs}
      '';
      environment =
        {
          HOME = "/var/lib/queued-build-hook";
          RUST_LOG = cfg.logLevel;
        }
        // cfg.extraEnvironment;
      serviceConfig =
        cfg.extraServiceConfig
        // {
          DynamicUser = true;
          StateDirectory = "queued-build-hook";
          Restart = "on-failure";
          Nice = cfg.nice;
          CPUSchedulingPolicy = cfg.cpuSchedulingPolicy;
          IOSchedulingClass = cfg.ioSchedulingClass;
          CPUWeight = cfg.cpuWeight;
          IOWeight = cfg.ioWeight;
          LoadCredential = lib.mapAttrsToList (k: v: "${k}:${v}") cfg.credentials;
        };
    };
  });
}
