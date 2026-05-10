{
  description = "Async post-build-hook queue daemon for pushing Nix store paths to a binary cache";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    git-hooks-nix = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs @ {flake-parts, ...}:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux" "aarch64-linux"];

      imports = [
        inputs.git-hooks-nix.flakeModule
      ];

      flake.nixosModules = {
        queued-build-hook = import ./module.nix inputs;
        default = inputs.self.nixosModules.queued-build-hook;
      };

      perSystem = {
        config,
        pkgs,
        system,
        ...
      }: let
        rust = pkgs.rust-bin.stable.latest.default;
        craneLib = (inputs.crane.mkLib pkgs).overrideToolchain rust;

        src = craneLib.cleanCargoSource ./.;

        commonArgs = {
          inherit src;
          strictDeps = true;
        };

        # Cache deps as their own layer so source changes don't redo them.
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # Vendored crate set so the pre-commit clippy hook resolves deps
        # without internet — both at `git commit` time and inside the
        # network-less `nix flake check` sandbox.
        cargoVendorDir = craneLib.vendorCargoDeps {inherit src;};

        clippyWithVendor = pkgs.writeShellApplication {
          name = "cargo-clippy-vendored";
          runtimeInputs = [rust];
          text = ''
            CARGO_HOME=$(mktemp -d)
            cp ${cargoVendorDir}/config.toml "$CARGO_HOME/config.toml"
            export CARGO_HOME
            exec cargo-clippy clippy --all-targets --offline "$@" -- --deny warnings
          '';
        };

        queued-build-hook = craneLib.buildPackage (commonArgs
          // {
            inherit cargoArtifacts;
            meta = {
              description = "Async post-build-hook queue daemon for pushing Nix store paths to a binary cache";
              license = pkgs.lib.licenses.mit;
              mainProgram = "queued-build-hook";
            };
          });
      in {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [inputs.rust-overlay.overlays.default];
        };

        # Pre-commit owns lint + format. Hooks run at `git commit` time AND
        # via `nix flake check` (the flakeModule auto-registers itself).
        # The clippy hook's entry is replaced with a wrapper that primes
        # CARGO_HOME with crane's vendored cargo cache, so cargo can resolve
        # deps in the network-less flake-check sandbox without a separate
        # crane derivation. Single source of truth for which lints run.
        pre-commit.settings.hooks = {
          alejandra.enable = true;
          rustfmt = {
            enable = true;
            packageOverrides.rustfmt = rust;
          };
          clippy = {
            enable = true;
            entry = pkgs.lib.mkForce "${clippyWithVendor}/bin/cargo-clippy-vendored";
            files = "\\.rs$";
            pass_filenames = false;
          };
        };

        packages = {
          default = queued-build-hook;
          inherit queued-build-hook;
        };

        checks = {
          inherit queued-build-hook;
        };

        formatter = pkgs.alejandra;

        devShells.default = craneLib.devShell {
          inputsFrom = [queued-build-hook];
          packages = with pkgs; [
            rust-analyzer
          ];
          shellHook = ''
            ${config.pre-commit.installationScript}
          '';
        };
      };
    };
}
