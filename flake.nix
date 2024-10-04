{
  description = "A flake for a Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in {
        devShell = pkgs.mkShell {
          buildInputs = [
            pkgs.rustup
            pkgs.cargo
            pkgs.rust-analyzer
            pkgs.clippy
            pkgs.cargo-tarpaulin
            pkgs.cargo-release
            pkgs.cargo-machete
            pkgs.rustfmt
            pkgs.pkg-config # Optional
          ];

          shellHook = ''
            rustup default stable
          '';
        };
      });
}

