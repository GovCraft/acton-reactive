{
  description = "A flake for a Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true; # Allow unfree packages
        };
      in
      {
        devShell = pkgs.mkShell {
          buildInputs = [
            pkgs.rustup
            pkgs.cargo
            pkgs.llvm
            pkgs.jetbrains.rust-rover
            pkgs.rust-analyzer
            pkgs.clippy
            pkgs.cargo-tarpaulin
            pkgs.cargo-release
            pkgs.tokei
            pkgs.vhs
            pkgs.cargo-machete
            pkgs.rustfmt
            pkgs.pkg-config # Optional
          ];

          shellHook = ''
            rustup default stable
          '';
        };
      }
    );
}
