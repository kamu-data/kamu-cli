{
  description = "Next-generation decentralized data lakehouse and a multi-party stream processing network";

  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url  = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        darwinPkgs = [ 
          pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
          pkgs.darwin.apple_sdk.frameworks.CoreServices
        ];
      in
      {
        devShells.default = with pkgs; mkShell {
          buildInputs = [
            rust-bin.nightly."2024-06-13".default
            rust-analyzer
            jq
            kubo
            flatbuffers
            protobuf
          ] ++ lib.optionals pkgs.stdenv.isDarwin darwinPkgs;

          shellHook = ''
          '';
        };
      }
    );
}
