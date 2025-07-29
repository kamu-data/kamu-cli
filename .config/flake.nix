{
  description = "Dev environment with supporting tools";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        flatc = pkgs.flatbuffers.overrideAttrs (_: {
          version = "24.12.23";
          src = pkgs.fetchFromGitHub {
            owner = "google";
            repo = "flatbuffers";
            rev = "v24.12.23";
            sha256 = "sha256-6L6Eb+2xGXEqLYITWsNNPW4FTvfPFSmChK4hLusk5gU=";
          };
        });
      in {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            flatc
            pkgs.protobuf
            pkgs.protoc-gen-prost
            pkgs.protoc-gen-tonic
          ];
        };
      });
}
