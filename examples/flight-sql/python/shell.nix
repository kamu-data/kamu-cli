# Development shell for NixOS
# Currently has to be kept in sync with `requirements.txt` manually
let
  pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/nixos-unstable.tar.gz") {};
in pkgs.mkShell {
  packages = [
    (pkgs.python3.withPackages (python-pkgs: with python-pkgs; [
      # adbc_driver_manager
      # adbc_driver_flightsql
      # flightsql-dbapi
      sqlalchemy
      pandas
      pyarrow
    ]))
  ];
}