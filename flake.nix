{
  description = "covid-19-puerto-rico";
  nixConfig.bash-prompt = "[nix(covid-19-puerto-rico)] ";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/22.11";
    flake-utils.url = "github:numtide/flake-utils";
    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, poetry2nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit (poetry2nix.legacyPackages.${system}) mkPoetryApplication;
        pkgs = nixpkgs.legacyPackages.${system}.pkgs;
      in rec {
        packages = {
          downloader = mkPoetryApplication {
            name = "covid-19-puerto-rico-downloader";
            projectDir = ./downloader;
            python = pkgs.python311;
            preferWheels = true;
            propagatedBuildInputs = with pkgs; [
              lbzip2 rclone
            ];
          };

          website = mkPoetryApplication {
            name = "covid-19-puerto-rico-website";
            projectDir = ./website;
            python = pkgs.python311;
            preferWheels = true;
          };

          default = pkgs.symlinkJoin {
            name = "covid-19-puerto-rico";
            paths = [
              packages.downloader
              packages.website
            ];
          };
        };
      }
    );
}