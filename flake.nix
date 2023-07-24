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
    let
      filterSystems = pred:
        let
          inherit (builtins) elemAt filter isList match;
          candidates = flake-utils.lib.defaultSystems;
          analyze = system:
            let matches = match "([[:alnum:]_]+)-([[:alnum:]_]+)" system;
            in { arch = elemAt matches 0; os = elemAt matches 1; };
        in
          filter (system: pred (analyze system)) candidates;

      baseOutputs = flake-utils.lib.eachDefaultSystem (system:
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
              propagatedBuildInputs = with pkgs; [
                rclone
              ];

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


      linuxSystems = filterSystems (sys: sys.os == "linux");
      dockerImages = flake-utils.lib.eachSystem linuxSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system}.pkgs;
        in {
          packages = {
            downloader-docker = pkgs.dockerTools.streamLayeredImage {
              name = "docker.io/sacundim/covid-19-puerto-rico-downloader";
              tag = "latest-nix-${system}";
              contents = [ baseOutputs.packages.${system}.downloader ];
            };

            website-docker = pkgs.dockerTools.streamLayeredImage {
              name = "docker.io/sacundim/covid-19-puerto-rico-website";
              tag = "latest-nix-${system}";
              contents = [ baseOutputs.packages.${system}.website ];
              config = {
                Cmd = [ "covid19pr" ];
              };
            };
          };
        }
      );

      mergeOutputs =
        let
          inherit (builtins) length;
          inherit (nixpkgs.lib.attrsets) recursiveUpdateUntil;
          mergeDepth = depth:
            recursiveUpdateUntil (path: l: r: length path > depth);
        in builtins.foldl' (mergeDepth 2) {};
    in mergeOutputs [
        baseOutputs
        dockerImages
    ];

}