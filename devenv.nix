{
  pkgs,
  lib,
  inputs,
  ...
}:

{
  # Fix bug: https://github.com/cachix/devenv/issues/2405
  disabledModules = [ "${inputs.devenv}/integrations/secretspec.nix" ];
  # https://devenv.sh/basics/

  # https://devenv.sh/packages/
  packages = with pkgs; [
    protobuf
    git-cliff
    cargo-dist
  ];

  # https://devenv.sh/variables/
  env = {
    LD_LIBRARY_PATH = lib.makeLibraryPath (
      with pkgs;
      [
      ]
    );
    PKG_CONFIG_PATH = lib.makeSearchPathOutput "dev" "lib/pkgconfig" (
      with pkgs;
      [
      ]
    );
  };

  # https://devenv.sh/languages/
  languages.rust = {
    enable = true;
    channel = "stable";
  };

  # https://devenv.sh/scripts/
  scripts.run.exec = "cargo run";
  scripts.build.exec = "cargo build";
  scripts.build-release.exec = "cargo build --release";
  scripts.changelog-gen.exec = ''
    VERSION=$1
    if [ -z "$VERSION" ]; then
      echo "Usage: changelog-gen <version>"
      exit 1
    fi

    git cliff --unreleased --tag "v$VERSION" --output /tmp/changelog-draft.md

    ''${EDITOR:-nano} /tmp/changelog-draft.md

    if [ -f CHANGELOG.md ]; then
      printf "# Changelog\n\n" > /tmp/changelog-merged.md
      grep -v "^# Changelog" /tmp/changelog-draft.md >> /tmp/changelog-merged.md
      grep -v "^# Changelog" CHANGELOG.md >> /tmp/changelog-merged.md
      cat -s /tmp/changelog-merged.md > /tmp/changelog-squeezed.md
      mv /tmp/changelog-squeezed.md CHANGELOG.md
    else
      cat -s /tmp/changelog-draft.md > CHANGELOG.md
    fi

    echo "CHANGELOG.md updated with new version $VERSION"
  '';

  enterShell = ''
    echo "🦀 Rust development environment loaded!"
    echo ""
    echo "Available commands:"
    echo "  run           - cargo run"
    echo "  build         - cargo build"  
    echo "  build-release - cargo build --release"
    echo ""
  '';
}
