{ pkgs, ... }:

{
  env.GREET = "devenv";

  packages = [
    pkgs.git
    pkgs.pyright
    pkgs.ruff-lsp
    pkgs.poetry
    pkgs.python311Packages.pytest
    pkgs.python311Packages.pytest-cov
  ];

  # https://devenv.sh/languages/
  languages.python.enable = true;

  services = {
    clickhouse = {
      enable = true;
      port = 9000;
    };
  };

  pre-commit.hooks = {
    check-merge-conflicts.enable = true;
    commitizen.enable = true;
    pylint.enable = true;
    pyright.enable = true;
    ripsecrets.enable = true;
    ruff.enable = true;
    pytest = {
      enable = true;
      name = "pytest";
      entry = "${pkgs.python311Packages.pytest}/bin/pytest";
      files = "\\.(py)$";
      types = [ "text" "python" ];
      language = "python";
    };
  };
}
