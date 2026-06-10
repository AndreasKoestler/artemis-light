# Contributing to artemis-light

Thanks for your interest in contributing! This document covers local setup,
how we work, and what we expect from a pull request.

## Local development setup

You need:

- **Rust** (nightly toolchain, matching CI) with `rustfmt` and `clippy`:

  ```bash
  rustup toolchain install nightly --component rustfmt clippy
  ```

- **Foundry** (`anvil` must be on `$PATH`) for the integration tests, which
  spin up a local Anvil node:

  ```bash
  curl -L https://foundry.paradigm.xyz | bash
  foundryup --install v1.7.1
  ```

Then build and test:

```bash
cargo build
cargo test --all-features   # full suite, requires anvil
cargo test --lib            # unit tests only, no external dependencies
```

## Before opening a PR

CI enforces formatting, lints (warnings are errors), and the full test suite.
Run the same checks locally:

```bash
cargo fmt --all -- --check
RUSTFLAGS="-Dwarnings" cargo clippy --all-features
cargo test --all-features
```

## Branches and pull requests

- Branch from `master` using a short, typed prefix: `feat/...`, `fix/...`,
  `docs/...`, `chore/...`.
- Keep PRs focused — one logical change per PR.
- Write commit messages in the imperative mood with a `type:` prefix, matching
  the existing history (e.g. `fix: stop losing events at startup`).
- Include tests for behaviour changes. Architectural decisions worth recording
  go in `docs/adr/`.
- Open the PR against `master`; a maintainer will review it.

## Reporting issues

Use GitHub issues for bugs and feature requests. For security
vulnerabilities, do **not** open a public issue — see [SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License, Version 2.0](LICENSE).
