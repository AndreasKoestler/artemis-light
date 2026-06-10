# Security Policy

## Reporting a vulnerability

Please **do not** report security vulnerabilities through public GitHub
issues, discussions, or pull requests.

Instead, use one of these private channels:

- **GitHub private vulnerability reporting** (preferred):
  [Report a vulnerability](https://github.com/AndreasKoestler/artemis-light/security/advisories/new)
- **Email:** andreas@last.net

Include as much of the following as you can:

- A description of the issue and its impact
- Steps to reproduce or a proof of concept
- Affected versions or commit range

You should receive an acknowledgement within a few business days. Please give
us a reasonable window to investigate and release a fix before any public
disclosure.

## Scope

This library connects to Ethereum-compatible RPC endpoints and can submit
transactions. Issues of particular interest include anything that could cause
loss of funds, leakage of signing keys, or corruption/replay of persisted
event state.
