# inbox-dedup

[![PyPI version](https://img.shields.io/pypi/v/trysac-inbox-dedup)](https://pypi.org/project/trysac-inbox-dedup/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-inbox-dedup)](LICENSE)
[![CI](https://github.com/Trysac/u145-inbox-dedup/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u145-inbox-dedup/actions/workflows/ci.yml)

Inbox pattern for idempotent message consumers: record message ids in the handler's transaction so redelivered messages are skipped, in SQLite or memory

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-inbox-dedup
# or
uv add trysac-inbox-dedup
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import inbox_dedup

# normalize() trims surrounding whitespace and lower-cases the text.
print(inbox_dedup.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    inbox_dedup.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
