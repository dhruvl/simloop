# Security

simloop is a testing library: it runs code under test inside a simulated
event loop and is not meant to run in production. A vulnerability here is
most likely to matter through CI, for example a trace or timeline artifact
that renders untrusted content unsafely.

## Supported versions

Fixes land on the latest release only.

## Reporting a vulnerability

Please report it privately through GitHub's
[security advisory form](https://github.com/dhruvl/simloop/security/advisories/new)
rather than in a public issue. You should get a reply within a week. Once a
fix is released, the advisory is published with credit to the reporter
unless they prefer otherwise.
