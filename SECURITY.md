# Security Policy

`agora-etl-plugins` is the official first-party plugin package for Agora's
production integrations. This policy covers security issues in the plugin
package itself, including Redis, Kafka, PostgreSQL, cron, distributed
coordination, and Anthropic provider surfaces.

## Supported Versions

| Package line | Supported for security fixes | Notes |
|---|---:|---|
| `0.4.x` | Yes | Current `agora-etl 0.4.x` plugin line. Requires `agora-etl>=0.4.5,<1` and supports Python `3.11`, `3.12`, and `3.13`. |
| `<0.4` | No | Upgrade to the current line before requesting security fixes. |

Security fixes should be released on the current supported line unless a
specific backport is explicitly announced.

## Reporting A Vulnerability

Do not disclose exploitable details in public GitHub issues, pull requests, or
discussions.

Preferred reporting path:

1. Use GitHub private vulnerability reporting for the repository if it is
   enabled.
2. If private reporting is not available, open a minimal public issue asking
   for a private security contact path. Do not include exploit details, secrets,
   payloads, stack traces with credentials, or production hostnames.

Please include privately:

- affected `agora-etl-plugins` version
- affected `agora-etl` version
- Python version and operating system
- affected plugin family, such as Redis, Kafka, PostgreSQL, Anthropic, cron, or
  distributed coordination
- a minimal reproduction or proof of impact
- whether credentials, tenant data, DLQ payloads, schema registry credentials,
  database rows, Kafka messages, Redis keys, or checkpoint state can be exposed
  or modified

## Vulnerability Handling

Maintainers should triage reports by impact:

- credential, token, secret-file, TLS, or authentication bypass issues
- data exposure through DLQ payloads, logs, metrics, traces, or replay records
- integrity failures that can silently drop, duplicate, corrupt, or misroute
  records
- unsafe defaults in Kafka, PostgreSQL, Redis, or Anthropic integrations
- denial-of-service vectors in public parsing, schema handling, replay, or
  batching surfaces

Confirmed vulnerabilities should receive:

1. a private fix plan
2. regression coverage where practical
3. a patched release for the supported line
4. public disclosure notes after a fix is available

## Dependency Security

The plugin package depends on external clients such as `redis`, `aiokafka`,
`psycopg`, `httpx`, `anthropic`, schema tooling, and their transitive
dependencies. Security updates in those dependencies should be evaluated
against the current compatibility range before widening or pinning versions.

For production deployments, keep `agora-etl`, `agora-etl-plugins`, Python, and
backend client libraries patched within the supported version ranges.
