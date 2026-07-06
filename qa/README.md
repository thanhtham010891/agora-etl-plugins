# Testkit Architecture

`agora-etl-plugins` now uses a manifest-driven test orchestration model.

The goal is simple:

- keep topology lifecycle in one place
- keep suite selection in one place
- keep release/nightly gates declarative
- let `Makefile` and GitHub Actions stay thin

## Source of truth

- Manifest: `qa/testkit.toml`
- Runner: `scripts/testkit.py`
- Compatibility facade: `Makefile`

Anything that decides:

- which Docker topology to start
- which environment variables a suite needs
- which pytest command belongs to a suite
- which suites form a release gate
- which lanes belong to a periodic matrix

should live in `qa/testkit.toml`, not inside ad-hoc shell logic.

Manifest commands may use `{python}` to bind to the active runner interpreter.

## Structure

The manifest is split into five layers:

1. `defaults.env`
Sets shared environment defaults for local and CI execution.

2. `env_groups.*`
Maps a named runtime profile to the env vars needed by a suite.

3. `topologies.*`
Declares `up`, `down`, and `status` commands for each Docker-backed stack.

4. `suites.*`
Declares the test command, required env, and topology dependencies for one logical verification slice.

5. `gates.*` and `matrix_lanes.*`
Composes suites into release gates and scheduled matrices.

## Extension rules

When adding new coverage:

- Add a new `topologies.<name>` entry only if the Docker lifecycle is new.
- Add a new `env_groups.<name>` entry only if the runtime contract is distinct.
- Add a new `suites.<name>` entry for every independently runnable verification slice.
- Add the suite to a `gates.*` block when it becomes part of release policy.
- Add a `matrix_lanes.<matrix>.<lane>` entry only for scheduled or manually selectable execution.

Avoid:

- embedding `pytest -k ...` logic directly in workflows
- duplicating `docker compose up/down` sequences in shell scripts
- adding backend-specific `make` aliases instead of a declarative testkit entry

## Common operations

Discover the available names first:

```bash
make catalog
```

Run one topology:

```bash
make topology ACTION=up TOPOLOGY=base
make topology ACTION=status TOPOLOGY=base
make topology ACTION=down TOPOLOGY=base
```

Run one suite:

```bash
make integration SUITE=kafka_redis_wedge
```

Run one release gate:

```bash
make gate GATE=release_base
```

Run selected matrix lanes:

```bash
make matrix MATRIX=periodic LANES=base,postgres
```

Dry-run any orchestration change before wiring it into CI:

```bash
python3 scripts/testkit.py --dry-run gate run release_base
python3 scripts/testkit.py --dry-run matrix run periodic --lanes base,postgres
```

Dry-run is read-only: it prints commands but creates neither topology nor test
artifact files.

## Maintenance contract

If a future change needs logic that does not fit the current manifest model, prefer extending `scripts/testkit.py` once instead of introducing another custom shell script. The professional baseline is one orchestration engine, many declarative entries.
