# Scripts Layout

This directory is organized by responsibility instead of file extension.

- `qa/`: declarative manifest and documentation for release/integration automation
- `security/`: Python CLI for local certificate and ACL asset generation
- `diagnostics/`: Python CLI for failure artifact collection
- `packaging/`: installed-wheel and release packaging verification
- `fixtures/`: lightweight runtime helpers used by integration environments

Rule of thumb:

- orchestration logic belongs in `scripts/testkit.py`
- Python CLI is the default for reusable automation logic
- shell wrappers are no longer the default path for automation
- new one-off scripts should not be added to the `scripts/` root
