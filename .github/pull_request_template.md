## Summary

<!-- What changes and why. Link the issue, if there is one. -->

## Checklist

- [ ] `uv run ruff check` and `uv run ruff format --check` pass
- [ ] `uv run mypy` passes
- [ ] `uv run python -m pytest` passes; new behaviour has tests
- [ ] Integration tests run (`pytest -m integration`, see CONTRIBUTING.md) if the change touches reads or the change feed
- [ ] `CHANGELOG.md` has an entry under `[Unreleased]` for user-visible changes
- [ ] README or `docs/` updated for changed options or behaviour
