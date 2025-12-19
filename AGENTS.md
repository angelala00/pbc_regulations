# Repository Guidelines

## Project Structure & Module Organization
- `pbc_regulations/` hosts the Python package with key modules like `crawler/`, `extractor/`, `structure/`, `searcher/`, `portal/`, and `mcp_server/`.
- `tests/` contains pytest-based test cases.
- `files/` is the main data workspace for PDFs, JSON outputs, and processed artifacts.
- Top-level configs include `pbc_config.json`, `policy_whitelist.json`, and `.env.example` for environment setup.
- `scripts/` and `icrawler/` contain helper tooling and batch workflows.

## Build, Test, and Development Commands
- `python -m pbc_regulations` runs the portal/interactive entry point.
- `python -m pbc_regulations.crawler` runs the regulation crawler.
- `python -m pbc_regulations.extractor.extract_policy_texts` extracts policy text from PDFs.
- `python -m pbc_regulations.structure` structures extracted data into JSON outputs.
- `pytest` runs the full test suite in `tests/`.

## Coding Style & Naming Conventions
- Follow PEP 8: 4-space indentation, snake_case for functions/variables, and PascalCase for classes.
- Keep module names lowercase with underscores (e.g., `extract_policy_texts.py`).
- Prefer explicit imports within package modules to keep entry points clear.

## Testing Guidelines
- Tests use `pytest`; name files `test_*.py` and tests `test_*`.
- Keep tests close to related functionality (mirrors `pbc_regulations/` structure).
- Run focused tests via `pytest tests/test_<name>.py`.

## Commit & Pull Request Guidelines
- Use short, imperative commit messages (e.g., "Add crawler retries", "Fix PDF parsing").
- PRs should describe changes, include relevant issue links, and note any data or config updates.
- Attach sample outputs or logs when changing crawler/extractor behavior.

## Security & Configuration Tips
- Copy `.env.example` to `.env` and set API keys locally; never commit secrets.
- Update `pbc_config.json` and `policy_whitelist.json` when changing data scope.
- Large generated files should stay under `files/` and remain out of version control.
