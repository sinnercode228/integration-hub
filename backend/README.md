# relay-integration-hub (backend)

Python package of **Relay** - the webhook integration hub (demo project).
Full documentation, architecture and run instructions: see the repository [README](../README.md).

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest            # tests, no network and no Docker required
relay serve       # API + in-process worker on http://127.0.0.1:8000
```
