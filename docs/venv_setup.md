# Python virtual environment setup

This project already includes `uvicorn` in `requirements.txt`.

Quick setup (Windows PowerShell):

```powershell
# Create venv and install deps (recommended)
.
# From project root
.
powershell -File scripts\setup_venv.ps1

# Activate venv
. .\.venv\Scripts\Activate.ps1

# Run FastAPI with uvicorn (example)
python -m uvicorn main:app --reload
```

Quick setup (macOS / Linux):

```bash
# From project root
./scripts/setup_venv.sh
source .venv/bin/activate
python -m uvicorn main:app --reload
```

Notes:
- The scripts create a `.venv` directory and install dependencies from `requirements.txt`.
- Adjust the `main:app` module path to the FastAPI entrypoint in the service you want to run (for example `services/agent_ia/main:app`).
