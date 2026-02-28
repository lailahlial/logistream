#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=".venv"
echo "Creating Python virtual environment in '$VENV_DIR'..."
python3 -m venv "$VENV_DIR"

echo "Activating venv and installing dependencies from requirements.txt..."
. "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip
pip install -r requirements.txt

echo "Done. Activate with: source $VENV_DIR/bin/activate"
echo "Run FastAPI with uvicorn (example): python -m uvicorn main:app --reload"
