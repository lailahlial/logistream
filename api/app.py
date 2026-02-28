"""
Vercel entrypoint for LogiStream Agent IA FastAPI application.
This file exports the FastAPI app for Vercel serverless deployment.
"""

import sys
import os

# Add services directory to path to allow imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "agent_ia"))

# Install missing dependencies on first import
try:
    import groq
except ImportError:
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "groq>=0.4.0"], check=False)
    import groq

# Import the FastAPI app from the agent_ia service
from main import app

# Export for Vercel
__all__ = ["app"]

