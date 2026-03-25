# package initializer; load environment variables from .env if present
from pathlib import Path

try:
    from dotenv import load_dotenv

    envpath = Path(__file__).parent.parent.parent / ".env"
    if envpath.exists():
        load_dotenv(dotenv_path=envpath)
except ImportError:
    # dotenv is optional; missing means env vars must be set another way
    pass
