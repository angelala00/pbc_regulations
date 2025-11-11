import os
from pathlib import Path

from dotenv import load_dotenv


def _load_environment() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(env_path, override=False)


_load_environment()


def get_env_variable(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Environment variable '{name}' is not set.")
    return value


def get_env_variable_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


LEGAL_SEARCH_API_KEY = get_env_variable("LEGAL_SEARCH_API_KEY")
LEGAL_SEARCH_BASE_URL = get_env_variable("LEGAL_SEARCH_BASE_URL")
LEGAL_SEARCH_MODEL_NAME = get_env_variable("LEGAL_SEARCH_MODEL_NAME")
LEGAL_SEARCH_USE_TWO_STAGE_FLOW = get_env_variable_bool(
    "LEGAL_SEARCH_USE_TWO_STAGE_FLOW",
    False,
)
