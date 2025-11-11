import asyncio
import importlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbc_regulations.agents.legal_search import gpts_regulation as gpts_module


class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=False)

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _DummyAsyncClient:
    def __init__(self, payload, call_log):
        self._payload = payload
        self._call_log = call_log

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None):
        self._call_log.append({"url": url, "params": params})
        return _DummyResponse(self._payload)


def _setup_catalog(monkeypatch, env_value: str, payload):
    monkeypatch.setenv("LEGAL_SEARCH_USE_AI_CATALOG", env_value)
    module = importlib.reload(gpts_module)
    call_log = []

    def _client_factory(*args, **kwargs):
        return _DummyAsyncClient(payload, call_log)

    monkeypatch.setattr(module.httpx, "AsyncClient", _client_factory)
    return module, call_log


def test_fetch_document_catalog_scope_all(monkeypatch):
    payload = [
        {"title": "Doc 1", "id": "doc-1"},
        {"title": "Doc 2", "id": "doc-2"},
    ]
    module, call_log = _setup_catalog(monkeypatch, "0", payload)

    content = asyncio.run(module.fetch_document_catalog())
    parsed = json.loads(content)

    assert parsed == payload
    assert call_log == [
        {
            "url": f"{module.BASE_URL}/api/policies",
            "params": {"scope": "all"},
        }
    ]


def test_fetch_document_catalog_ai_view(monkeypatch):
    payload = {
        "groups": [
            {
                "name": "国家法律",
                "entries": [
                    {
                        "title": "Doc 3",
                        "document_id": "doc-3",
                        "summary": "  新接口摘要  ",
                    },
                    {
                        "title": "Doc 4",
                        "document_id": "doc-4",
                        "summary": "",
                    },
                ],
            }
        ]
    }
    module, call_log = _setup_catalog(monkeypatch, "1", payload)

    content = asyncio.run(module.fetch_document_catalog())
    parsed = json.loads(content)

    assert parsed == [
        {"title": "Doc 3", "id": "doc-3", "summary": "新接口摘要"},
        {"title": "Doc 4", "id": "doc-4"},
    ]
    assert call_log == [
        {
            "url": f"{module.BASE_URL}/api/policies/catalog",
            "params": {"view": "ai"},
        }
    ]
