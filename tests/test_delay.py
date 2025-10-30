import importlib
import sys
import types
import urllib.parse


class _Tag:
    def __init__(self, href: str):
        self._href = href

    def __getitem__(self, key: str) -> str:
        return self._href


class _Soup:
    def __init__(self, text: str, parser: str):
        self._text = text

    def find_all(self, tag: str, href: bool = False):
        import re

        return [_Tag(h) for h in re.findall(r'href="(.*?)"', self._text)]


class DummyResponse:
    def __init__(self, text: str = ""):
        self.text = text
        self.content = b""

    def raise_for_status(self):
        pass


def _load_crawler(monkeypatch):
    requests_stub = types.SimpleNamespace()
    requests_stub.compat = types.SimpleNamespace(urljoin=urllib.parse.urljoin)
    requests_stub.get = lambda url: None
    monkeypatch.setitem(sys.modules, "requests", requests_stub)

    bs4_stub = types.SimpleNamespace(BeautifulSoup=_Soup)
    monkeypatch.setitem(sys.modules, "bs4", bs4_stub)

    pdfkit_stub = types.SimpleNamespace(from_url=lambda *a, **k: None)
    monkeypatch.setitem(sys.modules, "pdfkit", pdfkit_stub)

    sys.modules.pop("icrawler", None)
    module = importlib.import_module("icrawler")
    return importlib.reload(module)


def test_crawl_respects_delay(tmp_path, monkeypatch):
    crawler = _load_crawler(monkeypatch)

    html = '<a href="file.pdf">pdf</a>'
    monkeypatch.setattr(crawler.requests, "get", lambda url: DummyResponse(html))
    monkeypatch.setattr(crawler, "download_file", lambda url, out: None)
    monkeypatch.setattr(crawler, "save_page_as_pdf", lambda url, out: None)
    sleeps = []
    monkeypatch.setattr(crawler.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(crawler.random, "uniform", lambda a, b: b)

    crawler.crawl(["http://example.com"], tmp_path, delay=1, jitter=0.5)
    assert sleeps == [1.5, 1.5]
