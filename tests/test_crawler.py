import importlib
import sys
import types
import urllib.parse


class _Soup:
    def __init__(self, text: str, parser: str):
        self._text = text

    def find_all(self, tag: str, href: bool = False):
        return []


def _prepare_legacy_module(monkeypatch):
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


def test_safe_filename(monkeypatch):
    crawler = _prepare_legacy_module(monkeypatch)

    assert crawler.safe_filename("http://example.com/a?b=1") == "http___example_com_a_b_1"
    assert (
        crawler.safe_filename("中国人民银行公告[2010]第17号")
        == "中国人民银行公告_2010_第17号"
    )


def test_pbc_wrapper(monkeypatch):
    crawler = _prepare_legacy_module(monkeypatch)

    module = importlib.import_module("pbc_regulations.crawler.crawler")
    wrapped = importlib.reload(module)

    assert wrapped.crawl is crawler.crawl
    assert not hasattr(wrapped, "safe_filename")
