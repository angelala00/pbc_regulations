import builtins
import importlib
import json
import sys
import tempfile
import types
from pathlib import Path
import os
from datetime import datetime, timedelta

sys.modules.pop("bs4", None)
importlib.import_module("bs4")

from bs4 import BeautifulSoup

pdfkit_stub = types.SimpleNamespace(from_url=lambda *a, **k: None)
sys.modules.setdefault("pdfkit", pdfkit_stub)

from pbc_regulations.crawler import pbc_monitor
from pbc_regulations.crawler import parser as parser_module
from pbc_regulations.crawler import runner as runner_module
from pbc_regulations.crawler.task_models import TaskSpec
from pbc_regulations.utils.naming import slugify_name


def _make_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def test_listing_cache_is_fresh_when_cached_today(tmp_path):
    page_dir = tmp_path / "pages"
    page_dir.mkdir()
    cache_path = Path(
        pbc_monitor.build_cache_path_for_url(str(page_dir), "http://example.com/list")
    )
    cache_path.write_text("cached", encoding="utf-8")

    assert pbc_monitor._listing_cache_is_fresh(str(page_dir), "http://example.com/list")


def test_listing_cache_is_stale_after_previous_day(tmp_path):
    page_dir = tmp_path / "pages"
    page_dir.mkdir()
    cache_path = Path(
        pbc_monitor.build_cache_path_for_url(str(page_dir), "http://example.com/list")
    )
    cache_path.write_text("cached", encoding="utf-8")

    stale_time = datetime.now() - timedelta(days=2)
    os.utime(cache_path, (stale_time.timestamp(), stale_time.timestamp()))

    assert not pbc_monitor._listing_cache_is_fresh(
        str(page_dir), "http://example.com/list"
    )


def test_listing_cache_is_not_fresh_when_cached_previous_day(monkeypatch, tmp_path):
    page_dir = tmp_path / "pages"
    page_dir.mkdir()
    cache_path = Path(
        pbc_monitor.build_cache_path_for_url(str(page_dir), "http://example.com/list")
    )
    cache_path.write_text("cached", encoding="utf-8")

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            fixed = datetime(2024, 5, 2, 0, 30, 0)
            if tz is not None:
                return tz.fromutc(fixed.replace(tzinfo=tz))
            return fixed

        @classmethod
        def fromtimestamp(cls, t, tz=None):
            return datetime.fromtimestamp(t, tz)

    monkeypatch.setattr(pbc_monitor, "datetime", FixedDateTime)

    previous_day = datetime(2024, 5, 1, 23, 50, 0)
    os.utime(cache_path, (previous_day.timestamp(), previous_day.timestamp()))

    assert not pbc_monitor._listing_cache_is_fresh(
        str(page_dir), "http://example.com/list"
    )


def test_extract_file_links():
    html = """
    <html><body>
      <li>通知1：<a href="doc/notice1.PDF">下载</a></li>
      <div class="entry"><span>报告全文</span><a href="/files/report.docx">附件</a></div>
      <a href="index_2.html">下一页</a>
    </body></html>
    """
    soup = _make_soup(html)
    links = pbc_monitor.extract_file_links("http://example.com/list/index.html", soup)
    assert links == [
        (
            "http://example.com/list/doc/notice1.PDF",
            "通知1",
        ),
        (
            "http://example.com/files/report.docx",
            "报告全文",
        ),
    ]


def test_classify_document_type_wps():
    assert parser_module.classify_document_type("http://example.com/a.wps") == "word"


def test_structured_filename_uses_path_segments():
    name_html = pbc_monitor._structured_filename(
        "http://example.com/dir/sub/index.html",
        "html",
    )
    assert name_html == "dir_sub_index.html"
    name_pdf = pbc_monitor._structured_filename(
        "http://example.com/resource/cms/2025/08/file.pdf",
        "pdf",
    )
    assert name_pdf == "resource_cms_2025_08_file.pdf"
    name_word = pbc_monitor._structured_filename(
        "http://example.com/download?id=42",
        "word",
    )
    assert name_word.startswith("download") and name_word.endswith(".doc")


def test_structured_filename_uses_task_and_serial_when_available():
    name = pbc_monitor._structured_filename(
        "http://example.com/files/archive.zip",
        "archive",
        task_name="my task",
        entry_serial=12,
        doc_index=3,
    )
    assert name == "my_task_000012_003.zip"


def test_ensure_canonical_local_path_renames_to_task_scheme(tmp_path):
    original = tmp_path / "resource_cms_2025_08_file.pdf"
    original.write_text("data", encoding="utf-8")
    file_record = {"local_path": str(original)}
    doc_record = {"local_path": str(original)}
    url = "http://example.com/resource/cms/2025/08/file.pdf"

    renamed = pbc_monitor._ensure_canonical_local_path(
        file_record,
        doc_record,
        url,
        "pdf",
        task_name="demo-task",
        entry_serial=1,
        doc_index=2,
    )

    assert renamed is True
    expected_name = pbc_monitor._structured_filename(
        url,
        "pdf",
        task_name="demo-task",
        entry_serial=1,
        doc_index=2,
    )
    expected_path = original.with_name(expected_name)
    assert expected_path.exists()
    assert file_record["local_path"] == str(expected_path)
    assert doc_record["local_path"] == str(expected_path)


def test_extract_file_links_table_context():
    html = """
    <table>
      <tr>
        <td>中国人民银行公告〔2024〕第1号</td>
        <td><a href="/files/pbc1.doc">word</a> <a href="/files/pbc1.pdf">pdf</a></td>
      </tr>
    </table>
    """
    soup = _make_soup(html)
    links = pbc_monitor.extract_file_links("http://example.com/list/index.html", soup)
    assert links == [
        (
            "http://example.com/files/pbc1.doc",
            "中国人民银行公告〔2024〕第1号",
        ),
        (
            "http://example.com/files/pbc1.pdf",
            "中国人民银行公告〔2024〕第1号",
        )
    ]


def test_extract_file_links_multi_entry_container():
    html = """
    <div class="list">
      <p>标题甲 <a href="/files/a.pdf">下载</a></p>
      <p>标题乙 <a href="/files/b.pdf">下载</a></p>
    </div>
    """
    soup = _make_soup(html)
    links = pbc_monitor.extract_file_links("http://example.com/list/index.html", soup)
    assert links == [
        ("http://example.com/files/a.pdf", "标题甲"),
        ("http://example.com/files/b.pdf", "标题乙"),
    ]


def test_extract_file_links_prefers_title_attribute():
    html = """
    <p>
      公告：<a href="/files/full.pdf" title="中国人民银行公告〔2024〕第2号关于货币政策工具的公告">中国人民银行公告〔2024〕第2号...</a>
    </p>
    """
    soup = _make_soup(html)
    links = pbc_monitor.extract_file_links("http://example.com/list/index.html", soup)
    assert links == [
        (
            "http://example.com/files/full.pdf",
            "中国人民银行公告〔2024〕第2号关于货币政策工具的公告",
        )
    ]


def test_extract_file_links_supports_wps_extension():
    html = """
    <div>
      <a href="/files/rule.wps">word下载</a>
    </div>
    """
    soup = _make_soup(html)
    links = pbc_monitor.extract_file_links("http://example.com/list/index.html", soup)
    assert links == [
        ("http://example.com/files/rule.wps", "word下载"),
    ]


def test_extract_pagination_meta_from_onclick():
    html = """
    <div class="list_page">
      <a tagname="[HOMEPAGE]">首页</a>
      <a tagname="[PREVIOUSPAGE]">上一页</a>
      <a onclick="queryArticleByCondition(this,'/list/index2.html')" tagname="/list/index2.html">下一页</a>
      <a onclick="queryArticleByCondition(this,'/list/index4.html')" tagname="/list/index4.html">尾页</a>
    </div>
    """
    soup = _make_soup(html)
    meta = parser_module.extract_pagination_meta(
        "http://example.com/list/index.html",
        soup,
        "http://example.com/list/index.html",
    )
    assert meta["next"] == "http://example.com/list/index2.html"
    assert meta["last"] == "http://example.com/list/index4.html"
    assert meta["prev"] is None


def test_load_config_and_main(tmp_path):
    config_path = os.path.join(tmp_path, "pbc_config.json")
    output_dir = os.path.join(tmp_path, "downloads")
    state_path = os.path.join(tmp_path, "state.json")
    config_data = {
        "output_dir": output_dir,
        "start_url": "http://example.com/index.html",
        "state_file": state_path,
        "delay": 1.5,
        "jitter": 0.5,
        "timeout": 10,
        "artifact_dir": os.path.join(tmp_path, "artifacts"),
    }
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config_data, handle)

    captured = {}
    original_monitor_once = pbc_monitor.monitor_once
    try:
        def fake_monitor_once(
            start_url,
            out_dir,
            state_file,
            delay,
            jitter,
            timeout,
            page_cache_dir,
            verify_local=False,
            **kwargs,
        ):
            captured.update(
                {
                    "start_url": start_url,
                    "output_dir": out_dir,
                    "state_file": state_file,
                    "delay": delay,
                    "jitter": jitter,
                    "timeout": timeout,
                    "allowed_types": kwargs.get("allowed_types"),
                    "use_cache": kwargs.get("use_cache"),
                    "refresh_cache": kwargs.get("refresh_cache"),
                        "verify_local": verify_local,
                }
            )
            return []

        pbc_monitor.monitor_once = fake_monitor_once
        pbc_monitor.main(["--config", config_path, "--run-once"])
    finally:
        pbc_monitor.monitor_once = original_monitor_once

    assert captured["start_url"] == "http://example.com/index.html"
    assert captured["output_dir"] == output_dir
    assert captured["state_file"] == state_path
    assert captured["delay"] == 1.5
    assert captured["jitter"] == 0.5
    assert captured["timeout"] == 10.0


def test_main_cli_overrides_config(tmp_path):
    config_path = os.path.join(tmp_path, "pbc_config.json")
    output_dir = os.path.join(tmp_path, "downloads")
    state_path = os.path.join(tmp_path, "state.json")
    config_data = {
        "output_dir": output_dir,
        "start_url": "http://example.com/index.html",
        "state_file": state_path,
        "delay": 1.5,
        "artifact_dir": os.path.join(tmp_path, "artifacts"),
    }
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config_data, handle)

    captured = {}
    original_monitor_once = pbc_monitor.monitor_once
    try:
        def fake_monitor_once(
            start_url,
            out_dir,
            state_file,
            delay,
            jitter,
            timeout,
            page_cache_dir,
            verify_local=False,
            **kwargs,
        ):
            captured.update(
                {
                    "start_url": start_url,
                    "output_dir": out_dir,
                    "state_file": state_file,
                    "delay": delay,
                    "jitter": jitter,
                    "timeout": timeout,
                    "allowed_types": kwargs.get("allowed_types"),
                    "use_cache": kwargs.get("use_cache"),
                    "refresh_cache": kwargs.get("refresh_cache"),
                        "verify_local": verify_local,
                }
            )
            return []

        pbc_monitor.monitor_once = fake_monitor_once
        override_out = os.path.join(tmp_path, "custom_out")
        pbc_monitor.main(
            [
                "--config",
                config_path,
                "--run-once",
                override_out,
                "http://override.example/index.html",
                "--delay",
                "4.0",
            ]
        )
    finally:
        pbc_monitor.monitor_once = original_monitor_once

    assert captured["output_dir"] == override_out
    assert captured["start_url"] == "http://override.example/index.html"
    assert captured["delay"] == 4.0


def test_main_dump_structure(tmp_path):
    config_path = os.path.join(tmp_path, "pbc_config.json")
    sample_html = """
    <table>
      <tr>
        <td>1</td>
        <td><a href="detail1.html">公告一</a></td>
        <td>
          <a href="/files/a.doc">word</a>
          <a href="/files/a.pdf">pdf</a>
        </td>
      </tr>
    </table>
    """
    config_data = {
        "output_dir": os.path.join(tmp_path, "downloads"),
        "start_url": "http://example.com/list/index.html",
        "state_file": os.path.join(tmp_path, "state.json"),
        "artifact_dir": os.path.join(tmp_path, "artifacts"),
    }
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config_data, handle)

    original_iterate = pbc_monitor.iterate_listing_pages
    original_session = getattr(pbc_monitor.requests, "Session", None)
    try:
        def fake_iterate(session, start_url, delay, jitter, timeout, page_cache_dir=None, **kwargs):
            yield start_url, _make_soup(sample_html), None

        pbc_monitor.requests.Session = lambda: types.SimpleNamespace(headers={}, close=lambda: None)
        pbc_monitor.iterate_listing_pages = fake_iterate
        structure_path = os.path.join(tmp_path, "structure.json")
        pbc_monitor.main(["--config", config_path, "--build-page-structure", structure_path])
    finally:
        pbc_monitor.iterate_listing_pages = original_iterate
        if original_session is not None:
            pbc_monitor.requests.Session = original_session
        elif hasattr(pbc_monitor.requests, "Session"):
            delattr(pbc_monitor.requests, "Session")

    with open(structure_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    assert "entries" in data
    assert len(data["entries"]) == 1
    assert data.get("pages")
    assert data["pages"][0].get("html_path") is None
    assert data["pages"][0]["pagination"]["next"] is None
    entry = data["entries"][0]
    assert entry["serial"] == 1
    assert entry["title"] == "公告一"
    doc_titles = [doc["title"] for doc in entry["documents"]]
    assert doc_titles.count("公告一") >= 2


def test_main_fetch_page(tmp_path):
    config_path = os.path.join(tmp_path, "pbc_config.json")
    state_path = os.path.join(tmp_path, "state.json")
    config_data = {
        "output_dir": os.path.join(tmp_path, "downloads"),
        "start_url": "http://example.com/list/index.html",
        "state_file": state_path,
        "artifact_dir": os.path.join(tmp_path, "artifacts"),
    }
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config_data, handle)

    original_fetch_html = pbc_monitor.fetch_listing_html
    try:
        pbc_monitor.fetch_listing_html = lambda *a, **k: "<html>content</html>"
        html_path = os.path.join(tmp_path, "page.html")
        pbc_monitor.main(["--config", config_path, "--cache-start-page", html_path])
    finally:
        pbc_monitor.fetch_listing_html = original_fetch_html

    with open(html_path, "r", encoding="utf-8") as handle:
        assert handle.read() == "<html>content</html>"


def test_main_fetch_page_default_filename(tmp_path):
    config_path = os.path.join(tmp_path, "pbc_config.json")
    config_data = {
        "output_dir": os.path.join(tmp_path, "downloads"),
        "start_url": "http://example.com/list/index.html",
        "state_file": os.path.join(tmp_path, "state.json"),
        "artifact_dir": os.path.join(tmp_path, "artifacts"),
    }
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config_data, handle)

    original_fetch_html = pbc_monitor.fetch_listing_html
    cwd = os.getcwd()
    try:
        pbc_monitor.fetch_listing_html = lambda *a, **k: "<html>default</html>"
        os.chdir(tmp_path)
        pbc_monitor.main(["--config", config_path, "--cache-start-page"])
        default_html = os.path.join("artifacts", "pages", "page.html")
        with open(default_html, "r", encoding="utf-8") as handle:
            assert handle.read() == "<html>default</html>"
    finally:
        pbc_monitor.fetch_listing_html = original_fetch_html
        os.chdir(cwd)


def test_main_dump_from_file(tmp_path):
    html_file = os.path.join(tmp_path, "page.html")
    with open(html_file, "w", encoding="utf-8") as handle:
        handle.write(
            """
            <table>
              <tr>
                <td>1</td>
                <td>
                  <div class="gz_tit2">这是备注内容</div>
                  <a href="detail.html" title="中国人民银行公告甲">公告甲…</a>
                </td>
                <td><a href="/files/a.pdf">pdf</a></td>
              </tr>
            </table>
            """
        )

    original_print = builtins.print
    captured = []
    try:
        builtins.print = lambda *args, **kwargs: captured.append(args[0] if args else "")
        pbc_monitor.main(["--preview-page-structure", html_file])
    finally:
        builtins.print = original_print

    assert captured
    data = json.loads(captured[0])
    assert len(data["entries"]) == 1
    assert "pagination" in data
    assert data.get("pages")
    entry_title = data["entries"][0]["title"]
    assert entry_title == "中国人民银行公告甲"
    assert data["entries"][0]["remark"] == "这是备注内容"
    docs = data["entries"][0]["documents"]
    doc_urls = [doc["url"] for doc in docs]
    assert any(url.endswith("a.pdf") for url in doc_urls)
    pdf_docs = [doc for doc in docs if str(doc.get("url", "")).endswith("a.pdf")]
    assert pdf_docs and pdf_docs[0]["title"] == "中国人民银行公告甲"


def test_main_dump_from_file_default(tmp_path):
    pages_dir = os.path.join(tmp_path, "artifacts", "pages")
    os.makedirs(pages_dir, exist_ok=True)
    html_file = os.path.join(pages_dir, "page.html")
    with open(html_file, "w", encoding="utf-8") as handle:
        handle.write("<html><body>test</body></html>")

    original_print = builtins.print
    captured = []
    cwd = os.getcwd()
    try:
        builtins.print = lambda *args, **kwargs: captured.append(args[0] if args else "")
        os.chdir(tmp_path)
        pbc_monitor.main(["--preview-page-structure"])
    finally:
        builtins.print = original_print
        os.chdir(cwd)

    assert captured
    data = json.loads(captured[0])
    assert "entries" in data
    assert data.get("pages")
    assert data["pages"][0]["html_path"].endswith(os.path.join("artifacts", "pages", "page.html"))


def test_extract_file_links_nested_containers_clean_name():
    html = """
    <div class="item">
      <div class="title">中国人民银行公告〔2025〕第9号</div>
      <div class="links">
        <a href="/files/notice2025.docx">下载word版</a>
        <a href="/files/notice2025.pdf">PDF下载</a>
      </div>
    </div>
    """
    soup = _make_soup(html)
    links = pbc_monitor.extract_file_links("http://example.com/list/index.html", soup)
    assert links == [
        (
            "http://example.com/files/notice2025.docx",
            "中国人民银行公告〔2025〕第9号",
        ),
        (
            "http://example.com/files/notice2025.pdf",
            "中国人民银行公告〔2025〕第9号",
        ),
    ]


def test_extract_listing_entries_table_with_serials():
    html = """
    <table>
      <tr>
        <th>序号</th><th>标题</th><th>备注</th><th>下载</th>
      </tr>
      <tr>
        <td>1</td>
        <td><a href="detail1.html">公告甲</a> (2021年9月30日公布)</td>
        <td>自2022年1月1日起施行</td>
        <td>
          <a href="docs/notice1.doc">word版</a>
          <a href="docs/notice1.pdf">pdf版</a>
        </td>
      </tr>
    </table>
    """
    soup = _make_soup(html)
    entries = pbc_monitor.extract_listing_entries(
        "http://example.com/list/index.html", soup
    )
    assert entries == [
        {
            "serial": 1,
            "title": "公告甲",
            "remark": "(2021年9月30日公布) 自2022年1月1日起施行",
            "documents": [
                {
                    "type": "html",
                    "url": "http://example.com/list/detail1.html",
                    "title": "公告甲",
                },
                {
                    "type": "word",
                    "url": "http://example.com/list/docs/notice1.doc",
                    "title": "公告甲",
                },
                {
                    "type": "pdf",
                    "url": "http://example.com/list/docs/notice1.pdf",
                    "title": "公告甲",
                },
            ],
        }
    ]


def test_extract_pagination_links():
    html = """
    <html><body>
      <a href="index.html">1</a>
      <a href="index_1.html">下一页</a>
      <a href="index_3.html">3</a>
      <a href="/zhengwugongkai/4081330/4406346/4406348/index_5.html">尾页</a>
    </body></html>
    """
    soup = _make_soup(html)
    pages = pbc_monitor.extract_pagination_links(
        "http://www.pbc.gov.cn/zhengwugongkai/4081330/4406346/4406348/index.html",
        soup,
        "http://www.pbc.gov.cn/zhengwugongkai/4081330/4406346/4406348/index.html",
    )
    assert "http://www.pbc.gov.cn/zhengwugongkai/4081330/4406346/4406348/index_1.html" in pages
    assert "http://www.pbc.gov.cn/zhengwugongkai/4081330/4406346/4406348/index_3.html" in pages


def test_extract_pagination_links_ignores_detail_links_when_no_container():
    html = """
    <html><body>
      <ul>
        <li><a href="detail1.html">公告甲</a></li>
        <li><a href="detail2.html">公告乙</a></li>
      </ul>
      <div class="pager">
        <a href="index_2.html">下一页</a>
      </div>
    </body></html>
    """
    soup = _make_soup(html)
    pages = pbc_monitor.extract_pagination_links(
        "http://example.com/list/index.html",
        soup,
        "http://example.com/list/index.html",
    )
    assert pages == ["http://example.com/list/index_2.html"]


def test_state_roundtrip():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_path = os.path.join(tmpdir, "pbc_state.json")
        state = pbc_monitor.PBCState()
        entry_a = state.ensure_entry({"serial": 1, "title": "公告A", "remark": ""})
        state.merge_documents(
            entry_a,
            [
                {
                    "url": "http://example.com/a.pdf",
                    "type": "pdf",
                    "title": "公告A",
                }
            ],
        )
        state.mark_downloaded(
            entry_a,
            "http://example.com/a.pdf",
            "公告A",
            "pdf",
            "downloads/a.pdf",
        )
        entry_b = state.ensure_entry({"serial": 2, "title": "公告B", "remark": "备注"})
        state.merge_documents(
            entry_b,
            [
                {
                    "url": "http://example.com/b.pdf",
                    "type": "pdf",
                    "title": "公告B",
                }
            ],
        )
        state.mark_downloaded(
            entry_b,
            "http://example.com/b.pdf",
            "公告B",
            "pdf",
            None,
        )
        pbc_monitor.save_state(state_path, state)
        with open(state_path, "r", encoding="utf-8") as handle:
            stored = json.load(handle)
        assert stored == {
            "entries": [
                {
                    "serial": 1,
                    "title": "公告A",
                    "remark": "",
                    "documents": [
                        {
                            "type": "pdf",
                            "url": "http://example.com/a.pdf",
                            "title": "公告A",
                            "downloaded": True,
                            "local_path": "downloads/a.pdf",
                        }
                    ],
                },
                {
                    "serial": 2,
                    "title": "公告B",
                    "remark": "备注",
                    "documents": [
                        {
                            "type": "pdf",
                            "url": "http://example.com/b.pdf",
                            "title": "公告B",
                            "downloaded": True,
                        }
                    ],
                },
            ]
        }
        loaded = pbc_monitor.load_state(state_path)
        assert loaded.is_downloaded("http://example.com/a.pdf")
        assert loaded.is_downloaded("http://example.com/b.pdf")
        assert loaded.files["http://example.com/a.pdf"]["title"] == "公告A"
        assert loaded.to_jsonable() == stored


def test_ensure_entry_preserves_and_assigns_serials():
    state = pbc_monitor.PBCState()

    existing_entry = {
        "serial": 1,
        "title": "公告一",
        "remark": "",
        "documents": [
            {
                "url": "http://example.com/detail.html",
                "type": "html",
                "title": "详情",
            }
        ],
    }
    existing_id = state.ensure_entry(existing_entry)
    assert state.entries[existing_id]["serial"] == 1

    updated_entry = {
        "serial": 5,
        "title": "公告一更新",
        "remark": "已更新",
        "documents": [
            {
                "url": "http://example.com/detail.html",
                "type": "html",
                "title": "详情最新",
            }
        ],
    }
    state.ensure_entry(updated_entry)
    assert state.entries[existing_id]["serial"] == 1
    assert state.entries[existing_id]["title"] == "公告一更新"
    assert state.entries[existing_id]["remark"] == "已更新"

    new_entry = {
        "serial": 1,
        "title": "公告二",
        "remark": "",
        "documents": [
            {
                "url": "http://example.com/another.html",
                "type": "html",
                "title": "详情二",
            }
        ],
    }
    new_id = state.ensure_entry(new_entry)
    assert state.entries[new_id]["serial"] == 2

    third_entry = {
        "serial": 2,
        "title": "公告三",
        "remark": "",
        "documents": [
            {
                "url": "http://example.com/third.html",
                "type": "html",
                "title": "详情三",
            }
        ],
    }
    third_id = state.ensure_entry(third_entry)
    assert state.entries[third_id]["serial"] == 3


def test_load_state_from_legacy_list():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_path = os.path.join(tmpdir, "state.json")
        legacy = ["http://example.com/a.pdf", "http://example.com/b.pdf"]
        with open(state_path, "w", encoding="utf-8") as handle:
            json.dump(legacy, handle)
        loaded = pbc_monitor.load_state(state_path)
        assert loaded.is_downloaded("http://example.com/a.pdf")
        assert loaded.is_downloaded("http://example.com/b.pdf")
        jsonable = loaded.to_jsonable()
        found_urls = {
            doc.get("url")
            for entry in jsonable["entries"]
            for doc in entry.get("documents", [])
        }
        assert "http://example.com/a.pdf" in found_urls
        assert "http://example.com/b.pdf" in found_urls


def test_fetch_uses_apparent_encoding_for_iso8859():
    class FakeResponse:
        def __init__(self):
            self.encoding = "ISO-8859-1"
            self.apparent_encoding = "gbk"
            self._content = "名称".encode("gbk")

        def raise_for_status(self):
            return None

        @property
        def text(self):
            return self._content.decode(self.encoding, errors="strict")

    class FakeSession:
        def get(self, url, timeout):
            return FakeResponse()

    result = pbc_monitor._fetch(FakeSession(), "http://example.com", 0.0, 0.0, 10.0)
    assert result == "名称"


def test_compute_sleep_seconds_range():
    seconds = [pbc_monitor._compute_sleep_seconds(1, 2) for _ in range(10)]
    for value in seconds:
        assert 3600 <= value <= 7200


def test_collect_new_files_saves_state_on_each_download():
    html = """
    <html><body>
      <a href="file1.pdf">文件一</a>
      <a href="file2.pdf">文件二</a>
    </body></html>
    """

    def fake_iterate(session, start_url, delay, jitter, timeout, page_cache_dir=None, **kwargs):
        yield start_url, _make_soup(html), None

    download_calls = []

    def fake_download_document(session, file_url, output_dir, delay, jitter, timeout, doc_type):
        download_calls.append((file_url, doc_type))
        os.makedirs(output_dir, exist_ok=True)
        if file_url.endswith("file2.pdf"):
            raise RuntimeError("fail second download")
        return os.path.join(output_dir, os.path.basename(file_url))

    save_calls = []
    skip_messages = []
    original_iterate = pbc_monitor.iterate_listing_pages
    original_download = pbc_monitor.download_document
    original_save = pbc_monitor.save_state
    original_print = builtins.print
    try:
        pbc_monitor.iterate_listing_pages = fake_iterate
        pbc_monitor.download_document = fake_download_document

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "state.json")

            def wrapped_save_state(path, state_obj):
                save_calls.append((path, state_obj.to_jsonable()))
                original_save(path, state_obj)

            pbc_monitor.save_state = wrapped_save_state

            def fake_print(*args, **kwargs):
                message = " ".join(str(arg) for arg in args)
                skip_messages.append(message)

            builtins.print = fake_print

            state = pbc_monitor.PBCState()
            downloaded = pbc_monitor.collect_new_files(
                session=None,
                start_url="http://example.com/index.html",
                output_dir=os.path.join(tmpdir, "out"),
                state=state,
                delay=0.0,
                jitter=0.0,
                timeout=10.0,
                state_file=state_path,
                page_cache_dir=None,
            )

            assert downloaded == [os.path.join(tmpdir, "out", "file1.pdf")]
            assert download_calls == [
                ("http://example.com/file1.pdf", "pdf"),
                ("http://example.com/file2.pdf", "pdf"),
            ]
            assert save_calls
            first_call = save_calls[0]
            assert first_call[0] == state_path
            saved_urls = {
                doc["url"]
                for entry in first_call[1]["entries"]
                for doc in entry["documents"]
                if doc.get("downloaded")
            }
            assert saved_urls == {"http://example.com/file1.pdf"}

            pbc_monitor.save_state(state_path, state)
            jsonable = state.to_jsonable()
            assert save_calls[-1][1] == jsonable
            with open(state_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            assert data == jsonable
            assert state.is_downloaded("http://example.com/file1.pdf")

            skip_messages.clear()
            pbc_monitor.collect_new_files(
                session=None,
                start_url="http://example.com/index.html",
                output_dir=os.path.join(tmpdir, "out"),
                state=state,
                delay=0.0,
                jitter=0.0,
                timeout=10.0,
                state_file=state_path,
                page_cache_dir=None,
            )
            assert any("Skipping existing file" in msg for msg in skip_messages)
    finally:
        pbc_monitor.iterate_listing_pages = original_iterate
        pbc_monitor.download_document = original_download
        pbc_monitor.save_state = original_save
        builtins.print = original_print


def test_collect_new_files_updates_missing_name():
    html = """
    <html><body>
      <a href="file1.pdf">文件一</a>
    </body></html>
    """

    def fake_iterate(session, start_url, delay, jitter, timeout, page_cache_dir=None, **kwargs):
        yield start_url, _make_soup(html), None

    original_iterate = pbc_monitor.iterate_listing_pages
    original_save = pbc_monitor.save_state
    original_print = builtins.print
    messages = []

    try:
        pbc_monitor.iterate_listing_pages = fake_iterate
        def fake_save(path, state_obj):
            messages.append((path, state_obj.to_jsonable()))
            return original_save(path, state_obj)

        pbc_monitor.save_state = fake_save

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "state.json")
            state = pbc_monitor.PBCState()
            entry_id = state.ensure_entry({"serial": 1, "title": "", "remark": ""})
            state.merge_documents(
                entry_id,
                [
                    {
                        "url": "http://example.com/file1.pdf",
                        "type": "pdf",
                        "title": "",
                        "downloaded": True,
                    }
                ],
            )
            state.mark_downloaded(
                entry_id,
                "http://example.com/file1.pdf",
                "",
                "pdf",
                None,
            )

            def capture_print(*args, **kwargs):
                messages.append(" ".join(str(a) for a in args))

            builtins.print = capture_print

            pbc_monitor.collect_new_files(
                session=None,
                start_url="http://example.com/index.html",
                output_dir=os.path.join(tmpdir, "out"),
                state=state,
                delay=0.0,
                jitter=0.0,
                timeout=10.0,
                state_file=state_path,
                page_cache_dir=None,
            )

            assert state.files["http://example.com/file1.pdf"]["title"] == "文件一"
            assert any("Updated name for existing file" in msg for msg in messages if isinstance(msg, str))
    finally:
        pbc_monitor.iterate_listing_pages = original_iterate
        pbc_monitor.save_state = original_save
        builtins.print = original_print


def test_dump_structure_default_artifacts(tmp_path):
    config_path = os.path.join(tmp_path, "pbc_config.json")
    sample_html = """
    <table>
      <tr>
        <td>1</td>
        <td><a href="detail1.html">公告一</a></td>
      </tr>
    </table>
    """
    config_data = {
        "output_dir": os.path.join(tmp_path, "downloads"),
        "start_url": "http://example.com/list/index.html",
        "state_file": os.path.join(tmp_path, "state.json"),
    }
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config_data, handle)

    original_iterate = pbc_monitor.iterate_listing_pages
    original_session = getattr(pbc_monitor.requests, "Session", None)
    cwd = os.getcwd()
    try:
        def fake_iterate(session, start_url, delay, jitter, timeout, page_cache_dir=None, **kwargs):
            if page_cache_dir:
                os.makedirs(page_cache_dir, exist_ok=True)
                html_path = os.path.join(page_cache_dir, "page_001_index.html")
            else:
                html_path = os.path.join(tmp_path, "page_001_index.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(sample_html)
            yield start_url, _make_soup(sample_html), html_path
        pbc_monitor.iterate_listing_pages = fake_iterate
        pbc_monitor.requests.Session = lambda: types.SimpleNamespace(headers={}, close=lambda: None)
        os.chdir(tmp_path)
        pbc_monitor.main(["--config", config_path, "--build-page-structure"])
    finally:
        pbc_monitor.iterate_listing_pages = original_iterate
        if original_session is not None:
            pbc_monitor.requests.Session = original_session
        elif hasattr(pbc_monitor.requests, "Session"):
            delattr(pbc_monitor.requests, "Session")
        os.chdir(cwd)

    structure_path = os.path.join(
        tmp_path, "artifacts", "pages", "default_structure.json"
    )
    assert os.path.exists(structure_path)
    with open(structure_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    assert data["pages"]
    first_page_path = data["pages"][0]["html_path"]
    assert first_page_path.endswith("page_001_index.html")
    assert not os.path.isabs(first_page_path)
    assert first_page_path == os.path.join("pages", "page_001_index.html")


def test_snapshot_listing_overwrites_cached_pages(tmp_path):
    start_url = "http://example.com/list/index.html"
    page_cache_dir = os.path.join(tmp_path, "pages")
    counter = {"value": 0}

    def fake_fetch(session, url, delay, jitter, timeout):
        counter["value"] += 1
        return f"<html><body>version {counter['value']}</body></html>"

    original_fetch = pbc_monitor._fetch
    original_session = getattr(pbc_monitor.requests, "Session", None)
    try:
        pbc_monitor._fetch = fake_fetch
        pbc_monitor.requests.Session = lambda: types.SimpleNamespace(headers={})

        snapshot1 = pbc_monitor.snapshot_listing(
            start_url,
            delay=0.0,
            jitter=0.0,
            timeout=5.0,
            page_cache_dir=page_cache_dir,
        )
        page_path = snapshot1["pages"][0]["html_path"]
        with open(page_path, "r", encoding="utf-8") as handle:
            assert "version 1" in handle.read()

        snapshot2 = pbc_monitor.snapshot_listing(
            start_url,
            delay=0.0,
            jitter=0.0,
            timeout=5.0,
            page_cache_dir=page_cache_dir,
        )
        assert snapshot2["pages"][0]["html_path"] == page_path
        with open(page_path, "r", encoding="utf-8") as handle:
            assert "version 2" in handle.read()

        html_files = [name for name in os.listdir(page_cache_dir) if name.endswith(".html")]
        assert len(html_files) == 1
    finally:
        pbc_monitor._fetch = original_fetch
        if original_session is not None:
            pbc_monitor.requests.Session = original_session
        elif hasattr(pbc_monitor.requests, "Session"):
            delattr(pbc_monitor.requests, "Session")


def test_download_from_structure_downloads_files(tmp_path):
    structure_path = os.path.join(tmp_path, "structure.json")
    output_dir = os.path.join(tmp_path, "downloads")
    state_path = os.path.join(tmp_path, "state.json")
    structure_data = {
        "entries": [
            {
                "serial": 1,
                "title": "测试公告",
                "remark": "",
                "documents": [
                    {
                        "url": "http://example.com/detail.html",
                        "type": "html",
                        "title": "公告详情",
                    },
                    {
                        "url": "http://example.com/file1.pdf",
                        "type": "pdf",
                        "title": "附件一",
                    },
                ],
            }
        ]
    }
    with open(structure_path, "w", encoding="utf-8") as handle:
        json.dump(structure_data, handle)

    downloaded_targets = []
    download_calls = []
    original_download_document = pbc_monitor.download_document
    try:
        def fake_download_document(session, file_url, out_dir, delay, jitter, timeout, doc_type):
            download_calls.append((file_url, doc_type))
            os.makedirs(out_dir, exist_ok=True)
            name = os.path.basename(file_url)
            if not name:
                name = pbc_monitor.safe_filename(file_url)
            if (doc_type or "").lower() == "html" and not name.lower().endswith((".html", ".htm")):
                name = f"{name}.html"
            target = os.path.join(out_dir, name)
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(f"dummy for {doc_type}")
            downloaded_targets.append(target)
            return target

        pbc_monitor.download_document = fake_download_document
        result = pbc_monitor.download_from_structure(
            structure_path,
            output_dir,
            state_path,
            delay=0.1,
            jitter=0.0,
            timeout=5.0,
        )
    finally:
        pbc_monitor.download_document = original_download_document

    assert result == downloaded_targets
    assert download_calls == [
        ("http://example.com/detail.html", "html"),
        ("http://example.com/file1.pdf", "pdf"),
    ]
    assert len(downloaded_targets) == 2
    with open(state_path, "r", encoding="utf-8") as handle:
        state_data = json.load(handle)
    entry = state_data["entries"][0]
    documents = {doc["url"]: doc for doc in entry["documents"]}
    html_doc = documents["http://example.com/detail.html"]
    pdf_doc = documents["http://example.com/file1.pdf"]
    assert html_doc["downloaded"] is True
    assert html_doc["local_path"].endswith("detail.html")
    assert pdf_doc["downloaded"] is True
    assert pdf_doc["local_path"].endswith("file1.pdf")


def test_download_from_structure_ignores_javascript_links(tmp_path):
    structure_path = os.path.join(tmp_path, "structure.json")
    output_dir = os.path.join(tmp_path, "downloads")
    state_path = os.path.join(tmp_path, "state.json")
    javascript_url = "javascript:window.open('','_parent','');window.close();"
    pdf_url = "http://example.com/file2.pdf"
    structure_data = {
        "entries": [
            {
                "serial": 1,
                "title": "测试公告",
                "remark": "",
                "documents": [
                    {
                        "url": javascript_url,
                        "type": "html",
                        "title": "关闭窗口",
                    },
                    {
                        "url": pdf_url,
                        "type": "pdf",
                        "title": "附件二",
                    },
                ],
            }
        ]
    }
    with open(structure_path, "w", encoding="utf-8") as handle:
        json.dump(structure_data, handle)

    download_calls = []

    def fake_download_document(session, file_url, out_dir, delay, jitter, timeout, doc_type):
        download_calls.append((file_url, doc_type))
        os.makedirs(out_dir, exist_ok=True)
        name = os.path.basename(file_url)
        if not name:
            name = pbc_monitor.safe_filename(file_url)
        target = os.path.join(out_dir, name)
        with open(target, "w", encoding="utf-8") as fh:
            fh.write("dummy")
        return target

    original_download_document = pbc_monitor.download_document
    try:
        pbc_monitor.download_document = fake_download_document
        result = pbc_monitor.download_from_structure(
            structure_path,
            output_dir,
            state_path,
            delay=0.1,
            jitter=0.0,
            timeout=5.0,
        )
    finally:
        pbc_monitor.download_document = original_download_document

    assert download_calls == [(pdf_url, "pdf")]
    assert len(result) == 1
    with open(state_path, "r", encoding="utf-8") as handle:
        state_data = json.load(handle)
    documents = {doc["url"]: doc for doc in state_data["entries"][0]["documents"]}
    assert documents[pdf_url]["downloaded"] is True
    assert documents[javascript_url].get("downloaded") is not True


def test_download_document_html_uses_path_segments(tmp_path):
    original_fetch = pbc_monitor._fetch
    try:
        pbc_monitor._fetch = lambda session, url, delay, jitter, timeout: "<html>content</html>"
        path = pbc_monitor.download_document(
            session=None,
            file_url="http://example.com/dir/sub/index.html",
            output_dir=os.path.join(tmp_path, "out"),
            delay=0.0,
            jitter=0.0,
            timeout=5.0,
            doc_type="html",
        )
    finally:
        pbc_monitor._fetch = original_fetch

    assert os.path.basename(path) == "dir_sub_index.html"
    with open(path, "r", encoding="utf-8") as handle:
        assert "content" in handle.read()


def test_collect_new_files_downloads_html_documents(tmp_path):
    entries = [
        {
            "serial": 1,
            "title": "公告A",
            "remark": "",
            "documents": [
                {
                    "url": "http://example.com/detail.html",
                    "type": "html",
                    "title": "详情",
                },
                {
                    "url": "http://example.com/file.pdf",
                    "type": "pdf",
                    "title": "附件",
                },
            ],
        }
    ]

    def fake_iterate(session, start_url, delay, jitter, timeout, page_cache_dir=None, **kwargs):
        yield start_url, _make_soup("<html></html>"), None

    def fake_extract_listing_entries(page_url, soup):
        return entries

    download_calls = []

    def fake_download_document(session, file_url, output_dir, delay, jitter, timeout, doc_type):
        os.makedirs(output_dir, exist_ok=True)
        name = os.path.basename(file_url)
        if not name:
            name = pbc_monitor.safe_filename(file_url)
        if (doc_type or "").lower() == "html" and not name.lower().endswith((".html", ".htm")):
            name = f"{name}.html"
        target = os.path.join(output_dir, name)
        with open(target, "w", encoding="utf-8") as handle:
            handle.write(doc_type or "")
        download_calls.append((file_url, doc_type, target))
        return target

    original_iterate = pbc_monitor.iterate_listing_pages
    original_extract = pbc_monitor.extract_listing_entries
    original_download = pbc_monitor.download_document
    try:
        pbc_monitor.iterate_listing_pages = fake_iterate
        pbc_monitor.extract_listing_entries = fake_extract_listing_entries
        pbc_monitor.download_document = fake_download_document

        state = pbc_monitor.PBCState()
        output_dir = os.path.join(tmp_path, "out")
        downloaded = pbc_monitor.collect_new_files(
            session=None,
            start_url="http://example.com/index.html",
            output_dir=output_dir,
            state=state,
            delay=0.0,
            jitter=0.0,
            timeout=10.0,
            state_file=None,
            page_cache_dir=None,
        )

        assert len(downloaded) == 2
        assert download_calls == [
            ("http://example.com/detail.html", "html", os.path.join(output_dir, "detail.html")),
            ("http://example.com/file.pdf", "pdf", os.path.join(output_dir, "file.pdf")),
        ]
        assert state.is_downloaded("http://example.com/detail.html")
        html_doc = state.files["http://example.com/detail.html"]
        assert html_doc["local_path"].endswith("detail.html")
    finally:
        pbc_monitor.iterate_listing_pages = original_iterate
        pbc_monitor.extract_listing_entries = original_extract
        pbc_monitor.download_document = original_download


def test_collect_new_files_respects_allowed_types(tmp_path):
    entries = [
        {
            "serial": 1,
            "title": "公告A",
            "remark": "",
            "documents": [
                {
                    "url": "http://example.com/detail.html",
                    "type": "html",
                    "title": "详情",
                },
                {
                    "url": "http://example.com/file.pdf",
                    "type": "pdf",
                    "title": "附件",
                },
            ],
        }
    ]

    def fake_iterate(session, start_url, delay, jitter, timeout, page_cache_dir=None, **kwargs):
        yield start_url, _make_soup("<html></html>"), None

    def fake_extract_listing_entries(page_url, soup):
        return entries

    download_calls = []

    def fake_download_document(session, file_url, output_dir, delay, jitter, timeout, doc_type):
        os.makedirs(output_dir, exist_ok=True)
        target = os.path.join(output_dir, os.path.basename(file_url))
        with open(target, "w", encoding="utf-8") as handle:
            handle.write(doc_type or "")
        download_calls.append((file_url, doc_type))
        return target

    original_iterate = pbc_monitor.iterate_listing_pages
    original_extract = pbc_monitor.extract_listing_entries
    original_download = pbc_monitor.download_document
    try:
        pbc_monitor.iterate_listing_pages = fake_iterate
        pbc_monitor.extract_listing_entries = fake_extract_listing_entries
        pbc_monitor.download_document = fake_download_document

        state = pbc_monitor.PBCState()
        output_dir = os.path.join(tmp_path, "out")
        downloaded = pbc_monitor.collect_new_files(
            session=None,
            start_url="http://example.com/index.html",
            output_dir=output_dir,
            state=state,
            delay=0.0,
            jitter=0.0,
            timeout=10.0,
            state_file=None,
            page_cache_dir=None,
            allowed_types={"html"},
        )

        assert len(downloaded) == 1
        assert download_calls == [("http://example.com/detail.html", "html")]
        assert state.is_downloaded("http://example.com/detail.html")
        assert not state.is_downloaded("http://example.com/file.pdf")
    finally:
        pbc_monitor.iterate_listing_pages = original_iterate
        pbc_monitor.extract_listing_entries = original_extract
        pbc_monitor.download_document = original_download


def test_download_from_structure_skips_existing(tmp_path):
    structure_path = os.path.join(tmp_path, "structure.json")
    output_dir = os.path.join(tmp_path, "downloads")
    state_path = os.path.join(tmp_path, "state.json")
    structure_data = {
        "entries": [
            {
                "serial": 2,
                "title": "公告二",
                "remark": "",
                "documents": [
                    {
                        "url": "http://example.com/file2.pdf",
                        "type": "pdf",
                        "title": "新附件名",
                    }
                ],
            }
        ]
    }
    with open(structure_path, "w", encoding="utf-8") as handle:
        json.dump(structure_data, handle)

    existing_state = {
        "entries": [
            {
                "serial": 2,
                "title": "公告二",
                "remark": "",
                "documents": [
                    {
                        "url": "http://example.com/file2.pdf",
                        "type": "pdf",
                        "title": "旧附件名",
                        "downloaded": True,
                        "local_path": "/tmp/file2.pdf",
                    }
                ],
            }
        ]
    }
    with open(state_path, "w", encoding="utf-8") as handle:
        json.dump(existing_state, handle)

    original_download_document = pbc_monitor.download_document
    try:
        def fail_download_document(*_args, **_kwargs):
            raise AssertionError("download_document should not be called")

        pbc_monitor.download_document = fail_download_document
        result = pbc_monitor.download_from_structure(
            structure_path,
            output_dir,
            state_path,
            delay=0.1,
            jitter=0.0,
            timeout=5.0,
        )
    finally:
        pbc_monitor.download_document = original_download_document

    assert result == []
    with open(state_path, "r", encoding="utf-8") as handle:
        state_data = json.load(handle)
    document = state_data["entries"][0]["documents"][0]
    assert document["title"] == "新附件名"


def test_collect_new_files_verify_local_recovers_missing(tmp_path):
    document_url = "http://example.com/file.pdf"
    entry_data = [{
        "serial": 1,
        "title": "公告A",
        "remark": "",
        "documents": [
            {"url": document_url, "type": "pdf", "title": "附件"}
        ],
    }]

    def fake_iterate(session, start_url, delay, jitter, timeout, page_cache_dir=None, **kwargs):
        yield start_url, _make_soup("<html></html>"), None

    def fake_extract_listing_entries(page_url, soup):
        return entry_data

    downloads = []

    def fake_download_document(session, file_url, output_dir, delay, jitter, timeout, doc_type):
        os.makedirs(output_dir, exist_ok=True)
        target = os.path.join(output_dir, "file.pdf")
        with open(target, "w", encoding="utf-8") as handle:
            handle.write("re-downloaded")
        downloads.append(file_url)
        return target

    original_iterate = pbc_monitor.iterate_listing_pages
    original_extract = pbc_monitor.extract_listing_entries
    original_download = pbc_monitor.download_document
    try:
        pbc_monitor.iterate_listing_pages = fake_iterate
        pbc_monitor.extract_listing_entries = fake_extract_listing_entries
        pbc_monitor.download_document = fake_download_document

        state = pbc_monitor.PBCState()
        entry_id = state.ensure_entry(entry_data[0])
        state.merge_documents(entry_id, entry_data[0]["documents"])
        missing_path = os.path.join(tmp_path, "missing.pdf")
        state.mark_downloaded(entry_id, document_url, "附件", "pdf", missing_path)

        state_file = os.path.join(tmp_path, "state.json")
        output_dir = os.path.join(tmp_path, "out")

        # verify_local disabled -> treated as already downloaded
        result = pbc_monitor.collect_new_files(
            session=None,
            start_url="http://example.com/index.html",
            output_dir=output_dir,
            state=state,
            delay=0.0,
            jitter=0.0,
            timeout=5.0,
            state_file=state_file,
            page_cache_dir=None,
            verify_local=False,
        )
        assert result == []
        assert downloads == []
        assert state.is_downloaded(document_url)

        # enable verify_local -> should re-download missing file
        result = pbc_monitor.collect_new_files(
            session=None,
            start_url="http://example.com/index.html",
            output_dir=output_dir,
            state=state,
            delay=0.0,
            jitter=0.0,
            timeout=5.0,
            state_file=state_file,
            page_cache_dir=None,
            verify_local=True,
        )
        assert downloads == [document_url]
        assert len(result) == 1
        updated_record = state.files[document_url]
        assert updated_record.get("downloaded") is True
        local_path = updated_record.get("local_path")
        assert local_path and os.path.exists(local_path)
        expected_name = pbc_monitor._structured_filename(document_url, "pdf")
        assert Path(local_path).name == expected_name
    finally:
        pbc_monitor.iterate_listing_pages = original_iterate
        pbc_monitor.extract_listing_entries = original_extract
        pbc_monitor.download_document = original_download


def test_main_download_from_structure(tmp_path):
    artifact_dir = os.path.join(tmp_path, "artifacts")
    structure_dir = os.path.join(artifact_dir, "pages")
    os.makedirs(structure_dir, exist_ok=True)
    structure_path = os.path.join(structure_dir, "default_structure.json")
    with open(structure_path, "w", encoding="utf-8") as handle:
        json.dump({"entries": []}, handle)

    output_dir = os.path.join(tmp_path, "downloads")
    config_path = os.path.join(tmp_path, "config.json")
    config_data = {
        "output_dir": output_dir,
        "artifact_dir": artifact_dir,
    }
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config_data, handle)

    captured = {}
    original_download_from_structure = pbc_monitor.download_from_structure
    try:
        def fake_download_from_structure(
            structure_path_arg,
            out_dir,
            state_file,
            delay,
            jitter,
            timeout,
            verify_local=False,
            **kwargs,
        ):
            captured.update(
                {
                    "structure_path": structure_path_arg,
                    "output_dir": out_dir,
                    "state_file": state_file,
                    "delay": delay,
                    "jitter": jitter,
                    "timeout": timeout,
                        "allowed_types": kwargs.get("allowed_types"),
                        "verify_local": verify_local,
                }
            )
            return []

        pbc_monitor.download_from_structure = fake_download_from_structure
        pbc_monitor.main(["--config", config_path, "--download-from-structure"])
    finally:
        pbc_monitor.download_from_structure = original_download_from_structure

    assert captured["structure_path"] == structure_path
    assert captured["output_dir"] == output_dir
    expected_state = os.path.join(artifact_dir, "downloads", "default_state.json")
    assert captured["state_file"] == expected_state
    assert captured["delay"] == 3.0
    assert captured["jitter"] == 2.0
    assert captured["timeout"] == 30.0
    assert captured["allowed_types"] is None
    assert captured["verify_local"] is False


def test_main_download_from_structure_verify_local(tmp_path):
    artifact_dir = os.path.join(tmp_path, "artifacts")
    structure_dir = os.path.join(artifact_dir, "pages")
    os.makedirs(structure_dir, exist_ok=True)
    structure_path = os.path.join(structure_dir, "default_structure.json")
    with open(structure_path, "w", encoding="utf-8") as handle:
        json.dump({"entries": []}, handle)

    output_dir = os.path.join(tmp_path, "downloads")
    config_path = os.path.join(tmp_path, "config.json")
    config_data = {
        "output_dir": output_dir,
        "artifact_dir": artifact_dir,
        "verify_local": True,
    }
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config_data, handle)

    captured = {}
    original_download_from_structure = pbc_monitor.download_from_structure
    try:
        def fake_download_from_structure(
            structure_path_arg,
            out_dir,
            state_file,
            delay,
            jitter,
            timeout,
            verify_local=False,
            **kwargs,
        ):
            captured.update(
                {
                    "structure_path": structure_path_arg,
                    "output_dir": out_dir,
                    "state_file": state_file,
                    "delay": delay,
                    "jitter": jitter,
                    "timeout": timeout,
                        "allowed_types": kwargs.get("allowed_types"),
                        "verify_local": verify_local,
                }
            )
            return []

        pbc_monitor.download_from_structure = fake_download_from_structure
        pbc_monitor.main(["--config", config_path, "--download-from-structure"])
    finally:
        pbc_monitor.download_from_structure = original_download_from_structure

    assert captured["structure_path"] == structure_path
    assert captured["allowed_types"] is None
    assert captured["verify_local"] is True


def test_run_once_entry_history(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()

    args = types.SimpleNamespace(
        run_once=True,
        start_url=None,
        output_dir=None,
        state_file=None,
        build_structure=None,
        download_from_structure=None,
        cache_start_page=None,
        preview_page=None,
        delay=0.0,
        jitter=0.0,
        timeout=0.0,
        min_hours=0.0,
        max_hours=0.0,
        refresh_pages=False,
        use_cached_pages=False,
        no_use_cached_pages=False,
        cache_listing=False,
    )

    task = TaskSpec(
        name="Demo Task",
        start_url="http://example.com/list",
        output_dir="",
        state_file=None,
        structure_file=None,
        parser_spec=None,
        verify_local=False,
        raw_config={},
        from_task_list=False,
    )

    snapshots = [
        [
            {"serial": 1, "title": "公告A", "remark": "备注A", "documents": []},
            {"serial": 2, "title": "公告B", "remark": "备注B", "documents": []},
        ],
        [
            {"serial": 1, "title": "公告A", "remark": "备注A", "documents": []},
            {"serial": 2, "title": "公告B", "remark": "备注B", "documents": []},
            {"serial": 3, "title": "公告C", "remark": "备注C", "documents": []},
        ],
    ]
    snapshots_iter = iter(snapshots)
    last_snapshot = snapshots[-1]

    def fake_monitor_once(
        start_url,
        out_dir,
        state_file,
        delay,
        jitter,
        timeout,
        page_cache_dir,
        verify_local=False,
        **kwargs,
    ):
        snapshot = next(snapshots_iter, last_snapshot)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        if state_file:
            os.makedirs(os.path.dirname(state_file), exist_ok=True)
            with open(state_file, "w", encoding="utf-8") as handle:
                json.dump({"entries": snapshot}, handle, ensure_ascii=False, indent=2)
        return []

    monkeypatch.setattr(runner_module.core, "monitor_once", fake_monitor_once)

    config = {}

    runner_module._run_task(task, args, config, str(artifact_dir))

    slug = slugify_name(task.name)
    history_path = artifact_dir / "pages" / f"{slug}_history.json"
    assert history_path.exists()

    with open(history_path, "r", encoding="utf-8") as handle:
        history = json.load(handle)

    assert len(history) == 1
    first_record = history[0]
    assert first_record["entries_total"] == 2
    assert first_record["entries_diff"] == 2
    assert first_record["removed_entries"] == []
    first_entry_ids = first_record["entry_ids"]
    assert len(first_entry_ids) == 2
    new_entries_map = {item["entry_id"]: item for item in first_record["added_entries"]}
    first_entries_map = {item["entry_id"]: item for item in first_record["entries"]}
    assert set(first_entry_ids) == set(new_entries_map.keys()) == set(first_entries_map.keys())
    assert {item["serial"] for item in first_entries_map.values()} == {1, 2}
    assert {item["title"] for item in first_entries_map.values()} == {"公告A", "公告B"}
    assert {item["remark"] for item in first_entries_map.values()} == {"备注A", "备注B"}
    assert new_entries_map == first_entries_map

    runner_module._run_task(task, args, config, str(artifact_dir))

    with open(history_path, "r", encoding="utf-8") as handle:
        history = json.load(handle)

    assert len(history) == 2
    second_record = history[-1]
    assert second_record["entries_total"] == 3
    assert second_record["entries_diff"] == 1
    assert second_record["removed_entries"] == []
    second_entry_ids = second_record["entry_ids"]
    assert len(second_entry_ids) == 3
    second_entries_map = {item["entry_id"]: item for item in second_record["entries"]}
    second_new_map = {item["entry_id"]: item for item in second_record["added_entries"]}
    new_only_ids = set(second_entry_ids) - set(first_entry_ids)
    assert len(new_only_ids) == 1
    new_entry_id = next(iter(new_only_ids))
    assert second_new_map == {new_entry_id: second_entries_map[new_entry_id]}
    assert second_entries_map[new_entry_id] == {
        "entry_id": new_entry_id,
        "serial": 3,
        "title": "公告C",
        "remark": "备注C",
    }

    runner_module._run_task(task, args, config, str(artifact_dir))

    with open(history_path, "r", encoding="utf-8") as handle:
        history = json.load(handle)

    assert len(history) == 2
