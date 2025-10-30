import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from bs4 import BeautifulSoup

from pbc_regulations.crawler import parser_tiaofasi


BASE_URL = "http://www.pbc.gov.cn/tiaofasi/144941/144951/index.html"


def _make_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def test_extract_listing_entries_card_layout():
    html = """
    <div class="list_box">
      <div class="list_item">
        <div class="info">
          <a href="2024/11/05/notice/index.html" title="关于公开征求意见的通知">关于公开征求意见的通知</a>
          <div class="meta">
            <span class="date">2024-11-05</span>
            <a href="/tiaofasi/144941/144951/2024/11/notice.pdf">附件下载</a>
          </div>
        </div>
      </div>
      <div class="list_item">
        <div class="info">
          <a href="2024/11/03/another/index.html">金融机构管理要求</a>
          <span class="time">2024年11月3日</span>
        </div>
      </div>
    </div>
    """
    soup = _make_soup(html)
    entries = parser_tiaofasi.extract_listing_entries(BASE_URL, soup)
    assert len(entries) == 2

    first = entries[0]
    assert first["title"] == "关于公开征求意见的通知"
    assert first["remark"] == "2024-11-05"
    assert first["documents"][0]["url"].endswith("2024/11/05/notice/index.html")
    assert first["documents"][0]["type"] == "html"
    assert any(doc["url"].endswith("notice.pdf") for doc in first["documents"])

    second = entries[1]
    assert second["title"] == "金融机构管理要求"
    assert second["remark"] == "2024年11月3日"


def test_extract_file_links_from_tiaofasi_entries():
    html = """
    <div class="list_box">
      <div class="list_item">
        <div class="info">
          <a href="2024/10/01/item/index.html">通知</a>
          <div class="attachments">
            <a href="/tiaofasi/144941/144951/files/a.docx">附件一</a>
            <a href="/tiaofasi/144941/144951/files/b.pdf">附件二</a>
          </div>
        </div>
      </div>
    </div>
    """
    soup = _make_soup(html)
    links = parser_tiaofasi.extract_file_links(BASE_URL, soup)
    assert sorted(url for url, _ in links) == [
        "http://www.pbc.gov.cn/tiaofasi/144941/144951/files/a.docx",
        "http://www.pbc.gov.cn/tiaofasi/144941/144951/files/b.pdf",
    ]


def test_extract_listing_entries_direct_document_links():
    html = """
    <div class="list_box">
      <div class="list_item">
        <div class="info">
          <a href="/tiaofasi/resource/cms/2018/04/law.doc" title="中华人民共和国某法">中华人民共和国某法</a>
        </div>
      </div>
    </div>
    """
    soup = _make_soup(html)
    entries = parser_tiaofasi.extract_listing_entries(BASE_URL, soup)

    assert len(entries) == 1
    entry = entries[0]
    assert entry["title"] == "中华人民共和国某法"
    assert entry["documents"][0]["type"] == "word"
    assert entry["documents"][0]["url"] == (
        "http://www.pbc.gov.cn/tiaofasi/resource/cms/2018/04/law.doc"
    )

    links = parser_tiaofasi.extract_file_links(BASE_URL, soup)
    assert links == [
        ("http://www.pbc.gov.cn/tiaofasi/resource/cms/2018/04/law.doc", "中华人民共和国某法")
    ]
