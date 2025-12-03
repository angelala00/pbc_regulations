from pathlib import Path

from pbc_regulations.extractor import text_pipeline


def test_build_candidates_respects_preferred(tmp_path: Path) -> None:
    first = tmp_path / "a.pdf"
    first.touch()
    preferred = tmp_path / "b.pdf"
    preferred.touch()

    entry = {
        "title": "test",
        "documents": [
            {"path": str(first), "type": "pdf"},
            {"path": str(preferred), "type": "pdf", "preferred": True},
        ],
    }

    candidates = text_pipeline._build_candidates(entry, tmp_path)
    assert len(candidates) == 2
    assert candidates[0].path == preferred
    assert candidates[1].path == first
