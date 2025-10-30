from __future__ import annotations

import json
import os
from typing import Callable, Dict, List, Optional

from pbc_regulations.utils.naming import safe_filename
from pbc_regulations.utils.paths import (
    absolutize_artifact_path,
    infer_artifact_dir,
    relativize_artifact_path,
)

ClassifierFn = Callable[[str], str]


class PBCState:
    def __init__(self) -> None:
        self.entries: Dict[str, Dict[str, object]] = {}
        self.files: Dict[str, Dict[str, object]] = {}

    def _entry_id(self, entry: Dict[str, object]) -> str:
        documents = entry.get("documents") or []
        if isinstance(documents, list):
            for document in documents:
                if not isinstance(document, dict):
                    continue
                url_value = document.get("url")
                doc_type = document.get("type")
                if isinstance(url_value, str) and doc_type == "html":
                    return url_value
            for document in documents:
                if not isinstance(document, dict):
                    continue
                url_value = document.get("url")
                if isinstance(url_value, str) and url_value:
                    return url_value
        title = entry.get("title")
        remark = entry.get("remark")
        if isinstance(title, str) and title:
            key = f"title::{title}"
            if isinstance(remark, str) and remark:
                key = f"{key}::{remark}"
            return key
        serial = entry.get("serial")
        if isinstance(serial, int):
            return f"serial::{serial}"
        serialized = json.dumps(entry, ensure_ascii=False, sort_keys=True)
        return safe_filename(serialized)

    def _next_serial(self) -> int:
        highest = 0
        for candidate in self.entries.values():
            if not isinstance(candidate, dict):
                continue
            value = candidate.get("serial")
            if isinstance(value, int) and value > highest:
                highest = value
        return highest + 1

    def ensure_entry(self, entry: Dict[str, object]) -> str:
        entry_id: Optional[str] = None
        documents = entry.get("documents")
        if isinstance(documents, list):
            for document in documents:
                if not isinstance(document, dict):
                    continue
                url_value = document.get("url")
                if not isinstance(url_value, str) or not url_value:
                    continue
                file_record = self.files.get(url_value)
                if isinstance(file_record, dict):
                    existing_id = file_record.get("entry_id")
                    if isinstance(existing_id, str) and existing_id in self.entries:
                        entry_id = existing_id
                        break
                for existing_id, existing_entry in self.entries.items():
                    documents_list = existing_entry.get("documents", [])
                    if not isinstance(documents_list, list):
                        continue
                    for existing_doc in documents_list:
                        if (
                            isinstance(existing_doc, dict)
                            and existing_doc.get("url") == url_value
                        ):
                            entry_id = existing_id
                            break
                    if entry_id is not None:
                        break
                if entry_id is not None:
                    break
        if entry_id is None:
            entry_id = self._entry_id(entry)
        existing = self.entries.get(entry_id)
        serial = entry.get("serial")
        title = entry.get("title")
        remark = entry.get("remark")

        def serial_in_use(value: int, exclude: Optional[Dict[str, object]] = None) -> bool:
            for candidate in self.entries.values():
                if not isinstance(candidate, dict):
                    continue
                if exclude is not None and candidate is exclude:
                    continue
                if candidate.get("serial") == value:
                    return True
            return False

        if isinstance(existing, dict):
            if isinstance(title, str):
                existing["title"] = title
            if isinstance(remark, str):
                existing["remark"] = remark
            if isinstance(serial, int):
                current_serial = existing.get("serial")
                if not isinstance(current_serial, int):
                    candidate = serial if serial > 0 else None
                    if isinstance(candidate, int) and serial_in_use(candidate, exclude=existing):
                        candidate = None
                    if not isinstance(candidate, int):
                        candidate = self._next_serial()
                    existing["serial"] = candidate
            return entry_id

        assigned_serial: Optional[int] = None
        if isinstance(serial, int) and serial > 0 and not serial_in_use(serial):
            assigned_serial = serial
        if not isinstance(assigned_serial, int):
            assigned_serial = self._next_serial()

        self.entries[entry_id] = {
            "serial": assigned_serial,
            "title": title if isinstance(title, str) else "",
            "remark": remark if isinstance(remark, str) else "",
            "documents": [],
        }
        return entry_id

    def merge_documents(self, entry_id: str, documents: List[Dict[str, object]]) -> None:
        entry = self.entries.setdefault(entry_id, {"documents": []})
        existing_docs: Dict[str, Dict[str, object]] = {}
        for item in entry.get("documents", []):
            if isinstance(item, dict):
                url_value = item.get("url")
                if isinstance(url_value, str):
                    existing_docs[url_value] = item
        for document in documents:
            if not isinstance(document, dict):
                continue
            url_value = document.get("url")
            if not isinstance(url_value, str) or not url_value:
                continue
            doc_type = document.get("type")
            title = document.get("title")
            downloaded = document.get("downloaded")
            local_path = document.get("local_path")
            existing = existing_docs.get(url_value)
            if existing is None:
                entry.setdefault("documents", []).append(
                    {
                        "url": url_value,
                        "type": doc_type,
                        "title": title if isinstance(title, str) else "",
                        "downloaded": bool(downloaded),
                        "local_path": local_path if isinstance(local_path, str) else None,
                    }
                )
                existing_docs[url_value] = entry["documents"][-1]
            else:
                if isinstance(doc_type, str):
                    existing["type"] = doc_type
                if isinstance(title, str) and title:
                    existing["title"] = title
                if downloaded:
                    existing["downloaded"] = True
                if isinstance(local_path, str) and local_path:
                    existing["local_path"] = local_path
            self.files.setdefault(url_value, {})
            file_record = self.files[url_value]
            if isinstance(file_record, dict):
                file_record["entry_id"] = entry_id
                if isinstance(title, str) and title:
                    file_record["title"] = title
                if isinstance(doc_type, str) and doc_type:
                    file_record["type"] = doc_type
                if downloaded:
                    file_record["downloaded"] = True
                if isinstance(local_path, str) and local_path:
                    file_record["local_path"] = local_path

    def mark_downloaded(
        self,
        entry_id: str,
        url_value: str,
        title: str,
        doc_type: Optional[str],
        local_path: Optional[str],
    ) -> None:
        file_record = self.files.setdefault(url_value, {})
        file_record.update(
            {
                "entry_id": entry_id,
                "title": title,
                "type": doc_type,
                "downloaded": True,
                "local_path": local_path,
            }
        )
        entry = self.entries.setdefault(entry_id, {"documents": []})
        if not isinstance(entry.get("documents"), list):
            entry["documents"] = []
        documents = entry["documents"]
        for doc in documents:
            if isinstance(doc, dict) and doc.get("url") == url_value:
                doc.update(
                    {
                        "title": title,
                        "type": doc_type,
                        "downloaded": True,
                        "local_path": local_path,
                    }
                )
                break
        else:
            new_doc = {
                "url": url_value,
                "title": title,
                "type": doc_type,
                "downloaded": True,
            }
            if local_path:
                new_doc["local_path"] = local_path
            entry.setdefault("documents", []).append(new_doc)

    def clear_downloaded(self, url_value: str) -> None:
        file_record = self.files.get(url_value)
        if file_record:
            file_record["downloaded"] = False
            file_record.pop("local_path", None)
        for entry in self.entries.values():
            documents = entry.get("documents", [])
            if not isinstance(documents, list):
                continue
            for document in documents:
                if not isinstance(document, dict):
                    continue
                if document.get("url") == url_value:
                    document.pop("local_path", None)
                    if "downloaded" in document:
                        document.pop("downloaded", None)

    def update_document_title(self, url_value: str, title: str) -> None:
        if not title:
            return
        file_record = self.files.get(url_value)
        if file_record:
            file_record["title"] = title
        for entry in self.entries.values():
            for document in entry.get("documents", []):
                if isinstance(document, dict) and document.get("url") == url_value:
                    document["title"] = title

    def to_jsonable(
        self, *, artifact_dir: Optional[str] = None
    ) -> Dict[str, object]:
        entries_list: List[Dict[str, object]] = []
        for entry in self.entries.values():
            documents: List[Dict[str, object]] = []
            for document in entry.get("documents", []):
                if not isinstance(document, dict):
                    continue
                doc_output: Dict[str, object] = {
                    "type": document.get("type"),
                    "url": document.get("url"),
                    "title": document.get("title", ""),
                }
                if document.get("downloaded"):
                    doc_output["downloaded"] = True
                local_path = document.get("local_path")
                if isinstance(local_path, str) and local_path:
                    if artifact_dir:
                        doc_output["local_path"] = relativize_artifact_path(
                            local_path, artifact_dir
                        )
                    else:
                        doc_output["local_path"] = local_path
                documents.append(doc_output)
            entry_output: Dict[str, object] = {
                "serial": entry.get("serial"),
                "title": entry.get("title", ""),
                "remark": entry.get("remark", ""),
                "documents": documents,
            }
            entries_list.append(entry_output)
        entries_list.sort(
            key=lambda item: (
                item.get("serial") is None,
                item.get("serial") if isinstance(item.get("serial"), int) else 0,
                item.get("title", ""),
            )
        )
        return {"entries": entries_list}

    @classmethod
    def from_jsonable(
        cls,
        data: object,
        classifier: Optional[ClassifierFn] = None,
        *,
        artifact_dir: Optional[str] = None,
    ) -> "PBCState":
        classifier_fn = classifier or (lambda _url: "")
        state = cls()
        if isinstance(data, dict) and "entries" in data:
            entries = data.get("entries")
            if isinstance(entries, list):
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    normalized = {
                        "serial": entry.get("serial")
                        if isinstance(entry.get("serial"), int)
                        else None,
                        "title": entry.get("title", ""),
                        "remark": entry.get("remark", ""),
                    }
                    entry_id = state.ensure_entry(normalized)
                    documents: List[Dict[str, object]] = []
                    for document in entry.get("documents", []):
                        if not isinstance(document, dict):
                            continue
                        local_path_value = document.get("local_path")
                        if (
                            artifact_dir
                            and isinstance(local_path_value, str)
                            and local_path_value
                        ):
                            local_path_value = absolutize_artifact_path(
                                local_path_value, artifact_dir
                            )
                        documents.append(
                            {
                                "url": document.get("url"),
                                "type": document.get("type"),
                                "title": document.get("title", ""),
                                "downloaded": bool(document.get("downloaded")),
                                "local_path": local_path_value,
                            }
                        )
                    state.merge_documents(entry_id, documents)
            return state
        if isinstance(data, dict):
            converted_items = [
                {"url": url, "name": name}
                for url, name in data.items()
                if isinstance(url, str)
            ]
        elif isinstance(data, list):
            converted_items = []
            for item in data:
                if isinstance(item, str):
                    converted_items.append({"url": item, "name": ""})
                elif isinstance(item, dict):
                    converted_items.append(
                        {"url": item.get("url"), "name": item.get("name", "")}
                    )
        else:
            converted_items = []
        for converted in converted_items:
            url_value = converted.get("url")
            if not isinstance(url_value, str):
                continue
            name = converted.get("name")
            title = str(name) if name is not None else ""
            entry_id = state.ensure_entry({"title": title, "remark": ""})
            document = {
                "url": url_value,
                "type": classifier_fn(url_value),
                "title": title or url_value,
                "downloaded": True,
            }
            state.merge_documents(entry_id, [document])
        return state

    def is_downloaded(self, url_value: str) -> bool:
        record = self.files.get(url_value)
        if not isinstance(record, dict):
            return False
        return bool(record.get("downloaded"))


def load_state(state_file: Optional[str], classifier: ClassifierFn) -> PBCState:
    if not state_file or not os.path.exists(state_file):
        return PBCState()
    with open(state_file, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    artifact_dir = infer_artifact_dir(state_file)
    return PBCState.from_jsonable(
        data,
        classifier,
        artifact_dir=str(artifact_dir) if artifact_dir else None,
    )


def save_state(state_file: Optional[str], state: PBCState) -> None:
    if not state_file:
        return
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    artifact_dir = infer_artifact_dir(state_file)
    jsonable = (
        state.to_jsonable(artifact_dir=str(artifact_dir))
        if artifact_dir
        else state.to_jsonable()
    )
    with open(state_file, "w", encoding="utf-8") as fh:
        json.dump(jsonable, fh, ensure_ascii=False, indent=2)
