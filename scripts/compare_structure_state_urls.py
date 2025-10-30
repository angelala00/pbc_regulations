import json, pathlib
struct = json.loads(pathlib.Path("artifacts/pages/structure.json").read_text("utf-8"))
state = json.loads(pathlib.Path("artifacts/downloads/default_state.json").read_text("utf-8"))
struct_urls = {doc["url"] for entry in struct["entries"] for doc in entry["documents"]}
state_urls = {doc["url"] for entry in state["entries"] for doc in entry["documents"]}
print("structure unique URLs:", len(struct_urls))
print("state unique URLs:", len(state_urls))
print("missing in state:", struct_urls - state_urls)
