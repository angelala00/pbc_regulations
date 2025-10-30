import json
import subprocess
from pathlib import Path


def _run_node(script: str, *, cwd: Path) -> subprocess.CompletedProcess:
    """Execute a Node.js script and return the completed process."""

    return subprocess.run(
        ["node", "-e", script],
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        text=True,
    )


def test_extract_unique_summary_prefers_summary_pending(tmp_path):
    web_dir = Path(__file__).resolve().parents[1] / "pbc_regulations" / "web"
    main_js = web_dir / "main.js"
    source = main_js.read_text(encoding="utf-8")

    # Expose buildExtractSummaryCellFor for testing by attaching it to window.
    instrumented = source.replace(
        "  function buildExtractSummaryCell(task) {",
        "  window.__TEST_HOOKS__ = Object.assign(\n"
        "    window.__TEST_HOOKS__ || {},\n"
        "    { buildExtractSummaryCellFor }\n"
        "  );\n\n  function buildExtractSummaryCell(task) {",
    )

    task_payload = {
        "unique_entries_total": 40,
        "extract_unique_summary": {
            "total": "39",
            "success": "23",
            "pending": "16",
            "status_counts": {},
        },
    }

    script = f"""
const vm = require('vm');

function createElementStub() {{
  return new Proxy({{}}, {{
    get(target, prop) {{
      if (prop === 'classList') {{
        return {{
          toggle: () => {{}},
          add: () => {{}},
          remove: () => {{}},
          contains: () => false,
        }};
      }}
      if (prop === 'style') {{
        return target.style || (target.style = {{}});
      }}
      if (prop === 'querySelectorAll') {{
        return () => [];
      }}
      if (prop === 'appendChild' || prop === 'removeChild') {{
        return () => {{}};
      }}
      if (prop === 'addEventListener' || prop === 'removeEventListener') {{
        return () => {{}};
      }}
      if (prop === 'setAttribute' || prop === 'removeAttribute') {{
        return () => {{}};
      }}
      if (prop === 'textContent' || prop === 'innerHTML') {{
        return target[prop] || '';
      }}
      return (...args) => {{
        return undefined;
      }};
    }},
    set(target, prop, value) {{
      target[prop] = value;
      return true;
    }},
  }});
}}

const context = {{
  window: {{ __PBC_CONFIG__: {{}} }},
  document: {{
    readyState: 'complete',
    getElementById: () => createElementStub(),
    querySelectorAll: () => [],
    addEventListener: () => {{}},
  }},
  console,
  setInterval: () => {{}},
  clearInterval: () => {{}},
  setTimeout: () => {{}},
  clearTimeout: () => {{}},
  fetch: () => Promise.resolve({{ ok: true, json: () => Promise.resolve({{ tasks: [] }}) }}),
}};
context.window.document = context.document;
context.window.window = context.window;
context.window.navigator = {{ userAgent: 'node' }};
context.window.location = {{ href: 'http://localhost' }};

vm.createContext(context);
vm.runInContext({json.dumps(instrumented)}, context);
const hooks = context.window.__TEST_HOOKS__;
if (!hooks) {{
  throw new Error('Test hooks not installed');
}}
const html = hooks.buildExtractSummaryCellFor({json.dumps(task_payload)}, {{
  summaryKey: 'extract_unique_summary',
  totalKey: 'unique_entries_total',
}});
console.log(html);
"""

    result = _run_node(script, cwd=tmp_path)
    output = result.stdout.strip()

    assert "成功 23/39" in output
    assert "待处理 16" in output
    assert "待处理 17" not in output
