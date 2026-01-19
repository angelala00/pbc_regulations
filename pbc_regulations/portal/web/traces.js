(function () {
  const config = window.__PBC_CONFIG__ || {};
  const apiBase = typeof config.apiBase === "string" ? config.apiBase : "";
  const staticSnapshot = Boolean(config.staticSnapshot);

  const tracesBody = document.getElementById("traces-body");
  const tracesMessage = document.getElementById("traces-message");
  const traceDetail = document.getElementById("trace-detail");
  const traceDetailSubtitle = document.getElementById("trace-detail-subtitle");
  const generatedAtEls = document.querySelectorAll("[data-generated-at]");
  const splitter = document.getElementById("trace-splitter");
  const tracesMain = document.querySelector(".traces-main");
  let currentTrace = { summary: null, events: [] };
  let selectedEventTypes = new Set();
  let lastCustomSelection = new Set();
  const LLM_EVENT_TYPES = new Set([
    "model_request",
    "content_delta",
    "message_start",
    "message_end",
    "assistant_content",
    "model_error",
  ]);
  const TOOL_EVENT_TYPES = new Set(["node_start", "node_end", "tool_call"]);

  function buildUrl(base, path) {
    const normalizedPath =
      typeof path === "string" ? path.replace(/^\/+/, "") : "";
    if (!base) {
      return normalizedPath;
    }
    const normalizedBase = String(base || "").replace(/\/+$/, "");
    if (!normalizedPath) {
      return normalizedBase || "";
    }
    return `${normalizedBase}/${normalizedPath}`;
  }

  const tracesEndpoint = buildUrl(apiBase, "/api/traces");

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatDate(value) {
    if (!value) {
      return "—";
    }
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      return "—";
    }
    const pad = (num) => String(num).padStart(2, "0");
    return (
      date.getFullYear() +
      "-" +
      pad(date.getMonth() + 1) +
      "-" +
      pad(date.getDate()) +
      " " +
      pad(date.getHours()) +
      ":" +
      pad(date.getMinutes()) +
      ":" +
      pad(date.getSeconds())
    );
  }

  function formatDuration(value) {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return "—";
    }
    if (value < 1000) {
      return `${value} ms`;
    }
    return `${(value / 1000).toFixed(2)} s`;
  }

  function setMessage(text, isError = true) {
    if (!tracesMessage) {
      return;
    }
    if (!text) {
      tracesMessage.classList.add("hidden");
      tracesMessage.textContent = "";
      return;
    }
    tracesMessage.textContent = text;
    tracesMessage.classList.remove("hidden");
    if (isError) {
      tracesMessage.classList.remove("info");
    } else {
      tracesMessage.classList.add("info");
    }
  }

  function updateGeneratedAt(ts) {
    const formatted = formatDate(ts);
    generatedAtEls.forEach((el) => {
      el.textContent = formatted;
    });
  }

  function renderTraceRow(trace) {
    const traceId = trace.trace_id || "";
    const request = trace.request || {};
    const query = request.query || "—";
    const status = trace.status || "—";
    const startedAt = trace.started_at || null;
    const duration = trace.duration_ms;
    return `
      <tr data-trace-id="${escapeHtml(traceId)}">
        <td class="trace-id">
          <button type="button" class="link-button" data-trace="${escapeHtml(
            traceId
          )}">${escapeHtml(traceId.slice(0, 12))}</button>
        </td>
        <td>${escapeHtml(formatDate(startedAt))}</td>
        <td>${escapeHtml(formatDuration(duration))}</td>
        <td>${escapeHtml(status)}</td>
        <td>${escapeHtml(query)}</td>
      </tr>
    `;
  }

  function renderTraceList(traces) {
    if (!Array.isArray(traces) || !traces.length) {
      tracesBody.innerHTML = '<tr><td colspan="5" class="empty">No traces.</td></tr>';
      return;
    }
    tracesBody.innerHTML = traces.map(renderTraceRow).join("");
    tracesBody.querySelectorAll("button[data-trace]").forEach((button) => {
      button.addEventListener("click", () => {
        const traceId = button.getAttribute("data-trace");
        if (traceId) {
          loadTraceDetail(traceId);
        }
      });
    });
  }

  function renderDetail(summary, events, types, selectedTypes) {
    const request = (summary && summary.request) || {};
    const items = [
      ["Trace ID", summary.trace_id || "—"],
      ["Status", summary.status || "—"],
      ["Started", formatDate(summary.started_at)],
      ["Duration", formatDuration(summary.duration_ms)],
      ["Query", request.query || "—"],
      ["Task ID", request.task_id || "—"],
      ["Context ID", request.context_id || "—"],
    ];
    const metaRows = items
      .map(
        ([label, value]) =>
          `<div class="trace-meta__row"><span>${escapeHtml(
            label
          )}</span><strong>${escapeHtml(value)}</strong></div>`
      )
      .join("");
    const eventRows = (events || [])
      .map((event) => {
        const ts = formatDate(event.ts);
        const name = event.event || "event";
        const payload = event.payload || {};
        return `
          <div class="trace-event">
            <div class="trace-event__meta">
              <span>${escapeHtml(ts)}</span>
              <span class="trace-event__name">${escapeHtml(name)}</span>
              <span>#${escapeHtml(event.seq || "—")}</span>
            </div>
            <pre>${escapeHtml(JSON.stringify(payload, null, 2))}</pre>
          </div>
        `;
      })
      .join("");
    const typeList = types || [];
    const selected = selectedTypes || new Set();
    const allChecked =
      typeList.length > 0 && typeList.every((type) => selected.has(type));
    const llmTypes = typeList.filter((type) => LLM_EVENT_TYPES.has(type));
    const toolTypes = typeList.filter((type) => TOOL_EVENT_TYPES.has(type));
    const selectedCount = selected.size;
    const llmChecked =
      llmTypes.length > 0 &&
      selectedCount === llmTypes.length &&
      llmTypes.every((type) => selected.has(type));
    const toolChecked =
      toolTypes.length > 0 &&
      selectedCount === toolTypes.length &&
      toolTypes.every((type) => selected.has(type));
    const filterItems = typeList
      .map((type) => {
        const safeType = escapeHtml(type);
        const checked = selected.has(type) ? " checked" : "";
        return `
          <label class="trace-filter-item">
            <input type="checkbox" data-event-type="${safeType}"${checked} />
            <span>${safeType}</span>
          </label>
        `;
      })
      .join("");
    const filterDisabled = typeList.length <= 1 ? " disabled" : "";
    traceDetail.innerHTML = `
      <div class="trace-meta">${metaRows}</div>
      <div class="trace-detail__controls">
        <div class="trace-filter">
          <span>Event type</span>
          <div class="trace-filter-group trace-filter-group--presets">
            <label class="trace-filter-item">
              <input type="checkbox" data-event-type="__all__"${
                allChecked ? " checked" : ""
              }${filterDisabled} />
              <span>All</span>
            </label>
            <label class="trace-filter-item">
              <input type="checkbox" data-event-type="__llm__"${
                llmChecked ? " checked" : ""
              }${llmTypes.length === 0 ? " disabled" : ""} />
              <span>LLM</span>
            </label>
            <label class="trace-filter-item">
              <input type="checkbox" data-event-type="__tool__"${
                toolChecked ? " checked" : ""
              }${toolTypes.length === 0 ? " disabled" : ""} />
              <span>TOOL</span>
            </label>
          </div>
          <div class="trace-filter-divider"></div>
          <div class="trace-filter-group trace-filter-group--types" id="trace-event-filter">
            ${filterItems}
          </div>
        </div>
      </div>
      <div class="trace-events">
        ${eventRows || '<div class="empty">No events.</div>'}
      </div>
    `;
    const filter = traceDetail.querySelector(".trace-filter");
    if (filter) {
      filter.addEventListener("change", (event) => {
        const target = event.target;
        if (!target || target.tagName !== "INPUT") {
          return;
        }
        const type = target.getAttribute("data-event-type");
        if (!type) {
          return;
        }
        if (type === "__all__") {
          if (target.checked) {
            selectedEventTypes = new Set(typeList);
          } else {
            selectedEventTypes = new Set();
          }
          lastCustomSelection = new Set(selectedEventTypes);
        } else if (type === "__llm__") {
          if (target.checked) {
            if (selectedEventTypes.size > 0) {
              lastCustomSelection = new Set(selectedEventTypes);
            }
            selectedEventTypes = new Set(llmTypes);
          } else if (lastCustomSelection.size > 0) {
            selectedEventTypes = new Set(lastCustomSelection);
          } else {
            selectedEventTypes = new Set(typeList);
          }
        } else if (type === "__tool__") {
          if (target.checked) {
            if (selectedEventTypes.size > 0) {
              lastCustomSelection = new Set(selectedEventTypes);
            }
            selectedEventTypes = new Set(toolTypes);
          } else if (lastCustomSelection.size > 0) {
            selectedEventTypes = new Set(lastCustomSelection);
          } else {
            selectedEventTypes = new Set(typeList);
          }
        } else if (target.checked) {
          selectedEventTypes.add(type);
        } else {
          selectedEventTypes.delete(type);
        }
        renderCurrentTrace();
      });
    }
  }

  function buildEventTypes(events) {
    const seen = new Set();
    (events || []).forEach((event) => {
      const name = event && event.event ? String(event.event) : "event";
      seen.add(name);
    });
    return Array.from(seen).sort((a, b) => a.localeCompare(b));
  }

  function getFilteredEvents() {
    if (!selectedEventTypes || selectedEventTypes.size === 0) {
      return [];
    }
    if (selectedEventTypes.size === buildEventTypes(currentTrace.events).length) {
      return currentTrace.events || [];
    }
    return (currentTrace.events || []).filter(
      (event) => selectedEventTypes.has(String(event.event || "event"))
    );
  }

  function renderCurrentTrace() {
    if (!currentTrace.summary) {
      return;
    }
    const filtered = getFilteredEvents();
    if (filtered.length === 0 && (currentTrace.events || []).length > 0) {
      renderDetail(
        currentTrace.summary,
        [],
        buildEventTypes(currentTrace.events),
        selectedEventTypes
      );
      const empty = traceDetail.querySelector(".trace-events .empty");
      if (empty) {
        empty.textContent = "No events match the selected types.";
      }
      return;
    }
    renderDetail(
      currentTrace.summary,
      filtered,
      buildEventTypes(currentTrace.events),
      selectedEventTypes
    );
  }

  async function loadTraceDetail(traceId) {
    if (!traceId || staticSnapshot) {
      return;
    }
    traceDetailSubtitle.textContent = `Trace ${traceId}`;
    traceDetail.innerHTML = '<div class="empty">Loading…</div>';
    try {
      const response = await fetch(buildUrl(apiBase, `/api/traces/${traceId}`), {
        headers: { Accept: "application/json" },
      });
      if (!response.ok) {
        throw new Error(`Failed to load trace: ${response.status}`);
      }
      const payload = await response.json();
      currentTrace = {
        summary: payload.summary || {},
        events: payload.events || [],
      };
      const types = buildEventTypes(currentTrace.events);
      if (selectedEventTypes.size === 0) {
        selectedEventTypes = new Set(types);
      } else {
        selectedEventTypes = new Set(
          types.filter((type) => selectedEventTypes.has(type))
        );
        if (selectedEventTypes.size === 0) {
          selectedEventTypes = new Set(types);
        }
      }
      renderCurrentTrace();
    } catch (error) {
      traceDetail.innerHTML = `<div class="message">${
        error && error.message ? escapeHtml(error.message) : "Failed to load trace."
      }</div>`;
    }
  }

  async function loadTraces() {
    if (staticSnapshot) {
      setMessage("Trace API is disabled in static snapshot mode.", false);
      tracesBody.innerHTML =
        '<tr><td colspan="5" class="empty">API disabled.</td></tr>';
      return;
    }
    try {
      const response = await fetch(tracesEndpoint, {
        headers: { Accept: "application/json" },
      });
      if (!response.ok) {
        throw new Error(`Failed to load traces: ${response.status}`);
      }
      const payload = await response.json();
      renderTraceList(payload.results || []);
      updateGeneratedAt(Date.now());
      setMessage("", false);
    } catch (error) {
      setMessage(
        error && error.message ? error.message : "Failed to load traces.",
        true
      );
    }
  }

  loadTraces();

  if (splitter && tracesMain && window.matchMedia("(min-width: 1100px)").matches) {
    let isDragging = false;

    const onMove = (event) => {
      if (!isDragging) {
        return;
      }
      const rect = tracesMain.getBoundingClientRect();
      const x = Math.min(Math.max(event.clientX - rect.left, 200), rect.width - 300);
      const leftPct = x / rect.width;
      tracesMain.style.setProperty("--trace-left", `${(leftPct * 100).toFixed(2)}%`);
      tracesMain.style.setProperty(
        "--trace-right",
        `${((1 - leftPct) * 100).toFixed(2)}%`
      );
    };

    const stopDrag = () => {
      if (!isDragging) {
        return;
      }
      isDragging = false;
      splitter.classList.remove("is-dragging");
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", stopDrag);
    };

    splitter.addEventListener("mousedown", (event) => {
      event.preventDefault();
      isDragging = true;
      splitter.classList.add("is-dragging");
      window.addEventListener("mousemove", onMove);
      window.addEventListener("mouseup", stopDrag);
    });
  }
})();
