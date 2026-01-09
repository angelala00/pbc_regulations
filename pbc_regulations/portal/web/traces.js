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

  function renderDetail(summary, events) {
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
    traceDetail.innerHTML = `
      <div class="trace-meta">${metaRows}</div>
      <div class="trace-events">
        ${eventRows || '<div class="empty">No events.</div>'}
      </div>
    `;
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
      renderDetail(payload.summary || {}, payload.events || []);
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
