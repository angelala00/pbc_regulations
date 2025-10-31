(function () {
  const config = window.__PBC_CONFIG__ || {};
  const autoRefreshValue =
    typeof config.autoRefresh === "number" ? config.autoRefresh : null;
  const staticSnapshot = Boolean(config.staticSnapshot);
  const apiBase = typeof config.apiBase === "string" ? config.apiBase : "";
  const initialData = Array.isArray(config.initialData)
    ? config.initialData
    : null;

  const summaryCardsEl = document.getElementById("summary-cards");
  const tableBody = document.getElementById("tasks-body");
  const messageEl = document.getElementById("status-message");
  const generatedAtEls = document.querySelectorAll("[data-generated-at]");
  const autoRefreshEls = document.querySelectorAll("[data-auto-refresh]");
  const filtersSection = document.getElementById("task-filters");
  const filtersForm = document.getElementById("task-filter-form");
  const filterQueryInput = document.getElementById("task-filter-query");
  const filterStatusSelect = document.getElementById("task-filter-status");
  const filterStatusText = document.getElementById("task-filter-status-text");
  const filterToggleButton = document.getElementById("task-filter-toggle");
  const filterToggleText = document.getElementById("task-filter-toggle-text");
  const searchConfig =
    config && typeof config.search === "object" && config.search ? config.search : {};
  const searchEnabled = Boolean(searchConfig.enabled) && !staticSnapshot;

  let currentData = null;
  let summaryMeta = {
    uniqueEntries: null,
    scopedEntries: null,
  };
  const taskFilters = {
    query: "",
    status: "all",
  };

  let filtersExpanded = false;

  function toFiniteInt(value) {
    const parsed = toInt(value, null);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function computeUniqueEntriesTotal(tasks) {
    if (!Array.isArray(tasks) || !tasks.length) {
      return null;
    }
    let total = 0;
    let hasValue = false;
    tasks.forEach((task) => {
      if (!task || typeof task !== "object") {
        return;
      }
      const value = toFiniteInt(task.unique_entries_total);
      if (value !== null) {
        total += value;
        hasValue = true;
      }
    });
    return hasValue ? total : null;
  }

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

  const tasksEndpoint = buildUrl(apiBase, "/api/tasks");
  const policiesEndpoint = buildUrl(apiBase, "/api/policies");
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

  function toInt(value, defaultValue = 0) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string") {
      const parsed = Number.parseInt(value, 10);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
    return defaultValue;
  }

  function buildSummaryChip(label, options = {}) {
    const { kind, title, href } = options;
    const classes = ["summary-chip"];
    if (kind) {
      classes.push(`summary-chip--${kind}`);
    }
    if (href) {
      classes.push("summary-chip--link");
      const titleAttr = title ? ` title="${escapeHtml(title)}"` : "";
      return `<a class="${classes.join(" ")}" href="${escapeHtml(href)}"${titleAttr}>${escapeHtml(label)}</a>`;
    }
    const titleAttr = title ? ` title="${escapeHtml(title)}"` : "";
    return `<span class="${classes.join(" ")}"${titleAttr}>${escapeHtml(label)}</span>`;
  }

  function buildSummaryGroup(chips, metaLines) {
    const chipsHtml = chips && chips.length
      ? chips.join("")
      : '<span class="summary-placeholder">—</span>';
    const metaHtml = metaLines && metaLines.length
      ? `<div class="summary-meta">${metaLines.map((line) => escapeHtml(line)).join("<br>")}</div>`
      : "";
    return `<div class="summary-group">${chipsHtml}</div>${metaHtml}`;
  }

  function buildUniqueSummaryCell(task) {
    if (!task || typeof task !== "object") {
      return '<span class="summary-placeholder">—</span>';
    }
    let uniqueCount = null;
    const uniqueValue = task.unique_entries_total;
    if (typeof uniqueValue === "number" && Number.isFinite(uniqueValue)) {
      uniqueCount = uniqueValue;
    } else if (typeof uniqueValue === "string") {
      const parsed = Number.parseInt(uniqueValue, 10);
      if (Number.isFinite(parsed)) {
        uniqueCount = parsed;
      }
    }
    if (uniqueCount === null) {
      return '<span class="summary-placeholder">—</span>';
    }
    const chips = [];
    if (task.slug && !staticSnapshot) {
      const params = new URLSearchParams();
      params.set("slug", String(task.slug));
      if (task.name) {
        params.set("name", String(task.name));
      }
      params.set("count", String(uniqueCount));
      chips.push(
        buildSummaryChip(`条目 ${uniqueCount}`, {
          href: `entries.html?${params.toString()}`,
          title: task.name ? `${task.name} · 去重条目详情` : "去重条目详情",
          kind: uniqueCount ? null : "muted",
        })
      );
    } else {
      chips.push(
        buildSummaryChip(`条目 ${uniqueCount}`, {
          kind: uniqueCount ? null : "muted",
        })
      );
    }
    const uniqueSummary =
      task && task.extract_unique_summary &&
      typeof task.extract_unique_summary === "object"
        ? task.extract_unique_summary
        : null;
    const summaryTotal = uniqueSummary ? toFiniteInt(uniqueSummary.total) : null;
    const totalForCounts =
      uniqueCount !== null
        ? uniqueCount
        : summaryTotal !== null
        ? summaryTotal
        : null;
    let extractedCount = null;
    if (uniqueSummary) {
      if (Object.prototype.hasOwnProperty.call(uniqueSummary, "success")) {
        extractedCount = toInt(uniqueSummary.success);
      }
    }
    let pendingCount = null;
    if (uniqueSummary) {
      if (Object.prototype.hasOwnProperty.call(uniqueSummary, "pending")) {
        pendingCount = Math.max(toInt(uniqueSummary.pending), 0);
      }
    }
    if (pendingCount === null && summaryTotal !== null && extractedCount !== null) {
      pendingCount = Math.max(summaryTotal - extractedCount, 0);
    }
    if (pendingCount === null && totalForCounts !== null && extractedCount !== null) {
      pendingCount = Math.max(totalForCounts - extractedCount, 0);
    }
    if (extractedCount === null && summaryTotal !== null && pendingCount !== null) {
      extractedCount = Math.max(summaryTotal - pendingCount, 0);
    }
    if (extractedCount === null && totalForCounts !== null && pendingCount !== null) {
      extractedCount = Math.max(totalForCounts - pendingCount, 0);
    }
    const effectiveTotal =
      totalForCounts !== null
        ? totalForCounts
        : summaryTotal !== null
        ? summaryTotal
        : extractedCount !== null && pendingCount !== null
        ? extractedCount + pendingCount
        : null;
    if (extractedCount !== null) {
      let extractedKind = null;
      if (effectiveTotal && extractedCount >= effectiveTotal) {
        extractedKind = "success";
      } else if (!extractedCount) {
        extractedKind = "muted";
      }
      chips.push(
        buildSummaryChip(`已提取 ${extractedCount}`, {
          kind: extractedKind,
        })
      );
    }
    if (pendingCount !== null && pendingCount > 0) {
      chips.push(
        buildSummaryChip(`待处理 ${pendingCount}`, {
          kind: "warning",
        })
      );
    }
    const metaLines = [];
    const typeCounts = task && task.unique_entry_type_counts;
    if (typeCounts && typeof typeCounts === "object") {
      const entries = Object.entries(typeCounts)
        .map(([key, value]) => [key, Number(value)])
        .filter(([, value]) => Number.isFinite(value) && value > 0)
        .sort(([a], [b]) => a.localeCompare(b));
      if (entries.length) {
        const limit = 4;
        const parts = entries
          .slice(0, limit)
          .map(([key, value]) => `${key}:${value}`);
        if (entries.length > limit) {
          parts.push(`… 共 ${entries.length} 类`);
        }
        metaLines.push(`类型：${parts.join("、")}`);
      }
    }
    return buildSummaryGroup(chips, metaLines);
  }

  function buildEntriesLink(task, entriesCount) {
    if (!task || typeof task !== "object") {
      return null;
    }
    const label = `条目 ${entriesCount}`;
    if (!task.slug || staticSnapshot) {
      return buildSummaryChip(label, {
        kind: entriesCount ? null : "muted",
      });
    }
    const params = new URLSearchParams();
    params.set("slug", String(task.slug));
    if (task.name) {
      params.set("name", String(task.name));
    }
    params.set("count", String(entriesCount));
    const href = `entries.html?${params.toString()}`;
    const titleText = task.name ? `${task.name} · 条目详情` : "条目详情";
    return buildSummaryChip(label, {
      href,
      title: titleText,
    });
  }

  function buildPageSummaryCell(task) {
    if (!task || typeof task !== "object") {
      return '<span class="summary-placeholder">—</span>';
    }
    const entriesCount = toInt(task.entries_total);
    const pagesCached = toInt(task.pages_cached);
    const cacheFresh = Boolean(task.page_cache_fresh);

    const chips = [];
    chips.push(
      buildEntriesLink(task, entriesCount) ||
        buildSummaryChip(`条目 ${entriesCount}`, {
          kind: entriesCount ? null : "muted",
        })
    );
    chips.push(
      buildSummaryChip(`页面 ${pagesCached}`, {
        kind: pagesCached ? null : "muted",
      })
    );
    const historyAdded = toInt(task.entry_history_added);
    const historyRemoved = toInt(task.entry_history_removed);
    if (pagesCached) {
      chips.push(
        buildSummaryChip(cacheFresh ? "缓存 今日" : "缓存 需更新", {
          kind: cacheFresh ? "success" : "warning",
        })
      );
    }

    const metaLines = [];
    if (!pagesCached) {
      metaLines.push("尚未缓存页面");
    }
    const lastFetch = formatDate(task.page_cache_last_fetch);
    if (lastFetch !== "—") {
      metaLines.push(`上次缓存：${lastFetch}`);
    }
    const historyUpdated = formatDate(task.entry_history_updated_at);
    if (historyUpdated !== "—") {
      const historyDetails = [];
      if (historyAdded > 0) {
        historyDetails.push(`新增${historyAdded}条目`);
      }
      if (historyRemoved > 0) {
        historyDetails.push(`减少${historyRemoved}条目`);
      }
      const historyText = historyDetails.length
        ? `${historyUpdated} ${historyDetails.join("")}`
        : historyUpdated;
      metaLines.push(historyText);
    }

    return buildSummaryGroup(chips, metaLines);
  }

  function buildDownloadSummaryCell(task) {
    if (!task || typeof task !== "object") {
      return '<span class="summary-placeholder">—</span>';
    }
    const documentsTotal = toInt(task.documents_total);
    const downloadedTotal = toInt(task.downloaded_total);
    const pendingTotal = toInt(task.pending_total);
    const trackedFiles = toInt(task.tracked_files);
    const trackedDownloaded = toInt(task.tracked_downloaded);
    const outputFiles = toInt(task.output_files);
    const outputSize = toInt(task.output_size_bytes);

    const chips = [];
    chips.push(
      buildSummaryChip(`文档 ${documentsTotal}`, {
        kind: documentsTotal ? null : "muted",
      })
    );
    const downloadedKind =
      documentsTotal && downloadedTotal >= documentsTotal
        ? "success"
        : downloadedTotal
        ? null
        : "muted";
    chips.push(
      buildSummaryChip(`已下载 ${downloadedTotal}`, {
        kind: downloadedKind,
      })
    );
    if (pendingTotal > 0) {
      chips.push(
        buildSummaryChip(`待处理 ${pendingTotal}`, {
          kind: "warning",
        })
      );
    }

    const metaLines = [];
    if (outputFiles || outputSize) {
      metaLines.push(`输出：${outputFiles} 文件 · ${outputSize} 字节`);
    }
    const docTypeEntries = Object.entries(task.document_type_counts || {})
      .filter(([, value]) => Number(value) > 0)
      .sort(([a], [b]) => a.localeCompare(b));
    if (docTypeEntries.length) {
      const limit = 4;
      const parts = docTypeEntries
        .slice(0, limit)
        .map(([key, value]) => `${key}:${value}`);
      if (docTypeEntries.length > limit) {
        parts.push(`… 共 ${docTypeEntries.length} 类`);
      }
      metaLines.push(`类型：${parts.join("、")}`);
    }

    return buildSummaryGroup(chips, metaLines);
  }

  function buildExtractSummaryCellFor(task, options = {}) {
    if (!task || typeof task !== "object") {
      return '<span class="summary-placeholder">—</span>';
    }
    const summaryKey = options.summaryKey || "extract_summary";
    const totalKey = options.totalKey || "entries_total";
    const summary = task[summaryKey];
    if (!summary || typeof summary !== "object") {
      return '<span class="summary-placeholder">—</span>';
    }

    const totalFromTask = toInt(task[totalKey]);
    const summaryTotal = toFiniteInt(summary.total);
    let total = totalFromTask > 0 ? totalFromTask : summaryTotal || 0;
    const success = toInt(summary.success);
    const pendingFromSummary = toFiniteInt(summary.pending);
    let pendingValue =
      pendingFromSummary !== null ? Math.max(pendingFromSummary, 0) : null;
    if (pendingValue === null) {
      const baseTotal = total > 0 ? total : success;
      pendingValue = Math.max(baseTotal - success, 0);
    }

    const impliedTotal = success + pendingValue;
    if (impliedTotal > 0) {
      if (total <= 0) {
        total = impliedTotal;
      } else if (summaryTotal !== null && summaryTotal === impliedTotal) {
        total = impliedTotal;
      }
    }
    const pendingValueClamped = Math.max(pendingValue, 0);
    const requiresOcrRaw =
      summary.requires_ocr !== undefined && summary.requires_ocr !== null
        ? summary.requires_ocr
        : summary.need_ocr !== undefined && summary.need_ocr !== null
        ? summary.need_ocr
        : summary.needs_ocr;
    const needsOcr = toInt(requiresOcrRaw);
    const statusCounts =
      summary.status_counts && typeof summary.status_counts === "object"
        ? summary.status_counts
        : {};
    const empty = toInt(statusCounts.empty);
    const noSource = toInt(statusCounts.no_source);

    const effectiveTotal = total > 0 ? total : success + pendingValueClamped;
    const successKind =
      effectiveTotal && success === effectiveTotal ? "success" : success ? null : "muted";

    const chips = [];
    chips.push(
      buildSummaryChip(`成功 ${success}/${effectiveTotal}`, {
        kind: successKind,
      })
    );
    if (pendingValueClamped > 0) {
      chips.push(
        buildSummaryChip(`待处理 ${pendingValueClamped}`, {
          kind: "warning",
        })
      );
    }
    const ocrTypeEntries = Object.entries(summary.ocr_type_counts || {})
      .filter(([, value]) => Number(value) > 0)
      .sort(([a], [b]) => a.localeCompare(b));
    const ocrPageTotal = toInt(summary.ocr_page_total);
    if (empty > 0) {
      chips.push(
        buildSummaryChip(`空白 ${empty}`, {
          kind: "muted",
        })
      );
    }
    if (noSource > 0) {
      chips.push(
        buildSummaryChip(`无来源 ${noSource}`, {
          kind: "muted",
        })
      );
    }
    const reservedStatuses = new Set([
      "success",
      "needs_ocr",
      "error",
      "empty",
      "no_source",
    ]);
    Object.entries(statusCounts).forEach(([status, count]) => {
      if (!reservedStatuses.has(status) && Number(count) > 0) {
        chips.push(buildSummaryChip(`${status} ${count}`));
      }
    });

    const metaLines = [];
    const typeEntries = Object.entries(summary.type_counts || {})
      .filter(([, value]) => Number(value) > 0)
      .sort(([a], [b]) => a.localeCompare(b));
    if (typeEntries.length) {
      const limit = 4;
      const parts = typeEntries
        .slice(0, limit)
        .map(([key, value]) => `${key}:${value}`);
      if (typeEntries.length > limit) {
        parts.push(`… 共 ${typeEntries.length} 类`);
      }
      metaLines.push(`类型：${parts.join("、")}`);
    }
    if (needsOcr > 0) {
      const ocrParts = [];
      if (ocrTypeEntries.length) {
        const typeParts = ocrTypeEntries.map(
          ([key, value]) => `${key}:${value}`
        );
        ocrParts.push(`类型：${typeParts.join("、")}`);
      }
      if (ocrPageTotal > 0) {
        ocrParts.push(`页数 ${ocrPageTotal}`);
      }
      const suffix = ocrParts.length ? `（${ocrParts.join(" · ")}）` : "";
      metaLines.push(`OCR：${needsOcr}${suffix}`);
    }

    return buildSummaryGroup(chips, metaLines);
  }

  function buildExtractSummaryCell(task) {
    return buildExtractSummaryCellFor(task);
  }

  function summarizeUrl(url) {
    try {
      const parsed = new URL(url);
      let path = parsed.pathname || "";
      if (parsed.search) {
        path += parsed.search;
      }
      if (parsed.hash) {
        path += parsed.hash;
      }
      let display = (parsed.host || "") + path;
      if (!display) {
        display = url;
      }
      const maxLength = 60;
      if (display.length > maxLength) {
        display = display.slice(0, maxLength - 1) + "…";
      }
      return { display, full: parsed.href };
    } catch (error) {
      return { display: url, full: url };
    }
  }

  function hasActiveTaskFilters() {
    return Boolean(taskFilters.query) || taskFilters.status !== "all";
  }

  function applyTaskFilters(tasks) {
    if (!Array.isArray(tasks) || !tasks.length) {
      return [];
    }
    const query = taskFilters.query;
    const status = taskFilters.status;
    return tasks.filter((task) => {
      if (!task || typeof task !== "object") {
        return false;
      }
      if (status !== "all" && task.status !== status) {
        return false;
      }
      if (!query) {
        return true;
      }
      const pieces = [];
      if (task.name) {
        pieces.push(String(task.name));
      }
      if (task.slug) {
        pieces.push(String(task.slug));
      }
      if (task.start_url) {
        pieces.push(String(task.start_url));
      }
      if (!pieces.length) {
        return false;
      }
      return pieces
        .join(" ")
        .toLowerCase()
        .includes(query);
    });
  }

  function updateFilterStatus(totalCount, filteredCount) {
    if (!filterStatusText) {
      return;
    }
    if (!totalCount) {
      filterStatusText.textContent = "暂无任务数据。";
      return;
    }
    if (!hasActiveTaskFilters()) {
      filterStatusText.textContent = `共 ${totalCount} 个任务。`;
      return;
    }
    if (filteredCount) {
      filterStatusText.textContent = `共 ${totalCount} 个任务 · 已筛选出 ${filteredCount} 个。`;
    } else {
      filterStatusText.textContent = `共 ${totalCount} 个任务 · 没有符合筛选条件的任务。`;
    }
  }

  function setFiltersExpanded(expanded) {
    filtersExpanded = Boolean(expanded);
    if (filtersSection) {
      filtersSection.classList.toggle("hidden", !filtersExpanded);
    }
    if (filterToggleButton) {
      filterToggleButton.setAttribute(
        "aria-expanded",
        filtersExpanded ? "true" : "false"
      );
      filterToggleButton.classList.toggle("is-expanded", filtersExpanded);
    }
    if (filterToggleText) {
      filterToggleText.textContent = filtersExpanded
        ? "收起任务筛选"
        : "展开任务筛选";
    }
  }

  function renderFilteredTasks() {
    const data = Array.isArray(currentData) ? currentData : [];
    const totalCount = data.length;
    const filtered = applyTaskFilters(data);
    const hasFilters = hasActiveTaskFilters();
    let emptyMessage = "No tasks found";
    if (hasFilters && totalCount) {
      emptyMessage = "没有符合筛选条件的任务";
    }
    renderSummary(filtered);
    renderTasks(filtered, emptyMessage);
    updateFilterStatus(totalCount, filtered.length);
  }

  function initTaskFilters() {
    if (!filtersSection) {
      return;
    }
    setFiltersExpanded(false);

    updateFilterStatus(0, 0);

    if (filterToggleButton) {
      filterToggleButton.addEventListener("click", () => {
        const nextState = !filtersExpanded;
        setFiltersExpanded(nextState);
        if (nextState && filterQueryInput) {
          filterQueryInput.focus();
        }
      });
    }

    if (filterQueryInput) {
      filterQueryInput.addEventListener("input", () => {
        const value = filterQueryInput.value.trim().toLowerCase();
        if (taskFilters.query === value) {
          return;
        }
        taskFilters.query = value;
        if (Array.isArray(currentData)) {
          renderFilteredTasks();
        }
      });
    }

    if (filterStatusSelect) {
      filterStatusSelect.addEventListener("change", () => {
        const value = filterStatusSelect.value || "all";
        if (taskFilters.status === value) {
          return;
        }
        taskFilters.status = value;
        if (Array.isArray(currentData)) {
          renderFilteredTasks();
        }
      });
    }

    if (filtersForm) {
      filtersForm.addEventListener("reset", (event) => {
        event.preventDefault();
        taskFilters.query = "";
        taskFilters.status = "all";
        if (filterQueryInput) {
          filterQueryInput.value = "";
        }
        if (filterStatusSelect) {
          filterStatusSelect.value = "all";
        }
        if (Array.isArray(currentData)) {
          renderFilteredTasks();
        } else {
          updateFilterStatus(0, 0);
        }
      });
    }
  }

  function renderSummary(tasks) {
    if (!summaryCardsEl) {
      return;
    }
    const totals = tasks.reduce(
      (acc, task) => {
        if (task && typeof task === "object") {
          acc.entries += toInt(task.entries_total);
          const uniqueSummary = task.extract_unique_summary;
          if (uniqueSummary && typeof uniqueSummary === "object") {
            acc.extractedUnique += toInt(uniqueSummary.success);
          }
        }
        return acc;
      },
      { entries: 0, extractedUnique: 0 }
    );

    const cards = [
      { label: "Tasks", value: tasks.length },
      { label: "Entries", value: totals.entries },
      { label: "Entries (unique)", value: summaryMeta.uniqueEntries },
      { label: "Extracted (unique)", value: totals.extractedUnique },
      { label: "Entries (active)", value: summaryMeta.scopedEntries },
    ]
      .map(
        (card) =>
          '<div class="card"><div class="label">' +
          escapeHtml(card.label) +
          "</div><div class=\"value\">" +
          escapeHtml(
            String(
              card.value === null || card.value === undefined
                ? "—"
                : card.value
            )
          ) +
          "</div></div>"
      )
      .join("");

    summaryCardsEl.innerHTML = cards;
  }

  function setTableState(message, className) {
    const cellClass = className ? ` class="${className}"` : "";
    tableBody.innerHTML =
      `<tr><td colspan="5"${cellClass}>${escapeHtml(message)}</td></tr>`;
  }

  function renderTasks(tasks, emptyMessage = "No tasks found") {
    if (!Array.isArray(tasks) || !tasks.length) {
      setTableState(emptyMessage, "empty");
      return;
    }

    const rows = tasks
      .map((task) => {
        let urlHtml = "—";
        if (task.start_url) {
          const summary = summarizeUrl(task.start_url);
          urlHtml =
            `<a href="${escapeHtml(task.start_url)}" target="_blank" rel="noopener" title="${escapeHtml(
              summary.full
            )}">` +
            `<span class="url-text">${escapeHtml(summary.display)}</span>` +
            '<span class="external-icon" aria-hidden="true">↗</span>' +
            "</a>";
        }
        const uniqueCellHtml = buildUniqueSummaryCell(task);
        const pageCellHtml = buildPageSummaryCell(task);
        const downloadCellHtml = buildDownloadSummaryCell(task);
        const extractCellHtml = buildExtractSummaryCell(task);

        return `
          <tr>
            <td class="task-name">
              <div class="name">${escapeHtml(task.name || "—")}</div>
              <div class="url">${urlHtml}</div>
            </td>
            <td class="summary-cell summary-cell--pages">${pageCellHtml}</td>
            <td class="summary-cell summary-cell--downloads">${downloadCellHtml}</td>
            <td class="summary-cell summary-cell--unique">${uniqueCellHtml}</td>
            <td class="summary-cell summary-cell--extract">${extractCellHtml}</td>
          </tr>
        `;
      })
      .join("");

    tableBody.innerHTML = rows;
  }

  function renderDashboard(tasks, meta) {
    if (meta && typeof meta === "object" && meta !== null) {
      if (Object.prototype.hasOwnProperty.call(meta, "uniqueEntries")) {
        summaryMeta.uniqueEntries = meta.uniqueEntries;
      }
      if (Object.prototype.hasOwnProperty.call(meta, "scopedEntries")) {
        summaryMeta.scopedEntries = meta.scopedEntries;
      }
    }
    if (!Array.isArray(tasks)) {
      currentData = null;
      renderSummary([]);
      setTableState("No tasks found", "empty");
      updateFilterStatus(0, 0);
      return;
    }
    currentData = tasks.slice();
    const metaUnique =
      meta && typeof meta === "object" && Number.isFinite(meta.uniqueEntries)
        ? meta.uniqueEntries
        : null;
    if (!Number.isFinite(metaUnique)) {
      summaryMeta.uniqueEntries = computeUniqueEntriesTotal(currentData);
    }
    renderFilteredTasks();
  }

  function setGeneratedAt(value) {
    const formatted = formatDate(value);
    generatedAtEls.forEach((el) => {
      el.textContent = formatted;
    });
  }

  function setAutoRefreshDisplay(value) {
    let display;
    if (staticSnapshot) {
      display = "snapshot";
    } else if (typeof value === "number" && value > 0) {
      display = String(value);
    } else {
      display = "∞";
    }
    autoRefreshEls.forEach((el) => {
      el.textContent = display;
    });
  }

  function showMessage(text, kind = "error") {
    if (!messageEl) {
      return;
    }
    messageEl.textContent = text;
    messageEl.classList.remove("hidden", "info");
    if (kind === "info") {
      messageEl.classList.add("info");
    }
  }

  function hideMessage() {
    if (!messageEl) {
      return;
    }
    messageEl.textContent = "";
    messageEl.classList.add("hidden");
    messageEl.classList.remove("info");
  }

  async function loadPoliciesCount(options = {}) {
    if (!searchEnabled) {
      return null;
    }
    if (!policiesEndpoint) {
      return null;
    }

    try {
      const { scope } = options || {};
      let url = policiesEndpoint;
      if (scope) {
        const params = new URLSearchParams();
        params.set("scope", scope);
        const separator = url.includes("?") ? "&" : "?";
        url = `${url}${separator}${params.toString()}`;
      }
      const response = await fetch(url, {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
      }
      const payload = await response.json();
      if (!payload || typeof payload !== "object") {
        throw new Error("Unexpected response format");
      }
      const { result_count: resultCount, policies } = payload;
      if (typeof resultCount === "number" && Number.isFinite(resultCount)) {
        return resultCount;
      }
      if (typeof resultCount === "string") {
        const parsed = Number.parseInt(resultCount, 10);
        if (Number.isFinite(parsed)) {
          return parsed;
        }
      }
      if (Array.isArray(policies)) {
        return policies.length;
      }
    } catch (error) {
      console.error("Failed to load policies summary", error);
    }
    return null;
  }

  async function loadData() {
    if (staticSnapshot) {
      return;
    }

    if (!currentData) {
      setTableState("Loading…", "empty");
    }

    try {
      const policiesAllCountPromise = loadPoliciesCount({ scope: "all" });
      const policiesCountPromise = loadPoliciesCount();
      const response = await fetch(tasksEndpoint, {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
      }
      const payload = await response.json();
      if (!Array.isArray(payload)) {
        throw new Error("Unexpected response format");
      }
      const [policiesAllCount, policiesCount] = await Promise.all([
        policiesAllCountPromise,
        policiesCountPromise,
      ]);
      const meta = {};
      if (Number.isFinite(policiesAllCount)) {
        meta.uniqueEntries = policiesAllCount;
      }
      if (Number.isFinite(policiesCount)) {
        meta.scopedEntries = policiesCount;
      }
      const metaPayload = Object.keys(meta).length ? meta : undefined;
      renderDashboard(payload, metaPayload);
      hideMessage();
      setGeneratedAt(new Date());
    } catch (error) {
      console.error("Failed to load dashboard data", error);
      showMessage(`Failed to load dashboard data: ${error.message || error}`);
      if (!currentData) {
        setTableState("Unable to load data", "empty");
      }
    }
  }

  function init() {
    setAutoRefreshDisplay(autoRefreshValue);
    setGeneratedAt(config.generatedAt || null);
    initTaskFilters();

    if (initialData && initialData.length) {
      renderDashboard(initialData);
    } else if (initialData) {
      renderDashboard(initialData);
      setTableState("No tasks found", "empty");
    } else if (staticSnapshot) {
      setTableState("No data available", "empty");
    }

    if (!staticSnapshot) {
      loadData();
      if (autoRefreshValue && autoRefreshValue > 0) {
        setInterval(loadData, autoRefreshValue * 1000);
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
