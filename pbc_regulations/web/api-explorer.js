(function () {
  const config = window.__PBC_CONFIG__ || {};
  const staticSnapshot = Boolean(config.staticSnapshot);
  const apiBase = typeof config.apiBase === "string" ? config.apiBase : "";
  const explorerConfig =
    config && typeof config.apiExplorer === "object" && config.apiExplorer
      ? config.apiExplorer
      : {};
  const searchConfig =
    config && typeof config.search === "object" && config.search
      ? config.search
      : null;
  const searchEnabled =
    Boolean(searchConfig && searchConfig.enabled) && !staticSnapshot;
  const searchReason =
    searchConfig && typeof searchConfig.reason === "string"
      ? searchConfig.reason.trim()
      : "";
  const includePolicyFinderEndpointsOverride =
    explorerConfig &&
    typeof explorerConfig.includePolicyFinderEndpoints === "boolean"
      ? explorerConfig.includePolicyFinderEndpoints
      : null;
  const defaultSearchEndpoint =
    searchConfig &&
    typeof searchConfig.endpoint === "string" &&
    searchConfig.endpoint
      ? searchConfig.endpoint
      : "/api/search";
  const shouldIncludePolicyFinderEndpoints =
    includePolicyFinderEndpointsOverride !== null
      ? includePolicyFinderEndpointsOverride
      : true;

  const endpointListEl = document.getElementById("api-endpoint-list");
  const endpointTitleEl = document.getElementById("api-endpoint-title");
  const endpointDescriptionEl = document.getElementById(
    "api-endpoint-description",
  );
  const endpointHintEl = document.getElementById("api-endpoint-hint");
  const apiBaseEls = document.querySelectorAll("[data-api-base]");
  const staticNoteEl = document.getElementById("api-static-note");
  const requestForm = document.getElementById("api-request-form");
  const methodSelect = document.getElementById("api-request-method");
  const pathInput = document.getElementById("api-request-path");
  const queryInput = document.getElementById("api-request-query");
  const headersInput = document.getElementById("api-request-headers");
  const bodyInput = document.getElementById("api-request-body");
  const requestUrlEl = document.getElementById("api-request-url");
  const resetButton = document.getElementById("api-request-reset");
  const responseStatusEl = document.getElementById("api-response-status");
  const responseMetaEl = document.getElementById("api-response-meta");
  const responseBodyEl = document.getElementById("api-response-body");
  const responseHeadersEl = document.getElementById("api-response-headers");

  const defaultEndpoints = [
    {
      id: "tasks",
      name: "Tasks",
      method: "GET",
      path: "/api/tasks",
      description:
        "List all crawling tasks with counters, last update timestamps, and status metadata.",
      query: "",
      hint: "No parameters are required.",
      body: "",
    },
    {
      id: "task-entries",
      name: "Task entries",
      method: "GET",
      path: "/api/tasks/entries",
      description:
        "Fetch captured entries for one or more tasks. Provide one or multiple slugs to limit the response.",
      query: "slugs=task-a&slugs=task-b",
      hint:
        "Append one or more slugs with the ?slugs= parameter. Repeat the parameter or provide a comma-separated list.",
      body: "",
    },
    {
      id: "knowledge-query-post",
      name: "Knowledge query",
      method: "POST",
      path: "/api/knowledge/query",
      description:
        "Query the mock knowledge dictionary by supplying one or more keys.",
      query: "",
      hint:
        "Provide a JSON body with key_list, key, or keys fields. Values can repeat or use comma-separated strings.",
      body: '{"key_list": ["数字人民币监管", "合规"]}',
      headers: "{}",
    },
    {
      id: "asker-institution",
      name: "Asker institution",
      method: "POST",
      path: "/api/asker/institution",
      description:
        "Submit a single-turn question to the mocked institution asker service.",
      query: "",
      hint:
        "Provide a JSON body with the question field and optionally policy_hint for additional guidance.",
      body: '{"question": "数字人民币监管的主要政策有哪些？", "policy_hint": "央行相关法规"}',
      headers: "{\"Content-Type\": \"application/json\"}",
    },
    {
      id: "asker-institution-session",
      name: "Asker institution (session)",
      method: "POST",
      path: "/api/asker/institution/session",
      description:
        "Continue a multi-turn institution conversation or start a new session.",
      query: "",
      hint:
        "Include message and optionally session_id or policy_hint. Omit session_id to begin a new session.",
      body: '{"message": "继续介绍近期的监管动态", "session_id": "session-123"}',
      headers: "{\"Content-Type\": \"application/json\"}",
    },
    {
      id: "asker-legal",
      name: "Asker legal",
      method: "POST",
      path: "/api/asker/legal",
      description:
        "Retrieve a drafted legal style answer with references using the legal asker endpoint.",
      query: "",
      hint: "Send the legal question in the question field of the JSON payload.",
      body: '{"question": "数字人民币个人钱包有哪些合规要求？"}',
      headers: "{\"Content-Type\": \"application/json\"}",
    },
  ];

  if (shouldIncludePolicyFinderEndpoints) {
    const searchPath = defaultSearchEndpoint;
    const normalizedSearchPath = normalizePath(searchPath);
    let searchUrl = null;
    try {
      searchUrl = new URL(normalizedSearchPath, window.location.origin);
    } catch (error) {
      searchUrl = null;
    }
    const searchIsAbsolute = /^https?:\/\//i.test(normalizedSearchPath);
    const searchOrigin = searchUrl && searchIsAbsolute ? searchUrl.origin : "";
    const searchPathname = searchUrl ? searchUrl.pathname : normalizedSearchPath;
    const baseSuffix = "/search";
    let searchBasePath = searchPathname;
    if (typeof searchBasePath === "string") {
      const lower = searchBasePath.toLowerCase();
      if (lower.endsWith(baseSuffix)) {
        const nextValue = searchBasePath.slice(0, -baseSuffix.length);
        searchBasePath = nextValue || (searchOrigin ? "/" : "");
      }
    } else {
      searchBasePath = "";
    }
    const resolveFinderPath = (segment) => {
      const value = typeof segment === "string" ? segment.trim() : "";
      if (!value) {
        return normalizedSearchPath;
      }
      const relative = value.startsWith("/") ? value : `/${value}`;
      const base = !searchBasePath || searchBasePath === "/"
        ? ""
        : searchBasePath.replace(/\/$/, "");
      const combinedPath = (base + relative) || relative;
      const finalPath = combinedPath || relative || "/";
      if (searchOrigin) {
        return `${searchOrigin}${finalPath}`;
      }
      if (searchIsAbsolute && searchUrl) {
        return `${searchUrl.origin}${finalPath}`;
      }
      return finalPath;
    };

    const searchDisabledReason = !searchEnabled
      ? searchReason || "Policy search is currently unavailable."
      : "";

    defaultEndpoints.push(
      {
        id: "search-policies",
        name: "Policy catalog",
        method: "GET",
        path: resolveFinderPath("policies"),
        description:
          "List every indexed policy entry or filter the catalog by keyword.",
        query: "query=数字人民币",
        hint:
          "Use ?query= to narrow results. Omitting the parameter returns the full catalog sorted by title.",
        body: "",
        disabled: !searchEnabled,
        disabledReason: searchDisabledReason,
      },
      {
        id: "search-policy-detail",
        name: "Policy details",
        method: "GET",
        path: resolveFinderPath("policies/{policy_id}"),
        description:
          "Fetch a single policy by its identifier and optionally include text or outline details.",
        query: "include=meta&include=text",
        hint:
          "Add one or more ?include= values (meta, text, outline, all) to control the response payload.",
        body: "",
        disabled: !searchEnabled,
        disabledReason: searchDisabledReason,
      },
      {
        id: "search-clause",
        name: "Clause lookup",
        method: "GET",
        path: resolveFinderPath("clause"),
        description:
          "Resolve a specific clause or article from a policy by supplying the title and clause reference.",
        query: "title=中国人民银行公告(2024)第1号&item=第一条",
        hint:
          "Both ?title= and ?item= (or ?clause=/ ?article=) are required to locate the matching clause.",
        body: "",
        disabled: !searchEnabled,
        disabledReason: searchDisabledReason,
      },
    );
  }

  const customEndpoints = Array.isArray(explorerConfig.endpoints)
    ? explorerConfig.endpoints
        .map((item, index) => normalizeEndpoint(item, index))
        .filter(Boolean)
    : [];

  const endpoints = [];
  defaultEndpoints.forEach((endpoint) => {
    pushEndpoint(endpoints, endpoint);
  });
  customEndpoints.forEach((endpoint) => {
    pushEndpoint(endpoints, endpoint);
  });

  let activeEndpoint = endpoints.length ? endpoints[0] : null;

  setApiBaseText(apiBaseEls, apiBase);
  if (staticSnapshot && staticNoteEl) {
    staticNoteEl.classList.remove("hidden");
  }

  renderEndpointList();
  selectEndpoint(activeEndpoint ? activeEndpoint.id : "");
  resetResponse();
  updateRequestUrl();
  registerEventListeners();

  function buildUrl(base, path) {
    if (!base || /^https?:\/\//i.test(path)) {
      return path;
    }
    return base.replace(/\/+$/, "") + path;
  }

  function normalizePath(path) {
    if (typeof path !== "string") {
      return "/";
    }
    const trimmed = path.trim();
    if (!trimmed) {
      return "/";
    }
    if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
      return trimmed;
    }
    return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  }

  function pushEndpoint(list, endpoint) {
    if (!endpoint || !endpoint.id) {
      return;
    }
    const existingIndex = list.findIndex((item) => item.id === endpoint.id);
    if (existingIndex >= 0) {
      list.splice(existingIndex, 1, endpoint);
    } else {
      list.push(endpoint);
    }
  }

  function normalizeEndpoint(source, index) {
    if (!source || typeof source !== "object") {
      return null;
    }
    const id =
      typeof source.id === "string" && source.id.trim()
        ? source.id.trim()
        : `custom-${index}`;
    const name =
      typeof source.name === "string" && source.name.trim()
        ? source.name.trim()
        : "Custom endpoint";
    const method =
      typeof source.method === "string" && source.method.trim()
        ? source.method.trim().toUpperCase()
        : "GET";
    const path =
      typeof source.path === "string" && source.path.trim()
        ? source.path.trim()
        : "";
    if (!path) {
      return null;
    }
    const description =
      typeof source.description === "string" ? source.description : "";
    const query = typeof source.query === "string" ? source.query : "";
    const hint = typeof source.hint === "string" ? source.hint : "";
    const body = typeof source.body === "string" ? source.body : "";
    const headers = typeof source.headers === "string" ? source.headers : "";
    return { id, name, method, path, description, query, hint, body, headers };
  }

  function setApiBaseText(elements, base) {
    const displayValue = base ? base : "the current origin";
    elements.forEach((element) => {
      element.textContent = displayValue;
    });
  }

  function renderEndpointList() {
    if (!endpointListEl) {
      return;
    }
    endpointListEl.innerHTML = "";
    if (!endpoints.length) {
      const emptyItem = document.createElement("li");
      emptyItem.className = "api-endpoint-list__empty";
      emptyItem.textContent = "No endpoints are configured.";
      endpointListEl.appendChild(emptyItem);
      return;
    }
    endpoints.forEach((endpoint) => {
      const item = document.createElement("li");
      item.className = "api-endpoint-list__item";
      const button = document.createElement("button");
      button.type = "button";
      button.className = "api-endpoint-list__button";
      button.dataset.endpointId = endpoint.id;
      const nameEl = document.createElement("span");
      nameEl.className = "api-endpoint-list__name";
      nameEl.textContent = endpoint.name;
      const pathEl = document.createElement("span");
      pathEl.className = "api-endpoint-list__path";
      pathEl.textContent = `${endpoint.method} ${endpoint.path}`;
      button.appendChild(nameEl);
      button.appendChild(pathEl);
      if (endpoint.disabled) {
        button.classList.add("is-disabled");
        if (endpoint.disabledReason) {
          button.title = endpoint.disabledReason;
        }
        button.setAttribute("aria-disabled", "true");
      }
      button.addEventListener("click", () => {
        selectEndpoint(endpoint.id);
      });
      item.appendChild(button);
      endpointListEl.appendChild(item);
    });
  }

  function selectEndpoint(endpointId) {
    const endpoint = endpoints.find((item) => item.id === endpointId);
    if (!endpoint) {
      updateEndpointDetails(null);
      return;
    }
    activeEndpoint = endpoint;
    updateEndpointDetails(endpoint);
    applyEndpointToForm(endpoint);
    highlightActiveEndpoint(endpoint.id);
  }

  function updateEndpointDetails(endpoint) {
    if (endpointTitleEl) {
      let titleText = endpoint ? endpoint.name : "Request builder";
      if (endpoint && endpoint.disabled) {
        titleText += " (unavailable)";
      }
      endpointTitleEl.textContent = titleText;
    }
    if (endpointDescriptionEl) {
      const baseDescription = endpoint
        ? endpoint.description ||
          "Configure the request and send it to view the response."
        : "Choose an endpoint or enter a path to send a request.";
      const descriptionText = endpoint && endpoint.disabled && endpoint.disabledReason
        ? `${baseDescription} ${endpoint.disabledReason}`.trim()
        : baseDescription;
      endpointDescriptionEl.textContent = descriptionText;
    }
    if (endpointHintEl) {
      const hintParts = [];
      if (endpoint && endpoint.hint) {
        hintParts.push(endpoint.hint);
      }
      if (endpoint && endpoint.disabled && endpoint.disabledReason) {
        hintParts.push(endpoint.disabledReason);
      }
      const hintText = hintParts.join(" ").trim();
      if (hintText) {
        endpointHintEl.textContent = hintText;
        endpointHintEl.classList.remove("hidden");
      } else {
        endpointHintEl.textContent = "";
        endpointHintEl.classList.add("hidden");
      }
    }
  }

  function applyEndpointToForm(endpoint) {
    if (!endpoint || !requestForm) {
      return;
    }
    if (methodSelect) {
      methodSelect.value = endpoint.method || "GET";
    }
    if (pathInput) {
      pathInput.value = endpoint.path || "/";
    }
    if (queryInput) {
      queryInput.value = endpoint.query || "";
    }
    if (bodyInput) {
      bodyInput.value = endpoint.body || "";
    }
    if (headersInput) {
      headersInput.value = endpoint.headers || "";
    }
    resetResponse();
    updateRequestUrl();
  }

  function highlightActiveEndpoint(endpointId) {
    if (!endpointListEl) {
      return;
    }
    const buttons = endpointListEl.querySelectorAll(
      ".api-endpoint-list__button",
    );
    buttons.forEach((button) => {
      if (button instanceof HTMLButtonElement) {
        const isActive = button.dataset.endpointId === endpointId;
        button.classList.toggle("is-active", isActive);
      }
    });
  }

  function registerEventListeners() {
    if (requestForm) {
      requestForm.addEventListener("submit", handleFormSubmit);
    }
    if (resetButton) {
      resetButton.addEventListener("click", () => {
        if (activeEndpoint) {
          applyEndpointToForm(activeEndpoint);
        } else {
          if (requestForm) {
            requestForm.reset();
          }
          resetResponse();
          updateRequestUrl();
        }
      });
    }
    if (pathInput) {
      pathInput.addEventListener("input", updateRequestUrl);
    }
    if (queryInput) {
      queryInput.addEventListener("input", updateRequestUrl);
    }
  }

  function handleFormSubmit(event) {
    event.preventDefault();
    sendRequest().catch((error) => {
      console.error("API request failed", error);
    });
  }

  function updateRequestUrl() {
    if (!requestUrlEl) {
      return;
    }
    const pathValue = normalizePath(pathInput ? pathInput.value : "/");
    const queryValue = queryInput ? queryInput.value.trim() : "";
    let url = pathValue;
    if (queryValue) {
      url += pathValue.includes("?") ? "&" : "?";
      url += queryValue;
    }
    if (apiBase) {
      url = buildUrl(apiBase, pathValue);
      if (queryValue) {
        url += pathValue.includes("?") ? "&" : "?";
        url += queryValue;
      }
    }
    requestUrlEl.textContent = url;
  }

  function resetResponse() {
    if (responseStatusEl) {
      responseStatusEl.textContent =
        "Responses will appear here after you send a request.";
      responseStatusEl.classList.remove("is-error");
    }
    if (responseMetaEl) {
      responseMetaEl.textContent = "";
    }
    if (responseBodyEl) {
      responseBodyEl.textContent = "—";
    }
    if (responseHeadersEl) {
      responseHeadersEl.textContent = "—";
    }
  }

  async function sendRequest() {
    if (!requestForm || !pathInput) {
      return;
    }
    const method = methodSelect ? methodSelect.value : "GET";
    const rawPath = pathInput.value;
    const normalizedPath = normalizePath(rawPath);
    const queryValue = queryInput ? queryInput.value.trim() : "";
    const headersValue = headersInput ? headersInput.value.trim() : "";
    const bodyValue = bodyInput ? bodyInput.value : "";

    const requestUrl = (() => {
      const url = apiBase ? buildUrl(apiBase, normalizedPath) : normalizedPath;
      if (!queryValue) {
        return url;
      }
      return url + (url.includes("?") ? "&" : "?") + queryValue;
    })();

    const options = { method, headers: {} };

    let parsedHeaders = null;
    if (headersValue) {
      try {
        parsedHeaders = JSON.parse(headersValue);
      } catch (error) {
        setErrorState(
          "Unable to parse custom headers. Ensure the value is valid JSON.",
        );
        return;
      }
    }
    if (parsedHeaders && typeof parsedHeaders === "object") {
      Object.entries(parsedHeaders).forEach(([key, value]) => {
        if (typeof value === "string") {
          options.headers[key] = value;
        } else if (value !== null && value !== undefined) {
          options.headers[key] = String(value);
        }
      });
    }

    if (!options.headers.Accept) {
      options.headers.Accept = "application/json, text/plain, */*";
    }

    const methodAllowsBody = !/^(GET|HEAD)$/i.test(method);
    if (methodAllowsBody && bodyValue.trim()) {
      options.body = bodyValue;
      if (!options.headers["Content-Type"] && looksLikeJson(bodyValue)) {
        options.headers["Content-Type"] = "application/json";
      }
    }

    if (responseStatusEl) {
      responseStatusEl.textContent = "Sending request…";
      responseStatusEl.classList.remove("is-error");
    }
    if (responseMetaEl) {
      responseMetaEl.textContent = requestUrl;
    }

    let response;
    const startedAt = performance.now();
    try {
      response = await fetch(requestUrl, options);
    } catch (error) {
      setErrorState(`Request failed: ${error.message || error}`);
      return;
    }
    const elapsed = Math.round(performance.now() - startedAt);

    const statusLine = `${response.status} ${response.statusText}`.trim();
    if (responseStatusEl) {
      responseStatusEl.textContent = response.ok
        ? `Success · ${statusLine}`
        : `Error · ${statusLine}`;
      responseStatusEl.classList.toggle("is-error", !response.ok);
    }

    if (responseMetaEl) {
      responseMetaEl.textContent = `${requestUrl} · ${elapsed} ms`;
    }

    const responseText = await response.text();
    const formattedBody = formatResponseBody(responseText, response.headers);
    if (responseBodyEl) {
      responseBodyEl.textContent = formattedBody;
    }

    if (responseHeadersEl) {
      const headersArray = [];
      response.headers.forEach((value, key) => {
        headersArray.push(`${key}: ${value}`);
      });
      responseHeadersEl.textContent = headersArray.length
        ? headersArray.join("\n")
        : "—";
    }
  }

  function looksLikeJson(value) {
    const trimmed = value.trim();
    return trimmed.startsWith("{") || trimmed.startsWith("[");
  }

  function formatResponseBody(text, headers) {
    const contentType = headers && headers.get ? headers.get("content-type") : "";
    if (contentType && contentType.includes("application/json")) {
      try {
        const parsed = JSON.parse(text);
        return JSON.stringify(parsed, null, 2);
      } catch (error) {
        // fall through to plain text
      }
    }
    if (!text) {
      return "—";
    }
    return text;
  }

  function setErrorState(message) {
    if (responseStatusEl) {
      responseStatusEl.textContent = message;
      responseStatusEl.classList.add("is-error");
    }
    if (responseMetaEl) {
      responseMetaEl.textContent = "";
    }
    if (responseBodyEl) {
      responseBodyEl.textContent = "—";
    }
    if (responseHeadersEl) {
      responseHeadersEl.textContent = "—";
    }
  }
})();
