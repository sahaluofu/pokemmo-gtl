/* global echarts */
(() => {
  "use strict";

  const state = {
    health: null, overview: null, catalog: new Map(), categories: new Set(),
    items: [], events: [], barEvents: [], selectedItem: null,
    bars: [], forecasts: [], backtest: null,
    timeframe: "4h", windowDays: "90", chartType: "candlestick",
    charts: {}, itemRequest: 0, detailRequest: 0, overviewRequest: 0,
    busy: false, healthAvailable: false, activeTab: "overview",
  };
  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
  const numberFormat = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 0 });
  const decimalFormat = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 3 });
  const pctFormat = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2 });
  const signedPct = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2, signDisplay: "always" });
  const modelLabels = { naive: "Naive", statistical: "Statistical", lightgbm: "LightGBM" };
  const actionLabels = { collect: "采集", sync_catalog: "词典同步", forecast: "预测", simulate: "仿真" };
  const finite = (value) => value !== null && value !== undefined && value !== ""
    && Number.isFinite(Number(value));
  const numeric = (value) => finite(value) ? Number(value) : null;
  const escapeHTML = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[char]);
  const displayName = (item) => item?.name_zh || item?.name_en || ("物品 #" + (item?.item_id ?? "?"));
  const formatMoney = (value) => finite(value) ? numberFormat.format(Number(value)) + " ₽" : "--";
  const fmtMetric = (value) => finite(value) ? decimalFormat.format(Number(value)) : "--";
  const formatPercent = (value, signed = true) => finite(value)
    ? (signed ? signedPct : pctFormat).format(Number(value)) + "%" : "--";
  const formatRatio = (value, signed = false) => finite(value) ? formatPercent(Number(value) * 100, signed) : "--";
  const changeClass = (value) => !finite(value) ? "" : Number(value) >= 0 ? "change-positive" : "change-negative";

  function formatDate(value, short = false) {
    if (!finite(value)) return "--";
    const date = new Date(Number(value) * 1000);
    if (!Number.isFinite(date.getTime())) return "--";
    return date.toLocaleString("zh-CN", short
      ? { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }
      : { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false });
  }

  function setMessage(selector, message, kind = "") {
    const element = $(selector);
    if (!element) return;
    element.textContent = message;
    element.dataset.kind = kind;
  }

  function notice(message, kind = "error") {
    const element = $("#globalNotice");
    element.hidden = !message;
    element.textContent = message;
    element.dataset.kind = kind;
  }

  function safeStorage(key, value) {
    try {
      if (value === undefined) return localStorage.getItem(key);
      localStorage.setItem(key, value);
    } catch { /* Storage may be unavailable in private or local-file contexts. */ }
    return null;
  }

  async function api(path, options = {}) {
    const timeout = options.timeout ?? 30000;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    const { timeout: _timeout, ...request } = options;
    try {
      const response = await fetch(path, {
        ...request, cache: "no-store", credentials: "same-origin",
        signal: controller.signal,
        headers: { Accept: "application/json", ...request.headers },
      });
      if (response.status === 204) return null;
      const isJSON = (response.headers.get("content-type") || "").includes("json");
      const payload = isJSON ? await response.json() : null;
      if (!response.ok) {
        const error = new Error(payload?.error?.message || ("请求失败（HTTP " + response.status + "）"));
        error.status = response.status;
        throw error;
      }
      if (!isJSON) throw new Error("接口尚未就绪：预期收到 JSON 数据");
      return payload;
    } catch (error) {
      if (error.name === "AbortError") throw new Error("请求超时，请刷新数据查看任务状态");
      if (error instanceof TypeError) throw new Error("无法连接本地 API，请确认服务已启动");
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  const requestJSON = (method, body) => ({
    method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
  });

  function rememberItems(items) {
    for (const item of items) {
      if (!Number.isSafeInteger(item.item_id) || item.item_id <= 0) continue;
      state.catalog.set(item.item_id, { ...state.catalog.get(item.item_id), ...item });
      if (item.category) state.categories.add(item.category);
    }
  }

  function openTab(name, focus = false) {
    if (!["overview", "analysis", "backtest", "data"].includes(name)) return;
    state.activeTab = name;
    $$("[data-tab]").forEach((button) => {
      const active = button.dataset.tab === name;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", String(active));
      button.tabIndex = active ? 0 : -1;
      if (active && focus) button.focus();
    });
    $$(".tab-panel").forEach((panel) => {
      const active = panel.id === "view-" + name;
      panel.hidden = !active;
      panel.classList.toggle("active", active);
    });
    if (name === "analysis") renderAnalysis();
    if (name === "backtest") {
      if (state.backtest) renderBacktest(state.backtest);
      else renderEmptyChart("equityChart", "运行回测后显示净值曲线");
    }
    resizeCharts();
  }

  function initTabs() {
    $$("[data-tab]").forEach((button, index, tabs) => {
      button.addEventListener("click", () => openTab(button.dataset.tab));
      button.addEventListener("keydown", (event) => {
        let next;
        if (event.key === "ArrowRight") next = (index + 1) % tabs.length;
        if (event.key === "ArrowLeft") next = (index + tabs.length - 1) % tabs.length;
        if (event.key === "Home") next = 0;
        if (event.key === "End") next = tabs.length - 1;
        if (next !== undefined) {
          event.preventDefault();
          openTab(tabs[next].dataset.tab, true);
        }
      });
    });
    $$("[data-open-tab]").forEach((button) => button.addEventListener("click", () => openTab(button.dataset.openTab)));
  }

  function signalBadge(item) {
    const signal = item?.signal;
    const label = { BUY_WATCH: "看涨观察", SELL_CAUTION: "谨慎卖出", NEUTRAL: "中性" }[signal] || "暂无信号";
    const kind = signal === "BUY_WATCH" ? "buy" : signal === "SELL_CAUTION" ? "sell" : "";
    return '<span class="signal ' + kind + '">' + label + "</span>";
  }

  function itemButton(item) {
    return '<button class="item-link" type="button" data-select-item="' + item.item_id + '">'
      + escapeHTML(displayName(item)) + '</button><span class="item-id">#' + item.item_id + "</span>";
  }

  function watchButton(item) {
    return '<button class="watch-button" type="button" data-watch-item="' + item.item_id
      + '" aria-pressed="' + Boolean(item.is_watched) + '" aria-label="'
      + (item.is_watched ? "取消收藏 " : "收藏 ") + escapeHTML(displayName(item)) + '">'
      + (item.is_watched ? "★" : "☆") + "</button>";
  }

  function tableRows(selector, items, empty = "暂无可用数据") {
    const body = $(selector);
    if (!items.length) {
      body.innerHTML = '<tr><td class="empty-cell" colspan="5">' + escapeHTML(empty) + "</td></tr>";
      return;
    }
    body.innerHTML = items.map((item) => {
      let middle;
      let tail = "";
      if (selector === "#bullishRows") {
        middle = "<td>" + formatRatio(item.prob_up) + '</td><td class="' + changeClass(item.expected_net_roi)
          + '">' + formatRatio(item.expected_net_roi, true) + "</td>";
      } else if (selector === "#thinningRows") {
        middle = "<td>" + fmtMetric(item.depth_within_5pct) + '</td><td class="'
          + changeClass(finite(item.depth_change_24h_pct) ? -Number(item.depth_change_24h_pct) : null)
          + '">' + formatPercent(item.depth_change_24h_pct) + "</td>";
      } else {
        middle = '<td class="' + changeClass(item.change_24h_pct) + '">' + formatPercent(item.change_24h_pct) + "</td>";
        tail = "<td>" + watchButton(item) + "</td>";
      }
      return "<tr><td>" + itemButton(item) + "</td><td>" + formatMoney(item.current_price)
        + "</td>" + middle + "<td>" + signalBadge(item) + "</td>" + tail + "</tr>";
    }).join("");
  }

  function updateCountdown() {
    const next = state.overview?.next_event;
    const now = state.health?.env === "sim" && finite(state.health.last_snapshot_at)
      ? Number(state.health.last_snapshot_at) : Date.now() / 1000;
    if (!next || !finite(next.start_ts) || next.announced_at > now) {
      $("#eventCountdown").textContent = "暂无已公告节日";
      return;
    }
    const seconds = Number(next.start_ts) - now;
    const days = Math.floor(Math.max(0, seconds) / 86400);
    const hours = Math.floor((Math.max(0, seconds) % 86400) / 3600);
    const minutes = Math.floor((Math.max(0, seconds) % 3600) / 60);
    $("#eventCountdown").textContent = next.name + " · " + (seconds <= 0
      ? (next.end_ts >= now ? "进行中" : "已结束")
      : days ? days + " 天 " + hours + " 小时" : hours + " 小时 " + minutes + " 分");
  }

  function renderOverview(data) {
    state.overview = data;
    const lists = ["top_bullish", "top_thinning_depth", "top_movers_24h", "watchlist"];
    lists.forEach((key) => rememberItems(data[key] || []));
    tableRows("#bullishRows", data.top_bullish || []);
    tableRows("#thinningRows", data.top_thinning_depth || []);
    tableRows("#moverRows", data.top_movers_24h || []);
    tableRows("#watchlistRows", data.watchlist || [], "在单品分析中收藏物品，开始持续跟踪");
    $("#rpValue").textContent = finite(data.rp_index?.value) ? formatMoney(data.rp_index.value) + " / RP" : "--";
    $("#rpLevel").textContent = data.rp_index?.inflation_level
      || (finite(data.rp_index?.change_24h_pct) ? formatPercent(data.rp_index.change_24h_pct) : "暂无基准");
    updateCountdown();
    fillItemSelects();
  }

  async function loadOverview() {
    const version = ++state.overviewRequest;
    try {
      const data = await api("/api/overview?horizon=" + $("#overviewHorizon").value);
      if (version === state.overviewRequest) renderOverview(data);
    } catch (error) {
      if (version !== state.overviewRequest) return;
      ["#bullishRows", "#thinningRows", "#moverRows", "#watchlistRows"].forEach((selector) => tableRows(selector, [], error.message));
    }
  }

  async function loadHealth() {
    try {
      const priorJob = state.health?.active_job;
      const health = await api("/api/health", { timeout: 8000 });
      state.health = health;
      state.healthAvailable = true;
      const badge = $("#envBadge");
      badge.textContent = health.env.toUpperCase();
      badge.className = "env-badge " + health.env;
      $("#snapshotTime").textContent = formatDate(health.last_snapshot_at);
      $("#apiStatus").textContent = "API 正常 · " + health.item_count + " 个物品 · "
        + health.bar_count + " 根 K 线 · 配额 " + fmtMetric(health.rate_budget_remaining)
        + (health.active_job ? " · 任务：" + health.active_job : "");
      $("#envNotice").textContent = health.env === "live"
        ? "LIVE 环境 · 仿真已锁定；官方采集需要配置 Token。"
        : "SIM 环境 · 可生成本地仿真数据，历史时间以最新快照为准。";
      updateAdminControls();
      updateCountdown();
      if (priorJob && !health.active_job && !state.busy) {
        await Promise.allSettled([loadItems(), loadOverview(), loadEvents()]);
        if (state.selectedItem) await loadItemDetails();
      }
      return true;
    } catch (error) {
      state.healthAvailable = false;
      $("#envBadge").textContent = "离线";
      $("#envBadge").className = "env-badge";
      $("#apiStatus").textContent = "API 未连接 · " + error.message;
      $("#envNotice").textContent = "连接恢复后可执行数据任务";
      updateAdminControls();
      return false;
    }
  }

  function updateAdminControls() {
    const health = state.health;
    $$("[data-admin-action]").forEach((button) => {
      const action = button.dataset.adminAction;
      let reason = "";
      if (!state.healthAvailable || !health) reason = "等待 API 健康状态";
      else if (action === "simulate" && health.env === "live") reason = "LIVE 环境禁止运行仿真";
      else if (action === "collect" && !health.token_configured) reason = "尚未配置官方 API Token";
      else if (health.active_job || state.busy) reason = "任务运行中，请稍候";
      button.disabled = Boolean(reason);
      button.title = reason;
    });
    $("#runBacktest").disabled = !state.healthAvailable || Boolean(health?.active_job) || state.busy;
  }

  async function loadItems() {
    const version = ++state.itemRequest;
    const query = new URLSearchParams({
      q: $("#itemSearch").value.trim(), category: $("#categoryFilter").value,
      watched_only: String($("#watchedOnly").checked), limit: "100",
    });
    $("#itemList").setAttribute("aria-busy", "true");
    setMessage("#itemListStatus", "正在查询物品…");
    try {
      const items = await api("/api/items?" + query);
      if (version !== state.itemRequest) return;
      if (!Array.isArray(items)) throw new Error("物品接口应返回数组");
      state.items = items;
      rememberItems(items);
      renderItems();
      fillItemSelects();
      setMessage("#itemListStatus", items.length ? "显示 " + items.length + " 个物品" + (items.length === 100 ? "，可搜索缩小范围" : "") : "没有匹配物品");
      if (!state.selectedItem && items.length) await selectItem(items[0].item_id, false);
      return items;
    } catch (error) {
      if (version !== state.itemRequest) return;
      $("#itemList").innerHTML = '<p class="empty-cell">暂无物品数据</p>';
      setMessage("#itemListStatus", error.message, "error");
      return [];
    } finally {
      if (version === state.itemRequest) $("#itemList").setAttribute("aria-busy", "false");
    }
  }

  function renderItems() {
    $("#itemList").innerHTML = state.items.map((item) => {
      const selected = item.item_id === state.selectedItem?.item_id;
      return '<button class="item-option ' + (selected ? "selected" : "")
        + '" type="button" aria-pressed="' + selected + '" data-select-item="' + item.item_id + '">'
        + "<strong>" + escapeHTML(displayName(item)) + '</strong><span class="item-option-price">'
        + formatMoney(item.current_price) + "</span><span>" + escapeHTML(item.category || "未分类")
        + " · #" + item.item_id + '</span><span class="' + changeClass(item.change_24h_pct) + '">'
        + formatPercent(item.change_24h_pct) + "</span></button>";
    }).join("") || '<p class="empty-cell">没有匹配物品</p>';
  }

  function setOptions(selector, options, firstLabel, preferred = null) {
    const select = $(selector);
    const prior = preferred === null ? select.value : String(preferred);
    select.innerHTML = (firstLabel === null ? "" : '<option value="">' + firstLabel + "</option>") + options;
    if ([...select.options].some((option) => option.value === prior)) select.value = prior;
  }

  function fillItemSelects() {
    const categories = [...state.categories].sort();
    const categoryOptions = categories.map((value) => '<option value="' + escapeHTML(value) + '">' + escapeHTML(value) + "</option>").join("");
    setOptions("#categoryFilter", categoryOptions, "全部分类");
    setOptions("#backtestCategory", categoryOptions, "全部分类");
    const items = [...state.catalog.values()].sort((a, b) => displayName(a).localeCompare(displayName(b), "zh-CN"));
    const options = items.map((item) => '<option value="' + item.item_id + '">' + escapeHTML(displayName(item)) + " (#" + item.item_id + ")</option>").join("");
    setOptions("#quoteItem", options, items.length ? null : "暂无物品");
    setOptions("#backtestItem", options, "全部可用物品");
  }

  function renderSelected() {
    const item = state.selectedItem;
    if (!item) return;
    $("#selectedItemName").textContent = displayName(item);
    $("#selectedItemPrice").textContent = formatMoney(item.current_price);
    $("#selectedItemChange").textContent = formatPercent(item.change_24h_pct);
    $("#selectedItemChange").className = "change-value " + changeClass(item.change_24h_pct);
    $("#selectedWatch").textContent = item.is_watched ? "★ 已收藏" : "☆ 收藏";
    $("#selectedWatch").disabled = false;
    $("#selectedWatch").setAttribute("aria-pressed", String(Boolean(item.is_watched)));
    $("#exportCsv").disabled = false;
    $("#exportItemLabel").textContent = "导出：" + displayName(item) + " (#" + item.item_id + ")";
  }

  async function selectItem(itemId, navigate = true) {
    const item = state.catalog.get(Number(itemId));
    if (!item) return;
    state.selectedItem = item;
    state.bars = [];
    state.forecasts = [];
    state.barEvents = [];
    renderItems();
    renderSelected();
    fillItemSelects();
    $("#quoteItem").value = String(item.item_id);
    $("#feeForm").elements.max_stack_size.value = item.max_stack_size || 99;
    if (finite(item.current_price)) {
      $("#feeForm").elements.buy_price.value = Math.round(item.current_price);
      $("#feeForm").elements.sell_price.value = Math.max(1, Math.ceil(Number(item.current_price) * 1.08));
    }
    setMessage("#feeResult", "填写价格与数量后计算");
    $("#forecastSummary").innerHTML = "";
    if (navigate) openTab("analysis");
    ["priceChart", "depthChart", "driversChart"].forEach((id) => renderEmptyChart(id, "正在加载…"));
    await loadItemDetails();
  }

  async function setWatched(itemId, button) {
    const item = state.catalog.get(itemId);
    if (!item) return;
    button.disabled = true;
    try {
      const result = await api("/api/items/" + itemId + "/watch", requestJSON("POST", { is_watched: !item.is_watched }));
      const updated = { ...item, is_watched: result.is_watched };
      state.catalog.set(itemId, updated);
      state.items = state.items.map((entry) => entry.item_id === itemId ? updated : entry)
        .filter((entry) => !$("#watchedOnly").checked || entry.is_watched);
      if (state.selectedItem?.item_id === itemId) state.selectedItem = updated;
      renderSelected();
      renderItems();
      await loadOverview();
      notice("", "");
    } catch (error) {
      notice(error.message);
    } finally {
      button.disabled = false;
    }
  }

  async function loadItemDetails() {
    if (!state.selectedItem) return;
    const itemId = state.selectedItem.item_id;
    const version = ++state.detailRequest;
    const days = state.windowDays === "all" ? 0 : Number(state.windowDays);
    setMessage("#chartStatus", "正在加载 K 线与预测…");
    const results = await Promise.allSettled([
      api("/api/items/" + itemId + "/bars?timeframe=" + state.timeframe + "&days=" + days),
      api("/api/items/" + itemId + "/forecast?horizons=24,72,168"),
    ]);
    if (version !== state.detailRequest || itemId !== state.selectedItem?.item_id) return;
    const [barsResult, forecastsResult] = results;
    const messages = [];
    if (barsResult.status === "fulfilled") {
      const payload = barsResult.value;
      state.bars = [...(payload.bars || [])].sort((a, b) => a.bucket_ts - b.bucket_ts);
      state.barEvents = payload.events || [];
      if (payload.item) {
        rememberItems([payload.item]);
        state.selectedItem = state.catalog.get(itemId);
        renderSelected();
      }
    } else {
      state.bars = [];
      messages.push("K 线：" + barsResult.reason.message);
    }
    if (forecastsResult.status === "fulfilled") {
      state.forecasts = [...(forecastsResult.value.forecasts || [])].sort((a, b) => a.horizon_hours - b.horizon_hours);
    } else {
      state.forecasts = [];
      messages.push("预测：" + forecastsResult.reason.message);
    }
    const latest = state.bars.at(-1);
    setMessage("#chartStatus", messages.join("；") || (latest
      ? "最新 K 线 " + formatDate(latest.close_ts) + " · 预测以最新市场时间为锚点"
      : "暂无 K 线，可在数据中心录入报价或生成 SIM 数据"), messages.length ? "error" : "");
    renderAnalysis();
  }

  function chartInstance(id) {
    if (!window.echarts) {
      const element = document.getElementById(id);
      element.textContent = "本地 ECharts 尚未加载，请检查静态资源";
      element.classList.add("chart-unavailable");
      return null;
    }
    const element = document.getElementById(id);
    if (!state.charts[id] && !element.getBoundingClientRect().width) return null;
    if (!state.charts[id]) state.charts[id] = echarts.init(element);
    return state.charts[id];
  }

  const themeText = () => getComputedStyle(document.documentElement).getPropertyValue("--text-soft").trim();
  const splitLine = () => ({ lineStyle: { color: "rgba(130,150,140,.18)" } });

  function renderEmptyChart(id, message) {
    const chart = chartInstance(id);
    if (!chart) return;
    chart.setOption({
      title: { text: message, left: "center", top: "middle", textStyle: { color: themeText(), fontSize: 12, fontWeight: 400 } },
    }, true);
  }

  function visibleBars() {
    const latest = state.bars.at(-1);
    if (!latest || state.windowDays === "all") return state.bars;
    const cutoff = Number(latest.close_ts ?? latest.bucket_ts) - Number(state.windowDays) * 86400;
    return state.bars.filter((bar) => Number(bar.close_ts ?? bar.bucket_ts) >= cutoff);
  }

  function chartTooltip(params) {
    const points = (Array.isArray(params) ? params : [params])
      .filter((point) => point && Array.isArray(point.value));
    const ts = points[0]?.value?.[0];
    let text = escapeHTML(formatDate(Number(ts) / 1000));
    for (const point of points) {
      const values = point.value;
      if (point.seriesName === "K 线") {
        text += "<br>开 " + formatMoney(values[1]) + " / 收 " + formatMoney(values[2])
          + "<br>低 " + formatMoney(values[3]) + " / 高 " + formatMoney(values[4]);
      } else if (!["Q10–Q90 区间", "预测起点"].includes(point.seriesName)) {
        text += "<br>" + escapeHTML(point.seriesName) + "：" + formatMoney(values[1]);
      }
    }
    return text;
  }

  function renderPriceChart() {
    const chart = chartInstance("priceChart");
    if (!chart) return;
    const rows = visibleBars();
    if (!rows.length) return renderEmptyChart("priceChart", "所选物品暂无 K 线数据");
    const latest = rows.at(-1);
    const anchorTs = Number(latest.close_ts ?? latest.bucket_ts);
    const anchorPrice = latest.book_state && latest.book_state !== "stocked"
      ? null : numeric(latest.close_price ?? latest.robust_best_ask);
    const forecasts = state.forecasts.filter((entry) => [24, 72, 168].includes(entry.horizon_hours));
    const anchorMs = anchorTs * 1000;
    const future = forecasts.map((entry) => ({
      ...entry, ts: anchorMs + entry.horizon_hours * 3600000,
    }));
    const endTs = Math.max(anchorMs, ...future.map((entry) => entry.ts));
    const history = rows.map((bar) => {
      const prices = [
        numeric(bar.open_price), numeric(bar.close_price),
        numeric(bar.low_price), numeric(bar.high_price),
      ];
      const hasPrices = bar.book_state !== "empty_confirmed"
        && prices.every((price) => price !== null);
      // ECharts recognizes "-" as a missing candle dimension. Retain the
      // timestamp so empty books leave a gap rather than becoming zero prices.
      return [Number(bar.bucket_ts) * 1000, ...(hasPrices ? prices : ["-", "-", "-", "-"])];
    });
    const firstTs = Number(rows[0].bucket_ts) * 1000;
    const markers = state.barEvents.filter((event) => event.announced_at <= anchorTs
      && event.start_ts * 1000 >= firstTs && event.start_ts * 1000 <= endTs).map((event) => ({
      xAxis: Number(event.start_ts) * 1000,
      name: event.name,
      label: { formatter: () => event.name, color: themeText(), fontSize: 10 },
    }));
    const candle = {
      name: "K 线", type: "candlestick", data: history, encode: { x: 0, y: [1, 2, 3, 4] },
      itemStyle: { color: "#177b5c", color0: "#b54d47", borderColor: "#177b5c", borderColor0: "#b54d47" },
      barMaxWidth: 16,
    };
    const line = {
      name: "市场价格", type: "line", data: history.map((row) => [row[0], numeric(row[2])]),
      showSymbol: false, connectNulls: false, lineStyle: { color: "#247f87", width: 2 },
    };
    const priceSeries = state.chartType === "candlestick" ? candle : line;
    priceSeries.markLine = {
      symbol: "none", silent: true, lineStyle: { color: "#b45e58", type: "dashed", width: 1 },
      data: markers,
    };
    const quantile = (key) => [
      [anchorMs, anchorPrice],
      ...future.map((entry) => [entry.ts, numeric(entry[key])]),
    ];
    const bandRows = future.filter((entry) => finite(entry.pred_q10) && finite(entry.pred_q90));
    const polygon = [[anchorMs, anchorPrice], ...bandRows.map((entry) => [entry.ts, Number(entry.pred_q10)]),
      ...[...bandRows].reverse().map((entry) => [entry.ts, Number(entry.pred_q90)]), [anchorMs, anchorPrice]];
    const series = [priceSeries];
    if (future.length) {
      if (bandRows.length && anchorPrice !== null) series.push({
        name: "Q10–Q90 区间", type: "custom", silent: true, z: 1, clip: true,
        data: [[anchorMs, anchorPrice]], tooltip: { show: false },
        renderItem: (_params, chartApi) => ({
          type: "polygon", shape: { points: polygon.map((point) => chartApi.coord(point)) },
          style: { fill: "rgba(211,148,58,.18)", stroke: "transparent" },
        }),
      });
      [["Q10", "pred_q10", .65], ["Q50", "pred_q50", 1], ["Q90", "pred_q90", .65]].forEach(([name, key, opacity]) => {
        series.push({
          name, type: "line", data: quantile(key), showSymbol: true, symbolSize: name === "Q50" ? 6 : 3,
          connectNulls: false, z: 3, lineStyle: { color: "#b88632", type: "dashed", width: name === "Q50" ? 2 : 1, opacity },
          itemStyle: { color: "#b88632", opacity },
          label: name === "Q50" ? { show: true, position: "top", color: themeText(), fontSize: 10,
            formatter: (point) => point.dataIndex ? forecasts[point.dataIndex - 1]?.horizon_hours + "h" : "" } : { show: false },
        });
      });
    }
    chart.setOption({
      animation: false, backgroundColor: "transparent",
      tooltip: { trigger: "axis", confine: true, axisPointer: { type: "cross" }, formatter: chartTooltip },
      grid: { left: 68, right: 30, top: 22, bottom: 64 },
      xAxis: { type: "time", min: firstTs, max: endTs,
        axisLabel: { color: themeText(), hideOverlap: true, fontSize: 10, formatter: (value) => formatDate(value / 1000, true) },
        axisLine: { lineStyle: { color: "#aab8b2" } } },
      yAxis: { scale: true, splitLine: splitLine(),
        axisLabel: { color: themeText(), fontSize: 10, formatter: (value) => numberFormat.format(value) } },
      dataZoom: [{ type: "inside", filterMode: "none" },
        { type: "slider", filterMode: "none", height: 18, bottom: 7, borderColor: "transparent",
          textStyle: { color: themeText(), fontSize: 9 } }],
      series,
    }, true);
  }

  function renderDepthChart() {
    const chart = chartInstance("depthChart");
    if (!chart) return;
    const rows = visibleBars();
    if (!rows.length) return renderEmptyChart("depthChart", "暂无盘口或隐含成交数据");
    chart.setOption({
      animation: false, tooltip: { trigger: "axis", confine: true },
      legend: { top: 1, textStyle: { color: themeText(), fontSize: 10 } },
      grid: { left: 52, right: 56, top: 48, bottom: 30 },
      xAxis: { type: "time", axisLabel: { color: themeText(), hideOverlap: true, fontSize: 9,
        formatter: (value) => formatDate(value / 1000, true) } },
      yAxis: [{ type: "value", name: "挂量", nameTextStyle: { color: themeText() },
        axisLabel: { color: themeText(), fontSize: 9 }, splitLine: splitLine() },
      { type: "value", name: "隐含成交", nameTextStyle: { color: themeText() },
        axisLabel: { color: themeText(), fontSize: 9 }, splitLine: { show: false } }],
      series: [
        { name: "总挂量", type: "line", showSymbol: false, connectNulls: false,
          data: rows.map((bar) => [bar.bucket_ts * 1000, numeric(bar.total_listed_qty)]),
          lineStyle: { color: "#94aba1", width: 1 }, areaStyle: { color: "rgba(148,171,161,.12)" } },
        { name: "近盘深度", type: "bar", data: rows.map((bar) => [bar.bucket_ts * 1000, numeric(bar.depth_within_5pct)]),
          itemStyle: { color: "#247f87", opacity: .75 }, barMaxWidth: 12 },
        { name: "估算成交", type: "line", yAxisIndex: 1, showSymbol: false, connectNulls: false,
          data: rows.map((bar) => [bar.bucket_ts * 1000, numeric(bar.est_sold_qty_sum)]),
          lineStyle: { color: "#b88632", width: 2 } },
      ],
    }, true);
  }

  function renderForecastSummary() {
    $("#forecastSummary").innerHTML = [24, 72, 168].map((horizon) => {
      const forecast = state.forecasts.find((entry) => entry.horizon_hours === horizon);
      if (!forecast) return '<div class="forecast-cell"><h3>' + horizon + ' 小时</h3><p class="muted">暂无预测</p></div>';
      return '<div class="forecast-cell"><h3>' + horizon + " 小时 " + signalBadge(forecast) + "</h3>"
        + '<strong class="forecast-median">' + formatMoney(forecast.pred_q50) + '</strong><div class="forecast-range">'
        + "Q10 " + formatMoney(forecast.pred_q10) + " · Q90 " + formatMoney(forecast.pred_q90) + "</div>"
        + '<dl><div><dt>看涨概率</dt><dd>' + formatRatio(forecast.prob_up) + '</dd></div><div><dt>扣税净 ROI</dt><dd class="'
        + changeClass(forecast.expected_net_roi) + '">' + formatRatio(forecast.expected_net_roi, true)
        + "</dd></div></dl><p>" + escapeHTML(forecast.regime_note || "暂无档位说明") + "</p>"
        + '<small>' + escapeHTML(forecast.model_name) + " · " + formatDate(forecast.generated_at) + "</small></div>";
    }).join("");
  }

  function renderDrivers() {
    const chart = chartInstance("driversChart");
    if (!chart) return;
    const forecast = state.forecasts.find((entry) => entry.horizon_hours === 72) || state.forecasts[0];
    const drivers = [...(forecast?.top_drivers || [])].slice(0, 5).reverse();
    $("#driverHorizon").textContent = forecast ? forecast.horizon_hours + " 小时预测 · " + forecast.model_name : "暂无预测数据";
    if (!drivers.length) return renderEmptyChart("driversChart", "暂无驱动因子");
    chart.setOption({
      animation: false, grid: { left: 108, right: 24, top: 10, bottom: 22 },
      xAxis: { type: "value", axisLabel: { color: themeText(), fontSize: 9 }, splitLine: splitLine() },
      yAxis: { type: "category", data: drivers.map((row) => row.feature),
        axisLabel: { color: themeText(), fontSize: 10, width: 95, overflow: "truncate" } },
      series: [{ type: "bar", barMaxWidth: 16, data: drivers.map((row) => {
        const score = numeric(row.contribution);
        return { value: score, itemStyle: { color: row.direction === "negative" || score < 0 ? "#b54d47" : "#247f87" } };
      }) }],
      tooltip: { trigger: "axis", confine: true, renderMode: "richText", axisPointer: { type: "shadow" } },
    }, true);
  }

  function renderAnalysis() {
    renderPriceChart();
    renderDepthChart();
    renderForecastSummary();
    renderDrivers();
    resizeCharts();
  }

  async function submitForm(form, messageSelector, work) {
    const buttons = $$('button[type="submit"]', form);
    buttons.forEach((button) => { button.disabled = true; });
    form.setAttribute("aria-busy", "true");
    setMessage(messageSelector, "正在提交…");
    try { await work(); }
    catch (error) { setMessage(messageSelector, error.message, "error"); }
    finally {
      buttons.forEach((button) => { button.disabled = false; });
      form.setAttribute("aria-busy", "false");
    }
  }

  async function submitFees(event) {
    event.preventDefault();
    const form = event.currentTarget;
    if (!state.selectedItem) return setMessage("#feeResult", "请先选择物品", "error");
    const itemId = state.selectedItem.item_id;
    const body = {
      item_id: itemId, asset_class: state.selectedItem.asset_class,
      max_stack_size: Number(form.elements.max_stack_size.value),
      buy_price: Number(form.elements.buy_price.value), sell_price: Number(form.elements.sell_price.value),
      qty: Number(form.elements.qty.value), relist_rate: form.elements.relist_rate.value,
    };
    await submitForm(form, "#feeResult", async () => {
      const result = await api("/api/fees/simulate", requestJSON("POST", body));
      if (state.selectedItem?.item_id !== itemId) return;
      $("#feeResult").innerHTML = [
        ["槽位", result.slots_used], ["挂单费", formatMoney(result.listing_fee)], ["期望有效费", formatMoney(result.effective_fee)],
        ["净利润", formatMoney(result.net_profit)], ["净 ROI", formatRatio(result.net_roi, true)], ["保本卖价", formatMoney(result.breakeven_sell_price)],
      ].map(([label, value]) => '<span class="result-value"><small>' + label + "</small><strong>" + escapeHTML(value) + "</strong></span>").join("");
    });
  }

  function renderBacktest(result) {
    const rows = result.comparison_table;
    $("#backtestRows").innerHTML = rows.length ? rows.map((row) => {
      const m = row.metrics;
      return "<tr><td><strong>" + escapeHTML(modelLabels[row.model_name] || row.model_name)
        + '</strong></td><td class="' + changeClass(m.total_return) + '">' + formatRatio(m.total_return, true)
        + "</td><td>" + fmtMetric(m.sharpe_ratio) + "</td><td>" + formatRatio(m.max_drawdown)
        + "</td><td>" + formatRatio(m.win_rate) + "</td><td>" + fmtMetric(m.trade_count)
        + "</td><td>" + fmtMetric(m.pinball_loss) + "</td><td>" + fmtMetric(m.roc_auc)
        + "</td><td>" + formatRatio(m.interval_coverage) + "</td></tr>";
    }).join("") : '<tr><td class="empty-cell" colspan="9">没有可用回测结果</td></tr>';
    renderEquity(result.equity_curves);
  }

  function renderEquity(curves) {
    const chart = chartInstance("equityChart");
    if (!chart) return;
    if (!curves.length || !curves.some((curve) => curve.points.length)) return renderEmptyChart("equityChart", "运行回测后显示净值曲线");
    chart.setOption({
      animation: false, color: ["#247f87", "#b88632", "#9175b4"],
      tooltip: { trigger: "axis", confine: true, renderMode: "richText" },
      legend: { top: 2, textStyle: { color: themeText(), fontSize: 10 } },
      grid: { left: 68, right: 24, top: 38, bottom: 40 },
      xAxis: { type: "time", axisLabel: { color: themeText(), hideOverlap: true, fontSize: 9,
        formatter: (value) => formatDate(value / 1000, true) } },
      yAxis: { type: "value", scale: true, axisLabel: { color: themeText(), fontSize: 9 }, splitLine: splitLine() },
      series: curves.map((curve) => ({
        name: modelLabels[curve.model_name] || curve.model_name, type: "line", showSymbol: false, connectNulls: false,
        data: curve.points.map((point) => [point.ts * 1000, numeric(point.equity)]), lineStyle: { width: 2 },
      })),
    }, true);
  }

  async function runBacktest(event) {
    event.preventDefault();
    const form = event.currentTarget;
    const models = [...form.querySelectorAll('input[name="models"]:checked')].map((input) => input.value);
    if (!models.length) return setMessage("#backtestStatus", "至少选择一个模型", "error");
    const body = { horizon_hours: Number(form.elements.horizon_hours.value), models };
    if (form.elements.item_id.value) body.item_id = Number(form.elements.item_id.value);
    else if (form.elements.category.value) body.category = form.elements.category.value;
    if (!state.healthAvailable || state.busy || state.health?.active_job) return;
    state.busy = true;
    updateAdminControls();
    await submitForm(form, "#backtestStatus", async () => {
      setMessage("#backtestStatus", "正在执行回测，请稍候…");
      state.backtest = await api("/api/backtest/run", { ...requestJSON("POST", body), timeout: 600000 });
      renderBacktest(state.backtest);
      setMessage("#backtestStatus", "回测 #" + state.backtest.run_id + " 已完成 · 扣税净收益", "success");
    });
    state.busy = false;
    await loadHealth();
    updateAdminControls();
  }

  function toEpoch(value) {
    const ts = new Date(value).getTime() / 1000;
    if (!Number.isFinite(ts) || ts < 0) throw new Error("请输入有效时间");
    return Math.floor(ts);
  }

  async function submitQuote(event) {
    event.preventDefault();
    const form = event.currentTarget;
    await submitForm(form, "#quoteMessage", async () => {
      const body = { item_id: Number(form.elements.item_id.value), price: Number(form.elements.price.value) };
      if (form.elements.listed_qty.value !== "") body.listed_qty = Number(form.elements.listed_qty.value);
      if (form.elements.recorded_at.value) body.recorded_at = toEpoch(form.elements.recorded_at.value);
      if (form.elements.note.value.trim()) body.note = form.elements.note.value.trim();
      const result = await api("/api/quotes/manual", requestJSON("POST", body));
      setMessage("#quoteMessage", "报价已保存 · " + formatDate(result.recorded_at)
        + " · 保留快照 K 线 " + result.skipped_snapshot_bars + " 根", "success");
      await Promise.all([loadItems(), loadOverview(), loadHealth()]);
      if (state.selectedItem?.item_id === body.item_id) await loadItemDetails();
    });
  }

  async function importCSV(event) {
    event.preventDefault();
    const form = event.currentTarget;
    await submitForm(form, "#csvMessage", async () => {
      const file = form.elements.file.files[0];
      if (!file || !file.size) throw new Error("请选择非空 CSV 文件");
      if (file.size > 5 * 1024 * 1024) throw new Error("文件超过 5 MB，请分批导入");
      const payload = new FormData();
      payload.append("file", file);
      const result = await api("/api/quotes/import-csv", { method: "POST", body: payload, timeout: 180000 });
      const issues = result.errors.slice(0, 4).map((entry) => (entry.row ? "第 " + entry.row + " 行：" : "") + entry.message);
      setMessage("#csvMessage", "导入 " + result.imported_rows + " 行，跳过快照 K 线 "
        + result.skipped_snapshot_bars + " 根；错误 " + result.errors.length + " 条"
        + (issues.length ? " · " + issues.join("；") : ""), result.errors.length ? "warning" : "success");
      form.reset();
      await Promise.all([loadItems(), loadOverview(), loadHealth()]);
      if (state.selectedItem) await loadItemDetails();
    });
  }

  async function exportCSV() {
    if (!state.selectedItem) return setMessage("#csvMessage", "请先在单品分析中选择物品", "error");
    const itemId = state.selectedItem.item_id;
    const timeframe = $("#exportTimeframe").value;
    const button = $("#exportCsv");
    button.disabled = true;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 30000);
    try {
      const response = await fetch("/api/items/" + itemId + "/export.csv?timeframe=" + timeframe,
        { credentials: "same-origin", signal: controller.signal, headers: { Accept: "text/csv" } });
      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        throw new Error(payload?.error?.message || "导出失败（HTTP " + response.status + "）");
      }
      if (!(response.headers.get("content-type") || "").includes("text/csv")) throw new Error("导出接口未返回 CSV");
      const url = URL.createObjectURL(await response.blob());
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = "item-" + itemId + "-" + timeframe + ".csv";
      document.body.append(anchor);
      anchor.click();
      anchor.remove();
      setTimeout(() => URL.revokeObjectURL(url), 2000);
      setMessage("#csvMessage", "已导出 #" + itemId + " 的 CSV", "success");
    } catch (error) {
      setMessage("#csvMessage", error.name === "AbortError" ? "导出超时，请重试" : error.message, "error");
    } finally {
      clearTimeout(timer);
      button.disabled = !state.selectedItem;
    }
  }

  async function loadEvents() {
    try {
      const events = await api("/api/events");
      if (!Array.isArray(events)) throw new Error("事件接口应返回数组");
      state.events = events;
      renderEvents();
    } catch (error) {
      setMessage("#eventMessage", error.message, "error");
      $("#eventList").innerHTML = '<p class="empty-cell">暂无可用事件</p>';
    }
  }

  function renderEvents() {
    $("#eventList").innerHTML = [...state.events].sort((a, b) => a.start_ts - b.start_ts).map((event) =>
      '<div class="event-row"><div><strong>' + escapeHTML(event.name) + ' <span class="signal">'
      + escapeHTML(event.event_type) + "</span></strong><span>公告 " + formatDate(event.announced_at)
      + " · " + formatDate(event.start_ts) + " 至 " + formatDate(event.end_ts) + "</span>"
      + (event.notes ? "<span>" + escapeHTML(event.notes) + "</span>" : "")
      + '</div><button type="button" data-delete-event="' + event.event_id + '" aria-label="删除 '
      + escapeHTML(event.name) + '">删除</button></div>'
    ).join("") || '<p class="empty-cell">暂无事件</p>';
  }

  async function submitEvent(event) {
    event.preventDefault();
    const form = event.currentTarget;
    await submitForm(form, "#eventMessage", async () => {
      const body = {
        name: form.elements.name.value.trim(), event_type: form.elements.event_type.value.trim(),
        announced_at: toEpoch(form.elements.announced_at.value),
        start_ts: toEpoch(form.elements.start_ts.value), end_ts: toEpoch(form.elements.end_ts.value),
        impact_scope: form.elements.impact_scope.value.trim() || "all",
      };
      if (body.end_ts < body.start_ts) throw new Error("结束时间不能早于开始时间");
      if (form.elements.notes.value.trim()) body.notes = form.elements.notes.value.trim();
      await api("/api/events", requestJSON("POST", body));
      form.reset();
      setMessage("#eventMessage", "事件已添加", "success");
      await Promise.all([loadEvents(), loadOverview()]);
      if (state.selectedItem) await loadItemDetails();
    });
  }

  async function deleteEvent(eventId, button) {
    button.disabled = true;
    try {
      await api("/api/events/" + eventId, { method: "DELETE" });
      setMessage("#eventMessage", "事件已删除", "success");
      await Promise.all([loadEvents(), loadOverview()]);
      if (state.selectedItem) await loadItemDetails();
    } catch (error) {
      setMessage("#eventMessage", error.message, "error");
    } finally {
      button.disabled = false;
    }
  }

  async function triggerAdmin(action) {
    if (!state.healthAvailable || state.busy || state.health?.active_job) return;
    if (action === "simulate" && state.health.env !== "sim") {
      return setMessage("#jobMessage", "LIVE 环境禁止运行仿真", "error");
    }
    state.busy = true;
    updateAdminControls();
    setMessage("#jobMessage", "正在执行" + actionLabels[action] + "…");
    try {
      const result = await api("/api/admin/trigger", {
        ...requestJSON("POST", { action }), timeout: 600000,
      });
      setMessage("#jobMessage", actionLabels[action] + "任务：" + result.status
        + (result.active_job ? " · " + result.active_job : ""), "success");
      await Promise.all([loadItems(), loadOverview(), loadEvents()]);
      if (state.selectedItem) await loadItemDetails();
    } catch (error) {
      setMessage("#jobMessage", error.message, "error");
    } finally {
      state.busy = false;
      await loadHealth();
      updateAdminControls();
    }
  }

  function resizeCharts() {
    requestAnimationFrame(() => Object.values(state.charts).forEach((chart) => {
      const element = chart.getDom();
      if (element.getBoundingClientRect().width > 0) chart.resize();
    }));
  }

  function toggleTheme() {
    const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    safeStorage("gtl-theme", next);
    $("#themeToggle").setAttribute("aria-pressed", String(next === "dark"));
    Object.values(state.charts).forEach((chart) => chart.dispose());
    state.charts = {};
    renderAnalysis();
    if (state.backtest) renderBacktest(state.backtest);
  }

  function bindActions() {
    document.addEventListener("click", (event) => {
      const select = event.target.closest("[data-select-item]");
      if (select) selectItem(Number(select.dataset.selectItem));
      const watch = event.target.closest("[data-watch-item]");
      if (watch) setWatched(Number(watch.dataset.watchItem), watch);
      const remove = event.target.closest("[data-delete-event]");
      if (remove) deleteEvent(Number(remove.dataset.deleteEvent), remove);
    });
    $("#selectedWatch").addEventListener("click", (event) => {
      if (state.selectedItem) setWatched(state.selectedItem.item_id, event.currentTarget);
    });
    let timer;
    const search = (value) => {
      $("#itemSearch").value = value;
      $("#globalSearch").value = value;
      clearTimeout(timer);
      timer = setTimeout(loadItems, 220);
    };
    ["#itemSearch", "#globalSearch"].forEach((selector) => $(selector).addEventListener("input", (event) => {
      if (selector === "#globalSearch") {
        $("#categoryFilter").value = "";
        $("#watchedOnly").checked = false;
      }
      search(event.target.value);
    }));
    $("#globalSearch").addEventListener("keydown", async (event) => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      clearTimeout(timer);
      openTab("analysis");
      const results = await loadItems();
      if (results?.length) await selectItem(results[0].item_id);
    });
    $("#categoryFilter").addEventListener("change", loadItems);
    $("#watchedOnly").addEventListener("change", loadItems);
    $("#overviewHorizon").addEventListener("change", loadOverview);
    $("#timeframeSelect").addEventListener("change", (event) => {
      state.timeframe = event.target.value;
      loadItemDetails();
    });
    $("#refreshBars").addEventListener("click", loadItemDetails);
    $$("[data-window]").forEach((button) => button.addEventListener("click", () => {
      state.windowDays = button.dataset.window;
      $$("[data-window]").forEach((entry) => entry.classList.toggle("selected", entry === button));
      loadItemDetails();
    }));
    $$("[data-chart-type]").forEach((button) => button.addEventListener("click", () => {
      state.chartType = button.dataset.chartType;
      $$("[data-chart-type]").forEach((entry) => entry.classList.toggle("selected", entry === button));
      renderPriceChart();
    }));
    $("#backtestItem").addEventListener("change", () => {
      if ($("#backtestItem").value) $("#backtestCategory").value = "";
    });
    $("#backtestCategory").addEventListener("change", () => {
      if ($("#backtestCategory").value) $("#backtestItem").value = "";
    });
    $("#feeForm").addEventListener("submit", submitFees);
    $("#backtestForm").addEventListener("submit", runBacktest);
    $("#quoteForm").addEventListener("submit", submitQuote);
    $("#csvImportForm").addEventListener("submit", importCSV);
    $("#eventForm").addEventListener("submit", submitEvent);
    $("#exportCsv").addEventListener("click", exportCSV);
    $("#refreshEvents").addEventListener("click", loadEvents);
    $("#refreshAll").addEventListener("click", refreshAll);
    $("#themeToggle").addEventListener("click", toggleTheme);
    $$("[data-admin-action]").forEach((button) => button.addEventListener("click", () => triggerAdmin(button.dataset.adminAction)));
  }

  async function refreshAll() {
    $("#refreshAll").disabled = true;
    try {
      await Promise.allSettled([loadHealth(), loadOverview(), loadEvents(), loadItems()]);
      if (state.selectedItem) await loadItemDetails();
    } finally {
      $("#refreshAll").disabled = false;
    }
  }

  async function init() {
    const saved = safeStorage("gtl-theme");
    const dark = saved ? saved === "dark" : window.matchMedia?.("(prefers-color-scheme: dark)").matches;
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    $("#themeToggle").setAttribute("aria-pressed", String(Boolean(dark)));
    initTabs();
    bindActions();
    updateAdminControls();
    ["#bullishRows", "#thinningRows", "#moverRows", "#watchlistRows"].forEach((selector) => tableRows(selector, [], "正在读取市场数据…"));
    $("#backtestRows").innerHTML = '<tr><td colspan="9" class="empty-cell">运行回测后显示模型对比</td></tr>';
    renderForecastSummary();
    if (!window.echarts) notice("本地 ECharts 资源加载失败，请检查 /static/vendor/echarts.min.js");
    await refreshAll();
    window.addEventListener("resize", resizeCharts);
    setInterval(() => {
      updateCountdown();
      if (!document.hidden) {
        loadHealth();
        if (state.health?.active_job) loadOverview();
      }
    }, 15000);
    window.addEventListener("unhandledrejection", (event) => {
      notice(event.reason?.message || "页面操作未完成，请刷新数据");
      event.preventDefault();
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    init().catch((error) => notice(error.message || "工作台初始化失败"));
  });
})();
