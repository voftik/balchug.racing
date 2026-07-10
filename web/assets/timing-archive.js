(function () {
  "use strict";

  var API = "/api/timing";
  var $ = function (id) { return document.getElementById(id); };
  var root = $("timingArchive");
  if (!root) return;

  var elements = {
    select: $("timingSession"),
    empty: $("timingArchiveEmpty"),
    body: $("timingArchiveBody"),
    flag: $("timingFlag"),
    observed: $("timingObserved"),
    kpis: $("timingKpis"),
    chart: $("timingChart"),
    chartRange: $("timingChartRange"),
    events: $("timingEvents"),
    play: $("timingPlay"),
    stepBack: $("timingStepBack"),
    stepForward: $("timingStepForward"),
    reset: $("timingReset"),
    range: $("timingRange"),
    rate: $("timingRate"),
    time: $("timingTime")
  };

  var state = {
    entries: [],
    entry: null,
    manifest: null,
    atUs: null,
    payload: null,
    effectiveAtUs: null,
    playing: false,
    rate: 4,
    raf: 0,
    lastAnimationMs: 0,
    snapshotController: null,
    manifestController: null,
    snapshotTimer: 0,
    snapshotRequestId: 0,
    activeSnapshotRequestId: 0,
    snapshotInFlight: false,
    pendingSnapshotAtUs: null,
    lastSnapshotStartedMs: 0,
    selectionEpoch: 0,
    exactSnapshots: new Map(),
    renderedEffectiveAtUs: null,
    events: [],
    criticalEvents: [],
    lapEvents: [],
    visibleEvents: [],
    visibleEventWindowKey: "",
    eventWindowKey: "",
    chartBase: null,
    chartBaseKey: "",
    chartRangeText: "",
    observedText: ""
  };

  function asObject(value) { return value && typeof value === "object" ? value : {}; }
  function valueOrDash(value) { return value === null || value === undefined || value === "" ? "—" : String(value); }

  function formatLap(milliseconds) {
    if (typeof milliseconds !== "number" || !isFinite(milliseconds) || milliseconds < 0) return "—";
    var rounded = Math.round(milliseconds);
    var minutes = Math.floor(rounded / 60000);
    var seconds = Math.floor((rounded % 60000) / 1000);
    var millis = rounded % 1000;
    return minutes + ":" + String(seconds).padStart(2, "0") + "." + String(millis).padStart(3, "0");
  }

  function formatGap(milliseconds) {
    if (typeof milliseconds !== "number" || !isFinite(milliseconds)) return "—";
    return (milliseconds < 0 ? "-" : "+") + (Math.abs(milliseconds) / 1000).toFixed(3) + "с";
  }

  function formatElapsed(seconds) {
    seconds = Math.max(0, Math.floor(seconds || 0));
    var hours = Math.floor(seconds / 3600);
    var minutes = Math.floor((seconds % 3600) / 60);
    var rest = seconds % 60;
    return String(hours).padStart(2, "0") + ":" + String(minutes).padStart(2, "0") + ":" + String(rest).padStart(2, "0");
  }

  function parseElapsed(value) {
    var match = String(value || "").trim().match(/^(\d{1,3}):([0-5]?\d):([0-5]?\d)(?:[.,](\d{1,3}))?$/);
    if (!match) return null;
    var fractional = match[4] ? Number((match[4] + "000").slice(0, 3)) / 1000 : 0;
    return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3]) + fractional;
  }

  function archiveTimezone(timezone) {
    return timezone || (state.manifest && state.manifest.session && state.manifest.session.timezone_name) || "Europe/Moscow";
  }

  function formatAbsolute(atUs, timezone) {
    if (typeof atUs !== "number") return "—";
    try {
      return new Intl.DateTimeFormat("ru-RU", {
        timeZone: archiveTimezone(timezone),
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      }).format(new Date(atUs / 1000));
    } catch (error) {
      return new Date(atUs / 1000).toISOString().slice(11, 19);
    }
  }

  function formatEntryDate(atUs, timezone) {
    if (typeof atUs !== "number") return "—";
    try {
      return new Intl.DateTimeFormat("ru-RU", {
        timeZone: archiveTimezone(timezone),
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit"
      }).format(new Date(atUs / 1000));
    } catch (error) {
      return new Date(atUs / 1000).toISOString().replace("T", " ").slice(0, 16);
    }
  }

  function modeLabel(mode) {
    return { practice: "Практика", qualifying: "Квалификация", race: "Гонка" }[mode] || valueOrDash(mode);
  }

  function flagClass(flag) {
    return "flag-" + String(flag || "").toLowerCase().replace(/[^a-z0-9]+/g, "-");
  }

  function flagLabel(flag) {
    return {
      GREEN: "Green flag",
      RED: "Red flag",
      FCY: "Full Course Yellow",
      FULL_COURSE_YELLOW: "Full Course Yellow",
      SAFETY_CAR: "Safety Car",
      CODE_60: "Code 60",
      FINISH: "Finish flag",
      READY: "Ready",
      NOT_STARTED: "Not started",
      UNKNOWN: "Unknown"
    }[flag] || valueOrDash(flag);
  }

  function flagColor(flag) {
    return {
      GREEN: "rgba(46,172,114,.18)",
      RED: "rgba(240,20,61,.18)",
      FCY: "rgba(216,154,0,.2)",
      FULL_COURSE_YELLOW: "rgba(216,154,0,.2)",
      CODE_60: "rgba(216,154,0,.2)",
      SAFETY_CAR: "rgba(125,85,184,.18)",
      FINISH: "rgba(48,57,70,.18)"
    }[flag] || "rgba(110,126,152,.14)";
  }

  function fetchJson(url, options) {
    return fetch(url, options).then(function (response) {
      if (response.ok) return response.json();
      return response.json().catch(function () { return {}; }).then(function (payload) {
        var message = payload && (payload.detail || payload.message);
        throw new Error(message || ("HTTP " + response.status));
      });
    });
  }

  function setEmpty(message) {
    elements.empty.textContent = message;
    elements.empty.hidden = false;
    elements.body.hidden = true;
  }

  function controlsDisabled(disabled) {
    [elements.play, elements.stepBack, elements.stepForward, elements.reset, elements.range, elements.rate, elements.time].forEach(function (element) {
      element.disabled = disabled;
    });
  }

  function storeKey() { return "balchug_timing_archive_selection"; }

  function selectEntryId(entry) { return entry.session.id + ":" + entry.heat.generation; }

  function populateSessions(items) {
    state.entries = [];
    elements.select.replaceChildren();
    items.forEach(function (item) {
      (item.heats || []).forEach(function (heat) {
        state.entries.push({ session: item.session, heat: heat });
      });
    });
    if (!state.entries.length) {
      elements.select.disabled = true;
      setEmpty("Для архива пока нет сессий с сохранённой телеметрией.");
      return;
    }
    state.entries.forEach(function (entry) {
      var option = document.createElement("option");
      option.value = selectEntryId(entry);
      var duration = Math.max(0, Math.round((entry.heat.last_at_us - entry.heat.first_at_us) / 1000000));
      option.textContent = formatEntryDate(entry.heat.first_at_us, entry.session.timezone_name) + " · " +
        (entry.session.source_name || entry.session.source_slug || "Трасса") + " · " +
        modeLabel(entry.session.mode) + " · " + (entry.heat.external_name || ("Heat " + entry.heat.generation)) + " · " + formatElapsed(duration);
      elements.select.appendChild(option);
    });
    elements.select.disabled = false;
    var saved = null;
    try { saved = localStorage.getItem(storeKey()); } catch (error) {}
    var selected = state.entries.some(function (entry) { return selectEntryId(entry) === saved; }) ? saved : selectEntryId(state.entries[0]);
    elements.select.value = selected;
    loadSelectedEntry();
  }

  function currentEntry() {
    var selected = elements.select.value;
    return state.entries.find(function (entry) { return selectEntryId(entry) === selected; }) || null;
  }

  function loadSelectedEntry() {
    stopPlayback();
    var entry = currentEntry();
    if (!entry) return;
    state.selectionEpoch += 1;
    var epoch = state.selectionEpoch;
    if (state.manifestController) state.manifestController.abort();
    if (state.snapshotController) state.snapshotController.abort();
    window.clearTimeout(state.snapshotTimer);
    state.snapshotRequestId += 1;
    state.activeSnapshotRequestId = 0;
    state.snapshotInFlight = false;
    state.pendingSnapshotAtUs = null;
    state.entry = entry;
    state.manifest = null;
    state.payload = null;
    state.effectiveAtUs = null;
    state.atUs = null;
    state.exactSnapshots = new Map();
    state.renderedEffectiveAtUs = null;
    state.events = [];
    state.criticalEvents = [];
    state.lapEvents = [];
    state.visibleEvents = [];
    state.visibleEventWindowKey = "";
    state.eventWindowKey = "";
    state.chartBase = null;
    state.chartBaseKey = "";
    state.chartRangeText = "";
    state.observedText = "";
    controlsDisabled(true);
    setEmpty("Загрузка телеметрической сессии…");
    try { localStorage.setItem(storeKey(), selectEntryId(entry)); } catch (error) {}
    var url = API + "/sessions/" + encodeURIComponent(entry.session.id) + "/archive?generation=" + encodeURIComponent(entry.heat.generation) + "&max_points=720";
    state.manifestController = new AbortController();
    fetchJson(url, { signal: state.manifestController.signal }).then(function (manifest) {
      if (epoch !== state.selectionEpoch) return;
      state.manifest = manifest;
      state.atUs = manifest.range.first_at_us;
      elements.range.min = "0";
      elements.range.max = String(Math.max(0.1, (manifest.range.last_at_us - manifest.range.first_at_us) / 1000000));
      elements.range.step = "0.1";
      elements.body.hidden = false;
      elements.empty.hidden = true;
      controlsDisabled(false);
      buildEvents();
      setAt(state.atUs, true);
    }).catch(function (error) {
      if ((error && error.name === "AbortError") || epoch !== state.selectionEpoch) return;
      setEmpty("Архивная телеметрия недоступна: " + error.message);
    });
  }

  function keyframeAt(atUs) {
    var frames = state.manifest && state.manifest.keyframes || [];
    var low = 0;
    var high = frames.length - 1;
    var match = frames[0] || null;
    while (low <= high) {
      var middle = Math.floor((low + high) / 2);
      if (frames[middle].observed_at_us <= atUs) {
        match = frames[middle];
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    return match;
  }

  function setAt(atUs, requestExact) {
    if (!state.manifest) return;
    var range = state.manifest.range;
    state.atUs = Math.max(range.first_at_us, Math.min(range.last_at_us, Math.round(atUs)));
    elements.range.value = String((state.atUs - range.first_at_us) / 1000000);
    elements.time.value = formatElapsed((state.atUs - range.first_at_us) / 1000000);
    renderConfirmedSnapshot();
    drawChart();
    renderEvents();
    if (requestExact) scheduleExactSnapshot();
  }

  function scheduleExactSnapshot() {
    if (!state.manifest) return;
    window.clearTimeout(state.snapshotTimer);
    if (state.snapshotInFlight) {
      state.pendingSnapshotAtUs = state.atUs;
      if (!state.playing && state.snapshotController) state.snapshotController.abort();
      return;
    }
    if (state.playing) {
      if (performance.now() - state.lastSnapshotStartedMs < 160) {
        state.pendingSnapshotAtUs = state.atUs;
        return;
      }
      requestExactSnapshot();
      return;
    }
    state.snapshotTimer = window.setTimeout(requestExactSnapshot, 90);
  }

  function requestExactSnapshot() {
    if (!state.manifest || !state.entry) return;
    if (state.snapshotInFlight) {
      state.pendingSnapshotAtUs = state.atUs;
      return;
    }
    state.snapshotController = new AbortController();
    state.lastSnapshotStartedMs = performance.now();
    var requestedAtUs = state.atUs;
    var requestId = ++state.snapshotRequestId;
    state.activeSnapshotRequestId = requestId;
    state.snapshotInFlight = true;
    var epoch = state.selectionEpoch;
    var entryId = selectEntryId(state.entry);
    var url = API + "/sessions/" + encodeURIComponent(state.entry.session.id) + "/archive/snapshot?generation=" +
      encodeURIComponent(state.entry.heat.generation) + "&at_us=" + encodeURIComponent(requestedAtUs);
    fetchJson(url, { signal: state.snapshotController.signal }).then(function (response) {
      if (epoch !== state.selectionEpoch || requestId !== state.snapshotRequestId || !state.entry || selectEntryId(state.entry) !== entryId) return;
      var effectiveAtUs = response && response.playback && response.playback.effective_at_us;
      if (typeof effectiveAtUs !== "number" || !response.snapshot) return;
      cacheExactSnapshot(response.snapshot, effectiveAtUs);
      renderConfirmedSnapshot();
    }).catch(function (error) {
      if (error && error.name === "AbortError") return;
    }).then(function () {
      finishExactSnapshot(epoch, requestId);
    });
  }

  function finishExactSnapshot(epoch, requestId) {
    if (epoch !== state.selectionEpoch || requestId !== state.activeSnapshotRequestId) return;
    state.snapshotInFlight = false;
    state.activeSnapshotRequestId = 0;
    state.snapshotController = null;
    if (state.pendingSnapshotAtUs === null || !state.manifest) return;
    state.pendingSnapshotAtUs = null;
    var delay = state.playing ? Math.max(0, 160 - (performance.now() - state.lastSnapshotStartedMs)) : 0;
    if (delay > 0) state.snapshotTimer = window.setTimeout(requestExactSnapshot, delay);
    else requestExactSnapshot();
  }

  function cacheExactSnapshot(payload, effectiveAtUs) {
    if (state.exactSnapshots.has(effectiveAtUs)) return;
    state.exactSnapshots.set(effectiveAtUs, payload);
    while (state.exactSnapshots.size > 360) {
      state.exactSnapshots.delete(state.exactSnapshots.keys().next().value);
    }
  }

  function exactSnapshotAt(atUs) {
    var match = null;
    state.exactSnapshots.forEach(function (payload, effectiveAtUs) {
      if (effectiveAtUs <= atUs && (!match || effectiveAtUs > match.effectiveAtUs)) {
        match = { payload: payload, effectiveAtUs: effectiveAtUs };
      }
    });
    return match;
  }

  function renderConfirmedSnapshot() {
    if (!state.manifest || state.atUs === null) return;
    var snapshot = exactSnapshotAt(state.atUs);
    if (!snapshot && state.atUs === state.manifest.range.first_at_us) {
      var first = keyframeAt(state.atUs);
      if (first && first.observed_at_us === state.atUs) {
        snapshot = { payload: first.snapshot, effectiveAtUs: first.observed_at_us };
      }
    }
    if (!snapshot) {
      if (state.effectiveAtUs !== null && state.effectiveAtUs > state.atUs) clearFutureSnapshot();
      return;
    }
    if (state.renderedEffectiveAtUs === snapshot.effectiveAtUs && state.payload === snapshot.payload) {
      updateSnapshotTiming(snapshot.effectiveAtUs);
      return;
    }
    state.payload = snapshot.payload;
    state.effectiveAtUs = snapshot.effectiveAtUs;
    state.renderedEffectiveAtUs = snapshot.effectiveAtUs;
    renderSnapshot(snapshot.payload, snapshot.effectiveAtUs);
  }

  function clearFutureSnapshot() {
    state.payload = null;
    state.effectiveAtUs = null;
    state.renderedEffectiveAtUs = null;
    elements.flag.className = "ta-flag";
    elements.flag.querySelector("strong").textContent = "Загрузка среза";
    state.observedText = "курсор " + formatElapsed((state.atUs - state.manifest.range.first_at_us) / 1000000);
    elements.observed.textContent = state.observedText;
    elements.kpis.replaceChildren();
  }

  function updateSnapshotTiming(effectiveAtUs) {
    if (!state.manifest || typeof effectiveAtUs !== "number") return;
    var captured = formatElapsed((effectiveAtUs - state.manifest.range.first_at_us) / 1000000);
    var cursor = formatElapsed((state.atUs - state.manifest.range.first_at_us) / 1000000);
    var observedText = "срез " + captured + " · курсор " + cursor + " · " + formatAbsolute(effectiveAtUs);
    if (observedText !== state.observedText) {
      state.observedText = observedText;
      elements.observed.textContent = observedText;
    }
  }

  function renderSnapshot(payload, effectiveAtUs) {
    var snapshot = asObject(payload);
    var measured = asObject(snapshot.measured);
    var ours = asObject(measured.ours);
    var oursState = asObject(ours.state);
    var computed = asObject(snapshot.computed);
    var session = asObject(computed.session);
    var flag = asObject(measured.track_flag);
    var flagValue = flag.flag || session.track_flag;
    elements.flag.className = "ta-flag " + flagClass(flagValue);
    elements.flag.querySelector("strong").textContent = flagLabel(flagValue);
    updateSnapshotTiming(effectiveAtUs);

    var values = [
      ["POS", session.position_overall !== null && session.position_overall !== undefined ? "P" + session.position_overall : oursState.position_overall !== null && oursState.position_overall !== undefined ? "P" + oursState.position_overall : "—"],
      ["PIC", session.position_class !== null && session.position_class !== undefined ? "P" + session.position_class : oursState.position_class !== null && oursState.position_class !== undefined ? "P" + oursState.position_class : "—"],
      ["Круги", session.completed_laps !== null && session.completed_laps !== undefined ? session.completed_laps : oursState.laps],
      ["STATE", session.current_state || oursState.state_kind || oursState.state],
      ["Last", formatLap(session.last_lap_ms || oursState.last_lap_ms)],
      ["Pace5", formatLap(session.pace_5_ms)],
      ["Впереди", formatGap(session.gap_to_ahead_ms)],
      ["Сзади", formatGap(session.gap_to_behind_ms)],
      ["Шины", session.tyre_age_laps !== null && session.tyre_age_laps !== undefined ? session.tyre_age_laps + "L" : "—"],
      ["Питы", session.pits_completed !== null && session.pits_completed !== undefined ? session.pits_completed : "—"],
      ["Best", formatLap(session.best_lap_ms || oursState.best_lap_ms)],
      ["Экипаж", ours.start_number ? "#" + ours.start_number : session.ours_identity && session.ours_identity.start_number ? "#" + session.ours_identity.start_number : "—"]
    ];
    elements.kpis.replaceChildren();
    values.forEach(function (item) {
      var cell = document.createElement("div");
      cell.className = "ta-kpi";
      var value = document.createElement("b");
      value.textContent = valueOrDash(item[1]);
      value.title = value.textContent;
      var label = document.createElement("span");
      label.textContent = item[0];
      cell.appendChild(value);
      cell.appendChild(label);
      elements.kpis.appendChild(cell);
    });
  }

  function pointValue(point, key) {
    var snapshot = asObject(point.snapshot);
    var computed = asObject(snapshot.computed);
    var session = asObject(computed.session);
    var value = session[key];
    return typeof value === "number" && isFinite(value) ? value : null;
  }

  function drawStaticChart(context, width, height) {
    var padding = { left: 42, right: 10, top: 16, bottom: 16 };
    var usableWidth = Math.max(1, width - padding.left - padding.right);
    var range = state.manifest.range;
    var total = Math.max(1, range.last_at_us - range.first_at_us);
    var xAt = function (atUs) { return padding.left + (atUs - range.first_at_us) / total * usableWidth; };

    context.strokeStyle = "#E4E9F0";
    context.lineWidth = 1;
    [0.25, 0.5, 0.75].forEach(function (ratioY) {
      var y = Math.round(height * ratioY) + 0.5;
      context.beginPath(); context.moveTo(padding.left, y); context.lineTo(width - padding.right, y); context.stroke();
    });

    (state.manifest.markers.flags || []).forEach(function (flag) {
      var start = Math.max(range.first_at_us, flag.started_at_us || range.first_at_us);
      var end = Math.min(range.last_at_us, flag.ended_at_us || range.last_at_us);
      if (end < start) return;
      context.fillStyle = flagColor(flag.flag);
      context.fillRect(xAt(start), 0, Math.max(1, xAt(end) - xAt(start)), height);
    });

    function drawSeries(key, top, bandHeight, color, label, formatter) {
      var frames = state.manifest.keyframes || [];
      var values = frames.map(function (point) { return pointValue(point, key); }).filter(function (value) { return value !== null; });
      context.fillStyle = "#6E7E98";
      context.font = "10px Arial";
      context.fillText(label, 2, top + 10);
      if (!values.length) return;
      var min = Math.min.apply(Math, values);
      var max = Math.max.apply(Math, values);
      if (min === max) { min -= 1; max += 1; }
      var yAt = function (value) { return top + bandHeight - (value - min) / (max - min) * bandHeight; };
      context.strokeStyle = color;
      context.lineWidth = 1.6;
      context.beginPath();
      var previous = null;
      frames.forEach(function (point) {
        var value = pointValue(point, key);
        var x = xAt(point.observed_at_us);
        if (value === null) { previous = null; return; }
        var y = yAt(value);
        if (previous === null) context.moveTo(x, y);
        else { context.lineTo(x, previous.y); context.lineTo(x, y); }
        previous = { y: y };
      });
      context.stroke();
      context.fillStyle = "#6E7E98";
      context.fillText(formatter(min), width - padding.right - 46, top + 10);
      context.fillText(formatter(max), width - padding.right - 46, top + bandHeight);
    }

    drawSeries("pace_5_ms", 16, 57, "#F0143D", "Pace5", formatLap);
    drawSeries("gap_to_ahead_ms", 96, 57, "#1B365D", "Gap", formatGap);

    (state.manifest.markers.pits || []).forEach(function (pit) {
      if (pit.entered_at_us < range.first_at_us || pit.entered_at_us > range.last_at_us) return;
      var x = xAt(pit.entered_at_us);
      context.strokeStyle = "#122846";
      context.lineWidth = 1;
      context.beginPath(); context.moveTo(x, 0); context.lineTo(x, height); context.stroke();
    });
  }

  function drawChart() {
    if (!state.manifest) return;
    var canvas = elements.chart;
    var rect = canvas.getBoundingClientRect();
    var width = Math.max(1, Math.round(rect.width));
    var height = Math.max(1, Math.round(rect.height));
    var ratio = window.devicePixelRatio || 1;
    if (canvas.width !== width * ratio || canvas.height !== height * ratio) {
      canvas.width = width * ratio;
      canvas.height = height * ratio;
    }
    var range = state.manifest.range;
    var baseKey = [
      state.manifest.heat && state.manifest.heat.source_heat_id,
      range.first_at_us,
      range.last_at_us,
      state.manifest.keyframes.length,
      (state.manifest.markers.flags || []).length,
      (state.manifest.markers.pits || []).length,
      width,
      height,
      ratio
    ].join(":");
    if (!state.chartBase || state.chartBaseKey !== baseKey) {
      var base = document.createElement("canvas");
      base.width = width * ratio;
      base.height = height * ratio;
      var baseContext = base.getContext("2d");
      baseContext.setTransform(ratio, 0, 0, ratio, 0, 0);
      baseContext.clearRect(0, 0, width, height);
      drawStaticChart(baseContext, width, height);
      state.chartBase = base;
      state.chartBaseKey = baseKey;
    }
    var context = canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, width, height);
    context.drawImage(state.chartBase, 0, 0, width, height);
    var padding = { left: 42, right: 10 };
    var usableWidth = Math.max(1, width - padding.left - padding.right);
    var total = Math.max(1, range.last_at_us - range.first_at_us);
    var xAt = function (atUs) { return padding.left + (atUs - range.first_at_us) / total * usableWidth; };
    var playheadX = xAt(state.atUs);
    context.strokeStyle = "#F0143D";
    context.lineWidth = 2;
    context.beginPath(); context.moveTo(playheadX, 0); context.lineTo(playheadX, height); context.stroke();
    var chartRangeText = formatElapsed((state.atUs - range.first_at_us) / 1000000) + " / " + formatElapsed((range.last_at_us - range.first_at_us) / 1000000);
    if (chartRangeText !== state.chartRangeText) {
      state.chartRangeText = chartRangeText;
      elements.chartRange.textContent = chartRangeText;
    }
  }

  function buildEvents() {
    if (!state.manifest) return;
    var events = [];
    (state.manifest.markers.flags || []).forEach(function (flag, index) {
      if (typeof flag.started_at_us !== "number") return;
      events.push({
        id: "flag:" + flag.started_at_us + ":" + index,
        kind: "flag",
        atUs: flag.started_at_us,
        title: flagLabel(flag.flag),
        detail: flag.provider_label || "Смена статуса трассы"
      });
    });
    (state.manifest.markers.pits || []).forEach(function (pit, index) {
      if (typeof pit.entered_at_us !== "number") return;
      events.push({
        id: "pit:" + pit.entered_at_us + ":" + pit.stop_number + ":" + index,
        kind: "pit",
        atUs: pit.entered_at_us,
        title: "Пит-стоп #" + pit.stop_number,
        detail: pit.completed && typeof pit.pit_lane_ms === "number" ? formatLap(pit.pit_lane_ms) : "Въезд в пит-лейн"
      });
    });
    (state.manifest.markers.laps || []).forEach(function (lap, index) {
      if (typeof lap.completed_at_us !== "number") return;
      events.push({
        id: "lap:" + lap.completed_at_us + ":" + lap.lap_number + ":" + index,
        kind: "lap",
        atUs: lap.completed_at_us,
        title: "Круг " + lap.lap_number,
        detail: formatLap(lap.duration_ms)
      });
    });
    events.sort(function (left, right) { return left.atUs - right.atUs || left.id.localeCompare(right.id); });
    state.events = events;
    state.criticalEvents = events.filter(function (event) { return event.kind !== "lap"; });
    state.lapEvents = events.filter(function (event) { return event.kind === "lap"; });
    state.visibleEvents = [];
    state.visibleEventWindowKey = "";
    state.eventWindowKey = "";
  }

  function localLapWindow() {
    var laps = state.lapEvents;
    if (laps.length <= 24) return { key: "all", events: laps };
    var low = 0;
    var high = laps.length;
    while (low < high) {
      var middle = Math.floor((low + high) / 2);
      if (laps[middle].atUs < state.atUs) low = middle + 1;
      else high = middle;
    }
    var start = Math.max(0, low - 12);
    var end = Math.min(laps.length, low + 12);
    return { key: start + ":" + end, events: laps.slice(start, end) };
  }

  function visibleEvents() {
    var local = localLapWindow();
    if (state.visibleEventWindowKey === local.key) return state.visibleEvents;
    state.visibleEventWindowKey = local.key;
    state.visibleEvents = state.criticalEvents.concat(local.events).sort(function (left, right) { return left.atUs - right.atUs || left.id.localeCompare(right.id); });
    return state.visibleEvents;
  }

  function renderEvents() {
    if (!state.manifest) return;
    var events = visibleEvents();
    var activeBucket = Math.floor(state.atUs / 2_000_000);
    var key = activeBucket + "|" + events.map(function (event) { return event.id; }).join("|");
    if (key === state.eventWindowKey) return;
    state.eventWindowKey = key;
    elements.events.replaceChildren();
    if (!events.length) {
      var empty = document.createElement("div");
      empty.className = "ta-side-empty";
      empty.textContent = "В сохранённом интервале нет отдельных событий.";
      elements.events.appendChild(empty);
      return;
    }
    events.forEach(function (event) {
      var node = document.createElement("button");
      node.type = "button";
      node.className = "ta-event" + (Math.abs(event.atUs - state.atUs) < 2_000_000 ? " active" : "");
      var time = document.createElement("time");
      time.textContent = formatElapsed((event.atUs - state.manifest.range.first_at_us) / 1000000);
      var content = document.createElement("div");
      var title = document.createElement("b");
      title.textContent = event.title;
      var detail = document.createElement("span");
      detail.textContent = event.detail;
      content.appendChild(title); content.appendChild(detail);
      node.appendChild(time); node.appendChild(content);
      node.addEventListener("click", function () { stopPlayback(); setAt(event.atUs, true); });
      elements.events.appendChild(node);
    });
  }

  function stopPlayback() {
    var wasPlaying = state.playing;
    state.playing = false;
    window.cancelAnimationFrame(state.raf);
    state.raf = 0;
    elements.play.innerHTML = "&#9654;";
    elements.play.setAttribute("aria-label", "Воспроизвести");
    elements.play.title = "Воспроизвести";
    if (wasPlaying && state.pendingSnapshotAtUs !== null && !state.snapshotInFlight && state.manifest) {
      window.clearTimeout(state.snapshotTimer);
      state.snapshotTimer = window.setTimeout(requestExactSnapshot, 0);
    }
  }

  function playTick(now) {
    if (!state.playing || !state.manifest) return;
    var elapsedMs = Math.min(250, Math.max(0, now - state.lastAnimationMs));
    state.lastAnimationMs = now;
    var nextAtUs = state.atUs + elapsedMs * 1000 * state.rate;
    if (nextAtUs >= state.manifest.range.last_at_us) {
      setAt(state.manifest.range.last_at_us, true);
      stopPlayback();
      return;
    }
    setAt(nextAtUs, true);
    state.raf = window.requestAnimationFrame(playTick);
  }

  function togglePlayback() {
    if (!state.manifest) return;
    if (state.playing) { stopPlayback(); return; }
    if (state.atUs >= state.manifest.range.last_at_us) setAt(state.manifest.range.first_at_us, false);
    state.playing = true;
    state.lastAnimationMs = performance.now();
    elements.play.innerHTML = "&#10074;&#10074;";
    elements.play.setAttribute("aria-label", "Пауза");
    elements.play.title = "Пауза";
    state.raf = window.requestAnimationFrame(playTick);
  }

  function step(direction) {
    if (!state.manifest) return;
    stopPlayback();
    var frames = state.manifest.keyframes || [];
    if (!frames.length) return;
    var target = direction > 0 ? state.manifest.range.last_at_us : state.manifest.range.first_at_us;
    if (direction > 0) {
      for (var index = 0; index < frames.length; index += 1) {
        if (frames[index].observed_at_us > state.atUs) { target = frames[index].observed_at_us; break; }
      }
    } else {
      for (var reverse = frames.length - 1; reverse >= 0; reverse -= 1) {
        if (frames[reverse].observed_at_us < state.atUs) { target = frames[reverse].observed_at_us; break; }
      }
    }
    setAt(target, true);
  }

  elements.select.addEventListener("change", loadSelectedEntry);
  elements.play.addEventListener("click", togglePlayback);
  elements.stepBack.addEventListener("click", function () { step(-1); });
  elements.stepForward.addEventListener("click", function () { step(1); });
  elements.reset.addEventListener("click", function () {
    if (!state.manifest) return;
    stopPlayback(); setAt(state.manifest.range.first_at_us, true);
  });
  elements.range.addEventListener("input", function () {
    if (!state.manifest) return;
    stopPlayback(); setAt(state.manifest.range.first_at_us + Number(elements.range.value) * 1000000, false);
  });
  elements.range.addEventListener("change", function () { scheduleExactSnapshot(); });
  elements.rate.addEventListener("change", function () { state.rate = Number(elements.rate.value) || 1; });
  elements.time.addEventListener("change", function () {
    if (!state.manifest) return;
    var seconds = parseElapsed(elements.time.value);
    if (seconds === null) { elements.time.value = formatElapsed((state.atUs - state.manifest.range.first_at_us) / 1000000); return; }
    stopPlayback(); setAt(state.manifest.range.first_at_us + seconds * 1000000, true);
  });
  elements.time.addEventListener("keydown", function (event) { if (event.key === "Enter") elements.time.blur(); });
  elements.chart.addEventListener("click", function (event) {
    if (!state.manifest) return;
    var rect = elements.chart.getBoundingClientRect();
    var ratio = Math.max(0, Math.min(1, (event.clientX - rect.left - 42) / Math.max(1, rect.width - 52)));
    stopPlayback(); setAt(state.manifest.range.first_at_us + ratio * (state.manifest.range.last_at_us - state.manifest.range.first_at_us), true);
  });
  window.addEventListener("resize", function () { window.requestAnimationFrame(drawChart); });

  controlsDisabled(true);
  fetchJson(API + "/sessions/archive?limit=50").then(function (payload) {
    populateSessions(payload.items || []);
  }).catch(function (error) {
    elements.select.disabled = true;
    setEmpty("Не удалось загрузить телеметрический архив: " + error.message);
  });
})();
