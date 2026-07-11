(function () {
  "use strict";

  var API = "/api/timing";
  var $ = function (id) { return document.getElementById(id); };
  var root = $("timingArchive");
  var modal = $("timingModal");
  if (!root || !modal) return;

  var elements = {
    modal: modal,
    close: $("timingClose"),
    select: $("timingSession"),
    empty: $("timingArchiveEmpty"),
    coverage: $("timingCoverage"),
    coverageTitle: $("timingCoverageTitle"),
    coverageText: $("timingCoverageText"),
    body: $("timingArchiveBody"),
    flag: $("timingFlag"),
    observed: $("timingObserved"),
    kpis: $("timingKpis"),
    benchmark: $("timingBenchmark"),
    chart: $("timingChart"),
    chartTooltip: $("timingChartTooltip"),
    chartAxis: $("timingChartAxis"),
    chartRange: $("timingChartRange"),
    comparison: $("timingComparison"),
    comparisonLegend: $("timingComparisonLegend"),
    comparisonLegendKey: $("timingComparisonLegendKey"),
    comparisonLegendTitle: $("timingComparisonLegendTitle"),
    comparisonLegendText: $("timingComparisonLegendText"),
    pitPanel: $("timingPitPanel"),
    pitMeta: $("timingPitMeta"),
    pitChart: $("timingPitChart"),
    pitTooltip: $("timingPitTooltip"),
    pitAxis: $("timingPitAxis"),
    pitDescription: $("timingPitDescription"),
    lapPanel: $("timingLapPanel"),
    lapChart: $("timingLapChart"),
    lapAxis: $("timingLapAxis"),
    lapReadout: $("timingLapReadout"),
    lapLegend: $("timingLapLegend"),
    lapLegendKey: $("timingLapLegendKey"),
    lapLegendTitle: $("timingLapLegendTitle"),
    lapLegendText: $("timingLapLegendText"),
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
    chartGeometry: null,
    selectedLapPoint: null,
    hoveredLapPoint: null,
    hoveredTimelineAtUs: null,
    chartTooltipAnchor: null,
    pitBase: null,
    pitBaseKey: "",
    pitVisual: null,
    pitVisualKey: "",
    pitGeometry: null,
    pitLastPlayheadX: null,
    hoveredPit: null,
    pitTooltipAnchor: null,
    lapBase: null,
    lapBaseKey: "",
    lapGeometry: null,
    lapLastPlayheadX: null,
    lapReadoutKey: "",
    visualsLastDrawMs: 0,
    chartRangeText: "",
    observedText: "",
    kpiValues: null,
    flagValue: null,
    benchmarkValues: null,
    comparison: null,
    comparisonSelection: "all",
    comparisonCache: Object.create(null),
    comparisonController: null,
    comparisonRequestId: 0,
    comparisonRevision: 0,
    entriesLoaded: false,
    entriesLoading: false,
    pendingSelection: null,
    modalOpen: false,
    focusReturn: null,
    bodyOverflow: null,
    coverage: null
  };

  var COMPETITOR_COLORS = ["#007B91", "#526276", "#D08000", "#7D55B8", "#16824F", "#A44469"];

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

  function formatNounCount(value, one, few, many) {
    if (typeof value !== "number" || !isFinite(value) || value < 0) return "—";
    var count = Math.round(value);
    var lastTwo = count % 100;
    var last = count % 10;
    var suffix = lastTwo >= 11 && lastTwo <= 14 ? many :
      (last === 1 ? one : (last >= 2 && last <= 4 ? few : many));
    return count + " " + suffix;
  }

  function formatLapCount(value) {
    return formatNounCount(value, "круг", "круга", "кругов");
  }

  function formatPaceDelta(milliseconds) {
    if (typeof milliseconds !== "number" || !isFinite(milliseconds)) return "—";
    if (Math.abs(milliseconds) < 1) return "На одном темпе";
    var value = (Math.abs(milliseconds) / 1000).toFixed(3) + " с";
    return milliseconds < 0 ? "-" + value + " быстрее" : "+" + value + " медленнее";
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

  function sourceClockDefinition() {
    var axes = asObject(state.manifest && state.manifest.time_axes);
    var source = asObject(axes.source);
    return source.id === "timeservice" && Array.isArray(source.anchors) ? source : null;
  }

  function sourceClockAt(captureAtUs) {
    if (typeof captureAtUs !== "number") return null;
    var source = sourceClockDefinition();
    if (!source) return null;
    var anchors = source.anchors;
    var left = null;
    var right = null;
    for (var index = 0; index < anchors.length; index += 1) {
      var anchor = anchors[index];
      if (!anchor || typeof anchor.capture_at_us !== "number" || typeof anchor.calibrated_utc_at_us !== "number") continue;
      if (anchor.capture_at_us === captureAtUs) {
        return {
          calibratedAtUs: anchor.calibrated_utc_at_us,
          providerAtUs: anchor.provider_ts_time_us,
          basis: "provider_explicit"
        };
      }
      if (anchor.capture_at_us < captureAtUs) left = anchor;
      if (anchor.capture_at_us > captureAtUs) { right = anchor; break; }
    }
    if (!left || !right || left.connection_id !== right.connection_id) return null;
    var captureSpan = right.capture_at_us - left.capture_at_us;
    var maximumSpan = numericValue(source.interpolation_max_gap_us) || 0;
    if (captureSpan <= 0 || (maximumSpan && captureSpan > maximumSpan)) return null;
    var ratio = (captureAtUs - left.capture_at_us) / captureSpan;
    return {
      calibratedAtUs: Math.round(left.calibrated_utc_at_us + (right.calibrated_utc_at_us - left.calibrated_utc_at_us) * ratio),
      providerAtUs: (
        typeof left.provider_ts_time_us === "number" && typeof right.provider_ts_time_us === "number"
          ? Math.round(left.provider_ts_time_us + (right.provider_ts_time_us - left.provider_ts_time_us) * ratio)
          : null
      ),
      basis: "provider_interpolated"
    };
  }

  function formatClockLabel(atUs, compact, withDate) {
    if (typeof atUs !== "number") return "—";
    try {
      var options = {
        timeZone: archiveTimezone(),
        hour: "2-digit",
        minute: "2-digit"
      };
      if (!compact) options.second = "2-digit";
      if (withDate) { options.day = "2-digit"; options.month = "2-digit"; }
      return new Intl.DateTimeFormat("ru-RU", options).format(new Date(atUs / 1000));
    } catch (error) {
      return formatAbsolute(atUs);
    }
  }

  function clockDateKey(atUs) {
    if (typeof atUs !== "number") return "";
    try {
      return new Intl.DateTimeFormat("ru-RU", {
        timeZone: archiveTimezone(), day: "2-digit", month: "2-digit", year: "numeric"
      }).format(new Date(atUs / 1000));
    } catch (error) {
      return new Date(atUs / 1000).toISOString().slice(0, 10);
    }
  }

  function timelineClockAt(captureAtUs) {
    var source = sourceClockAt(captureAtUs);
    return source ? { atUs: source.calibratedAtUs, source: true, basis: source.basis } :
      { atUs: captureAtUs, source: false, basis: "capture_received" };
  }

  function timelineAxisTicks(range, width) {
    var count = width < 440 ? 3 : 5;
    var first = timelineClockAt(range.first_at_us);
    var last = timelineClockAt(range.last_at_us);
    var sourceAvailable = first.source && last.source;
    var compact = width < 440 && (range.last_at_us - range.first_at_us) >= 10 * 60 * 1000000;
    var withDate = clockDateKey(first.atUs) !== clockDateKey(last.atUs);
    var ticks = [];
    for (var index = 0; index < count; index += 1) {
      var ratio = count === 1 ? 0 : index / (count - 1);
      var captureAtUs = Math.round(range.first_at_us + (range.last_at_us - range.first_at_us) * ratio);
      var moment = timelineClockAt(captureAtUs);
      ticks.push({
        ratio: ratio,
        text: formatClockLabel(moment.atUs, compact, withDate),
        source: moment.source
      });
    }
    return { sourceAvailable: sourceAvailable, ticks: ticks };
  }

  function timelineLapTicks(range, laps) {
    var entries = [];
    (Array.isArray(laps) ? laps : []).forEach(function (lap) {
      var atUs = numericValue(lap && lap.completed_at_us);
      var lapNumber = numericValue(lap && lap.lap_number);
      if (atUs === null || lapNumber === null || atUs < range.first_at_us || atUs > range.last_at_us) return;
      entries.push({ atUs: atUs, lapNumber: Math.round(lapNumber) });
    });
    entries.sort(function (left, right) { return left.atUs - right.atUs || left.lapNumber - right.lapNumber; });
    return entries.map(function (entry, index) {
      var major = index === 0 || entry.lapNumber % 5 === 0;
      return {
        ratio: (entry.atUs - range.first_at_us) / Math.max(1, range.last_at_us - range.first_at_us),
        lapNumber: entry.lapNumber,
        major: major,
        label: major ? "#" + entry.lapNumber : ""
      };
    });
  }

  function renderTimelineAxis(element, geometry, ownLaps) {
    if (!element || !geometry || !geometry.range) return;
    var width = geometry.width || element.getBoundingClientRect().width || 1;
    var axis = timelineAxisTicks(geometry.range, width);
    var lapTicks = timelineLapTicks(geometry.range, ownLaps);
    var key = [
      geometry.range.first_at_us, geometry.range.last_at_us, Math.round(width), geometry.left, geometry.right,
      axis.sourceAvailable, axis.ticks.map(function (tick) { return tick.text; }).join("|"),
      lapTicks.map(function (tick) { return tick.lapNumber + ":" + tick.major; }).join("|")
    ].join(":");
    if (element.dataset.axisKey === key) return;
    element.dataset.axisKey = key;
    element.hidden = false;
    element.dataset.axisLabel = axis.sourceAvailable ? "Время табло" : "Время записи";
    element.dataset.lapLabel = lapTicks.length ? "Пройдено кругов" : "";
    element.classList.toggle("has-lap-axis", lapTicks.length > 0);
    var captureLapScope = archiveLapCountScope(historicalSnapshot(state.atUs)) === "capture_tracker";
    element.title = axis.sourceAvailable ?
      "Верхняя шкала: время Time Service из потока табло. Нижняя: " +
        (captureLapScope ? "зафиксированные круги с начала сохранённой записи" : "круги источника") + ", подпись через пять кругов." :
      "Верхняя шкала: время получения записи. Нижняя: " +
        (captureLapScope ? "зафиксированные круги с начала сохранённой записи" : "круги источника") + ", подпись через пять кругов.";
    element.style.setProperty("--axis-left", Math.round(geometry.left) + "px");
    element.style.setProperty("--axis-right", Math.round(geometry.right) + "px");
    element.replaceChildren();
    var plot = document.createElement("div");
    plot.className = "ta-stream-axis-plot";
    axis.ticks.forEach(function (tick) {
      var label = document.createElement("span");
      label.className = "ta-stream-axis-tick";
      label.style.left = (tick.ratio * 100) + "%";
      label.textContent = tick.text;
      plot.appendChild(label);
    });
    element.appendChild(plot);
    if (lapTicks.length) {
      var lapPlot = document.createElement("div");
      lapPlot.className = "ta-stream-lap-plot";
      lapTicks.forEach(function (tick) {
        var marker = document.createElement("span");
        marker.className = "ta-stream-lap-tick" + (tick.major ? " major" : "");
        marker.style.left = (tick.ratio * 100) + "%";
        marker.title = (captureLapScope ? "Зафиксированный круг #" : "Круг #") + tick.lapNumber;
        if (tick.label) {
          var label = document.createElement("b");
          label.textContent = tick.label;
          marker.appendChild(label);
        }
        lapPlot.appendChild(marker);
      });
      element.appendChild(lapPlot);
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

  function durationSeconds(firstAtUs, lastAtUs) {
    if (typeof firstAtUs !== "number" || typeof lastAtUs !== "number") return 0;
    return Math.max(0, Math.round((lastAtUs - firstAtUs) / 1000000));
  }

  function numericValue(value) {
    return typeof value === "number" && isFinite(value) ? value : null;
  }

  function coverageDetails(manifest) {
    var range = asObject(manifest.range);
    var heat = asObject(manifest.heat);
    var coverage = asObject(heat.coverage);
    var firstAtUs = numericValue(range.first_at_us);
    var lastAtUs = numericValue(range.last_at_us);
    var firstSnapshot = asObject((manifest.keyframes || [])[0]);
    var firstPayload = asObject(firstSnapshot.snapshot);
    var firstComputed = asObject(asObject(firstPayload.computed).session);
    var firstFlag = asObject(firstPayload.measured).track_flag;
    var carriedFinishAtUs = null;
    var carryFinish = (manifest.markers && manifest.markers.flags || []).some(function (flag) {
      var carried = flag && flag.flag === "FINISH" && (flag.carried_into_range || (numericValue(flag.started_at_us) !== null && firstAtUs !== null && flag.started_at_us < firstAtUs));
      if (carried && numericValue(flag.started_at_us) !== null) carriedFinishAtUs = flag.started_at_us;
      return carried;
    });
    var kind = coverage.kind;
    if (kind !== "replay" && kind !== "partial_capture" && kind !== "terminal_snapshot") {
      if (carryFinish || (firstFlag && firstFlag.flag === "FINISH" && firstComputed.channel_status === "OFFLINE")) kind = "terminal_snapshot";
      else if (durationSeconds(firstAtUs, lastAtUs) < 300) kind = "partial_capture";
      else kind = "replay";
    }
    return {
      kind: kind,
      sourceStartedAtUs: numericValue(coverage.source_started_at_us),
      captureStartedAtUs: numericValue(coverage.capture_started_at_us) || firstAtUs,
      finishAtUs: numericValue(coverage.finish_at_us) || carriedFinishAtUs,
      missingPrefixUs: numericValue(coverage.missing_prefix_us),
      firstAtUs: firstAtUs,
      lastAtUs: lastAtUs,
      durationSeconds: durationSeconds(firstAtUs, lastAtUs),
      carryFinish: carryFinish
    };
  }

  function renderCoverage(manifest) {
    var coverage = coverageDetails(manifest);
    state.coverage = coverage;
    root.classList.toggle("is-terminal-snapshot", coverage.kind === "terminal_snapshot");
    if (coverage.kind === "replay") {
      elements.coverage.hidden = true;
      elements.coverage.className = "ta-coverage";
      return;
    }
    var captureRange = formatAbsolute(coverage.captureStartedAtUs) + "–" + formatAbsolute(coverage.lastAtUs) + " (" + formatElapsed(coverage.durationSeconds) + ")";
    var text;
    if (coverage.kind === "terminal_snapshot") {
      text = "Сохранён только финальный срез " + captureRange + ".";
      if (coverage.sourceStartedAtUs !== null) text += " Сессия началась в " + formatAbsolute(coverage.sourceStartedAtUs) + ".";
      if (coverage.finishAtUs !== null) text += " Финиш был в " + formatAbsolute(coverage.finishAtUs) + ".";
      text += " Полная запись этой сессии не велась.";
      elements.coverageTitle.textContent = "Финальный срез";
      elements.coverage.className = "ta-coverage terminal";
    } else {
      text = "Сохранена часть телеметрии: " + captureRange + ". Воспроизведение доступно только для этого интервала.";
      if (coverage.sourceStartedAtUs !== null && coverage.captureStartedAtUs !== null && coverage.captureStartedAtUs > coverage.sourceStartedAtUs) {
        text += " Запись началась после старта сессии.";
      }
      if (coverage.finishAtUs !== null) text += " Финиш сессии: " + formatAbsolute(coverage.finishAtUs) + ".";
      if (coverage.missingPrefixUs !== null && coverage.missingPrefixUs > 0) text += " До записи пропущено " + formatElapsed(coverage.missingPrefixUs / 1000000) + ".";
      elements.coverageTitle.textContent = "Частичная запись";
      elements.coverage.className = "ta-coverage partial";
    }
    elements.coverageText.textContent = text;
    elements.coverage.hidden = false;
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

  function entryDuration(entry) {
    return durationSeconds(entry.heat && entry.heat.first_at_us, entry.heat && entry.heat.last_at_us);
  }

  function entryCoverageKind(entry) {
    return asObject(entry.heat && entry.heat.coverage).kind || "";
  }

  function compareEntryCoverage(left, right) {
    var duration = entryDuration(right) - entryDuration(left);
    if (duration) return duration;
    return (Number(right.heat && right.heat.point_count) || 0) - (Number(left.heat && left.heat.point_count) || 0);
  }

  function populateSessions(items) {
    state.entries = [];
    elements.select.replaceChildren();
    items.forEach(function (item) {
      (item.heats || []).forEach(function (heat) {
        state.entries.push({ session: item.session, heat: heat });
      });
    });
    state.entries.sort(compareEntryCoverage);
    if (!state.entries.length) {
      elements.select.disabled = true;
      setEmpty("Для архива пока нет сессий с сохранённой телеметрией.");
      return;
    }
    state.entries.forEach(function (entry) {
      var option = document.createElement("option");
      option.value = selectEntryId(entry);
      var duration = Math.max(0, Math.round((entry.heat.last_at_us - entry.heat.first_at_us) / 1000000));
      var prefix = entryCoverageKind(entry) === "terminal_snapshot" ? "Финальный срез · " : "";
      option.textContent = formatEntryDate(entry.heat.first_at_us, entry.session.timezone_name) + " · " +
        (entry.session.source_name || entry.session.source_slug || "Трасса") + " · " +
        prefix + modeLabel(entry.session.mode) + " · " + (entry.heat.external_name || ("Heat " + entry.heat.generation)) + " · " + formatElapsed(duration);
      elements.select.appendChild(option);
    });
    elements.select.disabled = false;
    var saved = null;
    try { saved = localStorage.getItem(storeKey()); } catch (error) {}
    var requested = state.pendingSelection;
    var selected = state.entries.some(function (entry) { return selectEntryId(entry) === requested; }) ? requested :
      (state.entries.some(function (entry) { return selectEntryId(entry) === saved; }) ? saved : selectEntryId(state.entries[0]));
    state.pendingSelection = null;
    elements.select.value = selected;
    if (state.modalOpen) loadSelectedEntry();
  }

  function ensureSessionsLoaded() {
    if (state.entriesLoaded) {
      var requested = state.pendingSelection;
      if (requested && state.entries.some(function (entry) { return selectEntryId(entry) === requested; })) {
        elements.select.value = requested;
      }
      state.pendingSelection = null;
      if (state.modalOpen && state.entries.length) loadSelectedEntry();
      return;
    }
    if (state.entriesLoading) return;
    state.entriesLoading = true;
    controlsDisabled(true);
    setEmpty("Загрузка сохранённой телеметрии…");
    fetchJson(API + "/sessions/archive?limit=50").then(function (payload) {
      state.entriesLoaded = true;
      populateSessions(payload.items || []);
    }).catch(function (error) {
      state.entriesLoaded = true;
      elements.select.disabled = true;
      setEmpty("Не удалось загрузить телеметрический архив: " + error.message);
    }).then(function () {
      state.entriesLoading = false;
    });
  }

  function currentEntry() {
    var selected = elements.select.value;
    return state.entries.find(function (entry) { return selectEntryId(entry) === selected; }) || null;
  }

  function comparisonKey(selection) { return selection || "ours"; }

  function comparisonParticipants() {
    var source = state.comparison || state.comparisonCache.all;
    return source && Array.isArray(source.participants) ? source.participants : [];
  }

  function comparisonParticipant(participantId) {
    return comparisonParticipants().find(function (participant) {
      return participant && participant.participant_id === participantId;
    }) || null;
  }

  function comparisonOursId() {
    var source = state.comparison || state.comparisonCache.all;
    return source && source.comparison && source.comparison.ours_participant_id || null;
  }

  function comparisonOptionLabel(participant) {
    var number = participant && participant.start_number ? "#" + participant.start_number + " · " : "";
    var team = participant && participant.team_name || "Соперник";
    var car = participant && participant.car_name ? " · " + participant.car_name : "";
    var driver = participant && participant.driver_name ? " · " + participant.driver_name : "";
    return number + team + car + driver;
  }

  function comparisonCompetitors() {
    var oursId = comparisonOursId();
    return comparisonParticipants().filter(function (participant) {
      return participant && participant.participant_id !== oursId;
    });
  }

  function competitorColor(participantId) {
    var index = comparisonCompetitors().findIndex(function (participant) {
      return participant.participant_id === participantId;
    });
    return COMPETITOR_COLORS[index >= 0 ? index % COMPETITOR_COLORS.length : 0];
  }

  function renderComparisonLegend() {
    var response = state.comparison;
    if (state.comparisonSelection === "ours" || !response || !response.comparison || !response.comparison.available) {
      elements.comparisonLegend.hidden = true;
      elements.comparisonLegend.replaceChildren();
      return;
    }
    var comparison = response.comparison;
    var participants = comparison.mode === "participant" ?
      [comparisonParticipant(comparison.participant_id)].filter(Boolean) : comparisonCompetitors();
    if (!participants.length) {
      elements.comparisonLegend.hidden = true;
      elements.comparisonLegend.replaceChildren();
      return;
    }
    elements.comparisonLegend.replaceChildren();
    elements.comparisonLegend.hidden = false;
    participants.forEach(function (participant) {
      var item = document.createElement("li");
      var key = document.createElement("i");
      key.className = "ta-legend-key competitor";
      key.style.background = competitorColor(participant.participant_id);
      var copy = document.createElement("span");
      var title = document.createElement("b");
      title.textContent = comparisonOptionLabel(participant);
      var detail = document.createElement("small");
      detail.textContent = "Реальные значения каждого круга";
      copy.appendChild(title);
      copy.appendChild(detail);
      item.appendChild(key);
      item.appendChild(copy);
      elements.comparisonLegend.appendChild(item);
    });
  }

  function renderComparisonSelector() {
    var participants = comparisonParticipants();
    var source = state.comparison || state.comparisonCache.all;
    var comparison = source && source.comparison;
    elements.comparison.replaceChildren();
    if (!comparison || !comparison.available || !participants.length) {
      var unavailable = document.createElement("option");
      unavailable.textContent = "Сравнение недоступно";
      elements.comparison.appendChild(unavailable);
      elements.comparison.disabled = true;
      renderComparisonLegend();
      return;
    }
    var oursId = comparison.ours_participant_id;
    var ourParticipant = participants.find(function (participant) { return participant.participant_id === oursId; });
    var ours = document.createElement("option");
    ours.value = "ours";
    ours.textContent = ourParticipant ? "Только " + comparisonOptionLabel(ourParticipant) : "Только BALCHUG Racing";
    elements.comparison.appendChild(ours);
    var aggregate = document.createElement("option");
    aggregate.value = "all";
    aggregate.textContent = "Все соперники " + (comparison.class_name || "класса") + " · наложение";
    elements.comparison.appendChild(aggregate);
    var group = document.createElement("optgroup");
    group.label = "Отдельный соперник";
    participants.filter(function (participant) { return participant.participant_id !== oursId; }).forEach(function (participant) {
      var option = document.createElement("option");
      option.value = "participant:" + participant.participant_id;
      option.textContent = comparisonOptionLabel(participant);
      group.appendChild(option);
    });
    if (group.children.length) elements.comparison.appendChild(group);
    var valid = Array.prototype.some.call(elements.comparison.options, function (option) {
      return option.value === state.comparisonSelection;
    });
    if (!valid) state.comparisonSelection = "all";
    elements.comparison.value = state.comparisonSelection;
    elements.comparison.disabled = false;
    renderComparisonLegend();
  }

  function renderComparisonLoading() {
    elements.comparison.replaceChildren();
    var option = document.createElement("option");
    option.textContent = "Загрузка…";
    elements.comparison.appendChild(option);
    elements.comparison.disabled = true;
    elements.comparisonLegend.hidden = true;
    elements.benchmark.hidden = true;
    elements.pitPanel.hidden = true;
    elements.lapPanel.hidden = true;
  }

  function applyComparison(response, selection) {
    state.comparison = response && response.comparison && response.comparison.available ? response : null;
    state.comparisonRevision += 1;
    renderComparisonSelector();
    renderComparisonLegend();
    if (state.payload) renderBenchmark(state.payload);
    drawChart();
  }

  function loadComparison(selection, epoch) {
    selection = comparisonKey(selection);
    state.comparisonSelection = selection;
    if (selection === "ours") {
      state.comparison = null;
      state.comparisonRevision += 1;
      renderComparisonSelector();
      renderComparisonLegend();
      if (state.payload) renderBenchmark(state.payload);
      drawChart();
      return;
    }
    if (state.comparisonCache[selection]) {
      applyComparison(state.comparisonCache[selection], selection);
      return;
    }
    if (!state.entry || !state.manifest) return;
    if (state.comparisonController) state.comparisonController.abort();
    state.comparison = null;
    state.comparisonRevision += 1;
    renderComparisonLegend();
    if (state.payload) renderBenchmark(state.payload);
    drawChart();
    var requestId = ++state.comparisonRequestId;
    var requestEpoch = epoch === undefined ? state.selectionEpoch : epoch;
    var participantId = selection.indexOf("participant:") === 0 ? selection.slice("participant:".length) : null;
    var url = API + "/sessions/" + encodeURIComponent(state.entry.session.id) + "/archive/comparison?generation=" +
      encodeURIComponent(state.entry.heat.generation) + "&mode=" + (participantId ? "participant" : "all");
    if (participantId) url += "&participant_id=" + encodeURIComponent(participantId);
    state.comparisonController = new AbortController();
    fetchJson(url, { signal: state.comparisonController.signal }).then(function (response) {
      if (requestEpoch !== state.selectionEpoch || requestId !== state.comparisonRequestId || selection !== state.comparisonSelection) return;
      state.comparisonCache[selection] = response;
      applyComparison(response, selection);
    }).catch(function (error) {
      if (error && error.name === "AbortError") return;
      if (requestEpoch !== state.selectionEpoch || requestId !== state.comparisonRequestId || selection !== state.comparisonSelection) return;
      state.comparison = null;
      state.comparisonRevision += 1;
      renderComparisonSelector();
      renderComparisonLegend();
      if (state.payload) renderBenchmark(state.payload);
      drawChart();
    }).then(function () {
      if (requestId === state.comparisonRequestId) state.comparisonController = null;
    });
  }

  function loadSelectedEntry() {
    stopPlayback();
    var entry = currentEntry();
    if (!entry) return;
    state.selectionEpoch += 1;
    var epoch = state.selectionEpoch;
    if (state.manifestController) state.manifestController.abort();
    if (state.snapshotController) state.snapshotController.abort();
    if (state.comparisonController) state.comparisonController.abort();
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
    state.chartGeometry = null;
    state.selectedLapPoint = null;
    state.hoveredLapPoint = null;
    state.hoveredTimelineAtUs = null;
    state.chartTooltipAnchor = null;
    state.pitBase = null;
    state.pitBaseKey = "";
    state.pitVisual = null;
    state.pitVisualKey = "";
    state.pitGeometry = null;
    state.pitLastPlayheadX = null;
    state.hoveredPit = null;
    state.pitTooltipAnchor = null;
    state.lapBase = null;
    state.lapBaseKey = "";
    state.lapGeometry = null;
    state.lapLastPlayheadX = null;
    state.lapReadoutKey = "";
    state.visualsLastDrawMs = 0;
    state.chartRangeText = "";
    state.observedText = "";
    state.kpiValues = null;
    state.flagValue = null;
    state.benchmarkValues = null;
    state.comparison = null;
    state.comparisonSelection = "all";
    state.comparisonCache = Object.create(null);
    state.comparisonController = null;
    state.comparisonRequestId += 1;
    state.comparisonRevision += 1;
    state.coverage = null;
    root.classList.remove("is-terminal-snapshot");
    elements.coverage.hidden = true;
    elements.coverage.className = "ta-coverage";
    elements.chartAxis.hidden = true;
    elements.pitAxis.hidden = true;
    elements.lapAxis.hidden = true;
    elements.lapReadout.hidden = true;
    hideArchiveTooltip(elements.chartTooltip);
    hideArchiveTooltip(elements.pitTooltip);
    renderComparisonLoading();
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
      renderCoverage(manifest);
      controlsDisabled(state.coverage && state.coverage.kind === "terminal_snapshot");
      buildEvents();
      setAt(state.atUs, !(state.coverage && state.coverage.kind === "terminal_snapshot"));
      if (!(state.coverage && state.coverage.kind === "terminal_snapshot")) loadComparison("all", epoch);
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
    if (!(state.coverage && state.coverage.kind === "terminal_snapshot")) {
      drawChart();
      renderEvents();
      if (requestExact) scheduleExactSnapshot();
    }
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
      // Exact archive snapshots retain detailed class source facts that are
      // deliberately omitted from the bounded manifest. Refresh the tooltip
      // so an inspected point can use the most precise stored gap context.
      if (state.chartGeometry) updateRawLapTooltip(state.chartGeometry);
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
    state.kpiValues = null;
    state.flagValue = null;
    state.benchmarkValues = null;
    elements.flag.className = "ta-flag";
    elements.flag.querySelector("strong").textContent = "Загрузка среза";
    state.observedText = "курсор " + formatElapsed((state.atUs - state.manifest.range.first_at_us) / 1000000);
    elements.observed.textContent = state.observedText;
    elements.kpis.replaceChildren();
    elements.benchmark.hidden = true;
  }

  function updateSnapshotTiming(effectiveAtUs) {
    if (!state.manifest || typeof effectiveAtUs !== "number") return;
    var captured = formatElapsed((effectiveAtUs - state.manifest.range.first_at_us) / 1000000);
    var cursor = formatElapsed((state.atUs - state.manifest.range.first_at_us) / 1000000);
    var sourceClock = sourceClockAt(effectiveAtUs);
    var observedMoment = sourceClock ? "табло " + formatAbsolute(sourceClock.calibratedAtUs) : "запись " + formatAbsolute(effectiveAtUs);
    var observedText = "срез " + captured + " · курсор " + cursor + " · " + observedMoment;
    if (observedText !== state.observedText) {
      state.observedText = observedText;
      elements.observed.textContent = observedText;
    }
  }

  function restartFlash(element) {
    element.classList.remove("is-changed");
    void element.offsetWidth;
    element.classList.add("is-changed");
  }

  function firstDefined() {
    for (var index = 0; index < arguments.length; index += 1) {
      if (arguments[index] !== null && arguments[index] !== undefined) return arguments[index];
    }
    return null;
  }

  function snapshotClassParticipants(payload) {
    var snapshot = asObject(payload);
    var participants = Array.isArray(snapshot.class_participants) ? snapshot.class_participants : [];
    return participants.map(function (raw) {
      var item = asObject(raw);
      var measured = asObject(item.measured);
      var computed = asObject(item.computed);
      var measuredState = asObject(measured.state);
      return {
        participantId: firstDefined(computed.participant_id, measured.participant_id),
        pace5: numericValue(computed.pace_5_ms),
        state: firstDefined(computed.current_state, measuredState.state_kind, measuredState.state),
        positionClass: firstDefined(computed.position_class, measuredState.position_class),
        tyreAge: numericValue(computed.tyre_age_laps),
        pits: numericValue(computed.pits_completed),
        driver: firstDefined(computed.current_driver_name, measuredState.driver_name)
      };
    }).filter(function (participant) { return typeof participant.participantId === "string" && participant.participantId; });
  }

  function onTrackPace(participant) {
    return participant && participant.state === "ON_TRACK" ? numericValue(participant.pace5) : null;
  }

  function median(values) {
    if (!values.length) return null;
    var ordered = values.slice().sort(function (left, right) { return left - right; });
    var middle = Math.floor(ordered.length / 2);
    return ordered.length % 2 ? ordered[middle] : (ordered[middle - 1] + ordered[middle]) / 2;
  }

  function benchmarkMetric(label, value, key, changed, className) {
    var cell = document.createElement("div");
    cell.className = "ta-benchmark-metric" + (changed ? " is-changed" : "") + (className ? " " + className : "");
    var result = document.createElement("b");
    result.textContent = valueOrDash(value);
    result.title = result.textContent;
    var caption = document.createElement("span");
    caption.textContent = label;
    cell.dataset.metric = key;
    cell.appendChild(result);
    cell.appendChild(caption);
    return cell;
  }

  function renderBenchmark(payload) {
    // Raw laps are the primary comparison surface.  Do not substitute them
    // with a rolling or cross-car aggregate in the archive player.
    elements.benchmark.replaceChildren();
    elements.benchmark.hidden = true;
    state.benchmarkValues = null;
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
    var normalizedFlag = valueOrDash(flagValue);
    var lapCountScope = archiveLapCountScope(snapshot);
    var visibleLaps = firstDefined(session.completed_laps, oursState.laps);
    var visibleTyreAge = firstDefined(session.tyre_age_laps, oursState.tyre_age_laps);
    var currentStint = numericValue(session.stint_number);
    var lapsValue = visibleLaps;
    if (lapCountScope === "capture_tracker" && numericValue(visibleLaps) !== null) {
      lapsValue = formatLapCount(numericValue(visibleLaps)) + " с начала записи";
    }
    var tyresValue = formatLapCount(visibleTyreAge);
    if (lapCountScope === "capture_tracker" && currentStint === 1 && numericValue(visibleTyreAge) !== null) {
      tyresValue = "не менее " + tyresValue;
    }
    var flagChanged = state.flagValue !== null && state.flagValue !== normalizedFlag;
    elements.flag.className = "ta-flag " + flagClass(flagValue);
    elements.flag.querySelector("strong").textContent = flagLabel(flagValue);
    if (flagChanged) restartFlash(elements.flag);
    state.flagValue = normalizedFlag;
    updateSnapshotTiming(effectiveAtUs);

    var values = [
      { id: "pos", label: "POS", value: session.position_overall !== null && session.position_overall !== undefined ? "P" + session.position_overall : oursState.position_overall !== null && oursState.position_overall !== undefined ? "P" + oursState.position_overall : "—" },
      { id: "class_leader_gap", label: "До лидера класса", value: formatArchiveRelationDistance(effectiveAtUs, snapshot, "class_leader") },
      { id: "laps", label: lapCountScope === "capture_tracker" ? "Круги в записи" : "Круги", value: lapsValue },
      { id: "state", label: "Состояние", value: firstDefined(session.current_state, oursState.state_kind, oursState.state) },
      { id: "last", label: "Последний по табло", value: formatLap(firstDefined(session.last_lap_ms, oursState.last_lap_ms)) },
      { id: "last_to_best", label: "К лучшему кругу", value: formatGap(
        numericValue(firstDefined(session.last_lap_ms, oursState.last_lap_ms)) !== null &&
        numericValue(firstDefined(session.best_lap_ms, oursState.best_lap_ms)) !== null ?
          numericValue(firstDefined(session.last_lap_ms, oursState.last_lap_ms)) - numericValue(firstDefined(session.best_lap_ms, oursState.best_lap_ms)) : null
      ) },
      { id: "ahead", label: "До соперника впереди", value: formatArchiveRelationDistance(effectiveAtUs, snapshot, "class_ahead") },
      { id: "behind", label: "До соперника сзади", value: formatBehindDistance(effectiveAtUs, snapshot) },
      { id: "tyres", label: "Возраст шин", value: tyresValue },
      { id: "pits", label: lapCountScope === "capture_tracker" ? "Питы в записи" : "Пит-стопы", value: firstDefined(session.pits_completed, oursState.provider_pit_count) },
      { id: "best", label: "Лучший по табло", value: formatLap(firstDefined(session.best_lap_ms, oursState.best_lap_ms)) },
      { id: "driver", label: "Пилот", value: firstDefined(oursState.driver_name, ours.current_driver_name, session.ours_identity && session.ours_identity.driver_name) }
    ];
    var nextValues = Object.create(null);
    elements.kpis.replaceChildren();
    values.forEach(function (item) {
      var displayValue = valueOrDash(item.value);
      nextValues[item.id] = displayValue;
      var cell = document.createElement("div");
      var changed = state.kpiValues !== null && state.kpiValues[item.id] !== displayValue;
      cell.className = "ta-kpi" + (changed ? " is-changed" : "");
      var value = document.createElement("b");
      value.textContent = displayValue;
      value.title = value.textContent;
      var label = document.createElement("span");
      label.textContent = item.label;
      cell.appendChild(value);
      cell.appendChild(label);
      elements.kpis.appendChild(cell);
    });
    state.kpiValues = nextValues;
    renderBenchmark(payload);
  }

  function pointValue(point, key) {
    var snapshot = asObject(point.snapshot);
    var computed = asObject(snapshot.computed);
    var session = asObject(computed.session);
    var value = session[key];
    return typeof value === "number" && isFinite(value) ? value : null;
  }

  function comparisonChartSamples() {
    var response = state.comparison;
    if (response && response.comparison && response.comparison.available && Array.isArray(response.points)) {
      return response.points.map(function (point) {
        return {
          atUs: point.observed_at_us,
          ours: numericValue(point.ours_pace_5_ms),
          benchmark: numericValue(point.benchmark_pace_5_ms),
          p25: numericValue(point.benchmark_p25_pace_5_ms),
          p75: numericValue(point.benchmark_p75_pace_5_ms),
          count: numericValue(point.benchmark_participant_count)
        };
      });
    }
    return (state.manifest.keyframes || []).map(function (point) {
      return { atUs: point.observed_at_us, ours: pointValue(point, "pace_5_ms"), benchmark: null, p25: null, p75: null, count: null };
    });
  }

  function comparisonVisualData() {
    if (state.comparison && state.comparison.comparison && state.comparison.comparison.available) return state.comparison;
    if (state.comparisonSelection === "ours" && state.comparisonCache.all && state.comparisonCache.all.comparison && state.comparisonCache.all.comparison.available) {
      return state.comparisonCache.all;
    }
    return null;
  }

  function prepareCanvas(canvas, height, clear) {
    var rect = canvas.getBoundingClientRect();
    var width = Math.max(1, Math.round(rect.width));
    var ratio = window.devicePixelRatio || 1;
    if (canvas.style.height !== height + "px") canvas.style.height = height + "px";
    if (canvas.width !== width * ratio || canvas.height !== height * ratio) {
      canvas.width = width * ratio;
      canvas.height = height * ratio;
    }
    var context = canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    if (clear !== false) context.clearRect(0, 0, width, height);
    return { context: context, width: width, height: height };
  }

  function staticCanvas(width, height, ratio) {
    var canvas = document.createElement("canvas");
    canvas.width = width * ratio;
    canvas.height = height * ratio;
    var context = canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, width, height);
    return { canvas: canvas, context: context };
  }

  function drawTimelinePlayhead(context, x, height) {
    context.strokeStyle = "#122846";
    context.lineWidth = 1.4;
    context.setLineDash([4, 3]);
    context.beginPath(); context.moveTo(x, 0); context.lineTo(x, height); context.stroke();
    context.setLineDash([]);
  }

  function drawTimelineFlags(context, xAt, width, height, range, left, right) {
    (state.manifest.markers.flags || []).forEach(function (flag) {
      var start = Math.max(range.first_at_us, flag.started_at_us || range.first_at_us);
      var end = Math.min(range.last_at_us, flag.ended_at_us || range.last_at_us);
      if (end < start) return;
      context.fillStyle = flagColor(flag.flag);
      context.fillRect(xAt(start), 0, Math.max(1, xAt(end) - xAt(start)), height);
    });
    context.strokeStyle = "#E4E9F0";
    context.lineWidth = 1;
    [0, 0.5, 1].forEach(function (ratio) {
      var x = left + (width - left - right) * ratio + 0.5;
      context.beginPath(); context.moveTo(x, 0); context.lineTo(x, height); context.stroke();
    });
  }

  function pitVisualConfiguration(data) {
    var key = [
      state.manifest && state.manifest.heat && state.manifest.heat.source_heat_id,
      state.comparisonSelection,
      state.comparisonRevision
    ].join(":");
    if (state.pitVisual && state.pitVisualKey === key) return state.pitVisual;
    var comparison = asObject(data.comparison);
    var oursId = comparison.ours_participant_id;
    var selectedId = state.comparisonSelection.indexOf("participant:") === 0 ?
      state.comparisonSelection.slice("participant:".length) : null;
    var participantIds = state.comparisonSelection === "ours" ? [oursId] :
      (selectedId ? [oursId, selectedId] : (data.participants || []).map(function (participant) { return participant.participant_id; }));
    var requiredIds = Object.create(null);
    participantIds.forEach(function (participantId) { requiredIds[participantId] = true; });
    var pitsByParticipant = Object.create(null);
    var pits = (data.pit_stops || []).filter(function (pit) {
      return !!requiredIds[pit.participant_id];
    });
    pits.forEach(function (pit) {
      if (!pitsByParticipant[pit.participant_id]) pitsByParticipant[pit.participant_id] = [];
      pitsByParticipant[pit.participant_id].push(pit);
    });
    // In aggregate mode every class car keeps a row.  An empty row is useful
    // evidence that no confirmed pit-stop was recorded for that car.
    var participants = (data.participants || []).filter(function (participant) {
      return !!requiredIds[participant.participant_id];
    });
    if (!participants.length) {
      var ours = (data.participants || []).find(function (participant) { return participant.participant_id === oursId; });
      if (ours) participants = [ours];
    }
    state.pitVisualKey = key;
    state.pitVisual = {
      key: key,
      oursId: oursId,
      selectedId: selectedId,
      participants: participants,
      pits: pits,
      pitsByParticipant: pitsByParticipant
    };
    return state.pitVisual;
  }

  function drawPitTimelineBase(context, visual, width, height) {
    var rowHeight = 30;
    var headerHeight = 4;
    var range = state.manifest.range;
    var left = width < 440 ? 92 : 136;
    var right = 10;
    var usableWidth = Math.max(1, width - left - right);
    var total = Math.max(1, range.last_at_us - range.first_at_us);
    var xAt = function (atUs) { return left + (atUs - range.first_at_us) / total * usableWidth; };
    var hitAreas = [];
    drawTimelineFlags(context, xAt, width, height, range, left, right);
    if (!visual.pits.length) {
      context.fillStyle = "#6E7E98";
      context.font = "11px Arial";
      context.fillText("В сохранённой части нет подтверждённых пит-стопов выбранных машин", left, headerHeight + 19);
    }
    var compactLabels = width < 440;
    visual.participants.forEach(function (participant, index) {
      var top = headerHeight + index * rowHeight;
      var isOurs = participant.participant_id === visual.oursId;
      var selected = participant.participant_id === visual.selectedId;
      context.strokeStyle = "#E4E9F0";
      context.lineWidth = 1;
      context.beginPath(); context.moveTo(0, top + rowHeight - 0.5); context.lineTo(width, top + rowHeight - 0.5); context.stroke();
      context.fillStyle = "#1B365D";
      context.font = "10px Arial";
      var teamName = String(participant.team_name || "Машина");
      var compactTeam = teamName.trim().split(/\s+/)[0].slice(0, 10);
      var label = (participant.start_number ? "#" + participant.start_number + " " : "") + (compactLabels ? compactTeam : teamName);
      context.save(); context.beginPath(); context.rect(0, top, Math.max(1, left - 6), rowHeight); context.clip(); context.fillText(label, 4, top + 19); context.restore();
      (visual.pitsByParticipant[participant.participant_id] || []).forEach(function (pit) {
        var start = numericValue(pit.timeline_started_at_us);
        if (start === null) return;
        var end = numericValue(pit.timeline_ended_at_us);
        if (end === null) end = range.last_at_us;
        var x = xAt(start);
        var endX = xAt(Math.max(start, Math.min(range.last_at_us, end)));
        var barWidth = Math.max(3, endX - x);
        hitAreas.push({
          participant: participant,
          pit: pit,
          x: x,
          y: top + 7,
          width: barWidth,
          height: 15,
          atUs: start
        });
        context.fillStyle = isOurs ? "#F0143D" : (selected ? "#007B91" : "#526276");
        context.globalAlpha = pit.completed ? 0.88 : 0.48;
        context.fillRect(x, top + 7, barWidth, 15);
        context.globalAlpha = 1;
        if (!pit.completed) {
          context.strokeStyle = "#122846";
          context.setLineDash([3, 2]);
          context.strokeRect(x + 0.5, top + 7.5, Math.max(2, barWidth - 1), 14);
          context.setLineDash([]);
        }
        if (barWidth > 44 && pit.pit_lane_ms !== null && pit.pit_lane_ms !== undefined) {
          context.fillStyle = "#fff";
          context.font = "9px Arial";
          context.fillText(formatLap(pit.pit_lane_ms), x + 4, top + 18);
        }
      });
    });
    return { left: left, right: right, range: range, width: width, height: height, hitAreas: hitAreas };
  }

  function pitParticipantLabel(participant) {
    var number = participant.start_number ? "#" + participant.start_number + " " : "";
    return number + String(participant.team_name || "Машина");
  }

  function findPitHit(geometry, x, y) {
    if (!geometry) return null;
    var areas = geometry.hitAreas || [];
    for (var index = areas.length - 1; index >= 0; index -= 1) {
      var area = areas[index];
      if (x >= area.x - 4 && x <= area.x + area.width + 4 && y >= area.y - 5 && y <= area.y + area.height + 5) return area;
    }
    return null;
  }

  function pitTooltipAnchor(hit) {
    return hit ? { x: hit.x + Math.min(hit.width / 2, 14), y: hit.y + hit.height / 2 } : null;
  }

  function renderPitTooltip(hit, geometry) {
    var tooltip = elements.pitTooltip;
    if (!tooltip || !hit || !geometry) {
      hideArchiveTooltip(tooltip);
      return;
    }
    var participant = hit.participant || {};
    var pit = hit.pit || {};
    var clock = timelineClockAt(hit.atUs);
    var color = participant.is_ours ? "#F0143D" : competitorColor(participant.participant_id);
    var crew = (participant.start_number ? "#" + participant.start_number + " · " : "") + String(participant.team_name || "Машина");
    var completed = pit.completed && numericValue(pit.pit_lane_ms) !== null;
    tooltip.style.setProperty("--ta-tooltip-accent", color);
    tooltip.replaceChildren();
    appendTooltipText(tooltip, "ta-tooltip-kicker", clock.source ? "Время табло" : "Время записи");
    appendTooltipText(tooltip, "ta-tooltip-time", formatAbsolute(clock.atUs));
    appendTooltipText(tooltip, "ta-tooltip-primary", completed ? formatLap(pit.pit_lane_ms) : "Пит-стоп не завершён");
    appendTooltipText(tooltip, "ta-tooltip-detail", "Пит-стоп #" + valueOrDash(pit.stop_number) + " · " + crew);
    var context = pit.carried_into_range ? "Начался до сохранённого интервала" :
      (completed ? "Полное время в пит-лейне" : "Въезд в пит-лейн");
    appendTooltipText(tooltip, "ta-tooltip-context", context);
    positionArchiveTooltip(tooltip, elements.pitChart, state.pitTooltipAnchor || pitTooltipAnchor(hit));
  }

  function updatePitTooltip(geometry) {
    renderPitTooltip(state.hoveredPit, geometry);
  }

  function renderPitSummary(visual) {
    var carsWithPits = visual.participants.filter(function (participant) {
      return (visual.pitsByParticipant[participant.participant_id] || []).length > 0;
    }).length;
    var participantCount = formatNounCount(visual.participants.length, "машина", "машины", "машин");
    var pitCars = carsWithPits === 1 ? "1 с пит-стопом" : carsWithPits + " с пит-стопами";
    elements.pitMeta.textContent = participantCount + " · " + pitCars;
    var description = visual.participants.map(function (participant) {
      var stops = visual.pitsByParticipant[participant.participant_id] || [];
      if (!stops.length) return pitParticipantLabel(participant) + ": подтверждённых пит-стопов нет";
      return pitParticipantLabel(participant) + ": " + stops.map(function (pit) {
        var entered = numericValue(pit.timeline_started_at_us);
        var moment = entered === null ? null : timelineClockAt(entered);
        var time = moment === null ? "" : " в " + formatAbsolute(moment.atUs);
        var duration = numericValue(pit.pit_lane_ms);
        return "пит-стоп #" + pit.stop_number + time + (duration === null ? ", незавершён" : ", " + formatLap(duration));
      }).join(", ");
    }).join(". ");
    elements.pitDescription.textContent = "Хронология пит-стопов. " + description;
  }

  function drawPitTimeline() {
    var data = comparisonVisualData();
    if (!data || !state.manifest || !Array.isArray(data.pit_stops)) {
      elements.pitPanel.hidden = true;
      elements.pitMeta.textContent = "";
      elements.pitDescription.textContent = "";
      elements.pitAxis.hidden = true;
      state.hoveredPit = null;
      state.pitTooltipAnchor = null;
      hideArchiveTooltip(elements.pitTooltip);
      state.pitBase = null;
      state.pitBaseKey = "";
      return;
    }
    var visual = pitVisualConfiguration(data);
    elements.pitPanel.hidden = false;
    var height = 4 + Math.max(1, visual.participants.length) * 30 + 6;
    var surface = prepareCanvas(elements.pitChart, height, false);
    var ratio = window.devicePixelRatio || 1;
    var baseKey = [visual.key, surface.width, surface.height, ratio].join(":");
    if (!state.pitBase || state.pitBaseKey !== baseKey) {
      renderPitSummary(visual);
      var base = staticCanvas(surface.width, surface.height, ratio);
      var geometry = drawPitTimelineBase(base.context, visual, surface.width, surface.height);
      state.pitBase = base.canvas;
      state.pitBaseKey = baseKey;
      state.pitGeometry = geometry;
      state.pitLastPlayheadX = null;
    }
    var geometry = state.pitGeometry;
    renderTimelineAxis(elements.pitAxis, geometry, data.lap_series && (data.lap_series.ours_raw || data.lap_series.ours));
    updatePitTooltip(geometry);
    var total = Math.max(1, geometry.range.last_at_us - geometry.range.first_at_us);
    var playhead = geometry.left + (state.atUs - geometry.range.first_at_us) / total * Math.max(1, surface.width - geometry.left - geometry.right);
    var playheadPixel = Math.round(playhead);
    if (state.pitLastPlayheadX === playheadPixel) return;
    surface.context.clearRect(0, 0, surface.width, surface.height);
    surface.context.drawImage(state.pitBase, 0, 0, surface.width, surface.height);
    drawTimelinePlayhead(surface.context, playhead, surface.height);
    state.pitLastPlayheadX = playheadPixel;
  }

  function cleanLapPoints(laps) {
    var points = [];
    var breakBefore = true;
    (Array.isArray(laps) ? laps : []).forEach(function (lap) {
      var duration = numericValue(lap.duration_ms);
      var clean = !!lap.is_clean && duration !== null;
      if (clean) points.push({
        atUs: lap.completed_at_us,
        value: duration,
        lapNumber: numericValue(lap.lap_number),
        breakBefore: breakBefore || lap.break_before === true
      });
      breakBefore = !clean;
    });
    return points;
  }

  function aggregateLapPoints(points) {
    var previousEnd = null;
    return (Array.isArray(points) ? points : []).map(function (point) {
      var start = numericValue(point.window_started_at_us);
      var end = numericValue(point.window_ended_at_us);
      var result = {
        atUs: start !== null && end !== null ? Math.round((start + end) / 2) : start,
        value: numericValue(point.median_duration_ms),
        p25: numericValue(point.p25_duration_ms),
        p75: numericValue(point.p75_duration_ms),
        count: numericValue(point.participant_count),
        windowStartedAtUs: start,
        windowEndedAtUs: end,
        breakBefore: previousEnd !== null && start !== previousEnd
      };
      previousEnd = end;
      return result;
    }).filter(function (point) { return point.atUs !== null && point.value !== null; });
  }

  function renderLapLegend(data) {
    if (state.comparisonSelection === "ours" || !data || !data.comparison || !data.comparison.available) {
      elements.lapLegend.hidden = true;
      return;
    }
    var comparison = data.comparison;
    elements.lapLegend.hidden = false;
    if (comparison.mode === "participant") {
      var participant = comparisonParticipant(comparison.participant_id);
      elements.lapLegendKey.className = "ta-legend-key competitor";
      elements.lapLegendTitle.textContent = participant ? comparisonOptionLabel(participant) : "Соперник";
      elements.lapLegendText.textContent = "Подтверждённые чистые круги выбранной машины";
    } else {
      elements.lapLegendKey.className = "ta-legend-key aggregate";
      elements.lapLegendTitle.textContent = "Медиана соперников";
      elements.lapLegendText.textContent = "Медиана чистых кругов в 60-секундном окне; полоса — межквартильный диапазон";
    }
  }

  function latestLapPoint(points, atUs) {
    var result = null;
    (points || []).forEach(function (point) {
      if (typeof point.atUs === "number" && point.atUs <= atUs) result = point;
    });
    return result;
  }

  function rawLapStatus(point) {
    if (!point) return "";
    if (point.value === null) return "время не передано источником";
    var labels = [];
    if (point.crossesPit) labels.push("через пит-стоп");
    if (point.isInLap) labels.push("in lap");
    if (point.isOutLap) labels.push("out lap");
    if (point.flag && point.flag !== "GREEN") labels.push("флаг " + flagLabel(point.flag));
    if (!point.isClean && !labels.length) labels.push("неподтверждённый круг");
    return labels.length ? labels.join(" · ") : "боевой круг";
  }

  function rawLapPoints(laps, participant) {
    return (Array.isArray(laps) ? laps : []).map(function (lap) {
      var atUs = numericValue(lap && lap.completed_at_us);
      if (atUs === null) return null;
      var duration = numericValue(lap.duration_ms);
      return {
        id: [lap.participant_id || participant && participant.participant_id || "", lap.lap_number, atUs].join(":"),
        atUs: atUs,
        value: duration,
        lapNumber: numericValue(lap.lap_number),
        participantId: lap.participant_id || participant && participant.participant_id || null,
        startNumber: lap.start_number || participant && participant.start_number || null,
        teamName: lap.team_name || participant && participant.team_name || null,
        isOurs: !!(participant && participant.is_ours),
        isClean: !!lap.is_clean && duration !== null,
        crossesPit: !!lap.crosses_pit,
        isInLap: !!lap.is_in_lap,
        isOutLap: !!lap.is_out_lap,
        flag: lap.flag || null
      };
    }).filter(Boolean);
  }

  function rawCompetitorSeries(data) {
    var lapSeries = asObject(data && data.lap_series);
    var roster = comparisonParticipants();
    var byId = Object.create(null);
    roster.forEach(function (participant) { byId[participant.participant_id] = participant; });
    var entries = Array.isArray(lapSeries.competitors) ? lapSeries.competitors : [];
    var selectedId = state.comparisonSelection.indexOf("participant:") === 0 ?
      state.comparisonSelection.slice("participant:".length) : null;
    var selectedEntries = entries.filter(function (entry) {
      return !selectedId || entry && entry.participant_id === selectedId;
    });
    if (!selectedEntries.length && selectedId && Array.isArray(lapSeries.benchmark)) {
      selectedEntries = [{ participant_id: selectedId, laps: lapSeries.benchmark }];
    }
    return selectedEntries.map(function (entry) {
      var participant = byId[entry.participant_id] || {
        participant_id: entry.participant_id,
        start_number: entry.start_number,
        team_name: entry.team_name,
        is_ours: false
      };
      return {
        participant: participant,
        color: competitorColor(participant.participant_id),
        points: rawLapPoints(entry.laps, participant)
      };
    });
  }

  function rawPointKey(point) {
    return point ? point.id : "";
  }

  function rawPointFromGeometry(geometry, point) {
    if (!geometry || !point) return null;
    var key = rawPointKey(point);
    return (geometry.allPoints || []).find(function (candidate) { return rawPointKey(candidate) === key; }) || null;
  }

  function findNearestRawLapPoint(geometry, x, y, maximumDistance) {
    if (!geometry || !geometry.domain) return null;
    var nearest = null;
    (geometry.allPoints || []).forEach(function (point) {
      var pointX = geometry.xAt(point.atUs);
      var pointY = rawPointY(geometry, point);
      if (pointY === null) return;
      var distance = Math.hypot(pointX - x, pointY - y);
      if (distance > maximumDistance) return;
      // BALCHUG is drawn last, so it remains the intentional tie-breaker
      // when two recorded facts occupy the same visible coordinate.
      if (!nearest || distance < nearest.distance ||
          (distance === nearest.distance && point.isOurs && !nearest.point.isOurs)) {
        nearest = { point: point, distance: distance };
      }
    });
    return nearest;
  }

  function hideArchiveTooltip(element) {
    if (!element) return;
    element.hidden = true;
  }

  function appendTooltipText(parent, className, text) {
    var node = document.createElement("span");
    node.className = className;
    node.textContent = text;
    parent.appendChild(node);
    return node;
  }

  function positionArchiveTooltip(element, canvas, anchor) {
    if (!element || !canvas || !anchor) return;
    var parent = element.parentElement;
    if (!parent) return;
    var parentRect = parent.getBoundingClientRect();
    var canvasRect = canvas.getBoundingClientRect();
    element.hidden = false;
    element.style.left = "8px";
    element.style.top = "8px";
    var width = element.offsetWidth;
    var height = element.offsetHeight;
    var pointLeft = canvasRect.left - parentRect.left + anchor.x;
    var pointTop = canvasRect.top - parentRect.top + anchor.y;
    var left = pointLeft + 14;
    if (left + width > parentRect.width - 8) left = pointLeft - width - 14;
    left = Math.max(8, Math.min(parentRect.width - width - 8, left));
    var top = pointTop - height - 12;
    if (top < 8) top = pointTop + 14;
    top = Math.max(8, Math.min(parentRect.height - height - 8, top));
    element.style.left = Math.round(left + parent.scrollLeft) + "px";
    element.style.top = Math.round(top + parent.scrollTop) + "px";
  }

  function historicalSnapshot(atUs) {
    var exact = exactSnapshotAt(atUs);
    var frame = keyframeAt(atUs);
    // The manifest may have advanced farther than a cached exact seek. Keep
    // tooltips on the newest durable state at or before the cursor.
    if (exact && (!frame || exact.effectiveAtUs >= frame.observed_at_us)) return asObject(exact.payload);
    return asObject(frame && frame.snapshot);
  }

  function archiveLapCountScope(snapshot) {
    var payload = asObject(snapshot);
    var derived = asObject(payload.archive_intervals);
    if (derived.lap_count_scope === "source_grid" || derived.lap_count_scope === "capture_tracker") {
      return derived.lap_count_scope;
    }
    var measured = asObject(payload.measured);
    var ours = asObject(measured.ours);
    var sourceLaps = numericValue(asObject(ours.state).laps);
    return sourceLaps === null ? "capture_tracker" : "source_grid";
  }

  function archiveLapCaption(lapNumber, atUs) {
    var scope = archiveLapCountScope(historicalSnapshot(atUs || state.atUs));
    return (scope === "capture_tracker" ? "Зафиксированный круг #" : "Круг #") + valueOrDash(lapNumber);
  }

  function archiveSourceTime(entry, field) {
    var item = asObject(entry);
    var computed = asObject(item.computed);
    var measured = asObject(item.measured);
    var measuredState = asObject(measured.state);
    var measuredValue = numericValue(measuredState[field + "_ms"]);
    var measuredKind = measuredState[field + "_kind"];
    if (measuredValue !== null && measuredKind === "TIME") return measuredValue;
    return numericValue(computed["source_" + field + "_ms"]);
  }

  function archiveExplicitLaps(entry) {
    var state = asObject(asObject(entry).measured).state;
    var laps = numericValue(asObject(state).laps);
    return laps !== null && laps >= 0 ? laps : null;
  }

  function archiveSourceRelationGap(snapshot, relation) {
    var payload = asObject(snapshot);
    var session = asObject(asObject(payload.computed).session);
    var targetId = session[relation + "_id"];
    var oursId = session.ours_participant_id;
    var participants = Array.isArray(payload.class_participants) ? payload.class_participants : [];
    if (!targetId || !oursId || !participants.length) return null;
    var ours = null;
    var target = null;
    participants.forEach(function (entry) {
      var item = asObject(entry);
      var computed = asObject(item.computed);
      var measured = asObject(item.measured);
      var id = computed.participant_id || measured.participant_id;
      if (id === oursId) ours = item;
      if (id === targetId) target = item;
    });
    if (!ours || !target) return null;
    if (oursId === targetId) return 0;
    var oursLaps = archiveExplicitLaps(ours);
    var targetLaps = archiveExplicitLaps(target);
    if (oursLaps !== null && targetLaps !== null && oursLaps !== targetLaps) return null;
    var oursComputed = asObject(ours.computed);
    var targetComputed = asObject(target.computed);
    var oursGap = archiveSourceTime(ours, "gap");
    var targetGap = archiveSourceTime(target, "gap");
    if (oursGap !== null && targetGap !== null) return Math.abs(oursGap - targetGap);
    var oursPos = numericValue(oursComputed.position_overall);
    var targetPos = numericValue(targetComputed.position_overall);
    if (oursPos === 1 && targetGap !== null) return targetGap;
    if (targetPos === 1 && oursGap !== null) return oursGap;
    // DIFF is source-provided only for absolute neighbours.  It is safe only
    // in that exact relationship and is never synthesized from lap times.
    if (oursPos !== null && targetPos === oursPos - 1) return archiveSourceTime(ours, "diff");
    if (oursPos !== null && targetPos === oursPos + 1) return archiveSourceTime(target, "diff");
    return null;
  }

  function archiveIntervalKeys(relation) {
    if (relation === "class_leader") return { gap: "gap_to_class_leader_ms", lap: "lap_delta_to_class_leader" };
    if (relation === "class_ahead") return { gap: "gap_to_ahead_ms", lap: "lap_delta_to_ahead" };
    return { gap: "gap_to_behind_ms", lap: "lap_delta_to_behind" };
  }

  function formatArchiveRelationDistance(atUs, snapshot, relation) {
    var payload = snapshot || historicalSnapshot(atUs);
    var session = asObject(asObject(payload.computed).session);
    var keys = archiveIntervalKeys(relation);
    var archiveIntervals = payload.archive_intervals;
    var hasArchiveIntervals = archiveIntervals && typeof archiveIntervals === "object" && !Array.isArray(archiveIntervals);
    if (hasArchiveIntervals) {
      var derivedGap = numericValue(archiveIntervals[keys.gap]);
      return derivedGap === null ? "—" : formatGap(derivedGap);
    }
    var gap = archiveSourceRelationGap(payload, relation);
    if (gap === null) gap = numericValue(session[keys.gap]);
    if (gap !== null) return formatGap(gap);
    return "—";
  }

  function formatBehindDistance(atUs, snapshot) {
    return formatArchiveRelationDistance(atUs, snapshot, "class_behind");
  }

  function rawLapTooltipAnchor(point, geometry) {
    return point && geometry ? { x: geometry.xAt(point.atUs), y: rawPointY(geometry, point) } : null;
  }

  function chartTimeAt(geometry, x) {
    if (!geometry || !geometry.range) return null;
    var usableWidth = Math.max(1, geometry.width - geometry.left - geometry.right);
    var ratio = Math.max(0, Math.min(1, (x - geometry.left) / usableWidth));
    return Math.round(geometry.range.first_at_us + ratio * (geometry.range.last_at_us - geometry.range.first_at_us));
  }

  function renderRawLapTooltip(point, geometry) {
    var tooltip = elements.chartTooltip;
    if (!tooltip || !point || !geometry) {
      hideArchiveTooltip(tooltip);
      return;
    }
    var clock = timelineClockAt(point.atUs);
    var color = point.isOurs ? "#F0143D" : competitorColor(point.participantId);
    var team = point.isOurs ? "BALCHUG Racing" : (point.teamName || "Соперник");
    var crew = (point.startNumber ? "#" + point.startNumber + " · " : "") + team;
    var gapText = formatBehindDistance(point.atUs);
    tooltip.style.setProperty("--ta-tooltip-accent", color);
    tooltip.replaceChildren();
    appendTooltipText(tooltip, "ta-tooltip-kicker", clock.source ? "Время табло" : "Время записи");
    appendTooltipText(tooltip, "ta-tooltip-time", formatAbsolute(clock.atUs));
    appendTooltipText(tooltip, "ta-tooltip-primary", point.value === null ? "Время не передано" : formatLap(point.value));
    appendTooltipText(tooltip, "ta-tooltip-detail", archiveLapCaption(point.lapNumber, point.atUs) + " · " + crew);
    appendTooltipText(tooltip, "ta-tooltip-context", rawLapStatus(point));
    var gap = appendTooltipText(tooltip, "ta-tooltip-gap", "До соперника сзади BALCHUG Racing");
    var gapValue = document.createElement("b");
    gapValue.textContent = gapText;
    gap.appendChild(gapValue);
    var anchor = state.chartTooltipAnchor || rawLapTooltipAnchor(point, geometry);
    positionArchiveTooltip(tooltip, elements.chart, anchor);
  }

  function renderTimelineTooltip(atUs, geometry) {
    var tooltip = elements.chartTooltip;
    if (!tooltip || atUs === null || !geometry) {
      hideArchiveTooltip(tooltip);
      return;
    }
    var clock = timelineClockAt(atUs);
    var snapshot = historicalSnapshot(atUs);
    var ownPoint = latestLapPoint(geometry.own, atUs);
    var lapLabel = ownPoint && ownPoint.lapNumber !== null
      ? archiveLapCaption(ownPoint.lapNumber, ownPoint.atUs)
      : "Зафиксированный круг не передан";
    tooltip.style.setProperty("--ta-tooltip-accent", "#1B365D");
    tooltip.replaceChildren();
    appendTooltipText(tooltip, "ta-tooltip-kicker", clock.source ? "Срез · время табло" : "Срез · время записи");
    appendTooltipText(tooltip, "ta-tooltip-time", formatAbsolute(clock.atUs));
    appendTooltipText(tooltip, "ta-tooltip-primary", formatBehindDistance(atUs));
    appendTooltipText(tooltip, "ta-tooltip-detail", "До соперника сзади BALCHUG Racing · " + lapLabel);
    appendTooltipText(tooltip, "ta-tooltip-context", "Наведите на точку, чтобы увидеть точное время круга");
    positionArchiveTooltip(tooltip, elements.chart, state.chartTooltipAnchor || { x: geometry.xAt(atUs), y: geometry.top + 8 });
  }

  function updateRawLapTooltip(geometry) {
    var point = rawPointFromGeometry(geometry, state.hoveredLapPoint) ||
      (state.hoveredTimelineAtUs === null ? rawPointFromGeometry(geometry, state.selectedLapPoint) : null);
    if (point) {
      renderRawLapTooltip(point, geometry);
      return;
    }
    renderTimelineTooltip(state.hoveredTimelineAtUs, geometry);
  }

  function lapMomentLabel(point) {
    if (!point) return "";
    var moment = timelineClockAt(point.atUs);
    return formatAbsolute(moment.atUs);
  }

  function lapReadoutItem(label, value, detail, className) {
    var item = document.createElement("div");
    item.className = "ta-lap-readout-item " + className;
    var title = document.createElement("b");
    title.textContent = value;
    title.title = value;
    var caption = document.createElement("span");
    caption.textContent = label + (detail ? " · " + detail : "");
    item.appendChild(title);
    item.appendChild(caption);
    return item;
  }

  function renderLapReadout(data, geometry) {
    if (!elements.lapReadout || !geometry) return;
    var selected = rawPointFromGeometry(geometry, state.selectedLapPoint);
    var own = latestLapPoint(geometry.own, state.atUs);
    var competitorPoints = (geometry.competitors || []).reduce(function (items, series) {
      return items.concat(series.points || []);
    }, []);
    var competitor = selected && !selected.isOurs ? selected : latestLapPoint(competitorPoints, state.atUs);
    function pointValue(point) {
      if (!point) return "Нет круга";
      return archiveLapCaption(point.lapNumber, point.atUs) + " · " + (point.value === null ? "время не передано" : formatLap(point.value));
    }
    function pointDetail(point) {
      return point ? lapMomentLabel(point) + " · " + rawLapStatus(point) : "до курсора";
    }
    var ownValue = pointValue(own);
    var competitorLabel = competitor ?
      (competitor.isOurs ? "BALCHUG Racing" : (competitor.startNumber ? "#" + competitor.startNumber + " · " : "") + (competitor.teamName || "Соперник")) :
      (state.comparisonSelection === "all" ? "Соперники" : "Выбранный соперник");
    var competitorValue = pointValue(competitor);
    var key = [state.comparisonSelection, rawPointKey(selected), rawPointKey(own), rawPointKey(competitor), ownValue, competitorValue].join("|");
    if (key === state.lapReadoutKey) return;
    state.lapReadoutKey = key;
    elements.lapReadout.style.gridTemplateColumns = state.comparisonSelection === "ours" ? "1fr" : "";
    elements.lapReadout.replaceChildren();
    if (selected) {
      var selectedLabel = selected.isOurs ? "Выбрано · BALCHUG Racing" : "Выбрано · " + competitorLabel;
      var selectedItem = lapReadoutItem(selectedLabel, pointValue(selected), pointDetail(selected), selected.isOurs ? "ours" : "competitor");
      selectedItem.classList.add("selected");
      elements.lapReadout.appendChild(selectedItem);
    }
    elements.lapReadout.appendChild(lapReadoutItem("BALCHUG Racing", ownValue, pointDetail(own), "ours"));
    if (state.comparisonSelection !== "ours") {
      elements.lapReadout.appendChild(lapReadoutItem(competitorLabel, competitorValue, pointDetail(competitor), "competitor"));
    }
    elements.lapReadout.hidden = false;
  }

  function drawFocusedLapPoint(context, point, geometry, color, prefix, offset) {
    if (!point || !geometry.yAt) return;
    var x = geometry.xAt(point.atUs);
    var y = geometry.yAt(point.value);
    context.save();
    context.fillStyle = "#fff";
    context.strokeStyle = color;
    context.lineWidth = 2;
    context.beginPath(); context.arc(x, y, 5, 0, Math.PI * 2); context.fill(); context.stroke();
    var label = prefix + formatLap(point.value);
    context.font = "10px Arial";
    var labelWidth = Math.ceil(context.measureText(label).width) + 8;
    var labelX = Math.max(geometry.left, Math.min(geometry.width - geometry.right - labelWidth, x + 7));
    var labelY = Math.max(12, Math.min(geometry.height - 4, y + offset));
    context.fillStyle = "rgba(5,8,13,.84)";
    context.fillRect(labelX - 3, labelY - 10, labelWidth, 14);
    context.fillStyle = "#fff";
    context.fillText(label, labelX + 1, labelY);
    context.restore();
  }

  function drawLapChartBase(context, data, width, height) {
    renderLapLegend(data);
    var range = state.manifest.range;
    var left = 42;
    var right = 10;
    var usableWidth = Math.max(1, width - left - right);
    var total = Math.max(1, range.last_at_us - range.first_at_us);
    var xAt = function (atUs) { return left + (atUs - range.first_at_us) / total * usableWidth; };
    drawTimelineFlags(context, xAt, width, height, range, left, right);
    var own = cleanLapPoints(data.lap_series.ours);
    var aggregate = data.lap_series.benchmark_kind === "minute_median";
    var benchmark = aggregate ? aggregateLapPoints(data.lap_series.benchmark) : cleanLapPoints(data.lap_series.benchmark);
    var values = own.concat(benchmark).map(function (point) { return point.value; });
    benchmark.forEach(function (point) { if (point.p25 !== null) values.push(point.p25); if (point.p75 !== null) values.push(point.p75); });
    if (!values.length) {
      context.fillStyle = "#6E7E98";
      context.font = "11px Arial";
      context.fillText("Нет подтверждённых чистых кругов в выбранном сравнении", left, height / 2);
      return { left: left, right: right, range: range, width: width, height: height, xAt: xAt, yAt: null, own: own, benchmark: benchmark, aggregate: aggregate };
    }
    var min = Math.min.apply(Math, values);
    var max = Math.max.apply(Math, values);
    if (min === max) { min -= 1; max += 1; }
    var yAt = function (value) { return 16 + (value - min) / (max - min) * (height - 32); };
    context.fillStyle = "#6E7E98";
    context.font = "10px Arial";
    context.fillText("Время", 2, 26);
    context.fillText(formatLap(min), width - right - 48, 26);
    context.fillText(formatLap(max), width - right - 48, height - 16);
    context.strokeStyle = "#E4E9F0";
    context.lineWidth = 1;
    [0.25, 0.5, 0.75].forEach(function (ratio) {
      var y = Math.round(16 + (height - 32) * ratio) + 0.5;
      context.beginPath(); context.moveTo(left, y); context.lineTo(width - right, y); context.stroke();
    });
    function drawBand(points) {
      var segment = [];
      function flush() {
        if (segment.length < 2) { segment = []; return; }
        context.fillStyle = "rgba(82,98,118,.14)";
        context.beginPath();
        segment.forEach(function (point, index) {
          var x = xAt(point.atUs);
          if (!index) context.moveTo(x, yAt(point.p25));
          else { context.lineTo(x, yAt(segment[index - 1].p25)); context.lineTo(x, yAt(point.p25)); }
        });
        for (var index = segment.length - 1; index >= 0; index -= 1) {
          var point = segment[index];
          if (index === segment.length - 1) context.lineTo(xAt(point.atUs), yAt(point.p75));
          else { context.lineTo(xAt(point.atUs), yAt(segment[index + 1].p75)); context.lineTo(xAt(point.atUs), yAt(point.p75)); }
        }
        context.closePath(); context.fill(); segment = [];
      }
      points.forEach(function (point) {
        if (point.count !== null && point.count >= 2 && point.p25 !== null && point.p75 !== null) segment.push(point);
        else flush();
      });
      flush();
    }
    function drawLapSeries(points, color, dash) {
      context.strokeStyle = color;
      context.lineWidth = 1.5;
      context.setLineDash(dash || []);
      context.beginPath();
      var previous = null;
      points.forEach(function (point) {
        var x = xAt(point.atUs);
        var y = yAt(point.value);
        if (previous === null || point.breakBefore) context.moveTo(x, y);
        else context.lineTo(x, y);
        previous = point;
      });
      context.stroke();
      context.setLineDash([]);
      context.fillStyle = color;
      points.forEach(function (point) {
        context.beginPath(); context.arc(xAt(point.atUs), yAt(point.value), 2.3, 0, Math.PI * 2); context.fill();
      });
    }
    if (aggregate) drawBand(benchmark);
    drawLapSeries(own, "#F0143D");
    if (state.comparisonSelection !== "ours") drawLapSeries(benchmark, aggregate ? "#526276" : "#007B91", aggregate ? [4, 3] : []);
    return { left: left, right: right, range: range, width: width, height: height, xAt: xAt, yAt: yAt, own: own, benchmark: benchmark, aggregate: aggregate };
  }

  function drawLapChart() {
    var data = comparisonVisualData();
    if (!data || !state.manifest || !data.lap_series) {
      elements.lapPanel.hidden = true;
      elements.lapAxis.hidden = true;
      elements.lapReadout.hidden = true;
      state.lapBase = null;
      state.lapBaseKey = "";
      return;
    }
    elements.lapPanel.hidden = false;
    var surface = prepareCanvas(elements.lapChart, 186, false);
    var ratio = window.devicePixelRatio || 1;
    var baseKey = [
      state.manifest.heat && state.manifest.heat.source_heat_id,
      state.comparisonSelection,
      state.comparisonRevision,
      surface.width,
      surface.height,
      ratio
    ].join(":");
    if (!state.lapBase || state.lapBaseKey !== baseKey) {
      var base = staticCanvas(surface.width, surface.height, ratio);
      state.lapGeometry = drawLapChartBase(base.context, data, surface.width, surface.height);
      state.lapBase = base.canvas;
      state.lapBaseKey = baseKey;
      state.lapLastPlayheadX = null;
    }
    var geometry = state.lapGeometry;
    renderTimelineAxis(elements.lapAxis, geometry, data.lap_series && (data.lap_series.ours_raw || data.lap_series.ours));
    renderLapReadout(data, geometry);
    var total = Math.max(1, geometry.range.last_at_us - geometry.range.first_at_us);
    var playhead = geometry.left + (state.atUs - geometry.range.first_at_us) / total * Math.max(1, surface.width - geometry.left - geometry.right);
    var playheadPixel = Math.round(playhead);
    var focusedOwn = latestLapPoint(geometry.own, state.atUs);
    var focusedBenchmark = latestLapPoint(geometry.benchmark, state.atUs);
    var visualKey = [playheadPixel, focusedOwn && focusedOwn.atUs, focusedBenchmark && focusedBenchmark.atUs].join(":");
    if (state.lapLastPlayheadX === visualKey) return;
    surface.context.clearRect(0, 0, surface.width, surface.height);
    surface.context.drawImage(state.lapBase, 0, 0, surface.width, surface.height);
    drawTimelinePlayhead(surface.context, playhead, surface.height);
    drawFocusedLapPoint(surface.context, focusedOwn, geometry, "#F0143D", "#", -8);
    if (state.comparisonSelection !== "ours") {
      drawFocusedLapPoint(surface.context, focusedBenchmark, geometry,
        geometry.aggregate ? "#526276" : "#007B91", geometry.aggregate ? "мед. " : "#", 18);
    }
    state.lapLastPlayheadX = visualKey;
  }

  function drawComparisonVisuals() {
    // The lap chart is the primary canvas. The pit timeline only needs a
    // readable playhead, so avoid redrawing it at every animation frame.
    var now = performance.now();
    if (state.playing && now - state.visualsLastDrawMs < 50) return;
    state.visualsLastDrawMs = now;
    drawPitTimeline();
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

    function rangeFor(values, includeZero) {
      var known = values.filter(function (value) { return value !== null; });
      if (includeZero) known.push(0);
      if (!known.length) return null;
      var min = Math.min.apply(Math, known);
      var max = Math.max.apply(Math, known);
      if (min === max) { min -= 1; max += 1; }
      return { min: min, max: max };
    }

    function drawLabels(label, top, bandHeight, bounds, formatter) {
      context.fillStyle = "#6E7E98";
      context.font = "10px Arial";
      context.fillText(label, 2, top + 10);
      if (!bounds) return;
      context.fillText(formatter(bounds.min), width - padding.right - 48, top + 10);
      context.fillText(formatter(bounds.max), width - padding.right - 48, top + bandHeight);
    }

    function drawStep(samples, key, yAt, color, dash) {
      context.strokeStyle = color;
      context.lineWidth = 1.6;
      context.setLineDash(dash || []);
      context.beginPath();
      var previous = null;
      samples.forEach(function (point) {
        var value = numericValue(point[key]);
        var x = xAt(point.atUs);
        if (value === null) { previous = null; return; }
        var y = yAt(value);
        if (previous === null) context.moveTo(x, y);
        else { context.lineTo(x, previous.y); context.lineTo(x, y); }
        previous = { y: y };
      });
      context.stroke();
      context.setLineDash([]);
    }

    function drawBand(samples, yAt) {
      var segment = [];
      function flush() {
        if (segment.length < 2) { segment = []; return; }
        context.fillStyle = "rgba(82,98,118,.14)";
        context.beginPath();
        segment.forEach(function (point, index) {
          var x = xAt(point.atUs);
          var y = yAt(point.p25);
          if (!index) context.moveTo(x, y);
          else { context.lineTo(x, yAt(segment[index - 1].p25)); context.lineTo(x, y); }
        });
        for (var index = segment.length - 1; index >= 0; index -= 1) {
          var point = segment[index];
          var x = xAt(point.atUs);
          var y = yAt(point.p75);
          if (index === segment.length - 1) context.lineTo(x, y);
          else { context.lineTo(x, yAt(segment[index + 1].p75)); context.lineTo(x, y); }
        }
        context.closePath();
        context.fill();
        segment = [];
      }
      samples.forEach(function (point) {
        if (point.count !== null && point.count >= 2 && point.p25 !== null && point.p75 !== null) segment.push(point);
        else flush();
      });
      flush();
    }

    function drawDelta(samples, yAt) {
      var previous = null;
      samples.forEach(function (point) {
        var value = point.ours !== null && point.benchmark !== null ? point.ours - point.benchmark : null;
        if (value === null) { previous = null; return; }
        var x = xAt(point.atUs);
        var y = yAt(value);
        if (previous !== null) {
          context.strokeStyle = value > 0 ? "#F0143D" : (value < 0 ? "#16824F" : "#526276");
          context.lineWidth = 1.7;
          context.setLineDash([]);
          context.beginPath();
          context.moveTo(previous.x, previous.y);
          context.lineTo(x, previous.y);
          context.lineTo(x, y);
          context.stroke();
        }
        previous = { x: x, y: y };
      });
    }

    var samples = comparisonChartSamples();
    var paceValues = [];
    samples.forEach(function (point) { [point.ours, point.benchmark, point.p25, point.p75].forEach(function (value) { if (value !== null) paceValues.push(value); }); });
    var paceBounds = rangeFor(paceValues, false);
    var paceTop = 16;
    var paceHeight = 57;
    drawLabels("Темп", paceTop, paceHeight, paceBounds, formatLap);
    if (paceBounds) {
      var paceY = function (value) { return paceTop + (value - paceBounds.min) / (paceBounds.max - paceBounds.min) * paceHeight; };
      var aggregate = state.comparison && state.comparison.comparison && state.comparison.comparison.mode === "all";
      if (aggregate) drawBand(samples, paceY);
      drawStep(samples, "ours", paceY, "#F0143D");
      if (state.comparisonSelection !== "ours") drawStep(samples, "benchmark", paceY, aggregate ? "#526276" : "#007B91", aggregate ? [4, 3] : []);
    }

    var deltaValues = samples.map(function (point) {
      return point.ours !== null && point.benchmark !== null ? point.ours - point.benchmark : null;
    });
    var deltaBounds = rangeFor(deltaValues, true);
    var deltaTop = 96;
    var deltaHeight = 57;
    drawLabels("Разница", deltaTop, deltaHeight, deltaBounds, formatGap);
    if (deltaBounds) {
      var deltaY = function (value) { return deltaTop + (value - deltaBounds.min) / (deltaBounds.max - deltaBounds.min) * deltaHeight; };
      context.strokeStyle = "#AAB4C3";
      context.lineWidth = 1;
      context.setLineDash([3, 3]);
      context.beginPath(); context.moveTo(padding.left, deltaY(0)); context.lineTo(width - padding.right, deltaY(0)); context.stroke();
      context.setLineDash([]);
      drawDelta(samples, deltaY);
    }

    (state.manifest.markers.pits || []).forEach(function (pit) {
      if (pit.entered_at_us < range.first_at_us || pit.entered_at_us > range.last_at_us) return;
      var x = xAt(pit.entered_at_us);
      context.strokeStyle = "#122846";
      context.lineWidth = 1;
      context.beginPath(); context.moveTo(x, 0); context.lineTo(x, height); context.stroke();
    });
  }

  function rawChartDomain(points) {
    var timed = points.filter(function (point) { return point.value !== null; });
    var reference = timed.filter(function (point) { return point.isClean; });
    var values = (reference.length ? reference : timed).map(function (point) { return point.value; });
    if (!values.length) return null;
    var min = Math.min.apply(Math, values);
    var max = Math.max.apply(Math, values);
    if (min === max) {
      var singlePadding = Math.max(1000, min * 0.03);
      return { min: Math.max(0, min - singlePadding), max: max + singlePadding };
    }
    var padding = Math.max(1000, (max - min) * 0.18);
    return { min: Math.max(0, min - padding), max: max + padding };
  }

  function rawPointY(geometry, point) {
    if (!geometry || !point) return null;
    if (point.value === null) return geometry.untimedY;
    if (point.value < geometry.domain.min) return geometry.top;
    if (point.value > geometry.domain.max) return geometry.bottom;
    return geometry.yAt(point.value);
  }

  function drawRawPoint(context, point, geometry, color) {
    var x = geometry.xAt(point.atUs);
    var y = rawPointY(geometry, point);
    if (y === null) return;
    context.save();
    context.strokeStyle = color;
    context.fillStyle = color;
    context.lineWidth = 1.8;
    if (point.value === null) {
      context.beginPath();
      context.moveTo(x - 3, y - 3); context.lineTo(x + 3, y + 3);
      context.moveTo(x + 3, y - 3); context.lineTo(x - 3, y + 3);
      context.stroke();
      context.restore();
      return;
    }
    var overflow = point.value < geometry.domain.min ? "fast" : (point.value > geometry.domain.max ? "slow" : null);
    if (overflow) {
      context.beginPath();
      if (overflow === "slow") {
        context.moveTo(x, y + 1); context.lineTo(x - 4, y - 6); context.lineTo(x + 4, y - 6);
      } else {
        context.moveTo(x, y - 1); context.lineTo(x - 4, y + 6); context.lineTo(x + 4, y + 6);
      }
      context.closePath();
      context.fill();
      context.restore();
      return;
    }
    if (point.isClean) {
      context.beginPath(); context.arc(x, y, 2.7, 0, Math.PI * 2); context.fill();
    } else {
      context.fillStyle = "#fff";
      context.fillRect(x - 3, y - 3, 6, 6);
      context.strokeRect(x - 3, y - 3, 6, 6);
    }
    context.restore();
  }

  function drawRawLapSeries(context, points, geometry, color, isOurs) {
    var previous = null;
    context.save();
    context.strokeStyle = color;
    context.lineWidth = isOurs ? 2.25 : 1.65;
    (points || []).forEach(function (point) {
      if (point.value === null) {
        previous = null;
        return;
      }
      var followsPreviousLap = previous && previous.lapNumber !== null && point.lapNumber !== null &&
        point.lapNumber === previous.lapNumber + 1;
      if (previous && followsPreviousLap) {
        var isRaceLine = previous.isClean && point.isClean;
        context.setLineDash(isRaceLine ? [] : [5, 4]);
        context.beginPath();
        context.moveTo(geometry.xAt(previous.atUs), rawPointY(geometry, previous));
        context.lineTo(geometry.xAt(point.atUs), rawPointY(geometry, point));
        context.stroke();
      }
      previous = point;
    });
    context.setLineDash([]);
    context.restore();
    (points || []).forEach(function (point) { drawRawPoint(context, point, geometry, color); });
  }

  function drawRawLapChartBase(context, data, width, height) {
    var range = state.manifest.range;
    var left = 42;
    var right = 10;
    var top = 16;
    var bottom = height - 26;
    var untimedY = height - 8;
    var usableWidth = Math.max(1, width - left - right);
    var total = Math.max(1, range.last_at_us - range.first_at_us);
    var xAt = function (atUs) { return left + (atUs - range.first_at_us) / total * usableWidth; };
    var oursParticipant = comparisonParticipants().find(function (participant) { return participant.is_ours; }) || {
      participant_id: comparisonOursId(), team_name: "BALCHUG Racing", is_ours: true
    };
    var own = rawLapPoints(asObject(data.lap_series).ours_raw || asObject(data.lap_series).ours, oursParticipant).filter(function (point) {
      return point.atUs >= range.first_at_us && point.atUs <= range.last_at_us;
    });
    var competitors = state.comparisonSelection === "ours" ? [] : rawCompetitorSeries(data);
    competitors.forEach(function (series) {
      series.points = series.points.filter(function (point) {
        return point.atUs >= range.first_at_us && point.atUs <= range.last_at_us;
      });
    });
    var allPoints = own.concat(competitors.reduce(function (items, series) { return items.concat(series.points); }, []));
    var domain = rawChartDomain(allPoints);
    drawTimelineFlags(context, xAt, width, height, range, left, right);
    context.strokeStyle = "#E4E9F0";
    context.lineWidth = 1;
    [0.25, 0.5, 0.75].forEach(function (ratio) {
      var y = Math.round(top + (bottom - top) * ratio) + 0.5;
      context.beginPath(); context.moveTo(left, y); context.lineTo(width - right, y); context.stroke();
    });
    if (!domain) {
      context.fillStyle = "#6E7E98";
      context.font = "11px Arial";
      context.fillText("В сохранённой части нет кругов с переданным временем", left, Math.round(height / 2));
      return {
        left: left, right: right, range: range, width: width, height: height, top: top, bottom: bottom,
        untimedY: untimedY, xAt: xAt, yAt: null, pointY: function () { return untimedY; }, domain: null,
        own: own, competitors: competitors, allPoints: allPoints
      };
    }
    var yAt = function (value) { return top + (value - domain.min) / (domain.max - domain.min) * (bottom - top); };
    context.fillStyle = "#6E7E98";
    context.font = "10px Arial";
    context.fillText("Темп", 2, top + 10);
    context.fillText(formatLap(domain.min), width - right - 48, top + 10);
    context.fillText(formatLap(domain.max), width - right - 48, bottom);
    context.font = "8.5px Arial";
    context.fillText("x", 4, untimedY + 3);
    var geometry = {
      left: left, right: right, range: range, width: width, height: height, top: top, bottom: bottom,
      untimedY: untimedY, xAt: xAt, yAt: yAt, domain: domain, own: own, competitors: competitors, allPoints: allPoints,
      pointY: function (point) { return rawPointY(this, point); }
    };
    competitors.forEach(function (series) { drawRawLapSeries(context, series.points, geometry, series.color, false); });
    drawRawLapSeries(context, own, geometry, "#F0143D", true);
    return geometry;
  }

  function drawSelectedRawPoint(context, point, geometry) {
    if (!point || !geometry || !geometry.domain) return;
    var x = geometry.xAt(point.atUs);
    var y = rawPointY(geometry, point);
    if (y === null) return;
    var color = point.isOurs ? "#F0143D" : competitorColor(point.participantId);
    context.save();
    context.strokeStyle = color;
    context.fillStyle = "#fff";
    context.lineWidth = 2.2;
    context.beginPath(); context.arc(x, y, 5, 0, Math.PI * 2); context.fill(); context.stroke();
    context.restore();
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
    var comparisonData = comparisonVisualData();
    var rawData = comparisonData || { lap_series: { ours: [], competitors: [] } };
    var baseKey = [
      state.manifest.heat && state.manifest.heat.source_heat_id,
      range.first_at_us,
      range.last_at_us,
      state.manifest.keyframes.length,
      (state.manifest.markers.flags || []).length,
      (state.manifest.markers.pits || []).length,
      state.comparisonSelection,
      state.comparisonRevision,
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
      state.chartGeometry = drawRawLapChartBase(baseContext, rawData, width, height);
      state.chartBase = base;
      state.chartBaseKey = baseKey;
    }
    var geometry = state.chartGeometry;
    renderTimelineAxis(elements.chartAxis, geometry || { left: 42, right: 10, range: range, width: width },
      comparisonData && comparisonData.lap_series && (comparisonData.lap_series.ours_raw || comparisonData.lap_series.ours));
    var context = canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, width, height);
    context.drawImage(state.chartBase, 0, 0, width, height);
    var playheadX = geometry ? geometry.xAt(state.atUs) : 42;
    context.strokeStyle = "#122846";
    context.lineWidth = 1.5;
    context.setLineDash([4, 3]);
    context.beginPath(); context.moveTo(playheadX, 0); context.lineTo(playheadX, height); context.stroke();
    context.setLineDash([]);
    if (comparisonData && geometry) {
      renderLapReadout(comparisonData, geometry);
      var activePoint = rawPointFromGeometry(geometry, state.hoveredLapPoint) ||
        (state.hoveredTimelineAtUs === null ? rawPointFromGeometry(geometry, state.selectedLapPoint) : null);
      drawSelectedRawPoint(context, activePoint, geometry);
      updateRawLapTooltip(geometry);
    } else {
      elements.lapReadout.hidden = true;
      hideArchiveTooltip(elements.chartTooltip);
    }
    var chartRangeText = formatElapsed((state.atUs - range.first_at_us) / 1000000) + " / " + formatElapsed((range.last_at_us - range.first_at_us) / 1000000);
    if (chartRangeText !== state.chartRangeText) {
      state.chartRangeText = chartRangeText;
      elements.chartRange.textContent = chartRangeText;
    }
    drawComparisonVisuals();
  }

  function buildEvents() {
    if (!state.manifest) return;
    var events = [];
    var range = state.manifest.range;
    (state.manifest.markers.flags || []).forEach(function (flag, index) {
      if (typeof flag.started_at_us !== "number" || flag.carried_into_range || flag.started_at_us < range.first_at_us || flag.started_at_us > range.last_at_us) return;
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
        title: archiveLapCaption(lap.lap_number, lap.completed_at_us),
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
    state.visualsLastDrawMs = 0;
    if (wasPlaying && state.modalOpen && state.manifest) drawChart();
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

  function openTimingModal(detail) {
    detail = asObject(detail);
    if (typeof detail.selection === "string" && detail.selection) state.pendingSelection = detail.selection;
    if (!state.modalOpen) {
      state.focusReturn = detail.trigger && typeof detail.trigger.focus === "function" ? detail.trigger : document.activeElement;
      state.bodyOverflow = document.body.style.overflow;
      state.modalOpen = true;
      elements.modal.classList.add("open");
      elements.modal.setAttribute("aria-hidden", "false");
      document.body.style.overflow = "hidden";
      window.requestAnimationFrame(function () { elements.close.focus(); });
    }
    ensureSessionsLoaded();
  }

  function closeTimingModal() {
    if (!state.modalOpen) return;
    stopPlayback();
    state.hoveredLapPoint = null;
    state.chartTooltipAnchor = null;
    state.hoveredPit = null;
    state.pitTooltipAnchor = null;
    hideArchiveTooltip(elements.chartTooltip);
    hideArchiveTooltip(elements.pitTooltip);
    state.modalOpen = false;
    elements.modal.classList.remove("open");
    elements.modal.setAttribute("aria-hidden", "true");
    document.body.style.overflow = state.bodyOverflow === null ? "" : state.bodyOverflow;
    state.bodyOverflow = null;
    var focusReturn = state.focusReturn;
    state.focusReturn = null;
    if (focusReturn && document.contains(focusReturn)) window.requestAnimationFrame(function () { focusReturn.focus(); });
  }

  function trapTimingFocus(event) {
    if (event.key !== "Tab" || !state.modalOpen) return;
    var nodes = elements.modal.querySelectorAll("button:not([disabled]),select:not([disabled]),input:not([disabled]),[href]");
    if (!nodes.length) return;
    var first = nodes[0];
    var last = nodes[nodes.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  elements.select.addEventListener("change", loadSelectedEntry);
  elements.comparison.addEventListener("change", function () {
    if (!state.manifest) return;
    loadComparison(elements.comparison.value);
  });
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
  elements.chart.addEventListener("pointermove", function (event) {
    var geometry = state.chartGeometry;
    if (!state.manifest || !geometry || !geometry.domain) return;
    var rect = elements.chart.getBoundingClientRect();
    var x = event.clientX - rect.left;
    var y = event.clientY - rect.top;
    var nearest = findNearestRawLapPoint(geometry, x, y, 20);
    var previousKey = rawPointKey(state.hoveredLapPoint);
    state.hoveredLapPoint = nearest ? nearest.point : null;
    state.hoveredTimelineAtUs = nearest ? null : chartTimeAt(geometry, x);
    state.chartTooltipAnchor = { x: x, y: y };
    if (previousKey !== rawPointKey(state.hoveredLapPoint)) drawChart();
    else updateRawLapTooltip(geometry);
  });
  elements.chart.addEventListener("pointerleave", function () {
    if (!state.chartGeometry) return;
    state.hoveredLapPoint = null;
    state.hoveredTimelineAtUs = null;
    state.chartTooltipAnchor = null;
    drawChart();
  });
  elements.chart.addEventListener("click", function (event) {
    if (!state.manifest) return;
    var rect = elements.chart.getBoundingClientRect();
    var geometry = state.chartGeometry;
    var x = event.clientX - rect.left;
    var y = event.clientY - rect.top;
    var nearest = findNearestRawLapPoint(geometry, x, y, 20);
    stopPlayback();
    if (nearest) {
      state.selectedLapPoint = nearest.point;
      state.hoveredLapPoint = nearest.point;
      state.hoveredTimelineAtUs = null;
      state.chartTooltipAnchor = { x: x, y: y };
      setAt(nearest.point.atUs, true);
      return;
    }
    state.selectedLapPoint = null;
    state.hoveredLapPoint = null;
    state.hoveredTimelineAtUs = chartTimeAt(geometry, x);
    state.chartTooltipAnchor = { x: x, y: y };
    setAt(state.hoveredTimelineAtUs, true);
  });
  elements.pitChart.addEventListener("pointermove", function (event) {
    var geometry = state.pitGeometry;
    if (!geometry) return;
    var rect = elements.pitChart.getBoundingClientRect();
    var hit = findPitHit(geometry, event.clientX - rect.left, event.clientY - rect.top);
    state.hoveredPit = hit;
    state.pitTooltipAnchor = hit ? { x: event.clientX - rect.left, y: event.clientY - rect.top } : null;
    updatePitTooltip(geometry);
  });
  elements.pitChart.addEventListener("pointerleave", function () {
    state.hoveredPit = null;
    state.pitTooltipAnchor = null;
    hideArchiveTooltip(elements.pitTooltip);
  });
  elements.pitChart.addEventListener("click", function (event) {
    var geometry = state.pitGeometry;
    if (!geometry) return;
    var rect = elements.pitChart.getBoundingClientRect();
    var hit = findPitHit(geometry, event.clientX - rect.left, event.clientY - rect.top);
    if (!hit) return;
    stopPlayback();
    state.hoveredPit = hit;
    state.pitTooltipAnchor = { x: event.clientX - rect.left, y: event.clientY - rect.top };
    setAt(hit.atUs, true);
    updatePitTooltip(geometry);
  });
  elements.lapChart.addEventListener("click", function (event) {
    if (!state.manifest || !state.lapGeometry) return;
    var geometry = state.lapGeometry;
    var rect = elements.lapChart.getBoundingClientRect();
    var x = event.clientX - rect.left;
    var candidates = geometry.own.concat(state.comparisonSelection === "ours" ? [] : geometry.benchmark);
    var nearest = null;
    candidates.forEach(function (point) {
      var distance = Math.abs(geometry.xAt(point.atUs) - x);
      if (distance <= 24 && (!nearest || distance < nearest.distance)) nearest = { point: point, distance: distance };
    });
    if (!nearest) return;
    stopPlayback(); setAt(nearest.point.atUs, true);
  });
  elements.close.addEventListener("click", closeTimingModal);
  elements.modal.addEventListener("click", function (event) { if (event.target === elements.modal) closeTimingModal(); });
  elements.modal.addEventListener("keydown", trapTimingFocus);
  document.addEventListener("keydown", function (event) { if (event.key === "Escape" && state.modalOpen) closeTimingModal(); });
  window.addEventListener("balchug:open-timing-archive", function (event) { openTimingModal(event.detail); });
  window.addEventListener("resize", function () {
    if (state.modalOpen) window.requestAnimationFrame(drawChart);
  });

  controlsDisabled(true);
})();
