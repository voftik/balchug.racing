(function () {
  "use strict";

  var API = "/api/timing";
  var ADMIN_TOKEN_KEY = "balchug_admin";
  var ENGINEER_TOKEN_KEY = "balchug_engineer_token";
  var PANEL_STATE_KEY = "balchug_timing_panel";
  var TAB_TITLES = {
    overview: "Тактический обзор",
    pace: "Темп по кругам",
    intervals: "Интервалы",
    pits: "Пит-стопы и стинты",
    "class": "Наш класс",
    events: "События"
  };
  var MODE_LABELS = { practice: "Практика", qualifying: "Квалификация", race: "Гонка" };
  var SERIES_KEYS = ["blue", "teal", "amber", "violet"];
  var SERIES_COLORS = {
    ours: "#F0143D",
    blue: "#1976b8",
    teal: "#148477",
    amber: "#b96d00",
    violet: "#7356a5"
  };

  function byId(id) { return document.getElementById(id); }
  function all(selector, root) { return Array.prototype.slice.call((root || document).querySelectorAll(selector)); }
  function isNumber(value) { return typeof value === "number" && isFinite(value); }
  function clamp(value, minimum, maximum) { return Math.max(minimum, Math.min(maximum, value)); }
  function html(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }
  function readJson(key, fallback) {
    try {
      var parsed = JSON.parse(localStorage.getItem(key));
      return parsed && typeof parsed === "object" ? parsed : fallback;
    } catch (error) { return fallback; }
  }
  function writeJson(key, value) {
    try { localStorage.setItem(key, JSON.stringify(value)); } catch (error) {}
  }
  function randomKey(prefix) {
    var suffix = window.crypto && typeof window.crypto.randomUUID === "function"
      ? window.crypto.randomUUID()
      : String(Date.now()) + "-" + String(Math.random()).slice(2);
    return prefix + "-" + suffix;
  }
  function formatDuration(seconds) {
    if (!isNumber(seconds)) return "—";
    var whole = Math.max(0, Math.floor(seconds));
    var hours = Math.floor(whole / 3600);
    var minutes = Math.floor((whole % 3600) / 60);
    var remainder = whole % 60;
    return String(hours).padStart(2, "0") + ":" + String(minutes).padStart(2, "0") + ":" + String(remainder).padStart(2, "0");
  }
  function formatLap(ms) {
    if (!isNumber(ms) || ms <= 0) return "—";
    var minutes = Math.floor(ms / 60000);
    var seconds = (ms - minutes * 60000) / 1000;
    return minutes + ":" + seconds.toFixed(3).padStart(6, "0");
  }
  function formatGap(ms) {
    if (!isNumber(ms)) return "—";
    return (ms / 1000).toFixed(3) + " с";
  }
  function formatLaps(value) {
    if (!isNumber(value)) return "—";
    var amount = Math.max(0, Math.floor(value));
    var remainder10 = amount % 10;
    var remainder100 = amount % 100;
    var noun = remainder10 === 1 && remainder100 !== 11 ? "круг" :
      (remainder10 >= 2 && remainder10 <= 4 && (remainder100 < 12 || remainder100 > 14) ? "круга" : "кругов");
    return amount + " " + noun;
  }
  function formatClockAt(us) {
    if (!isNumber(us)) return "—";
    return new Intl.DateTimeFormat("ru-RU", {
      timeZone: "Europe/Moscow", hour: "2-digit", minute: "2-digit", second: "2-digit"
    }).format(new Date(us / 1000));
  }
  function normalizeFlag(flag) {
    var value = String(flag || "UNKNOWN").toUpperCase();
    if (value === "FULL_COURSE_YELLOW") return "FCY";
    if (value === "SAFETY_CAR") return "SC";
    return value;
  }
  function modeLabel(mode) { return MODE_LABELS[mode] || "Режим не запущен"; }
  function participantLabel(participant) {
    if (!participant) return "—";
    var number = participant.startNumber ? "#" + participant.startNumber + " · " : "";
    return number + (participant.teamName || participant.carName || "Экипаж");
  }

  var dom = {
    workspace: byId("timingWorkspace"), suite: byId("engineerSuite"), panel: byId("engineerPanel"), iframe: byId("lt"),
    sessionBadge: byId("sessionBadge"), sessionClock: byId("sessionClock"), stop: byId("sessionStop"),
    panelFlagStrip: byId("panelFlagStrip"), panelFlag: byId("panelFlag"), panelFlagElapsed: byId("panelFlagElapsed"),
    panelMode: byId("panelMode"), panelHeat: byId("panelHeat"), freshness: byId("freshnessBadge"),
    panelSessionTime: byId("panelSessionTime"), panelIdentity: byId("panelIdentity"),
    position: byId("decisionPosition"), ahead: byId("decisionAhead"), behind: byId("decisionBehind"),
    pace: byId("decisionPace"), tyres: byId("decisionTyres"), pits: byId("decisionPits"),
    viewTitle: byId("panelViewTitle"), scroll: byId("panelScroll"),
    competitorTrigger: byId("competitorTrigger"), competitorLabel: byId("competitorTriggerLabel"),
    competitorSwatches: byId("competitorSwatches"), competitorPopover: byId("competitorPopover"),
    competitorSearch: byId("competitorSearch"), competitorAuto: byId("competitorAuto"),
    competitorList: byId("competitorList"), competitorLimit: byId("competitorLimit"),
    liveRegion: byId("panelLiveRegion"), tooltip: byId("timingTooltip"),
    raceDialog: byId("raceDialog"), raceForm: byId("raceForm"), raceError: byId("raceError"),
    duration: byId("raceDuration"), requiredPits: byId("requiredPits"), pitMinus: byId("pitMinus"), pitPlus: byId("pitPlus"),
    engineerDialog: byId("engineerDialog"), engineerForm: byId("engineerForm"), engineerToken: byId("engineerToken"),
    engineerError: byId("engineerError")
  };
  if (!dom.workspace || !dom.suite || !dom.panel) return;
  try {
    if (!localStorage.getItem(ADMIN_TOKEN_KEY)) return;
  } catch (error) { return; }
  document.body.classList.add("admin");
  dom.suite.hidden = false;

  var query = new URLSearchParams(location.search);
  var demoMode = query.get("demo");
  var savedPanel = readJson(PANEL_STATE_KEY, {});
  var state = {
    track: query.get("track") === "moscow" ? "moscow" : "igora",
    demo: demoMode === "1" || demoMode === "24h",
    longDemo: demoMode === "24h",
    tab: TAB_TITLES[savedPanel.tab] ? savedPanel.tab : "overview",
    activeSession: null,
    snapshot: null,
    view: null,
    stream: null,
    refreshTimer: null,
    clockTimer: null,
    lastSnapshotAt: 0,
    busy: false,
    competitorMode: "auto",
    selected: [],
    colors: {},
    pendingAuthorizedAction: null,
    raceDuration: 14400,
    requiredPits: 2,
    viewReady: {},
    charts: {},
    chartObservers: {},
    demoTick: 0,
    eventFilter: "all"
  };

  function storageKey() {
    var sessionId = state.view && state.view.sessionId ? state.view.sessionId : "idle";
    return "balchug_timing_display:" + state.track + ":" + sessionId;
  }

  function restoreDisplayState() {
    var saved = readJson(storageKey(), {});
    state.competitorMode = saved.mode === "manual" ? "manual" : "auto";
    state.selected = Array.isArray(saved.selected) ? saved.selected.slice(0, 3) : [];
    state.colors = saved.colors && typeof saved.colors === "object" ? saved.colors : {};
  }

  function persistDisplayState() {
    writeJson(storageKey(), { mode: state.competitorMode, selected: state.selected, colors: state.colors });
  }

  function emptyView() {
    return {
      sessionId: null, lifecycle: "idle", mode: null, heat: null, freshness: "OFFLINE",
      elapsedS: 0, remainingS: null, requiredPits: null, flag: "UNKNOWN", flagElapsedS: null,
      identityState: "pending", oursId: null, ours: null, participants: [], sessionMetric: {},
      ahead: null, behind: null, alerts: [], events: [], history: null
    };
  }

  function metricEntries(snapshot) {
    return snapshot && snapshot.computed && Array.isArray(snapshot.computed.metrics)
      ? snapshot.computed.metrics : [];
  }

  function snapshotToView(snapshot) {
    if (!snapshot || !snapshot.session) return emptyView();
    var entries = metricEntries(snapshot);
    var sessionMetric = {};
    var valuesById = {};
    entries.forEach(function (entry) {
      if (!entry || !entry.scope) return;
      if (entry.scope.kind === "session") sessionMetric = entry.values || {};
      if (entry.scope.kind === "participant") valuesById[entry.scope.key] = entry.values || {};
    });
    var measured = snapshot.measured && Array.isArray(snapshot.measured.participants)
      ? snapshot.measured.participants : [];
    var oursId = snapshot.session.our_participant_id || sessionMetric.ours_participant_id || null;
    var oursClass = snapshot.session.our_class || sessionMetric.ours_class_key || null;
    var participants = measured.map(function (source) {
      var metric = valuesById[source.participant_id] || {};
      var sourceState = source.state || {};
      return {
        id: source.participant_id,
        startNumber: source.start_number || metric.start_number || null,
        teamName: source.team_name || metric.team_name || null,
        driverName: source.driver_name || sourceState.current_driver_name || metric.current_driver_name || null,
        carName: source.car_name || metric.car_name || null,
        className: source.class_name || metric.class_name || null,
        classKey: source.class_key || metric.class_key || null,
        active: source.active !== false && metric.active !== false,
        isOurs: source.is_ours === true || metric.is_ours === true || source.participant_id === oursId,
        positionClass: isNumber(metric.position_class) ? metric.position_class : sourceState.position_class,
        positionOverall: isNumber(metric.position_overall) ? metric.position_overall : sourceState.position_overall,
        lastLapMs: isNumber(metric.last_lap_ms) ? metric.last_lap_ms : sourceState.last_lap_ms,
        bestLapMs: isNumber(metric.best_lap_ms) ? metric.best_lap_ms : sourceState.best_lap_ms,
        pace3Ms: metric.pace_3_ms, pace5Ms: metric.pace_5_ms, pace10Ms: metric.pace_10_ms,
        tyreAge: metric.tyre_age_laps, pitsCompleted: metric.pits_completed,
        stateKind: metric.current_state || sourceState.state_kind || "UNKNOWN",
        observedLaps: metric.observed_lap_count,
        stintNumber: metric.stint_number,
        stintElapsedS: metric.stint_elapsed_s,
        stintTrend: metric.stint_trend_ms_per_lap,
        pitHistory: Array.isArray(metric.pit_history) ? metric.pit_history : []
      };
    });
    var ours = participants.find(function (participant) { return participant.isOurs; }) || null;
    if (ours && oursClass) {
      participants = participants.filter(function (participant) {
        return participant.isOurs || participant.className === oursClass || participant.classKey === ours.classKey;
      });
    }
    participants.sort(function (left, right) {
      var leftPosition = isNumber(left.positionClass) ? left.positionClass : 9999;
      var rightPosition = isNumber(right.positionClass) ? right.positionClass : 9999;
      return leftPosition - rightPosition || String(left.startNumber || "").localeCompare(String(right.startNumber || ""));
    });
    var participantById = {};
    participants.forEach(function (participant) { participantById[participant.id] = participant; });
    var ahead = participantById[sessionMetric.class_ahead_id] || null;
    var behind = participantById[sessionMetric.class_behind_id] || null;
    var pitHistory = sessionMetric.pit_history || (ours ? ours.pitHistory : []) || [];
    var alerts = Array.isArray(sessionMetric.alerts) ? sessionMetric.alerts : [];
    var events = [];
    var flag = normalizeFlag(
      sessionMetric.track_flag ||
      (snapshot.measured && snapshot.measured.track_flag && snapshot.measured.track_flag.flag)
    );
    if (flag !== "UNKNOWN") {
      events.push({ kind: "flag", atUs: snapshot.freshness && snapshot.freshness.observed_at_us, text: "Флаг трассы: " + flag });
    }
    pitHistory.slice(-4).forEach(function (pit) {
      events.push({ kind: "pit", atUs: pit.pit_in_at_us, text: "BALCHUG: пит-стоп №" + pit.stop_number + " · " + formatGap(pit.pit_lane_duration_ms) });
    });
    alerts.slice(-4).forEach(function (alert) {
      events.push({ kind: "ours", atUs: alert.at_us, text: alertLabel(alert) });
    });
    events.sort(function (left, right) { return (right.atUs || 0) - (left.atUs || 0); });
    return {
      sessionId: snapshot.session.id,
      lifecycle: snapshot.session.lifecycle,
      mode: snapshot.session.mode,
      heat: snapshot.heat && snapshot.heat.external_name,
      freshness: snapshot.freshness && snapshot.freshness.status || sessionMetric.channel_status || "OFFLINE",
      elapsedS: isNumber(sessionMetric.session_elapsed_s)
        ? sessionMetric.session_elapsed_s
        : Math.max(0, ((snapshot.freshness && snapshot.freshness.computed_at_us) || Date.now() * 1000) / 1000000 - snapshot.session.started_at_us / 1000000),
      remainingS: sessionMetric.session_remaining_s,
      requiredPits: snapshot.session.required_pits,
      flag: flag,
      flagElapsedS: sessionMetric.flag_phase_elapsed_s,
      identityState: snapshot.session.identity_state,
      oursId: oursId,
      ours: ours,
      participants: participants,
      sessionMetric: sessionMetric,
      ahead: ahead,
      behind: behind,
      alerts: alerts,
      events: events,
      history: null
    };
  }

  function alertLabel(alert) {
    var labels = {
      ours_pit_too_long: "Пит-стоп дольше типичного",
      slow_lap: "Потеря темпа на последнем круге",
      mandatory_pit_due: "Приближается обязательный пит-стоп",
      threat: "Соперник сзади сокращает интервал",
      catch: "Сокращаем интервал до машины впереди"
    };
    return labels[alert && alert.key] || "Тактическое событие";
  }

  function demoView() {
    var ours = {
      id: "demo-21", startNumber: "21", teamName: "BALCHUG Racing", driverName: "Лобода Михаил",
      carName: "Ligier JS53 evo2", className: "CN PRO", classKey: "cn pro", active: true, isOurs: true,
      positionClass: 2, positionOverall: 3, lastLapMs: 106742, bestLapMs: 105911,
      pace3Ms: 106480, pace5Ms: 106620, pace10Ms: 106910, tyreAge: 12, pitsCompleted: 1,
      stateKind: "ON_TRACK", observedLaps: 37, stintNumber: 2, stintElapsedS: 1288,
      stintTrend: 42, pitHistory: [{ stop_number: 1, pit_in_at_us: 1783770960000000, pit_out_at_us: 1783771038400000, pit_in_lap: 25, pit_out_lap: 26, pit_lane_duration_ms: 78400 }]
    };
    var participants = [
      { id: "demo-9", startNumber: "9", teamName: "Про Моторспорт", driverName: "Мухин Игорь", carName: "Norma", className: "CN PRO", classKey: "cn pro", active: true, isOurs: false, positionClass: 1, positionOverall: 1, lastLapMs: 106105, bestLapMs: 105260, pace3Ms: 106310, pace5Ms: 106460, pace10Ms: 106720, tyreAge: 8, pitsCompleted: 2, stateKind: "ON_TRACK", observedLaps: 38, stintNumber: 3, stintElapsedS: 866, pitHistory: [] },
      ours,
      { id: "demo-29", startNumber: "29", teamName: "TEAMGARIS 29", driverName: "Сидорук Станислав", carName: "LIGIER JS P325", className: "CN PRO", classKey: "cn pro", active: true, isOurs: false, positionClass: 3, positionOverall: 4, lastLapMs: 107149, bestLapMs: 106887, pace3Ms: 106940, pace5Ms: 107080, pace10Ms: 107220, tyreAge: 16, pitsCompleted: 1, stateKind: "ON_TRACK", observedLaps: 37, stintNumber: 2, stintElapsedS: 1754, pitHistory: [] },
      { id: "demo-67", startNumber: "67", teamName: "Quasar Motorsport", driverName: "Громов Сергей", carName: "Ligier LMP3", className: "CN PRO", classKey: "cn pro", active: true, isOurs: false, positionClass: 4, positionOverall: 6, lastLapMs: 108221, bestLapMs: 107460, pace3Ms: 108050, pace5Ms: 108130, pace10Ms: 108340, tyreAge: 5, pitsCompleted: 2, stateKind: "ON_TRACK", observedLaps: 36, stintNumber: 3, stintElapsedS: 540, pitHistory: [] }
    ];
    var history = buildDemoHistory(participants, state.longDemo ? 720 : 25);
    return {
      sessionId: "demo-race-4h", lifecycle: "active", mode: "race", heat: "Race · 4 Hours",
      freshness: "LIVE", elapsedS: 6194, remainingS: 8206, requiredPits: 4,
      flag: "GREEN", flagElapsedS: 824, identityState: "resolved", oursId: ours.id,
      ours: ours, participants: participants,
      sessionMetric: {
        gap_to_ahead_ms: 8420, gap_to_behind_ms: 5110,
        closure_ahead: { "60": { slope_ms_per_lap: -310 } },
        closure_behind: { "60": { slope_ms_per_lap: -120 } },
        pace_delta_to_reference_ms: { class_ahead: 160, class_behind: -460 },
        pits_completed: 1, pits_required: 4, pits_remaining: 3,
        expected_remaining_laps_range: { minimum: 75, maximum: 78 },
        pit_history: ours.pitHistory,
        alerts: [
          { key: "catch", severity: "info", at_us: 1783772140000000 },
          { key: "mandatory_pit_due", severity: "warning", at_us: 1783772020000000 }
        ]
      },
      ahead: participants[0], behind: participants[2],
      alerts: [],
      events: [
        { kind: "ours", atUs: 1783772140000000, text: "Сокращаем интервал до #9 на 0.31 с/круг" },
        { kind: "flag", atUs: 1783771900000000, text: "GREEN: трасса свободна" },
        { kind: "pit", atUs: 1783771038400000, text: "BALCHUG: завершён пит-стоп №1 · 1:18.400" },
        { kind: "ours", atUs: 1783770870000000, text: "Новый лучший круг BALCHUG · 1:45.911" }
      ],
      history: history
    };
  }

  function buildDemoHistory(participants, pointCount) {
    var laps = [];
    for (var lap = 1; lap <= pointCount; lap += 1) laps.push(lap);
    var pace = {};
    var intervals = {};
    participants.forEach(function (participant, index) {
      pace[participant.id] = laps.map(function (lapNumber, point) {
        if ((point + index * 2) % 17 === 0) return null;
        var base = participant.pace5Ms || 107000;
        return base + Math.round(Math.sin((point + index) / 2.7) * 360 + Math.cos(point / 4.3) * 140);
      });
      intervals[participant.id] = laps.map(function (lapNumber, point) {
        if (participant.isOurs) return 0;
        var sign = participant.positionClass < 2 ? 1 : -1;
        var start = participant.id === "demo-9" ? 13200 : participant.id === "demo-29" ? 7200 : 14800;
        return sign * Math.max(900, start - point * (participant.id === "demo-9" ? 210 : 90));
      });
    });
    return { laps: laps, pace: pace, intervals: intervals };
  }

  function assignColors() {
    if (!state.view) return;
    var used = {};
    Object.keys(state.colors).forEach(function (id) { used[state.colors[id]] = true; });
    state.view.participants.filter(function (participant) { return !participant.isOurs; }).forEach(function (participant) {
      if (state.colors[participant.id]) return;
      var key = SERIES_KEYS.find(function (candidate) { return !used[candidate]; }) || SERIES_KEYS[Object.keys(state.colors).length % SERIES_KEYS.length];
      state.colors[participant.id] = key;
      used[key] = true;
    });
  }

  function autoSelection() {
    if (!state.view || !state.view.ours) return [];
    var ours = state.view.ours;
    var others = state.view.participants.filter(function (participant) { return !participant.isOurs && participant.active !== false; });
    var result = [];
    function add(participant) {
      if (participant && result.indexOf(participant.id) === -1 && result.length < 3) result.push(participant.id);
    }
    add(others.find(function (participant) { return participant.positionClass === 1; }));
    add(state.view.ahead || others.find(function (participant) { return participant.positionClass === ours.positionClass - 1; }));
    add(state.view.behind || others.find(function (participant) { return participant.positionClass === ours.positionClass + 1; }));
    others.slice().sort(function (left, right) {
      return Math.abs((left.positionClass || 99) - (ours.positionClass || 99)) - Math.abs((right.positionClass || 99) - (ours.positionClass || 99));
    }).forEach(add);
    return result;
  }

  function effectiveSelection() {
    if (state.competitorMode === "auto") return autoSelection();
    return state.selected.slice(0, 3);
  }

  function selectedParticipants() {
    var ids = effectiveSelection();
    var available = {};
    (state.view ? state.view.participants : []).forEach(function (participant) { available[participant.id] = participant; });
    return ids.map(function (id) {
      return available[id] || { id: id, startNumber: null, teamName: "Экипаж вне табло", active: false, out: true };
    });
  }

  function persistPanelState() {
    writeJson(PANEL_STATE_KEY, { tab: state.tab });
  }

  function switchTab(tab, focus) {
    if (!TAB_TITLES[tab]) return;
    state.tab = tab;
    all("[data-panel-tab]", dom.panel).forEach(function (button) {
      var active = button.dataset.panelTab === tab;
      button.setAttribute("aria-selected", String(active));
      button.tabIndex = active ? 0 : -1;
      if (active && focus) button.focus();
    });
    all("[data-panel-view]", dom.panel).forEach(function (view) {
      view.hidden = view.dataset.panelView !== tab;
    });
    dom.viewTitle.textContent = TAB_TITLES[tab];
    dom.scroll.scrollTop = 0;
    renderView(true);
    persistPanelState();
  }

  function render() {
    renderSessionConsole();
    renderSummary();
    renderCompetitorTrigger();
    renderView(false);
  }

  function renderSessionConsole() {
    var view = state.view || emptyView();
    var active = view.lifecycle === "active";
    all("[data-session-mode]").forEach(function (button) {
      var selected = active && button.dataset.sessionMode === view.mode;
      button.disabled = state.busy || active;
      button.classList.toggle("active", selected);
      button.setAttribute("aria-pressed", String(selected));
    });
    dom.stop.hidden = !active;
    dom.stop.disabled = state.busy;
    dom.sessionBadge.dataset.status = active ? "active" : state.busy ? "busy" : "idle";
    dom.sessionBadge.textContent = state.demo ? "Replay · " + (active ? "активен" : "готов") :
      state.busy ? "Подключение…" : active ? modeLabel(view.mode) + " · запись" : "Не запущены";
    dom.sessionClock.textContent = active
      ? formatDuration(view.elapsedS) + (isNumber(view.remainingS) ? " / −" + formatDuration(view.remainingS) : "")
      : "00:00:00";
  }

  function renderSummary() {
    var view = state.view || emptyView();
    var ours = view.ours;
    var metric = view.sessionMetric || {};
    var active = view.lifecycle === "active";
    dom.panelFlagStrip.dataset.flag = view.flag;
    dom.panelFlag.textContent = view.flag === "UNKNOWN" ? "Нет данных о флаге" : view.flag;
    dom.panelFlagElapsed.textContent = isNumber(view.flagElapsedS) ? formatDuration(view.flagElapsedS) : "—";
    dom.panelMode.textContent = active ? modeLabel(view.mode) : "Режим не запущен";
    dom.panelHeat.textContent = view.heat || "Инженерный анализ";
    dom.freshness.dataset.status = active ? view.freshness : "OFFLINE";
    dom.freshness.textContent = active ? view.freshness : "OFFLINE";
    dom.panelSessionTime.textContent = active
      ? ("прошло " + formatDuration(view.elapsedS) + (isNumber(view.remainingS) ? " · осталось " + formatDuration(view.remainingS) : ""))
      : "00:00:00";
    dom.panelIdentity.textContent = ours
      ? participantLabel(ours) + (ours.driverName ? " · " + ours.driverName : "")
      : view.identityState === "unresolved" ? "Экипаж определяется автоматически" : "BALCHUG Racing · #21";
    dom.position.textContent = ours && isNumber(ours.positionClass)
      ? "P" + ours.positionClass + (isNumber(ours.positionOverall) ? " · OA " + ours.positionOverall : "") : "—";
    dom.ahead.textContent = formatGap(metric.gap_to_ahead_ms);
    dom.behind.textContent = formatGap(metric.gap_to_behind_ms);
    dom.pace.textContent = ours ? formatLap(ours.lastLapMs) : "—";
    dom.tyres.textContent = ours ? formatLaps(ours.tyreAge) : "—";
    dom.pits.textContent = ours && isNumber(ours.pitsCompleted)
      ? ours.pitsCompleted + (isNumber(view.requiredPits) ? " / " + view.requiredPits : "") : "—";
  }

  function renderCompetitorTrigger() {
    assignColors();
    var selected = selectedParticipants();
    dom.competitorSwatches.innerHTML = '<i class="series-swatch" data-series="ours"></i>' + selected.map(function (participant) {
      return '<i class="series-swatch" data-series="' + html(state.colors[participant.id] || "blue") + '"></i>';
    }).join("");
    dom.competitorLabel.textContent = state.competitorMode === "auto"
      ? "Сравнение: авто · " + selected.length
      : "Сравнение: выбрано " + selected.length;
    dom.competitorAuto.setAttribute("aria-pressed", String(state.competitorMode === "auto"));
  }

  function renderView(force) {
    var viewElement = byId("view-" + state.tab);
    if (!viewElement) return;
    if (state.tab === "overview") renderOverview(viewElement);
    else if (state.tab === "pace") renderPace(viewElement, force);
    else if (state.tab === "intervals") renderIntervals(viewElement, force);
    else if (state.tab === "pits") renderPits(viewElement);
    else if (state.tab === "class") renderClass(viewElement);
    else if (state.tab === "events") renderEvents(viewElement);
  }

  function inactiveMarkup() {
    return '<div class="panel-empty"><h3>Анализ не запущен</h3><p>Экипаж, класс, возраст шин, круги и интервалы определяются автоматически после запуска сессии.</p></div>';
  }

  function renderOverview(element) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { element.innerHTML = inactiveMarkup(); return; }
    if (!view.ours) {
      element.innerHTML = '<div class="panel-empty"><h3>Определяем экипаж</h3><p>Запись source-feed уже идёт. Тактические метрики появятся после однозначного автоматического сопоставления BALCHUG Racing в классе.</p></div>';
      return;
    }
    var ours = view.ours;
    var metric = view.sessionMetric || {};
    var alerts = (metric.alerts || []).slice(-3).reverse();
    element.innerHTML =
      '<div class="panel-section"><div class="section-heading"><h3>Борьба на трассе</h3><span>наш класс</span></div>' +
        battleMarkup("До соперника впереди", view.ahead, metric.gap_to_ahead_ms, metric.pace_delta_to_reference_ms && metric.pace_delta_to_reference_ms.class_ahead) +
        battleMarkup("До соперника сзади", view.behind, metric.gap_to_behind_ms, metric.pace_delta_to_reference_ms && metric.pace_delta_to_reference_ms.class_behind) +
      '</div>' +
      '<div class="panel-section"><div class="section-heading"><h3>Темп и стинт</h3><span>обновление 1 с</span></div><div class="metric-grid">' +
        metricCell("Последний круг", formatLap(ours.lastLapMs)) +
        metricCell("Лучший круг", formatLap(ours.bestLapMs)) +
        metricCell("Pace3", formatLap(ours.pace3Ms)) +
        metricCell("Pace5", formatLap(ours.pace5Ms)) +
        metricCell("Pace10", formatLap(ours.pace10Ms)) +
        metricCell("Возраст шин", formatLaps(ours.tyreAge)) +
        metricCell("Текущий стинт", ours.stintNumber ? "№" + ours.stintNumber : "—") +
        metricCell("Время стинта", formatDuration(ours.stintElapsedS)) +
        metricCell("Тренд", isNumber(ours.stintTrend) ? (ours.stintTrend > 0 ? "+" : "") + ours.stintTrend.toFixed(0) + " мс/круг" : "—") +
      '</div></div>' +
      '<div class="panel-section"><div class="section-heading"><h3>Обязательные питы</h3><span>шины меняются на каждом пите</span></div><div class="metric-grid">' +
        metricCell("Выполнено", isNumber(ours.pitsCompleted) ? String(ours.pitsCompleted) : "—") +
        metricCell("Требуется", isNumber(view.requiredPits) ? String(view.requiredPits) : "—") +
        metricCell("Осталось", isNumber(view.requiredPits) && isNumber(ours.pitsCompleted) ? String(Math.max(0, view.requiredPits - ours.pitsCompleted)) : "—") +
      '</div></div>' +
      '<div class="panel-section"><div class="section-heading"><h3>Последние сигналы</h3><span>' + alerts.length + '</span></div>' +
        (alerts.length ? alerts.map(function (alert) {
          return '<div class="event-row"><span class="event-time">' + html(formatClockAt(alert.at_us)) + '</span><i class="event-mark" data-kind="ours"></i><span>' + html(alertLabel(alert)) + '</span></div>';
        }).join("") : '<p class="metric-context">Нет активных тактических сигналов.</p>') +
      '</div>';
  }

  function battleMarkup(label, participant, gapMs, paceDeltaMs) {
    var context = participant ? participantLabel(participant) : "Нет подтверждённого соседа";
    if (isNumber(paceDeltaMs)) {
      context += " · " + (paceDeltaMs > 0 ? "мы медленнее на " : "мы быстрее на ") + Math.abs(paceDeltaMs / 1000).toFixed(3) + " с";
    }
    return '<div class="battle-row"><div><span class="metric-label">' + html(label) + '</span><div class="battle-name">' + html(context) + '</div></div><b class="battle-number">' + html(formatGap(gapMs)) + '</b></div>';
  }

  function metricCell(label, value) {
    return '<div class="metric-cell"><span class="metric-label">' + html(label) + '</span><b class="metric-number">' + html(value) + '</b></div>';
  }

  function renderPace(element, force) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { destroyChart("pace"); element.innerHTML = inactiveMarkup(); return; }
    if (!state.viewReady.pace || force) {
      destroyChart("pace");
      element.innerHTML = '<div class="panel-section"><div class="section-heading"><h3>Время каждого круга</h3><span>линии разрываются на пропусках</span></div><div class="timing-chart" id="paceChart" tabindex="0" aria-label="График времени каждого круга"><div class="timing-chart-empty">История завершённых кругов загружается из source LAST.</div></div><div class="chart-legend" id="paceLegend"></div></div><div class="panel-section"><div class="section-heading"><h3>Сравнение скользящего темпа</h3><span>без медианного сглаживания кругов</span></div><div id="paceRows"></div></div>';
      state.viewReady.pace = true;
    }
    renderLegend(byId("paceLegend"));
    byId("paceRows").innerHTML = paceRowsMarkup();
    updateChart("pace", byId("paceChart"), "pace");
  }

  function renderIntervals(element, force) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { destroyChart("intervals"); element.innerHTML = inactiveMarkup(); return; }
    if (!state.viewReady.intervals || force) {
      destroyChart("intervals");
      element.innerHTML = '<div class="panel-section"><div class="section-heading"><h3>Интервал относительно BALCHUG</h3><span>выше — впереди, ниже — сзади</span></div><div class="timing-chart" id="intervalChart" tabindex="0" aria-label="График интервалов относительно BALCHUG Racing"><div class="timing-chart-empty">Интервалы появятся после подтверждённых source GAP/DIFF.</div></div><div class="chart-legend" id="intervalLegend"></div></div><div class="panel-section"><div class="section-heading"><h3>Текущие соседи</h3><span>только подтверждённые интервалы</span></div>' + battleMarkup("До соперника впереди", view.ahead, view.sessionMetric.gap_to_ahead_ms, null) + battleMarkup("До соперника сзади", view.behind, view.sessionMetric.gap_to_behind_ms, null) + '</div>';
      state.viewReady.intervals = true;
    }
    renderLegend(byId("intervalLegend"));
    updateChart("intervals", byId("intervalChart"), "intervals");
  }

  function paceRowsMarkup() {
    var participants = state.view && state.view.ours ? [state.view.ours].concat(selectedParticipants()) : [];
    return participants.map(function (participant) {
      return '<div class="battle-row"><div><span class="metric-label">' + html(participantLabel(participant)) + '</span><div class="battle-name">Pace3 ' + html(formatLap(participant.pace3Ms)) + ' · Pace10 ' + html(formatLap(participant.pace10Ms)) + '</div></div><b class="battle-number">' + html(formatLap(participant.pace5Ms)) + '</b></div>';
    }).join("");
  }

  function renderLegend(element) {
    if (!element || !state.view || !state.view.ours) return;
    var participants = [state.view.ours].concat(selectedParticipants());
    element.innerHTML = participants.map(function (participant) {
      var key = participant.isOurs ? "ours" : state.colors[participant.id] || "blue";
      return '<span class="chart-legend-item"><i class="legend-line" data-series="' + html(key) + '"></i><b>' + html(participantLabel(participant)) + '</b></span>';
    }).join("");
  }

  function chartData(kind) {
    var view = state.view;
    if (!view || !view.history || !view.ours) return null;
    var selected = [view.ours].concat(selectedParticipants());
    var source = kind === "pace" ? view.history.pace : view.history.intervals;
    return {
      participants: selected,
      values: [view.history.laps].concat(selected.map(function (participant) {
        return source[participant.id] || view.history.laps.map(function () { return null; });
      }))
    };
  }

  function updateChart(name, container, kind) {
    if (!container) return;
    var payload = chartData(kind);
    var empty = container.querySelector(".timing-chart-empty");
    if (!payload || typeof window.uPlot !== "function") {
      if (empty) empty.hidden = false;
      destroyChart(name);
      return;
    }
    if (empty) empty.hidden = true;
    var needsRebuild = !state.charts[name] || state.charts[name].series.length !== payload.values.length;
    if (needsRebuild) {
      destroyChart(name);
      all(".chart-point-tooltip", container).forEach(function (node) { node.remove(); });
      var pointTooltip = document.createElement("div");
      pointTooltip.className = "chart-point-tooltip";
      pointTooltip.hidden = true;
      container.appendChild(pointTooltip);
      var series = [{ label: "Круг" }].concat(payload.participants.map(function (participant) {
        var key = participant.isOurs ? "ours" : state.colors[participant.id] || "blue";
        return {
          label: participantLabel(participant),
          stroke: SERIES_COLORS[key],
          width: participant.isOurs ? 2.5 : 2,
          spanGaps: false,
          points: { show: true, size: participant.isOurs ? 6 : 5, width: 1.5 }
        };
      }));
      var options = {
        width: Math.max(280, container.clientWidth), height: container.clientHeight,
        padding: [12, 8, 0, 2],
        cursor: { drag: { x: true, y: false }, sync: { key: "balchug-live-charts" } },
        legend: { show: false },
        scales: { x: { time: false }, y: { auto: true } },
        axes: [
          { stroke: "#6E7E98", grid: { stroke: "#E4E9F0", width: 1 }, label: "Пройдено кругов", labelSize: 18, font: "10px sans-serif", size: 42 },
          { stroke: "#6E7E98", grid: { stroke: "#E4E9F0", width: 1 }, font: "10px sans-serif", size: 58, values: kind === "pace" ? function (plot, values) { return values.map(function (value) { return formatLap(value); }); } : function (plot, values) { return values.map(function (value) { return (value / 1000).toFixed(1) + "с"; }); } }
        ],
        series: series,
        hooks: {
          setCursor: [function (plot) { renderChartPointTooltip(plot, pointTooltip, payload, kind, container); }]
        }
      };
      state.charts[name] = new window.uPlot(options, payload.values, container);
      if (window.ResizeObserver) {
        state.chartObservers[name] = new ResizeObserver(function () {
          if (!state.charts[name] || !container.clientWidth) return;
          state.charts[name].setSize({ width: container.clientWidth, height: container.clientHeight });
        });
        state.chartObservers[name].observe(container);
      }
    } else {
      state.charts[name].setData(payload.values, false);
    }
  }

  function renderChartPointTooltip(plot, tooltip, payload, kind, container) {
    var index = plot.cursor && plot.cursor.idx;
    if (!isNumber(index) || index < 0 || !payload.values[0] || index >= payload.values[0].length) {
      tooltip.hidden = true;
      return;
    }
    var lap = payload.values[0][index];
    var rows = [];
    payload.participants.forEach(function (participant, participantIndex) {
      var value = payload.values[participantIndex + 1][index];
      if (!isNumber(value)) return;
      var formatted = kind === "pace"
        ? formatLap(value)
        : value === 0 ? "базовая линия" : Math.abs(value / 1000).toFixed(3) + " с · " + (value > 0 ? "впереди" : "сзади");
      rows.push('<span><b>' + html(participantLabel(participant)) + '</b> · ' + html(formatted) + '</span>');
    });
    if (!rows.length) { tooltip.hidden = true; return; }
    tooltip.innerHTML = '<strong>Круг ' + html(lap) + '</strong>' + rows.join("");
    tooltip.hidden = false;
    var left = (plot.cursor.left || 0) + 18;
    var top = (plot.cursor.top || 0) + 18;
    var width = tooltip.offsetWidth;
    var height = tooltip.offsetHeight;
    if (left + width > container.clientWidth - 8) left = Math.max(8, (plot.cursor.left || 0) - width - 18);
    if (top + height > container.clientHeight - 8) top = Math.max(8, (plot.cursor.top || 0) - height - 18);
    tooltip.style.left = left + "px";
    tooltip.style.top = top + "px";
  }

  function destroyChart(name) {
    if (state.chartObservers[name]) state.chartObservers[name].disconnect();
    if (state.charts[name]) state.charts[name].destroy();
    delete state.chartObservers[name];
    delete state.charts[name];
  }

  function renderPits(element) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { element.innerHTML = inactiveMarkup(); return; }
    var ours = view.ours;
    if (!ours) { element.innerHTML = '<div class="panel-empty"><h3>Питы пока недоступны</h3><p>Ожидается автоматическое определение нашего экипажа.</p></div>'; return; }
    var history = ours.pitHistory || view.sessionMetric.pit_history || [];
    element.innerHTML = '<div class="panel-section"><div class="section-heading"><h3>Обязательство</h3><span>по подтверждённым pit in/out</span></div><div class="metric-grid">' +
      metricCell("Выполнено", isNumber(ours.pitsCompleted) ? String(ours.pitsCompleted) : "—") +
      metricCell("Требуется", isNumber(view.requiredPits) ? String(view.requiredPits) : "—") +
      metricCell("Осталось", isNumber(view.requiredPits) && isNumber(ours.pitsCompleted) ? String(Math.max(0, view.requiredPits - ours.pitsCompleted)) : "—") +
      '</div></div><div class="panel-section"><div class="section-heading"><h3>История BALCHUG</h3><span>' + history.length + '</span></div>' +
      (history.length ? history.map(function (pit) {
        return '<div class="pit-row"><b class="pit-number">№' + html(pit.stop_number) + '</b><span>' + html(formatClockAt(pit.pit_in_at_us)) + ' → ' + html(formatClockAt(pit.pit_out_at_us)) + '<small class="metric-context">круг ' + html(pit.pit_in_lap == null ? "—" : pit.pit_in_lap) + ' → ' + html(pit.pit_out_lap == null ? "—" : pit.pit_out_lap) + '</small></span><b class="pit-duration">' + html(formatGap(pit.pit_lane_duration_ms)) + '</b></div>';
      }).join("") : '<p class="metric-context">Подтверждённых пит-стопов пока нет.</p>') + '</div>';
  }

  function renderClass(element) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { element.innerHTML = inactiveMarkup(); return; }
    var selected = effectiveSelection();
    element.innerHTML = '<table class="class-table"><thead><tr><th>PIC</th><th>Экипаж / машина</th><th>Last</th><th>Pace5</th><th>Шины</th><th>Питы</th><th>Сравнить</th></tr></thead><tbody>' +
      view.participants.map(function (participant) {
        var teamTooltip = participantLabel(participant) + (participant.driverName ? " · " + participant.driverName : "") + (participant.carName ? " · " + participant.carName : "");
        return '<tr class="' + (participant.isOurs ? "ours-row" : "") + '"><td>' + html(isNumber(participant.positionClass) ? participant.positionClass : "—") + '</td><td data-tooltip="' + html(teamTooltip) + '"><span class="class-team">#' + html(participant.startNumber || "—") + ' · ' + html(participant.teamName || "—") + '</span><span class="class-car">' + html(participant.carName || participant.driverName || "—") + '</span></td><td>' + html(formatLap(participant.lastLapMs)) + '</td><td>' + html(formatLap(participant.pace5Ms)) + '</td><td>' + html(isNumber(participant.tyreAge) ? participant.tyreAge + "L" : "—") + '</td><td>' + html(isNumber(participant.pitsCompleted) ? participant.pitsCompleted : "—") + '</td><td>' + (participant.isOurs ? '<span class="series-swatch" data-series="ours"></span>' : '<button class="eye-button" type="button" data-competitor-eye="' + html(participant.id) + '" aria-pressed="' + String(selected.indexOf(participant.id) !== -1) + '" aria-label="Показать ' + html(participantLabel(participant)) + ' на графиках">◉</button>') + '</td></tr>';
      }).join("") + '</tbody></table>';
  }

  function renderEvents(element) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { element.innerHTML = inactiveMarkup(); return; }
    var events = (view.events || []).filter(function (event) {
      if (state.eventFilter === "ours") return event.kind === "ours" || event.kind === "pit";
      if (state.eventFilter === "competitors") return event.kind === "competitor";
      if (state.eventFilter === "track") return event.kind === "flag" || event.kind === "track";
      return true;
    });
    var filters = [
      ["all", "Все"], ["ours", "BALCHUG"], ["competitors", "Соперники"], ["track", "Трасса"]
    ];
    element.innerHTML = '<div class="panel-section"><div class="section-heading"><h3>Хронология</h3><span>последние события</span></div><div class="event-filters" role="group" aria-label="Фильтр событий">' + filters.map(function (filter) {
      return '<button class="event-filter" type="button" data-event-filter="' + filter[0] + '" aria-pressed="' + String(state.eventFilter === filter[0]) + '">' + filter[1] + '</button>';
    }).join("") + '</div>' +
      (events.length ? events.map(function (event) {
        return '<div class="event-row"><span class="event-time">' + html(formatClockAt(event.atUs)) + '</span><i class="event-mark" data-kind="' + html(event.kind || "track") + '"></i><span>' + html(event.text) + '</span></div>';
      }).join("") : '<p class="metric-context">Событий пока нет.</p>') + '</div>';
  }

  function renderCompetitorList() {
    var view = state.view || emptyView();
    var queryText = String(dom.competitorSearch.value || "").trim().toLocaleLowerCase("ru-RU");
    var effective = effectiveSelection();
    var competitors = view.participants.filter(function (participant) {
      if (participant.isOurs) return false;
      var haystack = [participant.startNumber, participant.teamName, participant.driverName, participant.carName].join(" ").toLocaleLowerCase("ru-RU");
      return !queryText || haystack.indexOf(queryText) !== -1;
    });
    dom.competitorList.innerHTML = competitors.length ? competitors.map(function (participant) {
      var checked = effective.indexOf(participant.id) !== -1;
      var color = state.colors[participant.id] || "blue";
      return '<label class="competitor-option"><input type="checkbox" data-competitor-id="' + html(participant.id) + '" ' + (checked ? "checked" : "") + '><i class="series-swatch" data-series="' + html(color) + '"></i><span class="competitor-name"><b>P' + html(isNumber(participant.positionClass) ? participant.positionClass : "—") + ' · #' + html(participant.startNumber || "—") + ' · ' + html(participant.teamName || "—") + '</b><span>' + html(participant.carName || "—") + ' · ' + html(participant.driverName || "—") + '</span></span><span class="competitor-facts">' + html(formatLap(participant.pace5Ms)) + '<br>' + html(isNumber(participant.tyreAge) ? participant.tyreAge + "L" : "—") + ' · ' + html(isNumber(participant.pitsCompleted) ? participant.pitsCompleted + " пит" : "—") + '</span></label>';
    }).join("") : '<div class="panel-empty"><p>В нашем классе нет совпадений.</p></div>';
    dom.competitorLimit.textContent = "BALCHUG закреплён · выбрано " + effective.length + " из 3";
  }

  function openCompetitors() {
    if (!state.view || state.view.lifecycle !== "active") return;
    var triggerRect = dom.competitorTrigger.getBoundingClientRect();
    var panelRect = dom.panel.getBoundingClientRect();
    dom.competitorPopover.style.top = Math.max(8, triggerRect.bottom - panelRect.top + 6) + "px";
    dom.competitorPopover.style.maxHeight = Math.min(480, Math.max(180, window.innerHeight - triggerRect.bottom - 16)) + "px";
    dom.competitorPopover.classList.add("open");
    dom.competitorTrigger.setAttribute("aria-expanded", "true");
    renderCompetitorList();
    window.setTimeout(function () { dom.competitorSearch.focus(); }, 0);
  }

  function closeCompetitors() {
    dom.competitorPopover.classList.remove("open");
    dom.competitorTrigger.setAttribute("aria-expanded", "false");
    dom.competitorSearch.value = "";
  }

  function toggleCompetitor(id, checked) {
    if (!id) return;
    if (state.competitorMode === "auto") {
      state.competitorMode = "manual";
      state.selected = autoSelection();
    }
    var index = state.selected.indexOf(id);
    if (checked && index === -1) {
      if (state.selected.length >= 3) {
        announce("Можно выбрать не более трёх соперников");
        renderCompetitorList();
        return;
      }
      state.selected.push(id);
    } else if (!checked && index !== -1) state.selected.splice(index, 1);
    persistDisplayState();
    resetComparisonViews();
    render();
    renderCompetitorList();
  }

  function resetComparisonViews() {
    ["pace", "intervals"].forEach(function (name) {
      state.viewReady[name] = false;
      destroyChart(name);
    });
  }

  function announce(message) {
    dom.liveRegion.textContent = "";
    window.setTimeout(function () { dom.liveRegion.textContent = message; }, 10);
  }

  function setBusy(busy, label) {
    state.busy = busy;
    if (label) announce(label);
    renderSessionConsole();
  }

  function fetchJson(url, options) {
    return fetch(url, options).then(function (response) {
      return response.text().then(function (text) {
        var payload = text ? JSON.parse(text) : {};
        if (!response.ok) {
          var error = new Error(payload.detail || "HTTP " + response.status);
          error.status = response.status;
          throw error;
        }
        return payload;
      });
    });
  }

  function engineerToken() {
    try { return sessionStorage.getItem(ENGINEER_TOKEN_KEY) || ""; } catch (error) { return ""; }
  }

  function withEngineer(action) {
    if (state.demo) { action(); return; }
    if (engineerToken()) { action(); return; }
    state.pendingAuthorizedAction = action;
    dom.engineerError.textContent = "";
    dom.engineerToken.value = "";
    dom.engineerDialog.showModal();
    window.setTimeout(function () { dom.engineerToken.focus(); }, 0);
  }

  function mutationHeaders(prefix, includeJson) {
    var headers = {
      "Authorization": "Bearer " + engineerToken(),
      "Idempotency-Key": randomKey(prefix)
    };
    if (includeJson) headers["Content-Type"] = "application/json";
    return headers;
  }

  function requestStart(mode, raceOptions) {
    if (state.demo) {
      state.view = demoView();
      state.view.mode = mode;
      state.view.requiredPits = mode === "race" ? raceOptions.required_pits : null;
      state.view.remainingS = mode === "race" ? raceOptions.race_duration_s : null;
      state.activeSession = { id: state.view.sessionId, mode: mode, lifecycle: "active" };
      restoreDisplayState();
      render();
      return;
    }
    setBusy(true, "Создаём сессию " + modeLabel(mode));
    var body = { mode: mode };
    if (mode === "race") {
      body.race_duration_s = raceOptions.race_duration_s;
      body.required_pits = raceOptions.required_pits;
    }
    fetchJson(API + "/sources/" + encodeURIComponent(state.track) + "/sessions", {
      method: "POST", headers: mutationHeaders("create-" + mode, true), body: JSON.stringify(body)
    }).then(function (created) {
      var session = created.session;
      return fetchJson(API + "/sessions/" + encodeURIComponent(session.id) + "/start", {
        method: "POST", headers: mutationHeaders("start-" + session.id, false)
      });
    }).then(function (started) {
      state.activeSession = started.session;
      announce(modeLabel(mode) + " запущена");
      return loadSnapshot(started.session.id);
    }).then(function () {
      connectStream(state.activeSession.id);
    }).catch(function (error) {
      handleMutationError(error, function () { requestStart(mode, raceOptions); });
    }).finally(function () { setBusy(false); });
  }

  function handleMutationError(error, retry) {
    if (error && error.status === 401) {
      try { sessionStorage.removeItem(ENGINEER_TOKEN_KEY); } catch (storageError) {}
      state.pendingAuthorizedAction = retry || function () { loadActiveSession(); };
      dom.engineerError.textContent = "Токен не принят. Проверьте инженерный доступ.";
      if (!dom.engineerDialog.open) dom.engineerDialog.showModal();
      window.setTimeout(function () { dom.engineerToken.focus(); }, 0);
    }
    announce(error && error.message ? error.message : "Не удалось выполнить действие");
  }

  function stopSession() {
    if (!state.activeSession || !state.activeSession.id) return;
    if (state.demo) {
      state.view.lifecycle = "stopped";
      state.activeSession = null;
      closeStream();
      render();
      return;
    }
    setBusy(true, "Завершаем инженерную сессию");
    fetchJson(API + "/sessions/" + encodeURIComponent(state.activeSession.id) + "/stop", {
      method: "POST", headers: mutationHeaders("stop-" + state.activeSession.id, false)
    }).then(function (payload) {
      closeStream();
      state.activeSession = null;
      state.snapshot = null;
      state.view = emptyView();
      state.viewReady = {};
      announce("Инженерная сессия завершена");
      render();
    }).catch(function (error) {
      handleMutationError(error, stopSession);
    }).finally(function () { setBusy(false); });
  }

  function loadActiveSession() {
    closeStream();
    state.viewReady = {};
    resetComparisonViews();
    if (state.demo) {
      state.view = demoView();
      state.activeSession = { id: state.view.sessionId, mode: state.view.mode, lifecycle: state.view.lifecycle };
      restoreDisplayState();
      render();
      startDemoClock();
      return Promise.resolve();
    }
    setBusy(true);
    return fetchJson(API + "/sources/" + encodeURIComponent(state.track) + "/sessions/active")
      .then(function (payload) {
        state.activeSession = payload.session || null;
        if (!state.activeSession) {
          state.snapshot = null;
          state.view = emptyView();
          restoreDisplayState();
          render();
          return null;
        }
        return loadSnapshot(state.activeSession.id).then(function () { connectStream(state.activeSession.id); });
      }).catch(function (error) {
        state.view = emptyView();
        state.view.freshness = "OFFLINE";
        announce("Timing API недоступен: " + error.message);
        render();
      }).finally(function () { setBusy(false); });
  }

  function loadSnapshot(sessionId) {
    return fetchJson(API + "/sessions/" + encodeURIComponent(sessionId) + "/state").then(function (snapshot) {
      applySnapshot(snapshot);
      return snapshot;
    });
  }

  function applySnapshot(snapshot) {
    var previousSessionId = state.view && state.view.sessionId;
    state.snapshot = snapshot;
    state.view = snapshotToView(snapshot);
    state.lastSnapshotAt = Date.now();
    if (state.view.sessionId !== previousSessionId) restoreDisplayState();
    assignColors();
    if (state.competitorMode === "manual") {
      state.selected = state.selected.slice(0, 3);
    }
    render();
  }

  function scheduleSnapshotRefresh() {
    if (!state.activeSession || state.refreshTimer) return;
    state.refreshTimer = window.setTimeout(function () {
      state.refreshTimer = null;
      loadSnapshot(state.activeSession.id).catch(function () {});
    }, 850);
  }

  function connectStream(sessionId) {
    closeStream();
    if (!window.EventSource || state.demo) return;
    state.stream = new EventSource(API + "/sessions/" + encodeURIComponent(sessionId) + "/stream");
    ["snapshot", "reset"].forEach(function (type) {
      state.stream.addEventListener(type, function (event) {
        try { applySnapshot(JSON.parse(event.data)); } catch (error) { scheduleSnapshotRefresh(); }
      });
    });
    ["state", "metric", "lap", "flag", "pit", "alert", "quality"].forEach(function (type) {
      state.stream.addEventListener(type, scheduleSnapshotRefresh);
    });
    state.stream.onerror = function () {
      if (state.view && Date.now() - state.lastSnapshotAt > 10000) {
        state.view.freshness = "OFFLINE";
        renderSummary();
      }
    };
  }

  function closeStream() {
    if (state.stream) state.stream.close();
    state.stream = null;
    if (state.refreshTimer) window.clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
  }

  function startDemoClock() {
    if (state.clockTimer) window.clearInterval(state.clockTimer);
    state.clockTimer = window.setInterval(function () {
      if (!state.demo || !state.view || state.view.lifecycle !== "active") return;
      state.demoTick += 1;
      state.view.elapsedS += 1;
      if (isNumber(state.view.remainingS)) state.view.remainingS = Math.max(0, state.view.remainingS - 1);
      state.view.flagElapsedS += 1;
      if (state.view.ours) {
        state.view.ours.pace5Ms += state.demoTick % 2 ? 8 : -6;
        state.view.sessionMetric.gap_to_ahead_ms = Math.max(0, state.view.sessionMetric.gap_to_ahead_ms - 5);
        state.view.sessionMetric.gap_to_behind_ms += 2;
      }
      renderSessionConsole();
      renderSummary();
      if (state.tab === "overview") renderView(false);
    }, 1000);
  }

  function showRaceDialog() {
    state.raceDuration = 14400;
    state.requiredPits = 2;
    dom.requiredPits.textContent = "2";
    dom.raceError.textContent = "";
    all("[data-duration]", dom.duration).forEach(function (button) {
      button.setAttribute("aria-pressed", String(Number(button.dataset.duration) === state.raceDuration));
    });
    dom.raceDialog.showModal();
  }

  function setupTooltips() {
    var activeTarget = null;
    var suppressedTarget = null;
    function hide() {
      activeTarget = null;
      dom.tooltip.classList.remove("visible");
      dom.tooltip.textContent = "";
    }
    function show(target) {
      var text = target && target.getAttribute("data-tooltip");
      if (!text || target === suppressedTarget) return;
      activeTarget = target;
      dom.tooltip.textContent = text;
      dom.tooltip.classList.add("visible");
      var rect = target.getBoundingClientRect();
      var tip = dom.tooltip.getBoundingClientRect();
      var left = clamp(rect.left + rect.width / 2 - tip.width / 2, 8, window.innerWidth - tip.width - 8);
      var top = rect.bottom + 8;
      if (top + tip.height > window.innerHeight - 8) top = Math.max(8, rect.top - tip.height - 8);
      dom.tooltip.style.left = left + "px";
      dom.tooltip.style.top = top + "px";
    }
    document.addEventListener("mouseover", function (event) {
      var target = event.target.closest && event.target.closest("[data-tooltip]");
      if (target) show(target);
    });
    document.addEventListener("mouseout", function (event) {
      if (suppressedTarget && (!event.relatedTarget || !suppressedTarget.contains(event.relatedTarget))) suppressedTarget = null;
      if (activeTarget && (!event.relatedTarget || !activeTarget.contains(event.relatedTarget))) hide();
    });
    document.addEventListener("focusin", function (event) {
      var target = event.target.closest && event.target.closest("[data-tooltip]");
      if (target) show(target);
    });
    document.addEventListener("focusout", function () { suppressedTarget = null; hide(); });
    document.addEventListener("pointerdown", function (event) {
      suppressedTarget = event.target.closest && event.target.closest("[data-tooltip]");
      hide();
    }, true);
    window.addEventListener("scroll", hide, true);
    window.addEventListener("resize", hide);
  }

  dom.competitorTrigger.addEventListener("click", function () {
    if (dom.competitorPopover.classList.contains("open")) closeCompetitors(); else openCompetitors();
  });
  dom.competitorSearch.addEventListener("input", renderCompetitorList);
  dom.competitorAuto.addEventListener("click", function () {
    state.competitorMode = "auto";
    state.selected = [];
    persistDisplayState();
    resetComparisonViews();
    render();
    renderCompetitorList();
  });
  dom.competitorList.addEventListener("change", function (event) {
    if (event.target.matches("[data-competitor-id]")) toggleCompetitor(event.target.dataset.competitorId, event.target.checked);
  });
  dom.panel.addEventListener("click", function (event) {
    var eventFilter = event.target.closest && event.target.closest("[data-event-filter]");
    if (eventFilter) {
      state.eventFilter = eventFilter.dataset.eventFilter;
      renderEvents(byId("view-events"));
      return;
    }
    var eye = event.target.closest && event.target.closest("[data-competitor-eye]");
    if (!eye) return;
    toggleCompetitor(eye.dataset.competitorEye, eye.getAttribute("aria-pressed") !== "true");
  });
  all("[data-panel-tab]", dom.panel).forEach(function (button, index, buttons) {
    button.addEventListener("click", function () { switchTab(button.dataset.panelTab, false); });
    button.addEventListener("keydown", function (event) {
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight" && event.key !== "Home" && event.key !== "End") return;
      event.preventDefault();
      var next = index;
      if (event.key === "ArrowLeft") next = (index - 1 + buttons.length) % buttons.length;
      if (event.key === "ArrowRight") next = (index + 1) % buttons.length;
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = buttons.length - 1;
      switchTab(buttons[next].dataset.panelTab, true);
    });
  });
  all("[data-session-mode]").forEach(function (button) {
    button.addEventListener("click", function () {
      var mode = button.dataset.sessionMode;
      withEngineer(function () {
        if (mode === "race") showRaceDialog();
        else requestStart(mode, {});
      });
    });
  });
  dom.stop.addEventListener("click", function () { withEngineer(stopSession); });
  all("[data-duration]", dom.duration).forEach(function (button) {
    button.addEventListener("click", function () {
      state.raceDuration = Number(button.dataset.duration);
      all("[data-duration]", dom.duration).forEach(function (candidate) {
        candidate.setAttribute("aria-pressed", String(candidate === button));
      });
    });
  });
  dom.pitMinus.addEventListener("click", function () {
    state.requiredPits = clamp(state.requiredPits - 1, 2, 8);
    dom.requiredPits.textContent = String(state.requiredPits);
  });
  dom.pitPlus.addEventListener("click", function () {
    state.requiredPits = clamp(state.requiredPits + 1, 2, 8);
    dom.requiredPits.textContent = String(state.requiredPits);
  });
  dom.raceForm.addEventListener("submit", function (event) {
    event.preventDefault();
    dom.raceDialog.close();
    requestStart("race", { race_duration_s: state.raceDuration, required_pits: state.requiredPits });
  });
  byId("raceCancel").addEventListener("click", function () { dom.raceDialog.close(); });
  dom.engineerForm.addEventListener("submit", function (event) {
    event.preventDefault();
    var token = dom.engineerToken.value.trim();
    if (!token) { dom.engineerError.textContent = "Введите инженерный токен."; return; }
    try { sessionStorage.setItem(ENGINEER_TOKEN_KEY, token); } catch (error) {}
    var action = state.pendingAuthorizedAction;
    state.pendingAuthorizedAction = null;
    dom.engineerDialog.close();
    if (action) action();
  });
  byId("engineerCancel").addEventListener("click", function () {
    state.pendingAuthorizedAction = null;
    dom.engineerDialog.close();
  });
  dom.engineerDialog.addEventListener("cancel", function () { state.pendingAuthorizedAction = null; });
  document.addEventListener("click", function (event) {
    if (dom.competitorPopover.classList.contains("open") && !dom.competitorPopover.contains(event.target) && !dom.competitorTrigger.contains(event.target)) closeCompetitors();
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      if (dom.competitorPopover.classList.contains("open")) { closeCompetitors(); dom.competitorTrigger.focus(); event.preventDefault(); }
    }
  });
  window.addEventListener("balchug:trackchange", function (event) {
    var nextTrack = event.detail && event.detail.track;
    if (!nextTrack || nextTrack === state.track) return;
    state.track = nextTrack;
    loadActiveSession();
  });
  window.addEventListener("beforeunload", closeStream);

  setupTooltips();
  switchTab(state.tab, false);
  loadActiveSession();
}());
