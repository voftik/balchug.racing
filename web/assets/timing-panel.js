(function () {
  "use strict";

  var API = "/api/timing";
  var ADMIN_TOKEN_KEY = "balchug_admin";
  var ENGINEER_TOKEN_KEY = "balchug_engineer_token";
  var PANEL_STATE_KEY = "balchug_timing_panel";
  var LIVE_HISTORY_OVERLAP_US = 300000000;
  var SECTOR_KINDS = ["sector_1", "sector_2", "sector_3"];
  var chartMath = window.BalchugTimingChartMath;
  var TAB_TITLES = {
    overview: "Тактический обзор",
    pace: "Темп по кругам",
    intervals: "Интервалы",
    pits: "Пит-стопы и стинты",
    "class": "Наш класс",
    events: "События"
  };
  var MODE_LABELS = { practice: "Практика", qualifying: "Квалификация", race: "Гонка" };
  var OPERATION_LABELS = {
    WORKER_OFFLINE: "процесс записи телеметрии не работает",
    WORKER_STALE: "процесс записи телеметрии не отвечает",
    SOURCE_OFFLINE: "поток хронометража отключён",
    SOURCE_STALE: "данные хронометража поступают с задержкой",
    PROCESSING_QUEUE_LAG: "накопилась очередь необработанных кадров",
    FRAME_DECODE_FAILURE: "обнаружен необработанный кадр источника",
    RECONNECT_STORM: "источник слишком часто переподключается",
    CHECKPOINT_INVALID: "точка восстановления повреждена",
    CHECKPOINT_MISSING: "точка восстановления ещё не создана",
    RESULT_SCHEMA_DEGRADED: "изменилась схема таблицы результатов",
    RESULT_SCHEMA_PENDING: "схема таблицы результатов ещё не подтверждена",
    UNKNOWN_SOURCE_HANDLE: "источник передал новый тип сообщения",
    INGEST_RUN_FAILED: "последний цикл записи завершился с ошибкой",
    DISK_SPACE_LOW: "заканчивается место для телеметрии",
    DATABASE_SIZE_HIGH: "база телеметрии превысила рабочий объём",
    DATABASE_SIZE_UNAVAILABLE: "не удалось определить объём базы телеметрии",
    SCHEMA_MIGRATION_MISMATCH: "схема базы телеметрии неактуальна",
    DATABASE_UNAVAILABLE: "база телеметрии недоступна",
    DISK_STATUS_UNAVAILABLE: "не удалось проверить свободное место",
    SESSION_HEALTH_UNAVAILABLE: "не удалось проверить текущую сессию"
  };
  var SERIES_KEYS = ["blue", "teal", "amber", "violet"];
  var SERIES_COLORS = {
    ours: "#F0143D",
    blue: "#1976b8",
    teal: "#148477",
    amber: "#b96d00",
    violet: "#7356a5"
  };
  var SERIES_FILLS = {
    ours: "rgba(240, 20, 61, .18)",
    blue: "rgba(25, 118, 184, .18)",
    teal: "rgba(20, 132, 119, .18)",
    amber: "rgba(185, 109, 0, .18)",
    violet: "rgba(115, 86, 165, .18)"
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
  function formatSector(ms) {
    return isNumber(ms) && ms > 0 ? (ms / 1000).toFixed(3) + " с" : "—";
  }
  function formatGap(ms) {
    if (!isNumber(ms)) return "—";
    return (ms / 1000).toFixed(3) + " с";
  }
  function formatPitLaneTime(ms) {
    if (!isNumber(ms) || ms < 0) return "—";
    var value = Math.round(ms);
    var hours = Math.floor(value / 3600000);
    var minutes = Math.floor((value % 3600000) / 60000);
    var seconds = Math.floor((value % 60000) / 1000);
    var milliseconds = value % 1000;
    var tail = String(seconds).padStart(2, "0") + "." + String(milliseconds).padStart(3, "0");
    if (hours) return hours + ":" + String(minutes).padStart(2, "0") + ":" + tail;
    if (minutes) return minutes + ":" + tail;
    return seconds + "." + String(milliseconds).padStart(3, "0") + " с";
  }
  function formatGapTime(ms) {
    if (!isNumber(ms)) return "—";
    var sign = ms < 0 ? "−" : "+";
    var absolute = Math.abs(ms);
    var minutes = Math.floor(absolute / 60000);
    var seconds = (absolute - minutes * 60000) / 1000;
    return sign + (minutes ? minutes + ":" + seconds.toFixed(3).padStart(6, "0") : seconds.toFixed(3) + " с");
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
  function formatParticipantLaps(participant) {
    if (!participant || !isNumber(participant.observedLaps)) return "—";
    return (participant.lapCountExact ? "" : "≥") + String(Math.max(0, Math.floor(participant.observedLaps)));
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
  function mixedGap(behind, ahead) {
    var left = behind && behind.gapCoordinate;
    var right = ahead && ahead.gapCoordinate;
    if (!left || !right || left.status !== "EXACT" || right.status !== "EXACT") return null;
    if (!isNumber(left.gapToLeaderLaps) || !isNumber(right.gapToLeaderLaps) ||
        !isNumber(left.gapToLeaderResidualMs) || !isNumber(right.gapToLeaderResidualMs)) return null;
    return {
      laps: left.gapToLeaderLaps - right.gapToLeaderLaps,
      residualMs: left.gapToLeaderResidualMs - right.gapToLeaderResidualMs,
      observedAtUs: Math.min(left.observedAtUs || Infinity, right.observedAtUs || Infinity)
    };
  }
  function gapFromOverallLeader(participant) {
    var coordinate = participant && participant.gapCoordinate;
    if (!coordinate || coordinate.status !== "EXACT" ||
        !isNumber(coordinate.gapToLeaderLaps) || !isNumber(coordinate.gapToLeaderResidualMs)) return null;
    return {
      laps: coordinate.gapToLeaderLaps,
      residualMs: coordinate.gapToLeaderResidualMs,
      observedAtUs: coordinate.observedAtUs
    };
  }
  function formatMixedGap(value, fallbackMs) {
    if (!value || !isNumber(value.laps) || value.laps < 0) return formatGap(fallbackMs);
    if (value.laps === 0) return isNumber(value.residualMs) ? formatGap(Math.abs(value.residualMs)) : formatGap(fallbackMs);
    return formatLaps(value.laps) + (isNumber(value.residualMs) && value.residualMs !== 0 ? " " + formatGapTime(value.residualMs) : "");
  }

  var dom = {
    workspace: byId("timingWorkspace"), suite: byId("engineerSuite"), panel: byId("engineerPanel"), iframe: byId("lt"),
    operationalAlerts: byId("operationalAlerts"), operationalAlertTitle: byId("operationalAlertTitle"),
    operationalAlertText: byId("operationalAlertText"),
    sessionBadge: byId("sessionBadge"), sessionClock: byId("sessionClock"), stop: byId("sessionStop"),
    panelFlagStrip: byId("panelFlagStrip"), panelFlag: byId("panelFlag"), panelFlagElapsed: byId("panelFlagElapsed"),
    panelMode: byId("panelMode"), panelHeat: byId("panelHeat"), freshness: byId("freshnessBadge"),
    panelSessionTime: byId("panelSessionTime"), panelIdentity: byId("panelIdentity"),
    position: byId("decisionPosition"), laps: byId("decisionLaps"), leaderGap: byId("decisionLeaderGap"),
    ahead: byId("decisionAhead"), behind: byId("decisionBehind"),
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
    historyTimer: null,
    historyRequestSerial: 0,
    historyRequestKey: null,
    historyRequestInFlight: false,
    historyRefreshPending: false,
    historyForceFullPending: false,
    historyTimerForceFull: false,
    history: null,
    clockTimer: null,
    operationsTimer: null,
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
    chartPayloads: {},
    chartSignatures: {},
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

  function sessionClock(snapshot, sessionMetric, flag) {
    var session = snapshot.session || {};
    var duration = session.race_duration_s;
    var metricElapsed = sessionMetric.session_elapsed_s;
    if (session.mode === "race" && isNumber(duration)) {
      var maximumElapsed = duration + 6 * 60 * 60;
      var elapsed = isNumber(metricElapsed) && metricElapsed >= 0 && metricElapsed <= maximumElapsed
        ? metricElapsed
        : flag === "READY"
          ? 0
          : Math.max(0, ((snapshot.freshness && snapshot.freshness.computed_at_us) || Date.now() * 1000) / 1000000 - session.started_at_us / 1000000);
      return { elapsed: elapsed, remaining: Math.max(0, duration - elapsed) };
    }
    return {
      elapsed: isNumber(metricElapsed)
        ? metricElapsed
        : Math.max(0, ((snapshot.freshness && snapshot.freshness.computed_at_us) || Date.now() * 1000) / 1000000 - session.started_at_us / 1000000),
      remaining: sessionMetric.session_remaining_s
    };
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
      var lapCount = source.lap_count || {};
      var gapCoordinate = source.gap_coordinate || null;
      var canonicalLaps = isNumber(lapCount.completed_laps)
        ? lapCount.completed_laps
        : isNumber(lapCount.observed_complete_laps)
          ? lapCount.observed_complete_laps : null;
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
        observedLaps: isNumber(canonicalLaps) ? canonicalLaps : metric.observed_lap_count,
        lapCountExact: isNumber(lapCount.completed_laps) && lapCount.coverage_complete === true,
        exactLastLaps: lapCount.exact_last_laps,
        gapCoordinate: gapCoordinate ? {
          status: gapCoordinate.status,
          observedAtUs: gapCoordinate.observed_at_us,
          gapToLeaderLaps: gapCoordinate.gap_to_overall_leader_laps,
          gapToLeaderResidualMs: gapCoordinate.gap_to_overall_leader_residual_ms,
          lapGroupCompletedLaps: gapCoordinate.lap_group_completed_laps,
          raw: gapCoordinate.raw_gap_value
        } : null,
        stintNumber: metric.stint_number,
        stintElapsedS: metric.stint_elapsed_s,
        stintTrend: metric.stint_trend_ms_per_lap,
        stintSummary: Array.isArray(metric.stint_summary) ? metric.stint_summary : [],
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
    var clock = sessionClock(snapshot, sessionMetric, flag);
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
      elapsedS: clock.elapsed,
      remainingS: clock.remaining,
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
      history: state.history
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
      stateKind: "ON_TRACK", observedLaps: 37, lapCountExact: true,
      gapCoordinate: { status: "EXACT", observedAtUs: 1783771500000000, gapToLeaderLaps: 0, gapToLeaderResidualMs: 8420 },
      stintNumber: 2, stintElapsedS: 1288,
      stintTrend: 42,
      stintSummary: [
        { stint_number: 1, completed_laps: 25 },
        { stint_number: 2, completed_laps: 12 }
      ],
      pitHistory: [{ stop_number: 1, pit_in_at_us: 1783770960000000, pit_out_at_us: 1783771038400000, pit_in_lap: 25, pit_out_lap: 26, pit_lane_duration_ms: 78400 }]
    };
    var participants = [
      { id: "demo-9", startNumber: "9", teamName: "Про Моторспорт", driverName: "Мухин Игорь", carName: "Norma", className: "CN PRO", classKey: "cn pro", active: true, isOurs: false, positionClass: 1, positionOverall: 1, lastLapMs: 106105, bestLapMs: 105260, pace3Ms: 106310, pace5Ms: 106460, pace10Ms: 106720, tyreAge: 8, pitsCompleted: 2, stateKind: "ON_TRACK", observedLaps: 38, lapCountExact: true, gapCoordinate: { status: "EXACT", observedAtUs: 1783771500000000, gapToLeaderLaps: 0, gapToLeaderResidualMs: 0 }, stintNumber: 3, stintElapsedS: 866, stintSummary: [{ stint_number: 1, completed_laps: 14 }, { stint_number: 2, completed_laps: 16 }, { stint_number: 3, completed_laps: 8 }], pitHistory: [{ stop_number: 1, pit_in_at_us: 1783768200000000, pit_out_at_us: 1783768385010000, pit_in_lap: 14, pit_out_lap: 15, pit_lane_duration_ms: 185010 }, { stop_number: 2, pit_in_at_us: 1783770600000000, pit_out_at_us: 1783770783200000, pit_in_lap: 30, pit_out_lap: 31, pit_lane_duration_ms: 183200 }] },
      ours,
      { id: "demo-29", startNumber: "29", teamName: "TEAMGARIS 29", driverName: "Сидорук Станислав", carName: "LIGIER JS P325", className: "CN PRO", classKey: "cn pro", active: true, isOurs: false, positionClass: 3, positionOverall: 4, lastLapMs: 107149, bestLapMs: 106887, pace3Ms: 106940, pace5Ms: 107080, pace10Ms: 107220, tyreAge: 16, pitsCompleted: 1, stateKind: "ON_TRACK", observedLaps: 37, lapCountExact: true, gapCoordinate: { status: "EXACT", observedAtUs: 1783771500000000, gapToLeaderLaps: 0, gapToLeaderResidualMs: 13530 }, stintNumber: 2, stintElapsedS: 1754, stintSummary: [{ stint_number: 1, completed_laps: 21 }, { stint_number: 2, completed_laps: 16 }], pitHistory: [{ stop_number: 1, pit_in_at_us: 1783769350000000, pit_out_at_us: 1783769533240000, pit_in_lap: 21, pit_out_lap: 22, pit_lane_duration_ms: 183240 }] },
      { id: "demo-67", startNumber: "67", teamName: "Quasar Motorsport", driverName: "Громов Сергей", carName: "Ligier LMP3", className: "CN PRO", classKey: "cn pro", active: true, isOurs: false, positionClass: 4, positionOverall: 6, lastLapMs: 108221, bestLapMs: 107460, pace3Ms: 108050, pace5Ms: 108130, pace10Ms: 108340, tyreAge: 5, pitsCompleted: 2, stateKind: "ON_TRACK", observedLaps: 36, stintNumber: 3, stintElapsedS: 540, stintSummary: [{ stint_number: 1, completed_laps: 12 }, { stint_number: 2, completed_laps: 19 }, { stint_number: 3, completed_laps: 5 }], pitHistory: [{ stop_number: 1, pit_in_at_us: 1783767800000000, pit_out_at_us: 1783767984100000, pit_in_lap: 12, pit_out_lap: 13, pit_lane_duration_ms: 184100 }, { stop_number: 2, pit_in_at_us: 1783771080000000, pit_out_at_us: 1783771267550000, pit_in_lap: 31, pit_out_lap: 32, pit_lane_duration_ms: 187550 }] }
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
        battle_lap_trend: {
          ahead: { window_laps: 5, closure_ms_per_lap: 310, direction: "CLOSING", label: "догоняем", catch_laps: 27.16 },
          behind: { window_laps: 5, closure_ms_per_lap: -120, direction: "BEING_CAUGHT", label: "нас догоняют", catch_laps: 42.58 }
        },
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
    var lapSeries = {};
    var intervalPoints = [];
    var firstAtUs = 1783765946000000;
    var lastAtUs = 1783772140000000;
    function captureAt(point) {
      return Math.round(firstAtUs + (lastAtUs - firstAtUs) * point / Math.max(1, pointCount - 1));
    }
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
      lapSeries[participant.id] = {
        source_point_count: pointCount,
        truncated: false,
        points: laps.reduce(function (result, lapNumber, point) {
          if (pace[participant.id][point] == null) return result;
          var duration = pace[participant.id][point];
          var sector1 = Math.round(duration * 0.335 + Math.sin((point + index) / 3.1) * 110);
          var sector2 = Math.round(duration * 0.315 + Math.cos((point + index) / 4.2) * 90);
          var sector3 = duration - sector1 - sector2;
          var sectors = {
            sector_1: { duration_ms: sector1, source_cell_observation_id: point * 3 + 1 },
            sector_2: { duration_ms: sector2, source_cell_observation_id: point * 3 + 2 },
            sector_3: { duration_ms: sector3, source_cell_observation_id: point * 3 + 3 }
          };
          if ((point + index) % 23 === 0) sectors.sector_2 = null;
          result.push({
            capture_at_us: captureAt(point),
            completed_at_us: captureAt(point),
            capture_lap_index: lapNumber,
            lap_number: lapNumber,
            duration_ms: duration,
            sectors: sectors,
            flag: "GREEN"
          });
          return result;
        }, [])
      };
      if (!participant.isOurs) {
        laps.forEach(function (lapNumber, point) {
          var observedAtUs = captureAt(point);
          var relation = intervals[participant.id][point] >= 0 ? "ahead" : "behind";
          var explicitBreak = point > 0 && point % 53 === 0;
          intervalPoints.push({
            observed_at_us: observedAtUs,
            source_observed_at_us: explicitBreak ? null : observedAtUs,
            participant_id: participant.id,
            signed_ms: explicitBreak ? null : intervals[participant.id][point],
            relation: relation,
            status: explicitBreak ? "LAPPED" : "VALID",
            relation_kind: explicitBreak ? null : "GAP_PAIR_COMMON_OVERALL_LEADER",
            ours_laps: lapNumber,
            target_laps: lapNumber + (explicitBreak ? 1 : 0),
            ours_state_kind: "ON_TRACK",
            target_state_kind: "ON_TRACK",
            flag: "GREEN"
          });
        });
      }
    });
    intervalPoints.sort(function (left, right) {
      return left.observed_at_us - right.observed_at_us || left.participant_id.localeCompare(right.participant_id);
    });
    var pitStops = [];
    participants.forEach(function (participant) {
      (participant.pitHistory || []).forEach(function (pit) {
        pitStops.push({
          participant_id: participant.id,
          start_number: participant.startNumber,
          team_name: participant.teamName,
          is_ours: participant.isOurs,
          stop_number: pit.stop_number,
          entered_at_us: pit.pit_in_at_us,
          exited_at_us: pit.pit_out_at_us,
          timeline_started_at_us: pit.pit_in_at_us,
          timeline_ended_at_us: pit.pit_out_at_us,
          carried_into_range: false,
          entered_lap: pit.pit_in_lap,
          exited_lap: pit.pit_out_lap,
          pit_lane_ms: pit.pit_lane_duration_ms,
          completed: true
        });
      });
    });
    return {
      laps: laps,
      pace: pace,
      intervals: intervals,
      range: { first_at_us: firstAtUs, last_at_us: lastAtUs, max_points: pointCount },
      lap_series: lapSeries,
      interval_series: {
        source_point_count: intervalPoints.length,
        downsampled: false,
        points: intervalPoints
      },
      pit_stops: pitStops,
      flags: [
        { flag: "GREEN", started_at_us: firstAtUs, ended_at_us: 1783768200000000 },
        { flag: "FULL_COURSE_YELLOW", started_at_us: 1783768200000000, ended_at_us: 1783768500000000 },
        { flag: "GREEN", started_at_us: 1783768500000000, ended_at_us: 1783770800000000 },
        { flag: "SAFETY_CAR", started_at_us: 1783770800000000, ended_at_us: 1783771200000000 },
        { flag: "GREEN", started_at_us: 1783771200000000, ended_at_us: lastAtUs }
      ],
      ingest_gaps: [],
      time_axes: { source: { anchors: [], interpolation_max_gap_us: 90000000 } }
    };
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
    renderView(false);
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
    dom.laps.textContent = formatParticipantLaps(ours);
    dom.leaderGap.textContent = formatMixedGap(gapFromOverallLeader(ours), null);
    dom.ahead.textContent = formatMixedGap(mixedGap(ours, view.ahead), metric.gap_to_ahead_ms);
    dom.behind.textContent = formatMixedGap(mixedGap(view.behind, ours), metric.gap_to_behind_ms);
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
        battleMarkup("До соперника впереди", view.ahead, metric.gap_to_ahead_ms, mixedGap(ours, view.ahead), metric.battle_lap_trend && metric.battle_lap_trend.ahead, "ahead") +
        battleMarkup("До соперника сзади", view.behind, metric.gap_to_behind_ms, mixedGap(view.behind, ours), metric.battle_lap_trend && metric.battle_lap_trend.behind, "behind") +
      '</div>' +
      '<div class="panel-section"><div class="section-heading"><h3>Темп и стинт</h3><span>обновление 1 с</span></div><div class="metric-grid">' +
        metricCell("Последний круг", formatLap(ours.lastLapMs)) +
        metricCell("Лучший круг", formatLap(ours.bestLapMs)) +
        metricCell("Пройдено кругов", formatParticipantLaps(ours)) +
        metricCell("Pace3", formatLap(ours.pace3Ms)) +
        metricCell("Pace5", formatLap(ours.pace5Ms)) +
        metricCell("Pace10", formatLap(ours.pace10Ms)) +
        metricCell("Возраст шин", formatLaps(ours.tyreAge)) +
        metricCell("Текущий стинт", ours.stintNumber ? "№" + ours.stintNumber : "—") +
        metricCell("Время стинта", formatDuration(ours.stintElapsedS)) +
        metricCell("Тренд", isNumber(ours.stintTrend) ? (ours.stintTrend > 0 ? "+" : "") + ours.stintTrend.toFixed(0) + " мс/круг" : "—") +
      '</div></div>' +
      '<div class="panel-section"><div class="section-heading"><h3>Последние сигналы</h3><span>' + alerts.length + '</span></div>' +
        (alerts.length ? alerts.map(function (alert) {
          return '<div class="event-row"><span class="event-time">' + html(formatClockAt(alert.at_us)) + '</span><i class="event-mark" data-kind="ours"></i><span>' + html(alertLabel(alert)) + '</span></div>';
        }).join("") : '<p class="metric-context">Нет активных тактических сигналов.</p>') +
      '</div>';
  }

  function battleForecastLaps(value) {
    if (!isNumber(value) || value <= 0) return null;
    return value < 10
      ? value.toFixed(1).replace(".", ",") + " круга"
      : formatLaps(Math.round(value));
  }

  function battleTrendMarkup(trend, relation) {
    if (!trend || !isNumber(trend.closure_ms_per_lap) || !isNumber(trend.window_laps)) {
      return '<div class="battle-trend" data-tone="neutral"><b>Динамика недоступна</b><span>нет непрерывного окна 3 кругов</span></div>';
    }
    var favorable = trend.direction === "CLOSING" || trend.direction === "PULLING_AWAY";
    var unfavorable = trend.direction === "LOSING_GROUND" || trend.direction === "BEING_CAUGHT";
    var directionLabels = {
      CLOSING: "Догоняем",
      LOSING_GROUND: "Отстаём",
      PULLING_AWAY: "Отрываемся",
      BEING_CAUGHT: "Нас догоняют",
      STABLE: "Интервал стабилен"
    };
    var label = directionLabels[trend.direction] || String(trend.label || "Стабильно");
    var forecast = battleForecastLaps(trend.catch_laps);
    var forecastText = forecast
      ? (relation === "ahead" ? "догоним примерно через " : "нас догонят примерно через ") + forecast
      : "контакт не прогнозируется";
    return '<div class="battle-trend" data-tone="' + (favorable ? "good" : unfavorable ? "bad" : "neutral") + '"><b>' +
      html(label + " на " + Math.abs(trend.closure_ms_per_lap / 1000).toFixed(3) + " с/круг") + '</b><span>' +
      html("окно " + trend.window_laps + " кругов · " + forecastText) + '</span></div>';
  }

  function battleMarkup(label, participant, gapMs, mixedValue, trend, relation) {
    var context = participant ? participantLabel(participant) : "Нет подтверждённого соседа";
    return '<div class="battle-row"><div><span class="metric-label">' + html(label) + '</span><div class="battle-name">' + html(context) + '</div>' +
      (participant ? battleTrendMarkup(trend, relation) : "") + '</div><b class="battle-number">' + html(formatMixedGap(mixedValue, gapMs)) + '</b></div>';
  }

  function metricCell(label, value) {
    return '<div class="metric-cell"><span class="metric-label">' + html(label) + '</span><b class="metric-number">' + html(value) + '</b></div>';
  }

  function renderPace(element, force) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") {
      ["pace"].concat(SECTOR_KINDS).forEach(destroyChart);
      element.innerHTML = inactiveMarkup();
      return;
    }
    if (!state.viewReady.pace || force) {
      ["pace"].concat(SECTOR_KINDS).forEach(destroyChart);
      element.innerHTML =
        '<div class="panel-section"><div class="section-heading"><h3>Время каждого круга</h3><span>линии разрываются на пропусках</span></div>' +
          '<div class="timing-chart" id="paceChart" tabindex="0" aria-label="График времени каждого круга"><div class="timing-chart-empty">История завершённых кругов загружается из source LAST.</div></div>' +
          '<div class="chart-legend" id="paceLegend"></div></div>' +
        '<div class="panel-section sector-comparison" id="sectorComparison" hidden>' +
          '<div class="section-heading"><h3>Темп по секторам</h3><span>точные SECT 1–3 каждого круга</span></div>' +
          '<div class="chart-legend" id="sectorLegend"></div>' +
          '<div class="sector-chart-grid">' +
            '<div class="sector-chart-block" data-sector-block="sector_1"><h4>Сектор 1</h4><div class="timing-chart sector-chart" id="sector1Chart" tabindex="0" aria-label="График времени первого сектора"><div class="timing-chart-empty">Нет подтверждённых значений SECT 1.</div></div></div>' +
            '<div class="sector-chart-block" data-sector-block="sector_2"><h4>Сектор 2</h4><div class="timing-chart sector-chart" id="sector2Chart" tabindex="0" aria-label="График времени второго сектора"><div class="timing-chart-empty">Нет подтверждённых значений SECT 2.</div></div></div>' +
            '<div class="sector-chart-block" data-sector-block="sector_3"><h4>Сектор 3</h4><div class="timing-chart sector-chart" id="sector3Chart" tabindex="0" aria-label="График времени третьего сектора"><div class="timing-chart-empty">Нет подтверждённых значений SECT 3.</div></div></div>' +
          '</div>' +
        '</div>' +
        '<div class="panel-section"><div class="section-heading"><h3>Сравнение скользящего темпа</h3><span>без медианного сглаживания кругов</span></div><div id="paceRows"></div></div>';
      state.viewReady.pace = true;
    }
    renderLegend(byId("paceLegend"));
    byId("paceRows").innerHTML = paceRowsMarkup();
    updateChart("pace", byId("paceChart"), "pace");
    updateSectorCharts();
  }

  function renderIntervals(element, force) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { destroyChart("intervals"); element.innerHTML = inactiveMarkup(); return; }
    if (!state.viewReady.intervals || force) {
      destroyChart("intervals");
      element.innerHTML = '<div class="panel-section"><div class="section-heading"><h3>Интервал относительно BALCHUG</h3><span>выше — впереди, ниже — сзади</span></div><div class="timing-chart" id="intervalChart" tabindex="0" aria-label="График интервалов относительно BALCHUG Racing"><div class="timing-chart-empty">Интервалы появятся после подтверждённых source GAP/DIFF.</div></div><div class="chart-legend" id="intervalLegend"></div></div><div class="panel-section"><div class="section-heading"><h3>Текущие соседи</h3><span>круги и остаточное время табло</span></div>' + battleMarkup("До соперника впереди", view.ahead, view.sessionMetric.gap_to_ahead_ms, null, mixedGap(view.ours, view.ahead)) + battleMarkup("До соперника сзади", view.behind, view.sessionMetric.gap_to_behind_ms, null, mixedGap(view.behind, view.ours)) + '</div>';
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

  function isSectorKind(kind) {
    return SECTOR_KINDS.indexOf(kind) !== -1;
  }

  function sectorNumber(kind) {
    return isSectorKind(kind) ? Number(kind.slice(-1)) : null;
  }

  function sectorChartElement(kind) {
    return byId("sector" + sectorNumber(kind) + "Chart");
  }

  function updateSectorCharts() {
    var section = byId("sectorComparison");
    if (!section) return;
    var visible = false;
    SECTOR_KINDS.forEach(function (kind) {
      var available = Boolean(chartData(kind));
      var block = section.querySelector('[data-sector-block="' + kind + '"]');
      if (block) block.hidden = !available;
      if (available) {
        visible = true;
        updateChart(kind, sectorChartElement(kind), kind);
      } else {
        destroyChart(kind);
      }
    });
    section.hidden = !visible;
    if (visible) renderLegend(byId("sectorLegend"));
  }

  function sourceClockAt(history, captureAtUs) {
    var source = history && history.time_axes && history.time_axes.source;
    var anchors = source && Array.isArray(source.anchors) ? source.anchors : [];
    var maximumGap = source && isNumber(source.interpolation_max_gap_us)
      ? source.interpolation_max_gap_us : 90000000;
    var closest = null;
    anchors.forEach(function (anchor) {
      if (!isNumber(anchor.capture_at_us) || !isNumber(anchor.calibrated_utc_at_us)) return;
      var distance = Math.abs(anchor.capture_at_us - captureAtUs);
      if (distance <= maximumGap && (!closest || distance < closest.distance)) {
        closest = { anchor: anchor, distance: distance };
      }
    });
    return closest
      ? closest.anchor.calibrated_utc_at_us + (captureAtUs - closest.anchor.capture_at_us)
      : captureAtUs;
  }

  function chartClockLabel(history, captureSeconds) {
    if (!isNumber(captureSeconds)) return "";
    return formatClockAt(sourceClockAt(history, Math.round(captureSeconds * 1000000)));
  }

  function intervalOverlaps(startAtUs, endAtUs, leftAtUs, rightAtUs) {
    var effectiveEnd = isNumber(endAtUs) ? endAtUs : rightAtUs;
    return startAtUs < rightAtUs && effectiveEnd > leftAtUs;
  }

  function lapLineBreaks(history, participantId, previous, current) {
    if (!previous || !current) return false;
    if (isNumber(previous.lap_number) && isNumber(current.lap_number) && current.lap_number - previous.lap_number > 1) return true;
    var leftAtUs = previous.capture_at_us;
    var rightAtUs = current.capture_at_us;
    var gaps = Array.isArray(history.ingest_gaps) ? history.ingest_gaps : [];
    if (gaps.some(function (gap) {
      return intervalOverlaps(gap.started_at_us, gap.ended_at_us, leftAtUs, rightAtUs);
    })) return true;
    var pits = Array.isArray(history.pit_stops) ? history.pit_stops : [];
    return pits.some(function (pit) {
      return pit.participant_id === participantId && intervalOverlaps(pit.entered_at_us, pit.exited_at_us, leftAtUs, rightAtUs);
    });
  }

  function livePaceSeries(history, participant) {
    var payload = history.lap_series && history.lap_series[participant.id];
    var points = payload && Array.isArray(payload.points) ? payload.points : [];
    var x = [];
    var y = [];
    var meta = [];
    var previous = null;
    points.forEach(function (point) {
      if (!isNumber(point.capture_at_us) || !isNumber(point.duration_ms)) return;
      if (lapLineBreaks(history, participant.id, previous, point)) {
        x.push((previous.capture_at_us + point.capture_at_us) / 2000000);
        y.push(null);
        meta.push(null);
      }
      x.push(point.capture_at_us / 1000000);
      y.push(point.duration_ms);
      meta.push(point);
      previous = point;
    });
    return { participant: participant, x: x, y: y, meta: meta };
  }

  function sourceSectorDuration(point, kind) {
    var sector = point && point.sectors && point.sectors[kind];
    if (isNumber(sector)) return sector;
    return sector && isNumber(sector.duration_ms) ? sector.duration_ms : null;
  }

  function liveSectorSeries(history, participant, kind) {
    var payload = history.lap_series && history.lap_series[participant.id];
    var points = payload && Array.isArray(payload.points) ? payload.points : [];
    var x = [];
    var y = [];
    var meta = [];
    var previous = null;
    points.forEach(function (point) {
      if (!isNumber(point.capture_at_us)) return;
      if (lapLineBreaks(history, participant.id, previous, point)) {
        x.push((previous.capture_at_us + point.capture_at_us) / 2000000);
        y.push(null);
        meta.push(null);
      }
      var duration = sourceSectorDuration(point, kind);
      x.push(point.capture_at_us / 1000000);
      y.push(duration);
      meta.push(isNumber(duration) ? point : null);
      previous = point;
    });
    return { participant: participant, x: x, y: y, meta: meta };
  }

  function intervalLineBreaks(history, oursId, participantId, previous, current) {
    if (!previous || !current) return false;
    var previousHasLapPair = isNumber(previous.ours_laps) && isNumber(previous.target_laps);
    var currentHasLapPair = isNumber(current.ours_laps) && isNumber(current.target_laps);
    if (previousHasLapPair !== currentHasLapPair) return true;
    if (previousHasLapPair && currentHasLapPair &&
        previous.ours_laps - previous.target_laps !== current.ours_laps - current.target_laps) return true;
    var leftAtUs = previous.capture_at_us;
    var rightAtUs = current.capture_at_us;
    var gaps = Array.isArray(history.ingest_gaps) ? history.ingest_gaps : [];
    if (gaps.some(function (gap) {
      return intervalOverlaps(gap.started_at_us, gap.ended_at_us, leftAtUs, rightAtUs);
    })) return true;
    var pits = Array.isArray(history.pit_stops) ? history.pit_stops : [];
    return pits.some(function (pit) {
      return (pit.participant_id === participantId || pit.participant_id === oursId) &&
        intervalOverlaps(pit.entered_at_us, pit.exited_at_us, leftAtUs, rightAtUs);
    });
  }

  function liveIntervalSeries(history, participants) {
    var points = history.interval_series && Array.isArray(history.interval_series.points)
      ? history.interval_series.points : [];
    var byParticipant = {};
    participants.forEach(function (participant) {
      byParticipant[participant.id] = { participant: participant, x: [], y: [], meta: [], previous: null };
    });
    var ours = participants.find(function (participant) { return participant.isOurs; });
    var oursId = ours && ours.id;
    points.forEach(function (point) {
      if (point.participant_id) {
        var exactSeries = byParticipant[point.participant_id];
        if (!exactSeries) return;
        if (!isNumber(point.signed_ms)) {
          if (isNumber(point.observed_at_us)) {
            exactSeries.x.push(point.observed_at_us / 1000000);
            exactSeries.y.push(null);
            exactSeries.meta.push(null);
          }
          exactSeries.previous = null;
          return;
        }
        var exactMeta = {
          capture_at_us: point.observed_at_us,
          lap_number: point.ours_laps,
          capture_lap_index: point.ours_laps,
          target_laps: point.target_laps,
          ours_laps: point.ours_laps,
          flag: point.flag,
          interval_relation: point.relation,
          interval_status: point.status,
          ours_state_kind: point.ours_state_kind,
          target_state_kind: point.target_state_kind
        };
        if (intervalLineBreaks(history, oursId, point.participant_id, exactSeries.previous, exactMeta)) {
          exactSeries.x.push((exactSeries.previous.capture_at_us + point.observed_at_us) / 2000000);
          exactSeries.y.push(null);
          exactSeries.meta.push(null);
        }
        exactSeries.x.push(point.observed_at_us / 1000000);
        exactSeries.y.push(point.signed_ms);
        exactSeries.meta.push(exactMeta);
        exactSeries.previous = exactMeta;
        return;
      }
      var targetId = null;
      var value = null;
      if (point.ahead_participant_id && isNumber(point.ahead_ms)) {
        targetId = point.ahead_participant_id;
        value = point.ahead_ms;
      }
      if (point.behind_participant_id && isNumber(point.behind_ms)) {
        var behindSeries = byParticipant[point.behind_participant_id];
        if (behindSeries) {
          behindSeries.x.push(point.observed_at_us / 1000000);
          behindSeries.y.push(-point.behind_ms);
          behindSeries.meta.push({
            capture_at_us: point.observed_at_us,
            lap_number: point.ours_laps,
            capture_lap_index: point.ours_laps,
            flag: point.flag,
            interval_relation: "behind"
          });
        }
      }
      var targetSeries = byParticipant[targetId];
      if (targetSeries) {
        targetSeries.x.push(point.observed_at_us / 1000000);
        targetSeries.y.push(value);
        targetSeries.meta.push({
          capture_at_us: point.observed_at_us,
          lap_number: point.ours_laps,
          capture_lap_index: point.ours_laps,
          flag: point.flag,
          interval_relation: "ahead"
        });
      }
    });
    var competitorSeries = participants.filter(function (participant) { return !participant.isOurs; }).map(function (participant) {
      var series = byParticipant[participant.id];
      delete series.previous;
      return series;
    });
    var referenceX = [];
    competitorSeries.forEach(function (series) {
      series.x.forEach(function (value) { if (referenceX.indexOf(value) === -1) referenceX.push(value); });
    });
    referenceX.sort(function (left, right) { return left - right; });
    var oursSeries = {
      participant: ours,
      x: referenceX,
      y: referenceX.map(function () { return 0; }),
      meta: referenceX.map(function (value) { return { capture_at_us: value * 1000000, interval_relation: "reference" }; })
    };
    return [oursSeries].concat(competitorSeries);
  }

  function chartData(kind) {
    var view = state.view;
    if (!view || !view.history || !view.ours) return null;
    var participants = [view.ours].concat(selectedParticipants());
    var history = view.history;
    var liveHistory = history.lap_series && history.range;
    var series;
    var timeBased = Boolean(liveHistory);
    if (liveHistory) {
      if (kind === "pace") {
        series = participants.map(function (participant) { return livePaceSeries(history, participant); });
        if (chartMath) series = chartMath.filterPaceSeries(series).series;
      } else if (isSectorKind(kind)) {
        series = participants.map(function (participant) { return liveSectorSeries(history, participant, kind); });
      } else {
        series = liveIntervalSeries(history, participants);
      }
    } else {
      if (isSectorKind(kind)) return null;
      var source = kind === "pace" ? history.pace : history.intervals;
      series = participants.map(function (participant) {
        var values = source[participant.id] || history.laps.map(function () { return null; });
        return {
          participant: participant,
          x: history.laps.slice(),
          y: values,
          meta: history.laps.map(function (lap) { return { lap_number: lap, capture_lap_index: lap }; })
        };
      });
    }
    var hasValues = series.some(function (item) { return item.y.some(isNumber); });
    if (!hasValues) return null;
    var oursPoints = liveHistory && history.lap_series[view.ours.id]
      ? history.lap_series[view.ours.id].points : [];
    return {
      kind: kind,
      history: history,
      participants: participants,
      series: series,
      timeBased: timeBased,
      oursPoints: oursPoints,
      signature: kind + ":" + (timeBased ? "time:" : "lap:") + participants.map(function (participant) { return participant.id; }).join(","),
      values: [null].concat(series.map(function (item) { return [item.x, item.y]; }))
    };
  }

  function intervalEmptyMessage(history) {
    var points = history && history.interval_series && Array.isArray(history.interval_series.points)
      ? history.interval_series.points : [];
    var statuses = points.map(function (point) { return point.status; });
    if (statuses.indexOf("LAPPED") !== -1) {
      return "Секундный интервал недоступен: машины находятся на разных кругах.";
    }
    if (statuses.indexOf("NON_RACING_STATE") !== -1) {
      return "Секундная линия прервана: одна из машин находится вне гоночного состояния.";
    }
    if (points.length) {
      return "Нет согласованного секундного интервала для выбранных машин.";
    }
    return "Ожидается первый подтверждённый секундный интервал.";
  }

  function nearestIndex(values, target) {
    if (!values.length) return -1;
    var low = 0;
    var high = values.length - 1;
    while (low < high) {
      var middle = Math.floor((low + high) / 2);
      if (values[middle] < target) low = middle + 1; else high = middle;
    }
    if (low > 0 && Math.abs(values[low - 1] - target) <= Math.abs(values[low] - target)) return low - 1;
    return low;
  }

  function lapAxisSplits(name, minimum, maximum) {
    var payload = state.chartPayloads[name];
    if (!payload || !payload.timeBased) return [];
    return payload.oursPoints.filter(function (point) {
      var seconds = point.capture_at_us / 1000000;
      return isNumber(point.lap_number) && seconds >= minimum && seconds <= maximum;
    }).map(function (point) { return point.capture_at_us / 1000000; });
  }

  function lapAxisValues(name, values, chartWidth) {
    var payload = state.chartPayloads[name];
    if (!payload) return values.map(function () { return ""; });
    var laps = values.map(function (value) {
      var point = payload.oursPoints.find(function (candidate) {
        return Math.abs(candidate.capture_at_us / 1000000 - value) < 0.0001;
      });
      return point && point.lap_number;
    });
    var numericLaps = laps.filter(isNumber);
    var labelBudget = Math.max(2, Math.floor((chartWidth || 320) / 42));
    var lapSpan = numericLaps.length ? Math.max.apply(Math, numericLaps) - Math.min.apply(Math, numericLaps) : 0;
    var labelStep = Math.max(5, Math.ceil(lapSpan / labelBudget / 5) * 5);
    return values.map(function (value, index) {
      var lap = laps[index];
      return isNumber(lap) && lap % labelStep === 0 ? String(lap) : "";
    });
  }

  function chartTimelinePlugin(name) {
    return {
      hooks: {
        drawClear: [function (plot) {
          var payload = state.chartPayloads[name];
          if (!payload || !payload.timeBased) return;
          var history = payload.history;
          var ratio = window.uPlot.pxRatio || window.devicePixelRatio || 1;
          var context = plot.ctx;
          var top = plot.bbox.top;
          var height = plot.bbox.height;
          function xPosition(atUs) { return plot.valToPos(atUs / 1000000, "x", true); }
          context.save();
          (history.flags || []).forEach(function (flag) {
            var start = xPosition(flag.started_at_us);
            var end = xPosition(flag.ended_at_us || history.range.last_at_us);
            var color = flag.flag === "GREEN" ? "rgba(31, 157, 85, 0.08)" :
              (flag.flag === "YELLOW" || flag.flag === "FULL_COURSE_YELLOW" || flag.flag === "SAFETY_CAR") ? "rgba(244, 188, 41, 0.13)" :
                flag.flag === "RED" ? "rgba(240, 20, 61, 0.10)" : "rgba(110, 126, 152, 0.06)";
            context.fillStyle = color;
            context.fillRect(Math.min(start, end), top, Math.max(ratio, Math.abs(end - start)), height);
          });
          (history.ingest_gaps || []).forEach(function (gap) {
            var start = xPosition(gap.started_at_us);
            var end = xPosition(gap.ended_at_us || history.range.last_at_us);
            context.fillStyle = "rgba(31, 47, 75, 0.16)";
            context.fillRect(Math.min(start, end), top, Math.max(2 * ratio, Math.abs(end - start)), height);
          });
          (history.pit_stops || []).forEach(function (pit) {
            if (!payload.participants.some(function (participant) { return participant.id === pit.participant_id; })) return;
            var key = pit.participant_id === state.view.oursId ? "ours" : state.colors[pit.participant_id] || "blue";
            context.strokeStyle = SERIES_COLORS[key];
            context.lineWidth = pit.participant_id === state.view.oursId ? 2 * ratio : ratio;
            context.setLineDash([4 * ratio, 4 * ratio]);
            var x = xPosition(pit.entered_at_us);
            context.beginPath();
            context.moveTo(x, top);
            context.lineTo(x, top + height);
            context.stroke();
          });
          context.restore();
        }]
      }
    };
  }

  function updateChart(name, container, kind) {
    if (!container) return;
    var payload = chartData(kind);
    var empty = container.querySelector(".timing-chart-empty");
    if (!payload || typeof window.uPlot !== "function") {
      if (empty) {
        empty.hidden = false;
        if (kind === "intervals") empty.textContent = intervalEmptyMessage(state.view && state.view.history);
      }
      destroyChart(name);
      return;
    }
    if (empty) empty.hidden = true;
    var needsRebuild = !state.charts[name] || state.chartSignatures[name] !== payload.signature;
    if (needsRebuild) {
      destroyChart(name);
      state.chartPayloads[name] = payload;
      all(".chart-point-tooltip", container).forEach(function (node) { node.remove(); });
      var pointTooltip = document.createElement("div");
      pointTooltip.className = "chart-point-tooltip";
      pointTooltip.hidden = true;
      container.appendChild(pointTooltip);
      var series = [{}].concat(payload.series.map(function (item) {
        var participant = item.participant;
        var key = participant.isOurs ? "ours" : state.colors[participant.id] || "blue";
        return {
          label: participantLabel(participant),
          stroke: SERIES_COLORS[key],
          width: participant.isOurs ? 2.5 : 2,
          spanGaps: false,
          facets: [{ scale: "x", auto: true, sorted: 1 }, { scale: "y", auto: true }],
          points: { show: true, size: participant.isOurs ? 6 : 5, width: 1.5 }
        };
      }));
      var options = {
        mode: 2,
        width: Math.max(280, container.clientWidth), height: container.clientHeight,
        padding: [12, 8, 0, 2],
        cursor: { drag: { x: true, y: false }, sync: { key: "balchug-live-charts" } },
        legend: { show: false },
        scales: { x: { time: false }, y: { auto: true } },
        axes: [
          { scale: "x", stroke: "#6E7E98", grid: { stroke: "#E4E9F0", width: 1 }, label: payload.timeBased ? "Время табло" : "Пройдено кругов", labelSize: 18, font: "10px sans-serif", size: 42, values: function (plot, values) { return values.map(function (value) { return payload.timeBased ? chartClockLabel(state.chartPayloads[name].history, value) : String(Math.round(value)); }); } },
          { show: payload.timeBased, scale: "x", side: 2, stroke: "#6E7E98", grid: { show: false }, ticks: { show: true, size: 4, width: 1, stroke: "#A9B4C5" }, label: "Пройдено кругов", labelSize: 18, font: "10px sans-serif", size: 38, space: 1, splits: function (plot, axisIndex, minimum, maximum) { return lapAxisSplits(name, minimum, maximum); }, values: function (plot, values) { return lapAxisValues(name, values, plot.width); } },
          { stroke: "#6E7E98", grid: { stroke: "#E4E9F0", width: 1 }, font: "10px sans-serif", size: 58, values: kind === "pace" ? function (plot, values) { return values.map(function (value) { return formatLap(value); }); } : function (plot, values) { return values.map(function (value) { return (value / 1000).toFixed(isSectorKind(kind) ? 2 : 1) + "с"; }); } }
        ],
        series: series,
        plugins: [chartTimelinePlugin(name)],
        hooks: {
          setCursor: [function (plot) { renderChartPointTooltip(plot, pointTooltip, state.chartPayloads[name], kind, container); }]
        }
      };
      state.charts[name] = new window.uPlot(options, payload.values, container);
      state.chartSignatures[name] = payload.signature;
      if (window.ResizeObserver) {
        state.chartObservers[name] = new ResizeObserver(function () {
          if (!state.charts[name] || !container.clientWidth) return;
          state.charts[name].setSize({ width: container.clientWidth, height: container.clientHeight });
        });
        state.chartObservers[name].observe(container);
      }
    } else {
      state.chartPayloads[name] = payload;
      state.charts[name].setData(payload.values, false);
    }
  }

  function renderChartPointTooltip(plot, tooltip, payload, kind, container) {
    if (!container.matches(":hover") || !plot.cursor || !isNumber(plot.cursor.left) || plot.cursor.left < 0) {
      tooltip.hidden = true;
      return;
    }
    var targetX = plot.posToVal(plot.cursor.left, "x");
    var closest = null;
    payload.series.forEach(function (series) {
      var index = nearestIndex(series.x, targetX);
      if (index < 0 || !isNumber(series.y[index])) return;
      var pointX = plot.valToPos(series.x[index], "x");
      var pointY = plot.valToPos(series.y[index], "y");
      var distance = Math.pow(pointX - plot.cursor.left, 2) + Math.pow(pointY - plot.cursor.top, 2);
      if (!closest || distance < closest.distance) {
        closest = { series: series, index: index, distance: distance };
      }
    });
    if (!closest) { tooltip.hidden = true; return; }
    var participant = closest.series.participant;
    var value = closest.series.y[closest.index];
    var point = closest.series.meta[closest.index] || {};
    var captureAtUs = point.capture_at_us || closest.series.x[closest.index] * 1000000;
    var lap = point.lap_number;
    var captureLapIndex = point.capture_lap_index;
    var formatted = kind === "pace" ? formatLap(value) : isSectorKind(kind) ? formatSector(value) :
      value === 0 ? "Базовая линия BALCHUG" : Math.abs(value / 1000).toFixed(3) + " с · " + (value > 0 ? "впереди" : "сзади");
    tooltip.innerHTML = '<span class="chart-tooltip-kicker">Время табло</span>' +
      '<strong>' + html(payload.timeBased ? chartClockLabel(payload.history, captureAtUs / 1000000) : "Круг " + lap) + '</strong>' +
      '<b class="chart-tooltip-team">' + html(participantLabel(participant)) + '</b>' +
      '<span class="chart-tooltip-value">' + html(formatted) + '</span>' +
      (isSectorKind(kind) ? '<span>Сектор ' + html(sectorNumber(kind)) + '</span>' : '') +
      (isNumber(lap) ? '<span>Круг ' + html(lap) + '</span>' :
        (isNumber(captureLapIndex) ? '<span>Подтверждённое событие LAST №' + html(captureLapIndex) + ' · номер круга не передан</span>' : '<span>Номер круга не передан табло</span>')) +
      (point.driver_name ? '<span>' + html(point.driver_name) + '</span>' : '') +
      (point.flag ? '<span>Флаг: ' + html(normalizeFlag(point.flag)) + '</span>' : '');
    tooltip.hidden = false;
    var left = plot.over.offsetLeft + (plot.cursor.left || 0) + 18;
    var top = plot.over.offsetTop + (plot.cursor.top || 0) + 18;
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
    delete state.chartPayloads[name];
    delete state.chartSignatures[name];
  }

  function timelineRange(history) {
    var range = history && history.range;
    if (!range || !isNumber(range.first_at_us) || !isNumber(range.last_at_us) || range.last_at_us <= range.first_at_us) return null;
    return { firstAtUs: range.first_at_us, lastAtUs: range.last_at_us, durationUs: range.last_at_us - range.first_at_us };
  }

  function timelinePosition(atUs, range) {
    return clamp((atUs - range.firstAtUs) / range.durationUs * 100, 0, 100);
  }

  function participantTimelinePits(history, participantId) {
    return (history && Array.isArray(history.pit_stops) ? history.pit_stops : []).filter(function (pit) {
      return pit.participant_id === participantId && isNumber(pit.timeline_started_at_us || pit.entered_at_us);
    }).sort(function (left, right) {
      return (left.timeline_started_at_us || left.entered_at_us) - (right.timeline_started_at_us || right.entered_at_us);
    });
  }

  function stintAgeAt(participant, stintNumber, current) {
    if (current && stintNumber === participant.stintNumber && isNumber(participant.tyreAge)) return participant.tyreAge;
    var summary = (participant.stintSummary || []).find(function (item) { return item.stint_number === stintNumber; });
    return summary && isNumber(summary.completed_laps) ? summary.completed_laps : null;
  }

  function timelineSegments(history, participant, range) {
    var pits = participantTimelinePits(history, participant.id);
    var segments = [];
    var cursor = range.firstAtUs;
    var stintNumber = pits.length && isNumber(pits[0].stop_number)
      ? Math.max(1, pits[0].stop_number)
      : Math.max(1, participant.stintNumber || 1);
    pits.forEach(function (pit) {
      var pitStart = clamp(pit.timeline_started_at_us || pit.entered_at_us, range.firstAtUs, range.lastAtUs);
      var pitEndRaw = pit.timeline_ended_at_us || pit.exited_at_us || range.lastAtUs;
      var pitEnd = clamp(Math.max(pitStart, pitEndRaw), range.firstAtUs, range.lastAtUs);
      if (pitStart > cursor) {
        segments.push({ kind: "stint", startedAtUs: cursor, endedAtUs: pitStart, stintNumber: stintNumber });
      }
      if (pitEnd > pitStart) {
        segments.push({ kind: "pit", startedAtUs: pitStart, endedAtUs: pitEnd, pit: pit });
      }
      cursor = Math.max(cursor, pitEnd);
      stintNumber = isNumber(pit.stop_number) ? pit.stop_number + 1 : stintNumber + 1;
    });
    if (cursor < range.lastAtUs) {
      segments.push({ kind: "stint", startedAtUs: cursor, endedAtUs: range.lastAtUs, stintNumber: stintNumber, current: true });
    }
    return segments;
  }

  function timelineClock(history, atUs) {
    return formatClockAt(sourceClockAt(history, atUs));
  }

  function timelineSegmentTooltip(history, participant, segment) {
    var identity = participantLabel(participant);
    if (segment.kind === "stint") {
      var age = stintAgeAt(participant, segment.stintNumber, segment.current);
      return identity + "\nСтинт " + segment.stintNumber + (isNumber(age) ? " · возраст шин: " + formatLaps(age) : "") +
        "\nВремя табло: " + timelineClock(history, segment.startedAtUs) + " → " + timelineClock(history, segment.endedAtUs);
    }
    var pit = segment.pit;
    var laps = isNumber(pit.entered_lap) || isNumber(pit.exited_lap)
      ? "\nКруг " + (isNumber(pit.entered_lap) ? pit.entered_lap : "—") + " → " + (isNumber(pit.exited_lap) ? pit.exited_lap : "—") : "";
    var duration = isNumber(pit.pit_lane_ms) ? "\nПит-лейн: " + formatPitLaneTime(pit.pit_lane_ms) : "\nПит-лейн: измерение продолжается";
    return identity + "\nПит-стоп №" + (pit.stop_number || "—") +
      "\nВремя табло: " + timelineClock(history, segment.startedAtUs) + " → " + (pit.completed ? timelineClock(history, segment.endedAtUs) : "сейчас") + laps + duration;
  }

  function timelineFlagBands(history, range) {
    return (Array.isArray(history.flags) ? history.flags : []).map(function (flag) {
      if (!isNumber(flag.started_at_us)) return "";
      var start = timelinePosition(flag.started_at_us, range);
      var end = timelinePosition(isNumber(flag.ended_at_us) ? flag.ended_at_us : range.lastAtUs, range);
      if (end <= start) return "";
      return '<span class="stint-flag-band" data-flag="' + html(normalizeFlag(flag.flag)) + '" style="left:' + start.toFixed(4) + '%;width:' + (end - start).toFixed(4) + '%"></span>';
    }).join("");
  }

  function timelineAxis(history, range) {
    var ticks = [];
    for (var index = 0; index <= 4; index += 1) {
      var ratio = index / 4;
      var atUs = range.firstAtUs + range.durationUs * ratio;
      ticks.push('<span class="stint-axis-tick" data-edge="' + (index === 0 ? "start" : index === 4 ? "end" : "middle") + '" style="left:' + (ratio * 100) + '%"><i></i><b>' + html(timelineClock(history, atUs)) + '</b></span>');
    }
    return '<div class="stint-axis-label">Время табло</div><div class="stint-axis">' + ticks.join("") + '</div>';
  }

  function stintTimelineMarkup(history, participants) {
    var range = timelineRange(history);
    if (!range) return '<div class="panel-empty compact"><p>Временная шкала появится после первого сохранённого среза.</p></div>';
    var flags = timelineFlagBands(history, range);
    var rows = participants.map(function (participant) {
      var key = participant.isOurs ? "ours" : state.colors[participant.id] || "blue";
      var segments = timelineSegments(history, participant, range).map(function (segment) {
        var left = timelinePosition(segment.startedAtUs, range);
        var right = timelinePosition(segment.endedAtUs, range);
        var label = segment.kind === "pit" ? "П" + (segment.pit.stop_number || "") : "С" + segment.stintNumber;
        var age = segment.kind === "stint" ? stintAgeAt(participant, segment.stintNumber, segment.current) : null;
        if (segment.kind === "stint" && isNumber(age)) label += " · " + age + " кр";
        return '<span class="stint-segment" data-kind="' + segment.kind + '" data-series="' + html(key) + '" tabindex="0" aria-label="' + html(timelineSegmentTooltip(history, participant, segment).replace(/\n/g, ". ")) + '" data-tooltip="' + html(timelineSegmentTooltip(history, participant, segment)) + '" style="left:' + left.toFixed(4) + '%;width:' + Math.max(0, right - left).toFixed(4) + '%;--timeline-series:' + SERIES_COLORS[key] + ';--timeline-fill:' + SERIES_FILLS[key] + '"><b>' + html(label) + '</b></span>';
      }).join("");
      return '<div class="stint-timeline-row"' + (participant.isOurs ? ' data-ours="true"' : '') + '><div class="stint-row-label"><i class="series-swatch" data-series="' + html(key) + '"></i><span><b>' + html(participantLabel(participant)) + '</b><small>' + html(participant.driverName || participant.carName || "—") + '</small></span></div><div class="stint-track">' + flags + segments + '<span class="stint-now" aria-hidden="true"></span></div></div>';
    }).join("");
    return '<div class="stint-timeline" aria-label="Сравнение стинтов и пит-стопов"><div class="stint-timeline-axis-row">' + timelineAxis(history, range) + '</div>' + rows + '<div class="stint-timeline-legend"><span><i data-legend="stint"></i>На трассе</span><span><i data-legend="pit"></i>Пит-лейн</span><span><i data-legend="flag"></i>Фон: состояние трассы</span><span><i data-legend="now"></i>Последний срез</span></div></div>';
  }

  function renderPits(element) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { element.innerHTML = inactiveMarkup(); return; }
    var ours = view.ours;
    if (!ours) { element.innerHTML = '<div class="panel-empty"><h3>Питы пока недоступны</h3><p>Ожидается автоматическое определение нашего экипажа.</p></div>'; return; }
    var history = ours.pitHistory || view.sessionMetric.pit_history || [];
    var timelineParticipants = [ours].concat(selectedParticipants());
    var timeline = view.history
      ? stintTimelineMarkup(view.history, timelineParticipants)
      : '<div class="panel-empty compact"><p>Загружается хронология подтверждённых пит-стопов.</p></div>';
    element.innerHTML = '<div class="panel-section"><div class="section-heading"><h3>Обязательство</h3><span>по подтверждённым pit in/out</span></div><div class="metric-grid">' +
      metricCell("Выполнено", isNumber(ours.pitsCompleted) ? String(ours.pitsCompleted) : "—") +
      metricCell("Требуется", isNumber(view.requiredPits) ? String(view.requiredPits) : "—") +
      metricCell("Осталось", isNumber(view.requiredPits) && isNumber(ours.pitsCompleted) ? String(Math.max(0, view.requiredPits - ours.pitsCompleted)) : "—") +
      '</div></div><div class="panel-section"><div class="section-heading"><h3>Стинты и пит-стопы</h3><span>BALCHUG + выбранные соперники</span></div>' + timeline +
      '</div><div class="panel-section"><div class="section-heading"><h3>История BALCHUG</h3><span>' + history.length + '</span></div>' +
      (history.length ? history.map(function (pit) {
        return '<div class="pit-row"><b class="pit-number">№' + html(pit.stop_number) + '</b><span>' + html(formatClockAt(pit.pit_in_at_us)) + ' → ' + html(formatClockAt(pit.pit_out_at_us)) + '<small class="metric-context">круг ' + html(pit.pit_in_lap == null ? "—" : pit.pit_in_lap) + ' → ' + html(pit.pit_out_lap == null ? "—" : pit.pit_out_lap) + '</small></span><b class="pit-duration">' + html(formatPitLaneTime(pit.pit_lane_duration_ms)) + '</b></div>';
      }).join("") : '<p class="metric-context">Подтверждённых пит-стопов пока нет.</p>') + '</div>';
  }

  function renderClass(element) {
    var view = state.view || emptyView();
    if (view.lifecycle !== "active") { element.innerHTML = inactiveMarkup(); return; }
    var selected = effectiveSelection();
    element.innerHTML = '<table class="class-table"><thead><tr><th>PIC</th><th>Экипаж / машина</th><th>Круги</th><th>Last</th><th>Pace5</th><th>Шины</th><th>Питы</th><th>Сравнить</th></tr></thead><tbody>' +
      view.participants.map(function (participant) {
        var teamTooltip = participantLabel(participant) + (participant.driverName ? " · " + participant.driverName : "") + (participant.carName ? " · " + participant.carName : "");
        return '<tr class="' + (participant.isOurs ? "ours-row" : "") + '"><td>' + html(isNumber(participant.positionClass) ? participant.positionClass : "—") + '</td><td data-tooltip="' + html(teamTooltip) + '"><span class="class-team">#' + html(participant.startNumber || "—") + ' · ' + html(participant.teamName || "—") + '</span><span class="class-car">' + html(participant.carName || participant.driverName || "—") + '</span></td><td><b>' + html(formatParticipantLaps(participant)) + '</b></td><td>' + html(formatLap(participant.lastLapMs)) + '</td><td>' + html(formatLap(participant.pace5Ms)) + '</td><td>' + html(isNumber(participant.tyreAge) ? participant.tyreAge + "L" : "—") + '</td><td>' + html(isNumber(participant.pitsCompleted) ? participant.pitsCompleted : "—") + '</td><td>' + (participant.isOurs ? '<span class="series-swatch" data-series="ours"></span>' : '<button class="eye-button" type="button" data-competitor-eye="' + html(participant.id) + '" aria-pressed="' + String(selected.indexOf(participant.id) !== -1) + '" aria-label="Показать ' + html(participantLabel(participant)) + ' на графиках">◉</button>') + '</td></tr>';
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
    ["pace", "intervals"].concat(SECTOR_KINDS).forEach(function (name) {
      if (name === "pace" || name === "intervals") state.viewReady[name] = false;
      destroyChart(name);
    });
    scheduleHistoryRefresh(true);
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

  function renderOperationalHealth(report, requestError) {
    if (!dom.operationalAlerts) return;
    var alerts = report && Array.isArray(report.alerts) ? report.alerts : [];
    if (!requestError && (!report || report.status === "HEALTHY" || alerts.length === 0)) {
      dom.operationalAlerts.hidden = true;
      dom.operationalAlertText.textContent = "";
      return;
    }
    var critical = Boolean(requestError) || report.status === "CRITICAL" || alerts.some(function (alert) {
      return alert && alert.severity === "CRITICAL";
    });
    var messages = alerts.slice(0, 3).map(function (alert) {
      return OPERATION_LABELS[alert.code] || "неизвестное состояние подсистемы телеметрии";
    });
    if (requestError) messages = ["глобальный контроль телеметрии недоступен"];
    if (alerts.length > 3) messages.push("ещё проблем: " + String(alerts.length - 3));
    dom.operationalAlerts.dataset.severity = critical ? "CRITICAL" : "WARNING";
    dom.operationalAlerts.setAttribute("aria-live", critical ? "assertive" : "polite");
    dom.operationalAlertTitle.textContent = critical ? "Критическая проблема" : "Требуется внимание";
    dom.operationalAlertText.textContent = messages.join(" · ");
    dom.operationalAlerts.hidden = false;
  }

  function refreshOperationalHealth() {
    return fetchJson(API + "/health")
      .then(function (report) { renderOperationalHealth(report, false); })
      .catch(function () { renderOperationalHealth(null, true); });
  }

  function startOperationalHealth() {
    refreshOperationalHealth();
    if (state.operationsTimer) window.clearInterval(state.operationsTimer);
    state.operationsTimer = window.setInterval(refreshOperationalHealth, 5000);
  }

  function stopOperationalHealth() {
    if (state.operationsTimer) window.clearInterval(state.operationsTimer);
    state.operationsTimer = null;
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
    state.history = null;
    state.historyRequestKey = null;
    state.historyRefreshPending = false;
    state.historyForceFullPending = false;
    state.historyTimerForceFull = false;
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
    var nextSessionId = snapshot && snapshot.session && snapshot.session.id;
    if (previousSessionId && nextSessionId !== previousSessionId) {
      state.history = null;
      state.historyRequestKey = null;
    }
    state.snapshot = snapshot;
    state.view = snapshotToView(snapshot);
    state.view.history = state.history;
    state.lastSnapshotAt = Date.now();
    if (state.view.sessionId !== previousSessionId) restoreDisplayState();
    assignColors();
    if (state.competitorMode === "manual") {
      state.selected = state.selected.slice(0, 3);
    }
    render();
    scheduleHistoryRefresh(false);
  }

  function historyParticipantIds() {
    if (!state.view || !state.view.ours) return [];
    return [state.view.ours].concat(selectedParticipants()).map(function (participant) {
      return participant.id;
    }).filter(function (participantId, index, values) {
      return participantId && values.indexOf(participantId) === index;
    }).slice(0, 4);
  }

  function dashboardHistoryKey(sessionId, participantIds) {
    return sessionId + ":" + participantIds.join(",");
  }

  function mergeHistoryItems(previous, incoming, key, compare, merge) {
    var result = [];
    var indexes = Object.create(null);
    (previous || []).forEach(function (item) {
      var itemKey = key(item);
      if (itemKey == null || indexes[itemKey] != null) return;
      indexes[itemKey] = result.length;
      result.push(item);
    });
    (incoming || []).forEach(function (item) {
      var itemKey = key(item);
      if (itemKey == null) return;
      var index = indexes[itemKey];
      if (index == null) {
        indexes[itemKey] = result.length;
        result.push(item);
      } else {
        result[index] = merge ? merge(result[index], item) : item;
      }
    });
    if (compare) result.sort(compare);
    return result;
  }

  function lapHistoryKey(point) {
    if (point && point.source && point.source.cell_observation_id != null) {
      return "cell:" + point.source.cell_observation_id;
    }
    if (point && point.canonical_lap_id) return "canonical:" + point.canonical_lap_id;
    return point ? [point.capture_at_us, point.lap_number, point.duration_ms].join(":") : null;
  }

  function intervalHistoryKey(point) {
    return point ? [point.participant_id, point.relation, point.observed_at_us].join(":") : null;
  }

  function mergeFlagHistory(previous, incoming) {
    var result = (previous || []).slice();
    (incoming || []).forEach(function (next) {
      var nextEnd = isNumber(next.ended_at_us) ? next.ended_at_us : Infinity;
      var match = result.findIndex(function (current) {
        var currentEnd = isNumber(current.ended_at_us) ? current.ended_at_us : Infinity;
        return current.flag === next.flag && current.started_at_us <= nextEnd && next.started_at_us <= currentEnd;
      });
      if (match === -1) {
        result.push(next);
        return;
      }
      var current = result[match];
      result[match] = Object.assign({}, current, next, {
        started_at_us: Math.min(current.started_at_us, next.started_at_us),
        ended_at_us: next.ended_at_us == null ? null :
          (current.ended_at_us == null ? next.ended_at_us : Math.max(current.ended_at_us, next.ended_at_us)),
        carried_into_range: Boolean(current.carried_into_range && next.carried_into_range)
      });
    });
    result.sort(function (left, right) { return left.started_at_us - right.started_at_us; });
    return result;
  }

  function mergeTimeAxes(previous, incoming) {
    if (!previous) return incoming;
    if (!incoming) return previous;
    var previousSource = previous.source || {};
    var incomingSource = incoming.source || {};
    var anchors = mergeHistoryItems(
      previousSource.anchors,
      incomingSource.anchors,
      function (anchor) { return anchor ? [anchor.connection_id, anchor.capture_at_us].join(":") : null; },
      function (left, right) { return left.capture_at_us - right.capture_at_us; }
    );
    return {
      playback: previous.playback || incoming.playback,
      source: Object.assign({}, previousSource, incomingSource, {
        session_origin: previousSource.session_origin || incomingSource.session_origin,
        anchors: anchors
      })
    };
  }

  function mergeDashboardHistory(previous, incoming) {
    if (!previous || previous.session_id !== incoming.session_id) return incoming;
    var merged = Object.assign({}, previous, incoming);
    merged.range = Object.assign({}, previous.range, incoming.range, {
      first_at_us: Math.min(previous.range.first_at_us, incoming.range.first_at_us),
      last_at_us: Math.max(previous.range.last_at_us, incoming.range.last_at_us)
    });
    merged.participants = incoming.participants || previous.participants;
    merged.lap_series = {};
    var participantIds = Object.keys(previous.lap_series || {}).concat(Object.keys(incoming.lap_series || {}));
    participantIds.filter(function (id, index, values) { return values.indexOf(id) === index; }).forEach(function (id) {
      var oldSeries = previous.lap_series && previous.lap_series[id] || {};
      var nextSeries = incoming.lap_series && incoming.lap_series[id] || {};
      var points = mergeHistoryItems(
        oldSeries.points,
        nextSeries.points,
        lapHistoryKey,
        function (left, right) { return left.capture_at_us - right.capture_at_us; }
      );
      merged.lap_series[id] = {
        source_point_count: Math.max(oldSeries.source_point_count || 0, nextSeries.source_point_count || 0, points.length),
        truncated: Boolean(oldSeries.truncated || nextSeries.truncated),
        points: points
      };
    });
    var intervalPoints = mergeHistoryItems(
      previous.interval_series && previous.interval_series.points,
      incoming.interval_series && incoming.interval_series.points,
      intervalHistoryKey,
      function (left, right) {
        return left.observed_at_us - right.observed_at_us || String(left.participant_id).localeCompare(String(right.participant_id));
      }
    );
    merged.interval_series = Object.assign({}, previous.interval_series, incoming.interval_series, {
      source_point_count: Math.max(
        previous.interval_series && previous.interval_series.source_point_count || 0,
        incoming.interval_series && incoming.interval_series.source_point_count || 0,
        intervalPoints.length
      ),
      downsampled: Boolean(
        previous.interval_series && previous.interval_series.downsampled ||
        incoming.interval_series && incoming.interval_series.downsampled
      ),
      points: intervalPoints
    });
    merged.pit_stops = mergeHistoryItems(
      previous.pit_stops,
      incoming.pit_stops,
      function (pit) { return pit ? [pit.participant_id, pit.stop_number].join(":") : null; },
      function (left, right) { return left.timeline_started_at_us - right.timeline_started_at_us; },
      function (current, next) {
        return Object.assign({}, current, next, {
          carried_into_range: Boolean(current.carried_into_range && next.carried_into_range)
        });
      }
    );
    merged.flags = mergeFlagHistory(previous.flags, incoming.flags);
    merged.ingest_gaps = mergeHistoryItems(
      previous.ingest_gaps,
      incoming.ingest_gaps,
      function (gap) { return gap && gap.gap_id != null ? String(gap.gap_id) : null; },
      function (left, right) { return left.started_at_us - right.started_at_us; },
      function (current, next) {
        return Object.assign({}, current, next, {
          carried_into_range: Boolean(current.carried_into_range && next.carried_into_range)
        });
      }
    );
    merged.time_axes = mergeTimeAxes(previous.time_axes, incoming.time_axes);
    return merged;
  }

  function loadDashboardHistory(forceFull) {
    if (state.demo || !state.activeSession || !state.view || !state.view.ours) return Promise.resolve(null);
    if (state.historyRequestInFlight) {
      state.historyRefreshPending = true;
      state.historyForceFullPending = state.historyForceFullPending || Boolean(forceFull);
      return Promise.resolve(null);
    }
    var sessionId = state.activeSession.id;
    var participantIds = historyParticipantIds();
    if (!participantIds.length) return Promise.resolve(null);
    var requestKey = dashboardHistoryKey(sessionId, participantIds);
    var incremental = !forceFull && state.history && state.historyRequestKey === requestKey &&
      state.history.range && isNumber(state.history.range.last_at_us);
    var serial = ++state.historyRequestSerial;
    var queryString = participantIds.map(function (participantId) {
      return "participant_id=" + encodeURIComponent(participantId);
    }).join("&");
    if (incremental) {
      queryString += "&from_at_us=" + encodeURIComponent(Math.max(
        state.history.range.first_at_us || 0,
        state.history.range.last_at_us - LIVE_HISTORY_OVERLAP_US
      ));
    }
    state.historyRequestInFlight = true;
    return fetchJson(API + "/sessions/" + encodeURIComponent(sessionId) + "/dashboard/history?" + queryString)
      .then(function (payload) {
        if (serial !== state.historyRequestSerial || !state.activeSession || state.activeSession.id !== sessionId) return null;
        if (dashboardHistoryKey(sessionId, historyParticipantIds()) !== requestKey) return null;
        state.history = incremental ? mergeDashboardHistory(state.history, payload) : payload;
        state.historyRequestKey = requestKey;
        if (state.view && state.view.sessionId === sessionId) state.view.history = state.history;
        renderView(false);
        return state.history;
      }).catch(function (error) {
        if (serial === state.historyRequestSerial) announce("История телеметрии временно недоступна: " + error.message);
        return null;
      }).finally(function () {
        state.historyRequestInFlight = false;
        if (!state.historyRefreshPending) return;
        var nextForceFull = state.historyForceFullPending;
        state.historyRefreshPending = false;
        state.historyForceFullPending = false;
        scheduleHistoryRefresh(true, nextForceFull);
      });
  }

  function scheduleHistoryRefresh(immediate, forceFull) {
    if (state.demo || !state.activeSession || !state.view || !state.view.ours) return;
    var participantIds = historyParticipantIds();
    var requestKey = dashboardHistoryKey(state.activeSession.id, participantIds);
    if (!forceFull && !immediate && state.historyRequestKey === requestKey && state.history) return;
    if (state.historyRequestInFlight) {
      state.historyRefreshPending = true;
      state.historyForceFullPending = state.historyForceFullPending || Boolean(forceFull);
      return;
    }
    if (state.historyTimer) window.clearTimeout(state.historyTimer);
    state.historyTimerForceFull = state.historyTimerForceFull || Boolean(forceFull);
    state.historyTimer = window.setTimeout(function () {
      state.historyTimer = null;
      var nextForceFull = state.historyTimerForceFull;
      state.historyTimerForceFull = false;
      loadDashboardHistory(nextForceFull);
    }, immediate ? 0 : 120);
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
    state.stream.addEventListener("snapshot", function (event) {
      try { applySnapshot(JSON.parse(event.data)); } catch (error) { scheduleSnapshotRefresh(); }
    });
    state.stream.addEventListener("reset", function (event) {
      state.history = null;
      state.historyRequestKey = null;
      try { applySnapshot(JSON.parse(event.data)); } catch (error) { scheduleSnapshotRefresh(); }
    });
    ["state", "alert"].forEach(function (type) {
      state.stream.addEventListener(type, scheduleSnapshotRefresh);
    });
    state.stream.addEventListener("metric", function () {
      scheduleSnapshotRefresh();
      scheduleHistoryRefresh(true, false);
    });
    ["lap", "pit"].forEach(function (type) {
      state.stream.addEventListener(type, function () {
        scheduleSnapshotRefresh();
        scheduleHistoryRefresh(true, false);
      });
    });
    ["flag", "quality"].forEach(function (type) {
      state.stream.addEventListener(type, function () {
        scheduleSnapshotRefresh();
        scheduleHistoryRefresh(true, true);
      });
    });
    state.stream.onerror = function () {
      if (state.view && Date.now() - state.lastSnapshotAt > 10000) {
        state.view.freshness = "OFFLINE";
        renderSummary();
      }
    };
    scheduleHistoryRefresh(false);
  }

  function closeStream() {
    if (state.stream) state.stream.close();
    state.stream = null;
    if (state.refreshTimer) window.clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
    if (state.historyTimer) window.clearTimeout(state.historyTimer);
    state.historyTimer = null;
    state.historyTimerForceFull = false;
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
  window.addEventListener("beforeunload", function () {
    closeStream();
    stopOperationalHealth();
  });

  setupTooltips();
  switchTab(state.tab, false);
  startOperationalHealth();
  loadActiveSession();
}());
