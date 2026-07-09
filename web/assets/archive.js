(function () {
  "use strict";
  var API = "/api";
  var $ = function (id) { return document.getElementById(id); };
  var grid = $("grid"), countEl = $("count"), moreBtn = $("more");
  var modal = $("modal"), info = $("info"), player = $("player");
  var state = { offset: 0, limit: 48, total: 0, hls: null };

  // ---- форматтеры ----
  function fmtDur(s) { if (!s) return null; s = Math.round(s); var m = Math.floor(s / 60), ss = s % 60; var h = Math.floor(m / 60); m = m % 60; return (h ? h + ":" + String(m).padStart(2, "0") : m) + ":" + String(ss).padStart(2, "0"); }
  function fmtSize(b) { if (!b) return ""; return b >= 1e9 ? (b / 1e9).toFixed(1) + " ГБ" : Math.round(b / 1e6) + " МБ"; }
  function fmtLap(s) { if (!s) return null; var m = Math.floor(s / 60), r = (s % 60).toFixed(2); return (m ? m + ":" + String(r).padStart(5, "0") : r + " с"); }
  function esc(t) { var d = document.createElement("div"); d.textContent = t == null ? "" : t; return d.innerHTML; }

  function params() {
    var p = new URLSearchParams();
    ["q", "pilot", "track", "season", "stype", "sort"].forEach(function (k) { var v = $(k).value; if (v) p.set(k, v); });
    p.set("limit", state.limit); p.set("offset", state.offset);
    return p.toString();
  }

  // ---- фильтры ----
  fetch(API + "/filters").then(function (r) { return r.json(); }).then(function (f) {
    fill("pilot", f.pilots, "Пилот"); fill("track", f.tracks, "Трасса");
    fill("season", f.seasons, "Сезон"); fill("stype", f.types, "Тип");
  });
  function fill(id, arr, label) {
    var sel = $(id);
    (arr || []).forEach(function (v) { var o = document.createElement("option"); o.value = v; o.textContent = v; sel.appendChild(o); });
  }

  // ---- загрузка каталога ----
  function load(reset) {
    if (reset) { state.offset = 0; grid.innerHTML = ""; }
    countEl.textContent = "Загрузка…";
    fetch(API + "/catalog?" + params()).then(function (r) { return r.json(); }).then(function (d) {
      state.total = d.total;
      d.items.forEach(addCard);
      state.offset += d.items.length;
      countEl.textContent = "Найдено: " + d.total;
      moreBtn.style.display = state.offset < d.total ? "" : "none";
      if (!d.total) grid.innerHTML = '<div class="empty">Ничего не найдено</div>';
    }).catch(function () { countEl.textContent = "Ошибка загрузки"; });
  }

  function addCard(it) {
    var el = document.createElement("div"); el.className = "rec";
    var badges = "";
    if (it.source === "live") badges += '<span class="bdg live">Live</span>';
    else badges += '<span class="bdg">Onboard</span>';
    if (it.season) badges += '<span class="bdg">' + esc(it.season) + "</span>";
    var dur = fmtDur(it.duration);
    var durBadge = dur ? '<span class="bdg dur">' + dur + "</span>" : "";
    var thumb = it.thumb
      ? '<img loading="lazy" src="' + it.thumb + '" alt="">'
      : '<div class="ph">▶</div>';
    var metaBits = [];
    metaBits.push("<b>" + esc(it.pilot) + "</b>");
    if (it.track) metaBits.push(esc(it.track));
    metaBits.push(esc(it.date || ""));
    if (it.best_lap) metaBits.push("круг " + fmtLap(it.best_lap));
    if (!dur && it.size) metaBits.push(fmtSize(it.size));
    el.innerHTML =
      '<div class="thumb">' + thumb +
      '<div class="badges">' + badges + durBadge + '</div>' +
      '<div class="play">▶</div></div>' +
      '<div class="body"><div class="ttl">' + esc(it.title) + '</div>' +
      '<div class="meta">' + metaBits.join("") + '</div>' +
      '<div class="admin-actions"><button class="edit">Редактировать</button><button class="del">Удалить</button></div>' +
      '</div>';
    el.addEventListener("click", function () { openItem(it.id); });
    el.querySelector(".edit").addEventListener("click", function (e) { e.stopPropagation(); openItem(it.id); });
    el.querySelector(".del").addEventListener("click", function (e) { e.stopPropagation(); delItem(it, el); });
    grid.appendChild(el);
  }

  // ---- удаление записи (админ) ----
  function delItem(it, el) {
    if (!token()) return;
    if (!confirm("Удалить запись безвозвратно?\n\n" + it.title +
      "\n\nВидео и превью будут удалены из хранилища (телеметрия и отчёты сохранятся).")) return;
    fetch(API + "/admin/item/" + it.id, { method: "DELETE", headers: { "Authorization": "Bearer " + token() } })
      .then(function (r) { if (!r.ok) throw 0; return r.json(); })
      .then(function () {
        el.style.opacity = ".3";
        setTimeout(function () { el.remove(); }, 150);
        state.total = Math.max(0, state.total - 1);
        countEl.textContent = "Найдено: " + state.total;
      })
      .catch(function () { alert("Ошибка удаления. Проверьте, что режим Бориса включён."); });
  }

  // ---- модал ----
  function openItem(id) {
    fetch(API + "/item/" + id).then(function (r) { return r.json(); }).then(function (it) {
      // плеер
      if (state.hls) { try { state.hls.destroy(); } catch (e) {} state.hls = null; }
      player.removeAttribute("src"); player.load();
      if (it.hls && window.Hls && Hls.isSupported()) {
        // VOD: щедрый буфер (можно набирать вперёд) + адаптив качества под слабый канал.
        // Стартуем с низкой оценки полосы → начинаем с низкого качества без фризов, затем растём.
        state.hls = new Hls({
          maxBufferLength: 60, maxMaxBufferLength: 120, backBufferLength: 30,
          abrBandWidthUpFactor: 0.9, abrBandWidthFactor: 0.95,
          abrEwmaDefaultEstimate: 800000, startLevel: -1
        });
        state.hls.loadSource(it.hls); state.hls.attachMedia(player);
      } else if (it.hls && player.canPlayType("application/vnd.apple.mpegurl")) {
        player.src = it.hls;
      } else if (it.mp4) {
        player.src = it.mp4;
      }
      // нет HLS — ставим в очередь на адаптивный транскод (для след. открытия)
      if (!it.hls) { fetch(API + "/enqueue_hls/" + it.id, { method: "POST" }).catch(function () {}); }
      info.innerHTML = renderInfo(it);
      bindAdmin(it);
      modal.classList.add("open"); document.body.style.overflow = "hidden";
    });
  }

  function renderInfo(it) {
    var kv = [];
    function cell(k, v) { if (v) kv.push('<div class="c"><div class="k">' + k + '</div><div class="v">' + esc(v) + "</div></div>"); }
    cell("Пилот", it.pilot); cell("Трасса", it.track); cell("Дата", it.date);
    cell("Машина", it.car); cell("Тип", it.type);
    cell("Длительность", fmtDur(it.duration)); cell("Лучший круг", fmtLap(it.best_lap));
    if (it.laps) cell("Кругов", it.laps); cell("Размер", fmtSize(it.size));
    cell("Источник", it.source === "live" ? "LiveU Live" : "Онборд-камера");
    var files = "";
    if (it.related && it.related.length) {
      files = '<div class="files"><div class="fh">Телеметрия и отчёты (' + it.related.length + ")</div>" +
        it.related.map(function (f) {
          return '<a href="' + f.url + '" target="_blank" rel="noopener"><span>' + esc(f.name) +
            '</span><span class="sz">' + (f.category || "") + (f.size ? " · " + fmtSize(f.size) : "") + "</span></a>";
        }).join("") + "</div>";
    }
    var summary = it.notes ? '<div class="summary">' + esc(it.notes) + "</div>" : "";
    var dl = it.mp4 ? '<a href="' + it.mp4 + '" target="_blank" rel="noopener" style="margin-bottom:10px">Скачать видео (макс. качество)<span class="sz">' + fmtSize(it.size) + "</span></a>" : "";
    return '<h2>' + esc(it.title) + "</h2>" +
      '<div class="sub">' + esc(it.pilot) + (it.track ? " · " + esc(it.track) : "") + (it.date ? " · " + esc(it.date) : "") + "</div>" +
      '<div class="kv">' + kv.join("") + "</div>" + summary +
      '<div class="files">' + dl + "</div>" + files +
      '<div id="adminBox"></div>';
  }

  // ---- админ-правка ----
  function token() { return localStorage.getItem("balchug_admin") || ""; }
  var borisModal = $("borisModal"), borisWidget = $("borisWidget"), borisNote = $("borisNote");

  function openBoris() {
    borisWidget.classList.remove("loading");
    borisWidget.classList.toggle("checked", !!token());
    borisNote.className = "boris-note";
    borisNote.textContent = token()
      ? "Права активны. Снимите галочку, чтобы выйти из режима Бориса."
      : "Отметьте галочку, чтобы получить права редактирования.";
    borisModal.classList.add("open");
  }
  function closeBoris() { borisModal.classList.remove("open"); }

  $("adminBtn").addEventListener("click", function (e) { e.preventDefault(); openBoris(); });
  borisModal.addEventListener("click", function (e) { if (e.target === borisModal) closeBoris(); });
  document.addEventListener("keydown", function (e) { if (e.key === "Escape") closeBoris(); });

  borisWidget.addEventListener("click", function () {
    if (borisWidget.classList.contains("loading")) return;
    if (borisWidget.classList.contains("checked")) {           // снять права
      borisWidget.classList.remove("checked");
      localStorage.removeItem("balchug_admin");
      document.body.classList.remove("admin");
      borisNote.className = "boris-note";
      borisNote.textContent = "Режим редактирования выключен.";
      setTimeout(closeBoris, 700);
      return;
    }
    borisWidget.classList.add("loading");                      // «проверка»
    fetch(API + "/boris").then(function (r) { return r.json(); }).then(function (d) {
      borisWidget.classList.remove("loading");
      if (d && d.token) {
        localStorage.setItem("balchug_admin", d.token);
        document.body.classList.add("admin");
        borisWidget.classList.add("checked");
        borisNote.className = "boris-note ok";
        borisNote.textContent = "Подтверждено. Привет, Борис! Режим редактирования включён.";
        setTimeout(closeBoris, 1100);
      } else { borisNote.textContent = "Не удалось подтвердить. Попробуйте ещё раз."; }
    }).catch(function () { borisWidget.classList.remove("loading"); borisNote.textContent = "Ошибка соединения."; });
  });
  function bindAdmin(it) {
    if (!token()) return;
    var box = document.getElementById("adminBox");
    box.innerHTML =
      '<div class="admin-row"><span class="admin-hint">Правка:</span>' +
      '<input id="ed_pilot" placeholder="Пилот" value="' + esc(it.pilot) + '">' +
      '<input id="ed_track" placeholder="Трасса" value="' + esc(it.track) + '">' +
      '<input id="ed_type" placeholder="Тип" value="' + esc(it.type) + '">' +
      '<input id="ed_title" placeholder="Заголовок" value="' + esc(it.title) + '">' +
      '<button id="saveBtn">Сохранить</button></div>';
    document.getElementById("saveBtn").addEventListener("click", function () {
      var body = { pilot_name: document.getElementById("ed_pilot").value, track_name: document.getElementById("ed_track").value, session_type: document.getElementById("ed_type").value, title: document.getElementById("ed_title").value };
      fetch(API + "/admin/item/" + it.id, { method: "POST", headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token() }, body: JSON.stringify(body) })
        .then(function (r) { if (!r.ok) throw 0; return r.json(); })
        .then(function () { alert("Сохранено"); load(true); })
        .catch(function () { alert("Ошибка (проверьте токен)"); });
    });
  }

  function closeModal() {
    modal.classList.remove("open"); document.body.style.overflow = "";
    if (state.hls) { try { state.hls.destroy(); } catch (e) {} state.hls = null; }
    player.pause(); player.removeAttribute("src"); player.load();
  }
  $("close").addEventListener("click", closeModal);
  modal.addEventListener("click", function (e) { if (e.target === modal) closeModal(); });
  document.addEventListener("keydown", function (e) { if (e.key === "Escape") closeModal(); });

  // ---- события фильтров ----
  var deb;
  $("q").addEventListener("input", function () { clearTimeout(deb); deb = setTimeout(function () { load(true); }, 300); });
  ["pilot", "track", "season", "stype", "sort"].forEach(function (id) { $(id).addEventListener("change", function () { load(true); }); });
  moreBtn.addEventListener("click", function () { load(false); });

  if (token()) document.body.classList.add("admin");   // восстановить режим Бориса
  load(true);
  if (location.hash === "#boris") openBoris();
})();
