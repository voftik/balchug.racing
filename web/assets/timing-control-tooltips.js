(function () {
  "use strict";

  var modal = document.getElementById("timingModal");
  if (!modal) return;

  var tooltip = document.createElement("div");
  tooltip.id = "timingControlTooltip";
  tooltip.className = "timing-control-tooltip";
  tooltip.setAttribute("role", "tooltip");
  tooltip.hidden = true;
  document.body.appendChild(tooltip);

  var activeAnchor = null;
  var describedBy = new WeakMap();
  var showTimer = 0;

  function anchorFor(node) {
    if (!(node instanceof Element)) return null;
    var anchor = node.closest("#timingModal [data-tooltip]");
    return anchor && modal.contains(anchor) ? anchor : null;
  }

  function anchorIsVisible(anchor) {
    if (!anchor || !modal.classList.contains("open")) return false;
    if (!anchor.getClientRects().length) return false;
    return !!anchor.getAttribute("data-tooltip");
  }

  function removeDescription(anchor) {
    if (!anchor || !describedBy.has(anchor)) return;
    var original = describedBy.get(anchor);
    if (original === null) anchor.removeAttribute("aria-describedby");
    else anchor.setAttribute("aria-describedby", original);
    describedBy.delete(anchor);
  }

  function addDescription(anchor) {
    if (!anchor || describedBy.has(anchor)) return;
    var original = anchor.getAttribute("aria-describedby");
    describedBy.set(anchor, original);
    var tokens = (original || "").split(/\s+/).filter(Boolean);
    if (tokens.indexOf(tooltip.id) === -1) tokens.push(tooltip.id);
    anchor.setAttribute("aria-describedby", tokens.join(" "));
  }

  function hide(anchor) {
    window.clearTimeout(showTimer);
    if (anchor && activeAnchor !== anchor) return;
    removeDescription(activeAnchor);
    activeAnchor = null;
    tooltip.hidden = true;
    tooltip.textContent = "";
  }

  function clamp(value, minimum, maximum) {
    return Math.max(minimum, Math.min(maximum, value));
  }

  function position(anchor) {
    var rect = anchor.getBoundingClientRect();
    var gap = 12;
    var margin = 12;
    var width = tooltip.offsetWidth;
    var height = tooltip.offsetHeight;
    var preferredBelow = anchor.getAttribute("data-tooltip-placement") === "bottom";
    var roomAbove = rect.top - margin;
    var roomBelow = window.innerHeight - rect.bottom - margin;
    var below = preferredBelow
      ? (roomBelow >= height + gap || roomBelow >= roomAbove)
      : !(roomAbove >= height + gap || roomAbove >= roomBelow);
    var left = clamp(rect.left + rect.width / 2 - width / 2, margin, window.innerWidth - width - margin);
    var top = below ? rect.bottom + gap : rect.top - height - gap;
    top = clamp(top, margin, window.innerHeight - height - margin);
    var arrowLeft = clamp(rect.left + rect.width / 2 - left, 16, width - 16);
    tooltip.dataset.placement = below ? "below" : "above";
    tooltip.style.left = Math.round(left) + "px";
    tooltip.style.top = Math.round(top) + "px";
    tooltip.style.setProperty("--timing-tooltip-arrow-left", Math.round(arrowLeft) + "px");
  }

  function show(anchor) {
    if (!anchorIsVisible(anchor)) return;
    window.clearTimeout(showTimer);
    if (activeAnchor !== anchor) {
      removeDescription(activeAnchor);
      activeAnchor = anchor;
      tooltip.textContent = anchor.getAttribute("data-tooltip");
      addDescription(anchor);
    }
    tooltip.hidden = false;
    position(anchor);
  }

  function showFocused(anchor) {
    window.clearTimeout(showTimer);
    showTimer = window.setTimeout(function () {
      if (!anchor || !anchor.contains(document.activeElement)) return;
      var focused = document.activeElement;
      if (focused && focused.matches && focused.matches(":focus-visible")) show(anchor);
    }, 0);
  }

  modal.addEventListener("pointerover", function (event) {
    var anchor = anchorFor(event.target);
    if (anchor) show(anchor);
  });

  modal.addEventListener("pointerout", function (event) {
    var anchor = anchorFor(event.target);
    if (!anchor) return;
    var next = anchorFor(event.relatedTarget);
    if (next !== anchor) hide(anchor);
  });

  // A mouse click leaves a button focused. Tooltips are hover affordances in
  // that path, so do not keep one visible merely because focus persists.
  modal.addEventListener("pointerdown", function () { hide(); }, true);
  modal.addEventListener("click", function () { hide(); }, true);

  modal.addEventListener("focusin", function (event) {
    var anchor = anchorFor(event.target);
    if (anchor) showFocused(anchor);
  });
  modal.addEventListener("focusout", function (event) {
    var anchor = anchorFor(event.target);
    if (!anchor) return;
    var next = anchorFor(event.relatedTarget);
    if (next !== anchor) hide(anchor);
  });

  modal.addEventListener("scroll", function () { hide(); }, true);
  window.addEventListener("resize", function () { hide(); });
  window.addEventListener("scroll", function () { hide(); }, true);
  document.addEventListener("pointerdown", function (event) {
    if (!modal.contains(event.target)) hide();
  }, true);
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") hide();
  });
  new MutationObserver(function () {
    if (!modal.classList.contains("open")) hide();
  }).observe(modal, { attributes: true, attributeFilter: ["class"] });
})();
