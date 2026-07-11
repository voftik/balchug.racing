(function (root, factory) {
  "use strict";
  var api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.BalchugTimingChartMath = api;
}(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  function isNumber(value) {
    return typeof value === "number" && isFinite(value);
  }

  function median(values) {
    if (!values.length) return null;
    var ordered = values.slice().sort(function (left, right) { return left - right; });
    var middle = Math.floor(ordered.length / 2);
    return ordered.length % 2
      ? ordered[middle]
      : (ordered[middle - 1] + ordered[middle]) / 2;
  }

  function paceUpperBound(values) {
    var durations = values.filter(function (value) { return isNumber(value) && value > 0; });
    if (durations.length === 1) return durations[0] + Math.max(45000, durations[0] * 0.35);
    if (durations.length === 2) {
      var lower = Math.min(durations[0], durations[1]);
      return lower + Math.max(45000, lower * 0.35);
    }
    var center = median(durations);
    if (!isNumber(center)) return null;
    var deviation = median(durations.map(function (value) { return Math.abs(value - center); })) || 0;
    return center + Math.max(45000, center * 0.35, deviation * 8);
  }

  function filterPaceSeries(series) {
    var allValues = [];
    series.forEach(function (item) {
      (item.y || []).forEach(function (value) { if (isNumber(value)) allValues.push(value); });
    });
    var upperBound = paceUpperBound(allValues);
    var hiddenCount = 0;
    var filtered = series.map(function (item) {
      var y = (item.y || []).map(function (value) {
        if (isNumber(upperBound) && isNumber(value) && value > upperBound) {
          hiddenCount += 1;
          return null;
        }
        return value;
      });
      return Object.assign({}, item, {
        y: y,
        meta: (item.meta || []).map(function (point, index) { return isNumber(y[index]) ? point : null; })
      });
    });
    return { series: filtered, upperBound: upperBound, hiddenCount: hiddenCount };
  }

  return {
    median: median,
    paceUpperBound: paceUpperBound,
    filterPaceSeries: filterPaceSeries
  };
}));
