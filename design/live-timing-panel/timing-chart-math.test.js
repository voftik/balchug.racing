"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const math = require("../../web/assets/timing-chart-math.js");

function item(values) {
  return { y: values, meta: values.map((value, index) => ({ value, index })) };
}

test("keeps every ordinary lap from all selected cars", () => {
  const result = math.filterPaceSeries([
    item([105000, 106000, 107000, 109000]),
    item([106500, 108000, 110000, 112000])
  ]);
  assert.equal(result.hiddenCount, 0);
  assert.deepEqual(result.series.map((series) => series.y), [
    [105000, 106000, 107000, 109000],
    [106500, 108000, 110000, 112000]
  ]);
});

test("turns pit and stopped-lap spikes into explicit line gaps", () => {
  const result = math.filterPaceSeries([
    item([108000, 109000, 250000, 110000, 1500000]),
    item([107500, 108500, 109500, null, 111000])
  ]);
  assert.equal(result.hiddenCount, 2);
  assert.deepEqual(result.series[0].y, [108000, 109000, null, 110000, null]);
  assert.equal(result.series[0].meta[2], null);
  assert.equal(result.series[0].meta[4], null);
  assert.ok(result.upperBound > 111000 && result.upperBound < 250000);
});

test("uses one common bound for Balchug and every selected competitor", () => {
  const result = math.filterPaceSeries([
    item([100000, 101000, 102000]),
    item([130000, 131000, 132000]),
    item([600000])
  ]);
  assert.deepEqual(result.series[1].y, [130000, 131000, 132000]);
  assert.deepEqual(result.series[2].y, [null]);
});

test("small samples retain legitimate slow laps but reject another time regime", () => {
  assert.deepEqual(math.filterPaceSeries([item([110000, 150000])]).series[0].y, [110000, 150000]);
  assert.deepEqual(math.filterPaceSeries([item([110000, 1500000])]).series[0].y, [110000, null]);
});
