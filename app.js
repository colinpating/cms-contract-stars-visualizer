(function () {
  async function loadData() {
    if (window.STARS_DATA && Array.isArray(window.STARS_DATA.contract_records)) {
      return window.STARS_DATA;
    }

    const combined = {
      contract_records: [],
      contract_year_totals: [],
      parent_aggregates: [],
      parent_year_totals: [],
      all_ma_aggregates: [],
      all_ma_year_totals: []
    };

    for (let year = 2017; year <= 2026; year += 1) {
      const res = await fetch(`./data/years/${year}.json`, { cache: 'no-cache' });
      if (!res.ok) {
        throw new Error(`Failed to load data shard for ${year} (${res.status})`);
      }
      const shard = await res.json();
      combined.contract_records.push(...(shard.contract_records || []));
      combined.contract_year_totals.push(...(shard.contract_year_totals || []));
      combined.parent_aggregates.push(...(shard.parent_aggregates || []));
      combined.parent_year_totals.push(...(shard.parent_year_totals || []));
      combined.all_ma_aggregates.push(...(shard.all_ma_aggregates || []));
      combined.all_ma_year_totals.push(...(shard.all_ma_year_totals || []));
    }

    return combined;
  }

  (async function init() {
  const data = await loadData();
  const yearMin = 2017;
  const yearMax = 2026;
  const MAX_SERIES = 8;
  const PALETTE = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#17becf', '#8c564b', '#e377c2'];

  const metricMeta = {
    raw_measure_data: { label: 'Raw Measure Data', contractField: 'raw_measure_data', aggField: 'weighted_raw_measure_data', total: false },
    measure_stars: { label: 'Measure Stars', contractField: 'measure_stars', aggField: 'weighted_measure_stars', total: false },
    star_weight: { label: 'Star Weight', contractField: 'star_weight', aggField: 'weighted_star_weight', total: false },
    calculated_raw_stars_score: { label: 'Calculated Raw Stars Score', contractField: 'calculated_raw_stars_score', aggField: 'weighted_calculated_raw_stars_score', total: false },
    total_raw_stars_score: { label: 'Total Raw Stars Score', contractField: 'total_raw_stars_score_contract_year', aggField: 'weighted_total_raw_stars_score', total: true }
  };

  function normalizeParentOrganization(name) {
    if (name === 'Aetna Inc.') return 'CVS Health Corporation';
    return name || '';
  }

  function parseNum(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === 'string' && v.trim() === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  function buildNormalizedData(raw) {
    const normalizedContractRecords = raw.contract_records.map((r) => ({ ...r, parent_organization: normalizeParentOrganization(r.parent_organization) }));
    const normalizedContractYearTotals = raw.contract_year_totals.map((r) => ({ ...r, parent_organization: normalizeParentOrganization(r.parent_organization) }));

    function weightedAvg(rows, valueField, weightField) {
      let den = 0;
      let num = 0;
      for (const r of rows) {
        const v = parseNum(r[valueField]);
        const w = parseNum(r[weightField]);
        if (v === null || w === null || w <= 0) continue;
        den += w;
        num += v * w;
      }
      return den > 0 ? (num / den) : null;
    }

    const parentAggGroups = new Map();
    for (const r of normalizedContractRecords) {
      const k = `${r.rating_year}|${r.measure_name_canonical_key}|${r.parent_organization}`;
      if (!parentAggGroups.has(k)) parentAggGroups.set(k, []);
      parentAggGroups.get(k).push(r);
    }

    const normalizedParentAggregates = [];
    for (const rows of parentAggGroups.values()) {
      const first = rows[0];
      const contractIds = Array.from(new Set(rows.map((r) => r.contract_id))).sort();
      const codes = Array.from(new Set(rows.map((r) => r.measure_code_observed).filter(Boolean))).sort();
      const membersIncluded = rows.reduce((acc, r) => acc + (parseNum(r.enrollment_lives) || 0), 0);
      normalizedParentAggregates.push({
        rating_year: Number(first.rating_year),
        parent_organization: first.parent_organization,
        measure_name_canonical: first.measure_name_canonical,
        measure_name_canonical_key: first.measure_name_canonical_key,
        measure_code_observed: codes.join('|'),
        weighted_raw_measure_data: weightedAvg(rows, 'raw_measure_data', 'enrollment_lives'),
        weighted_measure_stars: weightedAvg(rows, 'measure_stars', 'enrollment_lives'),
        weighted_star_weight: weightedAvg(rows, 'star_weight', 'enrollment_lives'),
        weighted_calculated_raw_stars_score: weightedAvg(rows, 'calculated_raw_stars_score', 'enrollment_lives'),
        members_included: membersIncluded,
        contracts_included: contractIds.length
      });
    }

    const parentTotalGroups = new Map();
    for (const r of normalizedContractYearTotals) {
      const k = `${r.rating_year}|${r.parent_organization}`;
      if (!parentTotalGroups.has(k)) parentTotalGroups.set(k, []);
      parentTotalGroups.get(k).push(r);
    }

    const normalizedParentYearTotals = [];
    for (const rows of parentTotalGroups.values()) {
      const first = rows[0];
      const contractIds = Array.from(new Set(rows.map((r) => r.contract_id))).sort();
      const membersIncluded = rows.reduce((acc, r) => acc + (parseNum(r.enrollment_lives) || 0), 0);
      normalizedParentYearTotals.push({
        rating_year: Number(first.rating_year),
        parent_organization: first.parent_organization,
        weighted_total_raw_stars_score: weightedAvg(rows, 'total_raw_stars_score_contract_year', 'enrollment_lives'),
        members_included: membersIncluded,
        contracts_included: contractIds.length
      });
    }

    return {
      normalizedContractRecords,
      normalizedContractYearTotals,
      normalizedParentAggregates,
      normalizedParentYearTotals,
      allMaAggregates: raw.all_ma_aggregates,
      allMaYearTotals: raw.all_ma_year_totals
    };
  }

  const dataset = buildNormalizedData(data);

  const state = {
    tab: 'contract',
    activeScope: 'contract',
    metric: 'raw_measure_data',
    measureKey: '',
    search: '',
    seriesSelections: []
  };

  const hiddenSeries = new Set();

  const els = {
    tabs: Array.from(document.querySelectorAll('.tab')),
    meta: document.getElementById('meta'),
    measureSelect: document.getElementById('measureSelect'),
    metricSelect: document.getElementById('metricSelect'),
    exportBtn: document.getElementById('exportBtn'),
    chart: document.getElementById('chart'),
    chartTitle: document.getElementById('chartTitle'),
    chartLegend: document.getElementById('chartLegend'),
    tableHead: document.querySelector('#dataTable thead'),
    tableBody: document.querySelector('#dataTable tbody'),
    tooltip: document.getElementById('tooltip'),
    scopeSelect: document.getElementById('scopeSelect'),
    entitySearch: document.getElementById('entitySearch'),
    entityList: document.getElementById('entityList'),
    selectedChips: document.getElementById('selectedChips'),
    seriesCount: document.getElementById('seriesCount'),
    seriesMessage: document.getElementById('seriesMessage'),
    quickButtons: Array.from(document.querySelectorAll('.quick-btn'))
  };

  const quickTargets = {
    humana: { scope: 'parent', entityKey: 'Humana Inc.' },
    cvs: { scope: 'parent', entityKey: 'CVS Health Corporation' },
    unh: { scope: 'parent', entityKey: 'UnitedHealth Group, Inc.' },
    all_ma: { scope: 'all_ma', entityKey: 'all_ma' }
  };

  const measureMap = new Map();
  for (const r of dataset.normalizedContractRecords) {
    const key = r.measure_name_canonical_key || r.measure_name_normalized || r.measure_name_canonical || r.measure_name_raw;
    if (!key) continue;
    if (!measureMap.has(key)) {
      measureMap.set(key, {
        key,
        name: r.measure_name_canonical || r.measure_name_raw || key
      });
    }
  }

  const measures = Array.from(measureMap.values()).sort((a, b) => a.name.localeCompare(b.name));
  state.measureKey = measures[0] ? measures[0].key : '';

  function fmt(v, digits) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) return '';
    return Number(v).toFixed(digits || 4).replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1');
  }

  function scopeTitle(scope) {
    if (scope === 'parent') return 'Parent';
    if (scope === 'contract') return 'Contract';
    return 'All MA';
  }

  function seriesId(scope, entityKey) {
    return `${scope}:${entityKey}`;
  }

  function isTotalMetric() {
    return metricMeta[state.metric].total;
  }

  function getEntityOptions(scope) {
    if (scope === 'contract') {
      const ids = new Set();
      const source = isTotalMetric() ? dataset.normalizedContractYearTotals : dataset.normalizedContractRecords;
      for (const r of source) {
        if (!isTotalMetric() && r.measure_name_canonical_key !== state.measureKey) continue;
        ids.add(r.contract_id);
      }
      return Array.from(ids).sort().map((v) => ({ value: v, label: v }));
    }

    if (scope === 'parent') {
      const vals = new Set();
      const source = isTotalMetric() ? dataset.normalizedParentYearTotals : dataset.normalizedParentAggregates;
      for (const r of source) {
        if (!isTotalMetric() && r.measure_name_canonical_key !== state.measureKey) continue;
        if (r.parent_organization) vals.add(r.parent_organization);
      }
      return Array.from(vals).sort().map((v) => ({ value: v, label: v }));
    }

    return [{ value: 'all_ma', label: 'All MA' }];
  }

  function getColorForSeries(id) {
    const idx = state.seriesSelections.findIndex((s) => s.id === id);
    const pos = idx >= 0 ? idx : state.seriesSelections.length;
    return PALETTE[pos % PALETTE.length];
  }

  function findSelection(id) {
    return state.seriesSelections.find((s) => s.id === id);
  }

  function setMessage(text) {
    els.seriesMessage.textContent = text || '';
  }

  function addSelection(scope, entityKey) {
    const id = seriesId(scope, entityKey);
    if (findSelection(id)) return true;
    if (state.seriesSelections.length >= MAX_SERIES) {
      setMessage(`Maximum ${MAX_SERIES} series selected.`);
      return false;
    }

    const label = scope === 'all_ma' ? 'All MA' : entityKey;
    const sel = {
      id,
      scope,
      entityKey,
      label,
      color: getColorForSeries(id)
    };
    state.seriesSelections.push(sel);
    setMessage('');
    return true;
  }

  function removeSelection(id) {
    state.seriesSelections = state.seriesSelections.filter((s) => s.id !== id);
    hiddenSeries.delete(id);
    setMessage('');
  }

  function toggleSelection(scope, entityKey) {
    const id = seriesId(scope, entityKey);
    if (findSelection(id)) {
      removeSelection(id);
      return;
    }
    addSelection(scope, entityKey);
  }

  function getDefaultEntityForScope(scope) {
    const options = getEntityOptions(scope);
    return options.length ? options[0].value : '';
  }

  function buildMeasureOptions() {
    els.measureSelect.innerHTML = '';
    for (const m of measures) {
      const opt = document.createElement('option');
      opt.value = m.key;
      opt.textContent = m.name;
      els.measureSelect.appendChild(opt);
    }
    els.measureSelect.value = state.measureKey;
  }

  function renderEntityList() {
    const allOptions = getEntityOptions(state.activeScope);
    const q = state.search.trim().toLowerCase();
    const options = q ? allOptions.filter((o) => o.label.toLowerCase().includes(q)) : allOptions;

    els.entityList.innerHTML = '';
    for (const o of options) {
      const id = seriesId(state.activeScope, o.value);
      const row = document.createElement('label');
      row.className = 'entity-item';

      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.checked = Boolean(findSelection(id));
      cb.addEventListener('change', () => {
        toggleSelection(state.activeScope, o.value);
        refreshControls();
        render();
      });

      const text = document.createElement('span');
      text.className = 'entity-label';
      text.title = o.label;
      text.textContent = o.label;

      row.appendChild(cb);
      row.appendChild(text);
      els.entityList.appendChild(row);
    }

    if (!options.length) {
      const empty = document.createElement('div');
      empty.className = 'entity-label';
      empty.textContent = 'No matches.';
      els.entityList.appendChild(empty);
    }
  }

  function renderChips() {
    els.selectedChips.innerHTML = '';

    for (const s of state.seriesSelections) {
      const chip = document.createElement('div');
      chip.className = 'chip';

      const dot = document.createElement('span');
      dot.className = 'chip-color';
      dot.style.backgroundColor = s.color;

      const scope = document.createElement('span');
      scope.className = 'chip-scope';
      scope.textContent = scopeTitle(s.scope).toUpperCase();

      const label = document.createElement('span');
      label.textContent = s.label;

      const remove = document.createElement('button');
      remove.type = 'button';
      remove.className = 'chip-remove';
      remove.textContent = 'x';
      remove.title = 'Remove series';
      remove.addEventListener('click', () => {
        removeSelection(s.id);
        refreshControls();
        render();
      });

      chip.appendChild(dot);
      chip.appendChild(scope);
      chip.appendChild(label);
      chip.appendChild(remove);
      els.selectedChips.appendChild(chip);
    }

    if (!state.seriesSelections.length) {
      const empty = document.createElement('div');
      empty.className = 'entity-label';
      empty.textContent = 'Select one or more series to compare.';
      els.selectedChips.appendChild(empty);
    }

    els.seriesCount.textContent = String(state.seriesSelections.length);
  }

  function refreshQuickButtons() {
    for (const btn of els.quickButtons) {
      const key = btn.dataset.quick;
      const t = quickTargets[key];
      if (!t) continue;
      const id = seriesId(t.scope, t.entityKey);
      btn.classList.toggle('active', Boolean(findSelection(id)));
    }
  }

  function refreshControls() {
    els.scopeSelect.value = state.activeScope;
    renderEntityList();
    renderChips();
    refreshQuickButtons();
  }

  function niceStep(raw) {
    if (raw <= 0) return 1;
    const p = Math.pow(10, Math.floor(Math.log10(raw)));
    const n = raw / p;
    if (n <= 1) return 1 * p;
    if (n <= 2) return 2 * p;
    if (n <= 5) return 5 * p;
    return 10 * p;
  }

  function getYScale(seriesList) {
    const valid = [];
    for (const s of seriesList) {
      for (const r of s.rows) {
        if (typeof r.value === 'number') valid.push(r.value);
      }
    }
    if (!valid.length) return { min: 0, max: 1, ticks: [0, 0.25, 0.5, 0.75, 1] };

    if (state.metric === 'measure_stars') {
      return { min: 1, max: 5, ticks: [1, 2, 3, 4, 5] };
    }
    if (state.metric === 'calculated_raw_stars_score') {
      return { min: 0, max: 1, ticks: [0, 0.25, 0.5, 0.75, 1] };
    }

    const min = Math.min(...valid);
    const max = Math.max(...valid);
    if (state.metric === 'star_weight') {
      const top = Math.max(1, Math.ceil(max));
      const ticks = [];
      for (let i = 0; i <= top; i += 1) ticks.push(i);
      return { min: 0, max: top, ticks };
    }

    const span = Math.max(max - min, Math.abs(max) * 0.05, 0.0001);
    const rawStep = span / 5;
    const step = niceStep(rawStep);
    const yMin = Math.floor(min / step) * step;
    const yMax = Math.ceil(max / step) * step;
    const ticks = [];
    for (let v = yMin; v <= yMax + step / 2; v += step) ticks.push(v);
    return { min: yMin, max: yMax, ticks };
  }

  function getSeriesForSelection(selection) {
    const metric = metricMeta[state.metric];
    let rows = [];

    if (selection.scope === 'contract') {
      if (isTotalMetric()) {
        rows = dataset.normalizedContractYearTotals
          .filter((r) => r.contract_id === selection.entityKey)
          .map((r) => ({
            year: Number(r.rating_year),
            value: parseNum(r[metric.contractField]),
            entity: r.contract_id,
            scope: 'contract',
            enrollment_lives: parseNum(r.enrollment_lives),
            parent_organization: r.parent_organization,
            code: ''
          }));
      } else {
        rows = dataset.normalizedContractRecords
          .filter((r) => r.contract_id === selection.entityKey)
          .filter((r) => r.measure_name_canonical_key === state.measureKey)
          .map((r) => ({
            year: Number(r.rating_year),
            value: parseNum(r[metric.contractField]),
            entity: r.contract_id,
            scope: 'contract',
            enrollment_lives: parseNum(r.enrollment_lives),
            parent_organization: r.parent_organization,
            code: r.measure_code_observed || ''
          }));
      }
    } else if (selection.scope === 'parent') {
      if (isTotalMetric()) {
        rows = dataset.normalizedParentYearTotals
          .filter((r) => r.parent_organization === selection.entityKey)
          .map((r) => ({
            year: Number(r.rating_year),
            value: parseNum(r[metric.aggField]),
            entity: r.parent_organization,
            scope: 'parent',
            members_included: parseNum(r.members_included),
            contracts_included: parseNum(r.contracts_included),
            code: ''
          }));
      } else {
        rows = dataset.normalizedParentAggregates
          .filter((r) => r.parent_organization === selection.entityKey)
          .filter((r) => r.measure_name_canonical_key === state.measureKey)
          .map((r) => ({
            year: Number(r.rating_year),
            value: parseNum(r[metric.aggField]),
            entity: r.parent_organization,
            scope: 'parent',
            members_included: parseNum(r.members_included),
            contracts_included: parseNum(r.contracts_included),
            code: r.measure_code_observed || ''
          }));
      }
    } else {
      if (isTotalMetric()) {
        rows = dataset.allMaYearTotals.map((r) => ({
          year: Number(r.rating_year),
          value: parseNum(r[metric.aggField]),
          entity: 'All MA',
          scope: 'all_ma',
          members_included: parseNum(r.members_included),
          contracts_included: parseNum(r.contracts_included),
          code: ''
        }));
      } else {
        rows = dataset.allMaAggregates
          .filter((r) => r.measure_name_canonical_key === state.measureKey)
          .map((r) => ({
            year: Number(r.rating_year),
            value: parseNum(r[metric.aggField]),
            entity: 'All MA',
            scope: 'all_ma',
            members_included: parseNum(r.members_included),
            contracts_included: parseNum(r.contracts_included),
            code: r.measure_code_observed || ''
          }));
      }
    }

    const byYear = new Map();
    for (let y = yearMin; y <= yearMax; y += 1) {
      byYear.set(y, { year: y, value: null, entity: selection.label, scope: selection.scope, code: '' });
    }
    for (const r of rows) {
      if (r.year >= yearMin && r.year <= yearMax) byYear.set(r.year, r);
    }

    return {
      ...selection,
      rows: Array.from(byYear.values()).sort((a, b) => a.year - b.year)
    };
  }

  function hideTooltip() {
    els.tooltip.hidden = true;
    els.tooltip.innerHTML = '';
  }

  function showTooltip(evt, series, row) {
    const metricLabel = metricMeta[state.metric].label;
    const html = [
      `<div><strong>${row.year}</strong></div>`,
      `<div>Series: <strong>${scopeTitle(series.scope)} | ${series.label}</strong></div>`,
      `<div>${metricLabel}: <strong>${fmt(row.value, 6)}</strong></div>`
    ];

    if (row.code) html.push(`<div>Code(s): ${row.code}</div>`);
    if (row.parent_organization) html.push(`<div>Parent: ${row.parent_organization}</div>`);
    if (row.members_included) html.push(`<div>Members: ${fmt(row.members_included, 0)}</div>`);
    if (row.contracts_included) html.push(`<div>Contracts: ${fmt(row.contracts_included, 0)}</div>`);
    if (row.enrollment_lives) html.push(`<div>Enrollment Lives: ${fmt(row.enrollment_lives, 0)}</div>`);

    els.tooltip.innerHTML = html.join('');
    const rect = els.chart.getBoundingClientRect();
    const x = evt.clientX - rect.left + 12;
    const y = evt.clientY - rect.top - 12;
    els.tooltip.style.left = `${Math.max(8, x)}px`;
    els.tooltip.style.top = `${Math.max(8, y)}px`;
    els.tooltip.hidden = false;
  }

  function renderLegend(seriesList) {
    els.chartLegend.innerHTML = '';
    for (const s of seriesList) {
      const item = document.createElement('button');
      item.type = 'button';
      item.className = 'legend-item';
      if (hiddenSeries.has(s.id)) item.classList.add('off');

      const dot = document.createElement('span');
      dot.className = 'legend-dot';
      dot.style.backgroundColor = s.color;

      const label = document.createElement('span');
      label.textContent = `${scopeTitle(s.scope)} | ${s.label}`;

      item.appendChild(dot);
      item.appendChild(label);
      item.addEventListener('click', () => {
        if (hiddenSeries.has(s.id)) hiddenSeries.delete(s.id);
        else hiddenSeries.add(s.id);
        render();
      });

      els.chartLegend.appendChild(item);
    }
  }

  function renderChart(seriesList) {
    const svg = els.chart;
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    hideTooltip();

    const width = 1000;
    const height = 360;
    const pad = { l: 58, r: 20, t: 20, b: 38 };
    const visible = seriesList.filter((s) => !hiddenSeries.has(s.id));
    const scale = getYScale(visible);

    const x = (year) => pad.l + ((year - yearMin) / (yearMax - yearMin)) * (width - pad.l - pad.r);
    const y = (val) => pad.t + (1 - (val - scale.min) / (scale.max - scale.min || 1)) * (height - pad.t - pad.b);

    for (const tv of scale.ticks) {
      const gy = y(tv);
      const gl = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      gl.setAttribute('class', 'grid');
      gl.setAttribute('x1', pad.l);
      gl.setAttribute('y1', gy);
      gl.setAttribute('x2', width - pad.r);
      gl.setAttribute('y2', gy);
      svg.appendChild(gl);

      const tx = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      tx.setAttribute('class', 'tick');
      tx.setAttribute('x', 6);
      tx.setAttribute('y', gy + 4);
      tx.textContent = fmt(tv, 3);
      svg.appendChild(tx);
    }

    const axisX = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    axisX.setAttribute('class', 'axis');
    axisX.setAttribute('x1', pad.l);
    axisX.setAttribute('y1', height - pad.b);
    axisX.setAttribute('x2', width - pad.r);
    axisX.setAttribute('y2', height - pad.b);
    svg.appendChild(axisX);

    const axisY = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    axisY.setAttribute('class', 'axis');
    axisY.setAttribute('x1', pad.l);
    axisY.setAttribute('y1', pad.t);
    axisY.setAttribute('x2', pad.l);
    axisY.setAttribute('y2', height - pad.b);
    svg.appendChild(axisY);

    for (let yr = yearMin; yr <= yearMax; yr += 1) {
      const tx = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      tx.setAttribute('class', 'tick');
      tx.setAttribute('x', x(yr) - 13);
      tx.setAttribute('y', height - 12);
      tx.textContent = String(yr);
      svg.appendChild(tx);
    }

    for (const series of visible) {
      let d = '';
      let started = false;
      for (const r of series.rows) {
        if (typeof r.value !== 'number') {
          started = false;
          continue;
        }
        const px = x(r.year);
        const py = y(r.value);
        d += `${started ? 'L' : 'M'} ${px} ${py} `;
        started = true;
      }

      if (d) {
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('class', 'series');
        path.setAttribute('d', d.trim());
        path.style.stroke = series.color;
        svg.appendChild(path);
      }

      for (const r of series.rows) {
        if (typeof r.value !== 'number') continue;
        const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        dot.setAttribute('class', 'dot');
        dot.setAttribute('cx', x(r.year));
        dot.setAttribute('cy', y(r.value));
        dot.setAttribute('r', 4.2);
        dot.style.fill = series.color;
        svg.appendChild(dot);

        const hit = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        hit.setAttribute('class', 'dot-hit');
        hit.setAttribute('cx', x(r.year));
        hit.setAttribute('cy', y(r.value));
        hit.setAttribute('r', 10);
        hit.addEventListener('mouseenter', (evt) => showTooltip(evt, series, r));
        hit.addEventListener('mousemove', (evt) => showTooltip(evt, series, r));
        hit.addEventListener('mouseleave', hideTooltip);
        svg.appendChild(hit);
      }
    }
  }

  function buildTableRows(seriesList) {
    const rows = [];
    for (const s of seriesList) {
      if (hiddenSeries.has(s.id)) continue;
      for (const r of s.rows) {
        if (typeof r.value !== 'number') continue;
        rows.push({
          year: r.year,
          scope: scopeTitle(s.scope),
          entity: s.label,
          metric: metricMeta[state.metric].label,
          value: r.value,
          members_or_lives: r.members_included || r.enrollment_lives || '',
          contracts: r.contracts_included || '',
          codes: r.code || ''
        });
      }
    }
    rows.sort((a, b) => (a.year - b.year) || a.scope.localeCompare(b.scope) || a.entity.localeCompare(b.entity));
    return rows;
  }

  function renderTable(rows) {
    els.tableHead.innerHTML = '<tr><th>Year</th><th>Scope</th><th>Entity</th><th>Metric</th><th>Value</th><th>Members/Lives</th><th>Contracts</th><th>Code(s)</th></tr>';
    els.tableBody.innerHTML = rows.map((r) => `<tr><td>${r.year}</td><td>${r.scope}</td><td>${r.entity}</td><td>${r.metric}</td><td>${fmt(r.value, 6)}</td><td>${r.members_or_lives}</td><td>${r.contracts}</td><td>${r.codes}</td></tr>`).join('');
  }

  function toCsv(rows) {
    const headers = ['year', 'scope', 'entity', 'metric', 'value', 'members_or_lives', 'contracts', 'codes'];
    const esc = (v) => {
      const s = String(v == null ? '' : v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    return [headers.join(','), ...rows.map((r) => headers.map((h) => esc(r[h])).join(','))].join('\n');
  }

  function downloadCsv(rows) {
    const csv = toCsv(rows);
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `cms_stars_compare_${state.metric}_${state.measureKey || 'total'}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  function render() {
    const useMeasure = !isTotalMetric();
    els.measureSelect.disabled = !useMeasure;

    const seriesList = state.seriesSelections.map(getSeriesForSelection);
    renderLegend(seriesList);
    renderChart(seriesList);

    const tableRows = buildTableRows(seriesList);
    renderTable(tableRows);

    const measureName = measureMap.get(state.measureKey) ? measureMap.get(state.measureKey).name : state.measureKey;
    const metricLabel = metricMeta[state.metric].label;
    els.chartTitle.textContent = useMeasure ? `${metricLabel} | ${measureName}` : metricLabel;

    const visibleCount = seriesList.filter((s) => !hiddenSeries.has(s.id)).length;
    const generatedAt = data.metadata && data.metadata.generated_at_utc ? data.metadata.generated_at_utc : 'unknown';
    els.meta.textContent = `Selected series: ${state.seriesSelections.length} | Visible: ${visibleCount} | Generated UTC: ${generatedAt}`;

    els.exportBtn.onclick = () => downloadCsv(tableRows);
  }

  els.tabs.forEach((tab) => {
    tab.addEventListener('click', () => {
      state.tab = tab.dataset.tab;
      state.activeScope = tab.dataset.tab;
      els.tabs.forEach((t) => t.classList.toggle('active', t === tab));
      els.scopeSelect.value = state.activeScope;
      refreshControls();
      render();
    });
  });

  els.measureSelect.addEventListener('change', (e) => {
    state.measureKey = e.target.value;
    refreshControls();
    render();
  });

  els.metricSelect.addEventListener('change', (e) => {
    state.metric = e.target.value;
    refreshControls();
    render();
  });

  els.scopeSelect.addEventListener('change', (e) => {
    state.activeScope = e.target.value;
    refreshControls();
  });

  els.entitySearch.addEventListener('input', (e) => {
    state.search = e.target.value;
    renderEntityList();
  });

  for (const btn of els.quickButtons) {
    btn.addEventListener('click', () => {
      const key = btn.dataset.quick;
      const target = quickTargets[key];
      if (!target) return;
      toggleSelection(target.scope, target.entityKey);
      refreshControls();
      render();
    });
  }

  buildMeasureOptions();

  addSelection('all_ma', 'all_ma');
  const firstContract = getDefaultEntityForScope('contract');
  if (firstContract) addSelection('contract', firstContract);

  refreshControls();
  render();  })().catch((err) => {
    document.body.innerHTML = `<p style="padding:20px;font-family:sans-serif">Data missing or failed to load: ${String(err && err.message ? err.message : err)}</p>`;
  });
})();
