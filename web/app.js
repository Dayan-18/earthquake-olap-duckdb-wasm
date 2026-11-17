// Earthquake OLAP Explorer — every query below runs inside the browser with DuckDB-WASM.
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

const TABLES = ['fact_earthquake', 'dim_date', 'dim_region', 'dim_magnitude', 'dim_depth'];
const MAG_COLORS = ['#fdc28c', '#fb9a5b', '#f26b3a', '#d9431e', '#a8240f', '#6b0d05'];
const $ = (id) => document.getElementById(id);

let conn;
let meta;
const charts = {};
let map, mapLayer;

// ---------------------------------------------------------------- DuckDB setup
async function initDuckDB() {
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();

  // Download the Parquet star schema once and materialise it in the in-browser database.
  await Promise.all(TABLES.map(async (t) => {
    const res = await fetch(`data/${t}.parquet`);
    if (!res.ok) throw new Error(`Could not load data/${t}.parquet (HTTP ${res.status})`);
    await db.registerFileBuffer(`${t}.parquet`, new Uint8Array(await res.arrayBuffer()));
  }));
  for (const t of TABLES) {
    await conn.query(`CREATE TABLE ${t} AS SELECT * FROM read_parquet('${t}.parquet')`);
  }
}

async function q(sql) {
  const t0 = performance.now();
  const table = await conn.query(sql);
  const ms = performance.now() - t0;
  const fields = table.schema.fields.map((f) => ({ name: f.name, type: String(f.type) }));
  const rows = table.toArray().map((r) => {
    const o = r.toJSON();
    for (const k in o) if (typeof o[k] === 'bigint') o[k] = Number(o[k]);
    return o;
  });
  return { rows, ms, fields };
}

// ---------------------------------------------------------------- filters
const esc = (s) => String(s).replace(/'/g, "''");
const isDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s);
const GRAINS = {
  year:    "strftime(bucket, '%Y')",
  quarter: "strftime(bucket, '%Y') || '-Q' || quarter(bucket)",
  month:   "strftime(bucket, '%Y-%m')",
  week:    "strftime(bucket, '%Y-%m-%d')",
  day:     "strftime(bucket, '%Y-%m-%d')",
};

function state() {
  const from = isDate($('f-from').value) ? $('f-from').value : meta.start_date;
  const to = isDate($('f-to').value) ? $('f-to').value : meta.end_date;
  const mag = Math.min(10, Math.max(-2, Number($('f-mag').value) || 2.5));
  const grain = GRAINS[$('f-grain').value] ? $('f-grain').value : 'month';
  return { from, to, mag, grain, country: $('f-country').value, depth: $('f-depth').value };
}

const STAR = `fact_earthquake f
  JOIN dim_date d USING (date_key)
  JOIN dim_region r USING (region_key)
  JOIN dim_magnitude m USING (magnitude_key)
  JOIN dim_depth dp USING (depth_key)`;

function where(s) {
  const c = [
    `d.full_date BETWEEN DATE '${s.from}' AND DATE '${s.to}'`,
    `f.magnitude >= ${s.mag}`,
  ];
  if (s.country) c.push(`r.country = '${esc(s.country)}'`);
  if (s.depth) c.push(`f.depth_key = ${Number(s.depth)}`);
  return 'WHERE ' + c.join(' AND ');
}

// ---------------------------------------------------------------- chart helpers
const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
function baseOptions(extra = {}) {
  const text = css('--muted');
  const grid = css('--grid');
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: { legend: { labels: { color: text, boxWidth: 12 } } },
    scales: {
      x: { ticks: { color: text, maxRotation: 0, autoSkip: true }, grid: { color: grid } },
      y: { ticks: { color: text }, grid: { color: grid }, beginAtZero: true },
    },
    ...extra,
  };
}
function drawChart(key, canvasId, config) {
  if (charts[key]) charts[key].destroy();
  charts[key] = new Chart($(canvasId), config);
}
const fmt = (n, d = 0) => Number(n ?? 0).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });

// ---------------------------------------------------------------- dashboard panels
async function kpis(s) {
  const r = await q(`
    SELECT count(*)::INT AS n,
           round(avg(f.magnitude), 2) AS avg_mag,
           max(f.magnitude) AS max_mag,
           arg_max(f.place, f.magnitude) AS max_place,
           round(avg(f.depth_km), 1) AS avg_depth
    FROM ${STAR} ${where(s)}`);
  const k = r.rows[0];
  $('k-count').textContent = fmt(k.n);
  $('k-avg').textContent = k.n ? fmt(k.avg_mag, 2) : '–';
  $('k-max').textContent = k.n ? 'M' + fmt(k.max_mag, 1) : '–';
  $('k-max-place').textContent = k.max_place ?? '';
  $('k-max-place').title = k.max_place ?? '';
  $('k-depth').textContent = k.n ? fmt(k.avg_depth, 1) + ' km' : '–';
  return r.ms;
}

async function timeline(s) {
  const r = await q(`
    WITH b AS (
      SELECT date_trunc('${s.grain}', d.full_date) AS bucket, f.magnitude_key
      FROM ${STAR} ${where(s)}
    )
    SELECT ${GRAINS[s.grain]} AS label, bucket, magnitude_key, count(*)::INT AS n
    FROM b GROUP BY ALL ORDER BY bucket`);
  const labels = [...new Set(r.rows.map((x) => x.label))];
  const idx = new Map(labels.map((l, i) => [l, i]));
  const classes = (await q('SELECT magnitude_key, magnitude_class FROM dim_magnitude ORDER BY 1')).rows;
  const datasets = classes.map((c) => {
    const data = new Array(labels.length).fill(0);
    r.rows.filter((x) => x.magnitude_key === c.magnitude_key).forEach((x) => { data[idx.get(x.label)] = x.n; });
    return { label: c.magnitude_class, data, backgroundColor: MAG_COLORS[c.magnitude_key - 1], stack: 's' };
  }).filter((ds) => ds.data.some((v) => v > 0));

  const opts = baseOptions();
  opts.scales.x.stacked = true;
  opts.scales.y.stacked = true;
  opts.plugins.tooltip = { mode: 'index', intersect: false };
  drawChart('time', 'c-time', { type: 'bar', data: { labels, datasets }, options: opts });
  $('grain-label').textContent = `${labels.length} ${s.grain} buckets`;
  return r.ms;
}

async function geography(s) {
  const level = s.country ? 'r.region_name' : 'r.country';
  const r = await q(`
    SELECT ${level} AS name, count(*)::INT AS n, round(max(f.magnitude), 1) AS max_mag
    FROM ${STAR} ${where(s)}
    GROUP BY 1 ORDER BY n DESC LIMIT 15`);
  $('geo-title').textContent = s.country ? `Regions in ${s.country}` : 'Top 15 countries / areas';
  $('geo-hint').textContent = s.country ? 'Drill-down level: region (reset country to roll up)' : 'Click a bar to drill down';

  const opts = baseOptions({ indexAxis: 'y' });
  opts.plugins.legend = { display: false };
  opts.scales.y.ticks.autoSkip = false;
  opts.plugins.tooltip = { callbacks: { afterLabel: (c) => `Max magnitude: M${r.rows[c.dataIndex].max_mag}` } };
  opts.onClick = (_e, els) => {
    if (s.country || !els.length) return;
    $('f-country').value = r.rows[els[0].index].name;
    refresh();
  };
  opts.onHover = (e, els) => { e.native.target.style.cursor = !s.country && els.length ? 'pointer' : 'default'; };
  drawChart('geo', 'c-geo', {
    type: 'bar',
    data: { labels: r.rows.map((x) => x.name), datasets: [{ label: 'Earthquakes', data: r.rows.map((x) => x.n), backgroundColor: css('--accent') }] },
    options: opts,
  });
  return r.ms;
}

async function pivot(s) {
  const r = await q(`
    SELECT f.depth_key, f.magnitude_key, count(*)::INT AS n
    FROM ${STAR} ${where(s)} GROUP BY ALL`);
  const depths = (await q('SELECT depth_key, depth_class, min_km::DOUBLE AS min_km, max_km::DOUBLE AS max_km FROM dim_depth ORDER BY 1')).rows;
  const mags = (await q('SELECT magnitude_key, magnitude_class FROM dim_magnitude ORDER BY 1')).rows;
  const cell = new Map(r.rows.map((x) => [`${x.depth_key}-${x.magnitude_key}`, x.n]));
  const max = Math.max(1, ...r.rows.map((x) => x.n));
  const accent = css('--accent');

  const head = `<tr><th>Depth \\ Magnitude</th>${mags.map((m) => `<th>${m.magnitude_class}</th>`).join('')}<th>Total</th></tr>`;
  const colTotals = new Array(mags.length).fill(0);
  const body = depths.map((d) => {
    let rowTotal = 0;
    const tds = mags.map((m, i) => {
      const n = cell.get(`${d.depth_key}-${m.magnitude_key}`) ?? 0;
      rowTotal += n; colTotals[i] += n;
      const alpha = n ? (0.12 + 0.75 * Math.log1p(n) / Math.log1p(max)).toFixed(2) : 0;
      const bg = n ? `background:color-mix(in srgb, ${accent} ${Math.round(alpha * 100)}%, transparent)` : '';
      return `<td class="cell" style="${bg}">${n ? fmt(n) : '·'}</td>`;
    }).join('');
    const range = d.depth_key === 3 ? `≥ ${d.min_km} km` : `${Math.max(0, d.min_km)}–${d.max_km} km`;
    return `<tr><td>${d.depth_class} <span class="muted">(${range})</span></td>${tds}<td class="total">${fmt(rowTotal)}</td></tr>`;
  }).join('');
  const grand = colTotals.reduce((a, b) => a + b, 0);
  const foot = `<tr class="total"><td>Total</td>${colTotals.map((n) => `<td>${fmt(n)}</td>`).join('')}<td>${fmt(grand)}</td></tr>`;
  $('pivot').innerHTML = `<table class="pivot">${head}${body}${foot}</table>`;
  return r.ms;
}

async function mapPanel(s) {
  const r = await q(`
    SELECT f.latitude, f.longitude, f.magnitude, f.magnitude_key, f.depth_km, f.place,
           strftime(f.event_ts, '%Y-%m-%d %H:%M UTC') AS ts
    FROM ${STAR} ${where(s)}
    ORDER BY f.magnitude DESC LIMIT 500`);
  if (!map) {
    map = L.map('map', { worldCopyJump: true }).setView([10, 0], 2);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 8,
      attribution: '&copy; OpenStreetMap contributors',
    }).addTo(map);
  }
  if (mapLayer) mapLayer.remove();
  mapLayer = L.layerGroup(r.rows.map((e) => {
    const color = MAG_COLORS[e.magnitude_key - 1];
    return L.circleMarker([e.latitude, e.longitude], {
      radius: Math.max(3, (e.magnitude - 2) * 2.6), color, weight: 1, fillColor: color, fillOpacity: 0.55,
    }).bindPopup(`<strong>M${e.magnitude}</strong> · ${e.place}<br>${e.ts}<br>Depth: ${e.depth_km} km`);
  })).addTo(map);
  return r.ms;
}

let running = false;
let pending = false;
async function refresh() {
  if (running) { pending = true; return; }
  running = true;
  try {
    const s = state();
    $('f-mag-val').textContent = s.mag.toFixed(1);
    let ms = 0;
    for (const panel of [kpis, timeline, geography, pivot, mapPanel]) ms += await panel(s);
    $('k-ms').textContent = `${fmt(ms, 0)} ms`;
  } catch (err) {
    console.error(err);
    setStatus('err', 'Query error: ' + err.message);
  } finally {
    running = false;
    if (pending) { pending = false; refresh(); }
  }
}

// ---------------------------------------------------------------- SQL console
const EXAMPLES = [
  ['Roll-up: events by year and quarter (GROUP BY ROLLUP)', `SELECT d.year, d.quarter, count(*) AS events, round(avg(f.magnitude), 2) AS avg_mag
FROM fact_earthquake f
JOIN dim_date d USING (date_key)
GROUP BY ROLLUP (d.year, d.quarter)
ORDER BY d.year NULLS LAST, d.quarter NULLS LAST;`],
  ['Cube: country x magnitude class (GROUP BY CUBE)', `SELECT r.country, m.magnitude_class, count(*) AS events
FROM fact_earthquake f
JOIN dim_region r USING (region_key)
JOIN dim_magnitude m USING (magnitude_key)
WHERE r.country IN ('Japan', 'Chile', 'Peru', 'Indonesia', 'Mexico')
GROUP BY CUBE (r.country, m.magnitude_class)
ORDER BY r.country NULLS LAST, m.magnitude_class NULLS LAST;`],
  ['Window: monthly trend with 3-month moving average', `WITH monthly AS (
  SELECT date_trunc('month', d.full_date) AS month, count(*) AS events
  FROM fact_earthquake f JOIN dim_date d USING (date_key)
  GROUP BY 1
)
SELECT strftime(month, '%Y-%m') AS month, events,
       round(avg(events) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) AS moving_avg_3m,
       events - lag(events) OVER (ORDER BY month) AS change_vs_prev
FROM monthly ORDER BY month;`],
  ['Slice: strongest earthquakes in South America (Peru, Chile, Ecuador...)', `SELECT strftime(f.event_ts, '%Y-%m-%d %H:%M') AS time_utc, f.magnitude, f.depth_km, r.country, f.place
FROM fact_earthquake f JOIN dim_region r USING (region_key)
WHERE r.country IN ('Peru', 'Chile', 'Ecuador', 'Colombia', 'Bolivia', 'Argentina')
ORDER BY f.magnitude DESC
LIMIT 25;`],
  ['Ranking: top 3 regions per country (QUALIFY)', `SELECT r.country, r.region_name, count(*) AS events,
       rank() OVER (PARTITION BY r.country ORDER BY count(*) DESC) AS rnk
FROM fact_earthquake f JOIN dim_region r USING (region_key)
GROUP BY r.country, r.region_name
QUALIFY rnk <= 3 AND r.country IN ('United States', 'Mexico')
ORDER BY r.country, rnk;`],
  ['Distribution: hour of day (UTC) vs magnitude', `SELECT f.hour_utc, count(*) AS events, round(avg(f.magnitude), 2) AS avg_mag,
       bar(count(*), 0, 3500, 30) AS histogram
FROM fact_earthquake f
GROUP BY 1 ORDER BY 1;`],
  ['Schema: describe the fact table', `DESCRIBE fact_earthquake;`],
];

function formatCell(v, type) {
  if (v === null || v === undefined) return '<span class="muted">NULL</span>';
  if (type.startsWith('Date') && typeof v === 'number') return new Date(v).toISOString().slice(0, 10);
  if (type.startsWith('Timestamp') && typeof v === 'number') return new Date(v).toISOString().replace('T', ' ').slice(0, 19);
  if (typeof v === 'number') return Number.isInteger(v) ? String(v) : fmt(v, 2);
  if (typeof v === 'object' && !Array.isArray(v) && String(v) !== '[object Object]') return String(v);
  return String(v).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

async function runConsole() {
  const out = $('sql-out');
  try {
    const r = await q($('sql').value);
    const shown = r.rows.slice(0, 500);
    $('sql-info').textContent = `${fmt(r.rows.length)} rows · ${fmt(r.ms, 1)} ms${r.rows.length > 500 ? ' · showing first 500' : ''}`;
    out.innerHTML = `<table><tr>${r.fields.map((f) => `<th>${f.name}</th>`).join('')}</tr>${
      shown.map((row) => `<tr>${r.fields.map((f) => `<td>${formatCell(row[f.name], f.type)}</td>`).join('')}</tr>`).join('')
    }</table>`;
  } catch (err) {
    $('sql-info').textContent = '';
    out.innerHTML = `<div class="error">${formatCell(err.message, '')}</div>`;
  }
}

// ---------------------------------------------------------------- boot
function setStatus(kind, text) {
  const el = $('status');
  el.className = `pill ${kind}`;
  el.textContent = text;
}

async function main() {
  try {
    meta = await (await fetch('data/metadata.json')).json();
    $('updated').textContent = `Data refreshed ${meta.generated_at_utc.replace('T', ' ').replace('Z', ' UTC')}`;
    setStatus('loading', 'Loading Parquet into DuckDB-WASM…');
    const t0 = performance.now();
    await initDuckDB();
    setStatus('ok', `DuckDB-WASM ready · ${fmt(meta.row_counts.fact_earthquake)} facts in ${fmt((performance.now() - t0) / 1000, 1)} s`);

    for (const id of ['f-from', 'f-to']) {
      $(id).min = meta.start_date;
      $(id).max = meta.end_date;
    }
    $('f-from').value = meta.start_date;
    $('f-to').value = meta.end_date;

    const countries = await q(`SELECT r.country, count(*)::INT AS n FROM fact_earthquake f JOIN dim_region r USING (region_key) GROUP BY 1 ORDER BY n DESC`);
    $('f-country').insertAdjacentHTML('beforeend', countries.rows
      .map((c) => `<option value="${formatCell(c.country, '').replace(/"/g, '&quot;')}">${formatCell(c.country, '')} (${fmt(c.n)})</option>`).join(''));
    const depths = await q('SELECT depth_key, depth_class FROM dim_depth ORDER BY 1');
    $('f-depth').insertAdjacentHTML('beforeend', depths.rows.map((d) => `<option value="${d.depth_key}">${d.depth_class}</option>`).join(''));

    let timer;
    const debounced = () => { clearTimeout(timer); timer = setTimeout(refresh, 150); };
    for (const id of ['f-from', 'f-to', 'f-country', 'f-depth', 'f-grain']) $(id).addEventListener('change', refresh);
    $('f-mag').addEventListener('input', () => { $('f-mag-val').textContent = Number($('f-mag').value).toFixed(1); debounced(); });
    $('f-reset').addEventListener('click', () => {
      $('f-from').value = meta.start_date; $('f-to').value = meta.end_date;
      $('f-mag').value = 2.5; $('f-country').value = ''; $('f-depth').value = ''; $('f-grain').value = 'month';
      refresh();
    });

    $('sql-examples').innerHTML = EXAMPLES.map(([label], i) => `<option value="${i}">${label}</option>`).join('');
    $('sql-examples').addEventListener('change', () => { $('sql').value = EXAMPLES[$('sql-examples').value][1]; runConsole(); });
    $('sql-run').addEventListener('click', runConsole);
    $('sql').addEventListener('keydown', (e) => { if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') runConsole(); });
    $('sql').value = EXAMPLES[0][1];

    await refresh();
    await runConsole();
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', refresh);
  } catch (err) {
    console.error(err);
    setStatus('err', 'Failed: ' + err.message);
  }
}

main();
