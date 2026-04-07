"""
CAISO LMP Pricing Dashboard
=============================
Serves today's and yesterday's RTM + HASP LMP data on one page.
ALL four sections are cached server-side in background threads.
Browser polls every 5 seconds until each section is ready.
Deploy to Render.com.

Requirements: requests, flask, gunicorn
"""

import io
import re
import time
import zipfile
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify

OASIS_URL = "https://oasis.caiso.com/oasisapi/SingleZip"
NODE      = "ELAP_PACE-APND"
VERSION   = "1"
TZ_PT     = ZoneInfo("America/Los_Angeles")
TZ_UTC    = ZoneInfo("UTC")

app = Flask(__name__)

# Cache for all four sections. None = not ready yet.
_cache = {
    "today_rtm":      None,
    "today_hasp":     None,
    "yesterday_rtm":  None,
    "yesterday_hasp": None,
}

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


def fetch_hour(hr, date_pt, market, query):
    start_pt  = date_pt + timedelta(hours=hr)
    end_pt    = start_pt + timedelta(hours=1)
    start_utc = start_pt.astimezone(TZ_UTC)
    end_utc   = end_pt.astimezone(TZ_UTC)

    params = {
        "queryname":     query,
        "market_run_id": market,
        "grp_type":      "ALL_APNODES",
        "node":          NODE,
        "startdatetime": start_utc.strftime("%Y%m%dT%H:%M-0000"),
        "enddatetime":   end_utc.strftime("%Y%m%dT%H:%M-0000"),
        "version":       VERSION,
        "resultformat":  "6",
    }

    resp = requests.get(OASIS_URL, params=params, timeout=60)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            with zf.open(name) as f:
                raw = f.read()
            if raw.strip().startswith(b"<"):
                text = raw.decode("utf-8", errors="replace")
                err  = re.search(r"<m:ERR_DESC>(.*?)</m:ERR_DESC>", text)
                raise ValueError(err.group(1) if err else "CAISO XML error")
            lines = raw.decode("utf-8").strip().split("\n")
            hdr   = [h.strip().strip('"') for h in lines[0].split(",")]
            rows  = []
            for line in lines[1:]:
                vals = line.split(",")
                obj  = {hdr[i]: vals[i].strip().strip('"') for i in range(len(hdr))}
                if obj.get("NODE") == NODE and obj.get("LMP_TYPE") == "LMP":
                    rows.append(obj)
            return rows
    return []


def fetch_all_hours(label, date_pt, market, query, num_hours):
    all_rows = []
    for hr in range(num_hours):
        try:
            rows = fetch_hour(hr, date_pt, market, query)
            all_rows.extend(rows)
            print(f"  [{label}] Hour {hr:02d}: {len(rows)} rows")
        except Exception as e:
            print(f"  [{label}] Hour {hr:02d}: SKIPPED ({e})")
        time.sleep(10)
    return all_rows


def warm_all():
    now_pt       = datetime.now(tz=TZ_PT)
    today_pt     = now_pt.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_pt = (now_pt - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    hours_so_far = now_pt.hour

    print("Warming yesterday RTM cache...")
    _cache["yesterday_rtm"] = fetch_all_hours("RTM Yesterday", yesterday_pt, "RTM", "PRC_INTVL_LMP", 24)
    print("Yesterday RTM ready!")

    print("Warming yesterday HASP cache...")
    _cache["yesterday_hasp"] = fetch_all_hours("HASP Yesterday", yesterday_pt, "HASP", "PRC_HASP_LMP", 24)
    print("Yesterday HASP ready!")

    print(f"Warming today RTM cache ({hours_so_far} hours)...")
    _cache["today_rtm"] = fetch_all_hours("RTM Today", today_pt, "RTM", "PRC_INTVL_LMP", hours_so_far)
    print("Today RTM ready!")

    print(f"Warming today HASP cache ({hours_so_far} hours)...")
    _cache["today_hasp"] = fetch_all_hours("HASP Today", today_pt, "HASP", "PRC_HASP_LMP", hours_so_far)
    print("Today HASP ready! All sections loaded.")


def refresh_today_loop():
    """After warm_all finishes, refresh today's data at the top of each hour."""
    while True:
        now_pt    = datetime.now(tz=TZ_PT)
        next_hour = now_pt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        sleep_sec = (next_hour - now_pt).total_seconds()
        print(f"  [Scheduler] Next today refresh in {int(sleep_sec)}s")
        time.sleep(sleep_sec)

        now_pt       = datetime.now(tz=TZ_PT)
        today_pt     = now_pt.replace(hour=0, minute=0, second=0, microsecond=0)
        hours_so_far = now_pt.hour

        print(f"  [Scheduler] Refreshing today RTM ({hours_so_far} hours)...")
        _cache["today_rtm"] = fetch_all_hours("RTM Today", today_pt, "RTM", "PRC_INTVL_LMP", hours_so_far)

        print(f"  [Scheduler] Refreshing today HASP ({hours_so_far} hours)...")
        _cache["today_hasp"] = fetch_all_hours("HASP Today", today_pt, "HASP", "PRC_HASP_LMP", hours_so_far)

        print("  [Scheduler] Today refresh complete!")


def startup():
    warm_all()
    threading.Thread(target=refresh_today_loop, daemon=True).start()


threading.Thread(target=startup, daemon=True).start()


# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/today/rtm")
def today_rtm():
    if _cache["today_rtm"] is None:
        return jsonify({"error": "still_loading"}), 503
    return jsonify(_cache["today_rtm"])

@app.route("/today/hasp")
def today_hasp():
    if _cache["today_hasp"] is None:
        return jsonify({"error": "still_loading"}), 503
    return jsonify(_cache["today_hasp"])

@app.route("/yesterday/rtm")
def yesterday_rtm():
    if _cache["yesterday_rtm"] is None:
        return jsonify({"error": "still_loading"}), 503
    return jsonify(_cache["yesterday_rtm"])

@app.route("/yesterday/hasp")
def yesterday_hasp():
    if _cache["yesterday_hasp"] is None:
        return jsonify({"error": "still_loading"}), 503
    return jsonify(_cache["yesterday_hasp"])


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CAISO LMP Pricing - ELAP_PACE-APND</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: Arial, sans-serif; background: #f0f4f8; color: #222; }
  .section-header {
    background: #1F4E79; color: #fff; padding: 16px 24px;
    display: flex; align-items: center; justify-content: space-between;
    margin-top: 24px;
  }
  .section-header:first-of-type { margin-top: 0; }
  .section-header h1 { font-size: 18px; }
  .section-header .meta { font-size: 12px; opacity: .75; text-align: right; }
  .cards { display: flex; gap: 16px; padding: 20px 24px 0; flex-wrap: wrap; }
  .card { background: #fff; border-radius: 8px; padding: 16px 20px; flex: 1; min-width: 140px; box-shadow: 0 1px 4px rgba(0,0,0,.1); }
  .card .label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: .5px; }
  .card .value { font-size: 26px; font-weight: bold; margin-top: 4px; }
  .card .value.pos { color: #1a6b2f; }
  .card .value.neg { color: #b91c1c; }
  .chart-wrap { padding: 20px 24px 0; }
  .chart-wrap h2 { font-size: 13px; color: #444; margin-bottom: 8px; }
  canvas { background: #fff; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.1); width: 100% !important; }
  .table-wrap { padding: 20px 24px 24px; overflow-x: auto; }
  .table-wrap h2 { font-size: 13px; color: #444; margin-bottom: 8px; }
  table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,.1); font-size: 13px; }
  thead tr { background: #1F4E79; color: #fff; }
  th, td { padding: 8px 14px; text-align: center; border-bottom: 1px solid #e5e7eb; }
  tbody tr:nth-child(even) { background: #D6E4F0; }
  tbody tr:hover { background: #bfd5ec; }
  td.neg { color: #b91c1c; font-weight: bold; }
  td.pos { color: #1a6b2f; }
  .status { text-align: center; padding: 40px; color: #666; font-size: 14px; }
  .spinner { display: inline-block; width: 24px; height: 24px; border: 3px solid #ccc; border-top-color: #1F4E79; border-radius: 50%; animation: spin .8s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .divider { height: 4px; background: #1F4E79; opacity: 0.2; margin-top: 24px; }
  .divider-heavy { height: 8px; background: #1F4E79; opacity: 0.5; margin-top: 24px; }
</style>
</head>
<body>

<!-- Today RTM -->
<div class="section-header">
  <div>
    <h1>RTM 5-Min LMP - Today So Far | ELAP_PACE-APND</h1>
    <div class="meta" id="todayRtmRefreshed">Loading...</div>
  </div>
  <div><button onclick="loadSection('today','rtm')" style="background:#fff;color:#1F4E79;border:none;padding:7px 14px;border-radius:6px;cursor:pointer;font-weight:bold;font-size:13px;">Refresh</button></div>
</div>
<div class="cards">
  <div class="card"><div class="label">Latest</div><div class="value" id="today-rtm-cLatest">-</div></div>
  <div class="card"><div class="label">High</div><div class="value pos" id="today-rtm-cHigh">-</div></div>
  <div class="card"><div class="label">Low</div><div class="value neg" id="today-rtm-cLow">-</div></div>
  <div class="card"><div class="label">Avg</div><div class="value" id="today-rtm-cAvg">-</div></div>
  <div class="card"><div class="label">Hours</div><div class="value" id="today-rtm-cHours">-</div></div>
</div>
<div class="chart-wrap"><h2>5-Min LMP ($/MWh)</h2><canvas id="today-rtm-chart" height="180"></canvas></div>
<div class="table-wrap"><h2>Hourly Avg</h2><div id="today-rtm-table"><div class="status"><span class="spinner"></span> Server warming up data...</div></div></div>

<div class="divider"></div>

<!-- Yesterday RTM -->
<div class="section-header">
  <div>
    <h1>RTM 5-Min LMP - Yesterday | ELAP_PACE-APND</h1>
    <div class="meta" id="yestRtmRefreshed">Loading...</div>
  </div>
  <div><button onclick="loadSection('yesterday','rtm')" style="background:#fff;color:#1F4E79;border:none;padding:7px 14px;border-radius:6px;cursor:pointer;font-weight:bold;font-size:13px;">Refresh</button></div>
</div>
<div class="cards">
  <div class="card"><div class="label">High</div><div class="value pos" id="yest-rtm-cHigh">-</div></div>
  <div class="card"><div class="label">Low</div><div class="value neg" id="yest-rtm-cLow">-</div></div>
  <div class="card"><div class="label">Avg</div><div class="value" id="yest-rtm-cAvg">-</div></div>
  <div class="card"><div class="label">On-Peak Avg</div><div class="value" id="yest-rtm-cOnPeak">-</div></div>
  <div class="card"><div class="label">Off-Peak Avg</div><div class="value" id="yest-rtm-cOffPeak">-</div></div>
</div>
<div class="chart-wrap"><h2>5-Min LMP ($/MWh)</h2><canvas id="yest-rtm-chart" height="180"></canvas></div>
<div class="table-wrap"><h2>Hourly Avg</h2><div id="yest-rtm-table"><div class="status"><span class="spinner"></span> Server warming up data...</div></div></div>

<div class="divider-heavy"></div>

<!-- Today HASP -->
<div class="section-header">
  <div>
    <h1>HASP 15-Min LMP - Today So Far | ELAP_PACE-APND</h1>
    <div class="meta" id="todayHaspRefreshed">Loading...</div>
  </div>
  <div><button onclick="loadSection('today','hasp')" style="background:#fff;color:#1F4E79;border:none;padding:7px 14px;border-radius:6px;cursor:pointer;font-weight:bold;font-size:13px;">Refresh</button></div>
</div>
<div class="cards">
  <div class="card"><div class="label">Latest</div><div class="value" id="today-hasp-cLatest">-</div></div>
  <div class="card"><div class="label">High</div><div class="value pos" id="today-hasp-cHigh">-</div></div>
  <div class="card"><div class="label">Low</div><div class="value neg" id="today-hasp-cLow">-</div></div>
  <div class="card"><div class="label">Avg</div><div class="value" id="today-hasp-cAvg">-</div></div>
  <div class="card"><div class="label">Hours</div><div class="value" id="today-hasp-cHours">-</div></div>
</div>
<div class="chart-wrap"><h2>15-Min LMP ($/MWh)</h2><canvas id="today-hasp-chart" height="180"></canvas></div>
<div class="table-wrap"><h2>Hourly Avg</h2><div id="today-hasp-table"><div class="status"><span class="spinner"></span> Server warming up data...</div></div></div>

<div class="divider"></div>

<!-- Yesterday HASP -->
<div class="section-header">
  <div>
    <h1>HASP 15-Min LMP - Yesterday | ELAP_PACE-APND</h1>
    <div class="meta" id="yestHaspRefreshed">Loading...</div>
  </div>
  <div><button onclick="loadSection('yesterday','hasp')" style="background:#fff;color:#1F4E79;border:none;padding:7px 14px;border-radius:6px;cursor:pointer;font-weight:bold;font-size:13px;">Refresh</button></div>
</div>
<div class="cards">
  <div class="card"><div class="label">High</div><div class="value pos" id="yest-hasp-cHigh">-</div></div>
  <div class="card"><div class="label">Low</div><div class="value neg" id="yest-hasp-cLow">-</div></div>
  <div class="card"><div class="label">Avg</div><div class="value" id="yest-hasp-cAvg">-</div></div>
  <div class="card"><div class="label">On-Peak Avg</div><div class="value" id="yest-hasp-cOnPeak">-</div></div>
  <div class="card"><div class="label">Off-Peak Avg</div><div class="value" id="yest-hasp-cOffPeak">-</div></div>
</div>
<div class="chart-wrap"><h2>15-Min LMP ($/MWh)</h2><canvas id="yest-hasp-chart" height="180"></canvas></div>
<div class="table-wrap"><h2>Hourly Avg</h2><div id="yest-hasp-table"><div class="status"><span class="spinner"></span> Server warming up data...</div></div></div>

<script>
var charts = {};
var polling = {};

function nowPT() {
  return new Date(new Date().toLocaleString("en-US", {timeZone:"America/Los_Angeles"}));
}
function dateStr(d) {
  return d.toLocaleDateString("en-US", {timeZone:"America/Los_Angeles"});
}

function ensureChart(cb) {
  if (window.Chart) { cb(); return; }
  var s = document.createElement("script");
  s.src = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js";
  s.onload = cb;
  document.head.appendChild(s);
}

function renderChart(id, labels, lmps) {
  ensureChart(function() {
    if (charts[id]) charts[id].destroy();
    var colors = lmps.map(function(v) { return v >= 0 ? "rgba(26,107,47,0.8)" : "rgba(185,28,28,0.8)"; });
    charts[id] = new Chart(document.getElementById(id).getContext("2d"), {
      type: "bar",
      data: { labels: labels, datasets: [{ label: "LMP ($/MWh)", data: lmps, backgroundColor: colors, borderWidth: 0 }] },
      options: {
        responsive: true,
        plugins: { legend: { display: false },
          tooltip: { callbacks: { label: function(c) { return " $" + c.parsed.y.toFixed(4) + "/MWh"; } } } },
        scales: {
          x: { ticks: { maxTicksLimit: 24, font: { size: 10 } } },
          y: { ticks: { callback: function(v) { return "$" + v; } }, grid: { color: "#e5e7eb" } }
        }
      }
    });
  });
}

function renderTable(id, rows) {
  var byHr = {};
  rows.forEach(function(r) { if (!byHr[r.hr]) byHr[r.hr]=[]; byHr[r.hr].push(r.lmp); });
  var tbl = '<table><thead><tr><th>Oper Hour</th><th>Avg ($/MWh)</th><th>Min</th><th>Max</th></tr></thead><tbody>';
  Object.keys(byHr).sort(function(a,b){return +a-+b;}).forEach(function(h) {
    var vals = byHr[h];
    var avg = vals.reduce(function(a,b){return a+b;},0)/vals.length;
    var min = Math.min.apply(null,vals), max = Math.max.apply(null,vals);
    tbl += '<tr><td>'+h+'</td><td class="'+(avg<0?"neg":"pos")+'">'+avg.toFixed(4)+
           '</td><td class="'+(min<0?"neg":"")+'">'+min.toFixed(4)+
           '</td><td class="'+(max<0?"neg":"pos")+'">'+max.toFixed(4)+'</td></tr>';
  });
  tbl += '</tbody></table>';
  document.getElementById(id).innerHTML = tbl;
}

function colorVal(id, v) {
  var el = document.getElementById(id);
  el.textContent = "$" + v.toFixed(2);
  el.className = "value " + (v >= 0 ? "pos" : "neg");
}

function renderSectionData(day, market, allRows) {
  var prefix  = day + "-" + market;
  var refId   = day === "today" ? ("today" + (market === "rtm" ? "Rtm" : "Hasp") + "Refreshed") : ("yest" + (market === "rtm" ? "Rtm" : "Hasp") + "Refreshed");
  var tableId = prefix + "-table";
  var chartId = prefix + "-chart";

  if (!allRows || !allRows.length) {
    document.getElementById(tableId).innerHTML = '<div class="status">No data available yet.</div>';
    return;
  }

  var rows = allRows.map(function(r) {
    return {
      time:   r["INTERVALSTARTTIME_GMT"],
      hr:     parseFloat(r["OPR_HR"]),
      lmp:    parseFloat(r["MW"]),
      timePT: new Date(r["INTERVALSTARTTIME_GMT"]).toLocaleTimeString("en-US",
                {hour:"2-digit", minute:"2-digit", timeZone:"America/Los_Angeles", hour12:false})
    };
  }).sort(function(a,b) { return a.time < b.time ? -1 : 1; });

  var lmps    = rows.map(function(r) { return r.lmp; });
  var onPeak  = rows.filter(function(r) { return r.hr >= 7 && r.hr <= 22; }).map(function(r) { return r.lmp; });
  var offPeak = rows.filter(function(r) { return r.hr < 7 || r.hr > 22; }).map(function(r) { return r.lmp; });
  var sum     = function(a) { return a.reduce(function(x,y){return x+y;},0); };

  if (day === "today") {
    colorVal(prefix + "-cLatest", lmps[lmps.length-1]);
    document.getElementById(prefix + "-cHours").textContent = new Set(rows.map(function(r){return r.hr;})).size;
  } else {
    document.getElementById(prefix + "-cOnPeak").textContent  = onPeak.length  ? "$" + (sum(onPeak)/onPeak.length).toFixed(2)   : "-";
    document.getElementById(prefix + "-cOffPeak").textContent = offPeak.length ? "$" + (sum(offPeak)/offPeak.length).toFixed(2) : "-";
  }
  colorVal(prefix + "-cHigh", Math.max.apply(null, lmps));
  colorVal(prefix + "-cLow",  Math.min.apply(null, lmps));
  document.getElementById(prefix + "-cAvg").textContent = "$" + (sum(lmps)/lmps.length).toFixed(2);

  renderChart(chartId, rows.map(function(r){return r.timePT;}), lmps);
  renderTable(tableId, rows);

  var now = nowPT();
  var d   = day === "today" ? now : new Date(now.getFullYear(), now.getMonth(), now.getDate()-1);
  document.getElementById(refId).textContent = dateStr(d) + " | Refreshed: " + now.toLocaleTimeString("en-US",{timeZone:"America/Los_Angeles"}) + " PT";
}

function loadSection(day, market) {
  var key     = day + "-" + market;
  var tableId = key + "-table";
  var refId   = day === "today" ? ("today" + (market === "rtm" ? "Rtm" : "Hasp") + "Refreshed") : ("yest" + (market === "rtm" ? "Rtm" : "Hasp") + "Refreshed");

  if (polling[key]) { clearTimeout(polling[key]); polling[key] = null; }

  document.getElementById(tableId).innerHTML = '<div class="status"><span class="spinner"></span> Checking server cache...</div>';
  document.getElementById(refId).textContent = "Loading...";

  function attempt() {
    fetch("/" + day + "/" + market)
      .then(function(resp) {
        return resp.json().then(function(data) { return {status: resp.status, data: data}; });
      })
      .then(function(result) {
        if (result.status === 503) {
          document.getElementById(tableId).innerHTML = '<div class="status"><span class="spinner"></span> Server is still fetching data from CAISO... checking again in 10s</div>';
          polling[key] = setTimeout(attempt, 10000);
        } else {
          renderSectionData(day, market, result.data);
        }
      })
      .catch(function(e) {
        document.getElementById(tableId).innerHTML = '<div class="status">Error: ' + e.message + ' &nbsp;<button onclick="loadSection(\'' + day + '\',\'' + market + '\')">Retry</button></div>';
      });
  }

  attempt();
}

document.addEventListener("DOMContentLoaded", function() {
  loadSection("yesterday", "rtm");
  loadSection("yesterday", "hasp");
  loadSection("today",     "rtm");
  loadSection("today",     "hasp");

  // Auto-refresh today sections at 30 seconds past the top of each hour
  function scheduleRefresh() {
    var now  = nowPT();
    var next = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours()+1, 0, 30);
    setTimeout(function() {
      loadSection("today", "rtm");
      loadSection("today", "hasp");
      scheduleRefresh();
    }, next - now);
  }
  scheduleRefresh();
});
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8765)
