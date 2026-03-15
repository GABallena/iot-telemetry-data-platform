
from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.log import get_logger
from src.storage.client import get_storage_client

logger = get_logger("dashboard")

DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8050"))
TEMPLATE_DIR = Path(__file__).parent / "templates"


def _load_json(key: str) -> dict | list | None:
    s3 = get_storage_client()
    if not s3.exists(key):
        return None
    return json.loads(s3.get_object(key).decode())


def _load_parquet_as_dicts(key: str) -> list[dict]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    s3 = get_storage_client()
    if not s3.exists(key):
        return []
    data = s3.get_object(key)
    table = pq.ParquetFile(pa.BufferReader(data)).read()
    rows = []
    for i in range(len(table)):
        row = {}
        for col in table.column_names:
            val = table.column(col)[i].as_py()
            row[col] = val
        rows.append(row)
    return rows


def get_downtime_data() -> list[dict]:
    return _load_parquet_as_dicts("analytics/factory_downtime_metrics.parquet")


def get_machine_health_data() -> list[dict]:
    return _load_parquet_as_dicts("analytics/machine_health_features.parquet")


def get_scrap_data() -> list[dict]:
    return _load_parquet_as_dicts("analytics/scrap_rate_metrics.parquet")


def get_telemetry_status() -> list[dict]:
    from datetime import UTC, datetime

    import pyarrow as pa
    import pyarrow.parquet as pq

    s3 = get_storage_client()
    machines = _load_parquet_as_dicts("staging/machines/machines.parquet")
    if not s3.exists("staging/telemetry/telemetry.parquet"):
        return [{"machine_id": m["machine_id"], "status": "no_data"} for m in machines]

    telemetry_data = s3.get_object("staging/telemetry/telemetry.parquet")
    telemetry = pq.ParquetFile(pa.BufferReader(telemetry_data)).read()

    latest: dict[str, datetime] = {}
    for i in range(len(telemetry)):
        mid = telemetry.column("machine_id")[i].as_py()
        ts = telemetry.column("timestamp")[i].as_py()
        if mid not in latest or ts > latest[mid]:
            latest[mid] = ts

    now = datetime.now(UTC)
    results = []
    for m in machines:
        mid = m["machine_id"]
        if mid in latest:
            last = latest[mid]
            if last.tzinfo is None:
                from datetime import timezone
                last = last.replace(tzinfo=timezone.utc)
            hours_since = (now - last).total_seconds() / 3600
            results.append({
                "machine_id": mid,
                "last_event": last.isoformat(),
                "hours_since": round(hours_since, 1),
                "status": "silent" if hours_since > 1 else "active",
            })
        else:
            results.append({"machine_id": mid, "last_event": None, "status": "no_data"})

    return results


def get_alerts() -> dict:
    return _load_json("metadata/alerts/latest.json") or {"alerts": [], "total_alerts": 0}


def _render_index() -> str:
    downtime = get_downtime_data()
    health = get_machine_health_data()
    scrap = get_scrap_data()
    tel_status = get_telemetry_status()
    alerts = get_alerts()

    downtime.sort(key=lambda r: r.get("total_downtime_minutes", 0), reverse=True)
    health.sort(key=lambda r: r.get("max_temperature_c", 0) or 0, reverse=True)
    scrap.sort(key=lambda r: r.get("scrap_rate", 0) or 0, reverse=True)

    abnormal_machines = [h for h in health if (h.get("max_temperature_c") or 0) > 85]
    silent_machines = [t for t in tel_status if t["status"] != "active"]
    high_scrap = [s for s in scrap if (s.get("scrap_rate") or 0) > 0.05]

    alert_list = alerts.get("alerts", [])

    def _dt_rows():
        rows = []
        for d in downtime:
            rows.append(f"""<tr>
                <td>{d.get('factory_id','')}</td>
                <td>{d.get('total_downtime_minutes',0):,}</td>
                <td>{d.get('avg_downtime_minutes',0):.1f}</td>
                <td>{d.get('downtime_event_count',0)}</td>
            </tr>""")
        return "\n".join(rows)

    def _health_rows():
        rows = []
        for h in abnormal_machines[:10]:
            max_t = h.get("max_temperature_c") or 0
            cls = "danger" if max_t > 90 else "warn"
            rows.append(f"""<tr>
                <td>{h.get('machine_id','')}</td>
                <td class="{cls}">{max_t:.1f}°C</td>
                <td>{h.get('avg_temperature_c',0):.1f}°C</td>
                <td>{h.get('max_vibration_mm_s',0):.2f}</td>
                <td>{h.get('event_count',0)}</td>
            </tr>""")
        return "\n".join(rows)

    def _scrap_rows():
        rows = []
        for s in high_scrap[:10]:
            rows.append(f"""<tr>
                <td>{s.get('machine_id','')}</td>
                <td class="warn">{(s.get('scrap_rate',0))*100:.1f}%</td>
                <td>{s.get('total_scrap_units',0):,}</td>
                <td>{s.get('total_units_produced',0):,}</td>
            </tr>""")
        return "\n".join(rows)

    def _telemetry_rows():
        rows = []
        for t in silent_machines[:10]:
            rows.append(f"""<tr>
                <td>{t.get('machine_id','')}</td>
                <td class="warn">{t.get('status','')}</td>
                <td>{t.get('last_event','never')}</td>
                <td>{t.get('hours_since','—')}</td>
            </tr>""")
        return "\n".join(rows)

    def _alert_rows():
        rows = []
        for a in alert_list:
            sev = a.get("severity", "info")
            cls = "danger" if sev == "critical" else "warn" if sev == "warning" else ""
            rows.append(f"""<tr>
                <td class="{cls}">{sev.upper()}</td>
                <td>{a.get('alert_type','')}</td>
                <td>{a.get('message','')}</td>
            </tr>""")
        return "\n".join(rows)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Telemetry Pipeline Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background:
  h1 {{ color:
  h2 {{ color:
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 1.2rem; margin-top: 1rem; }}
  .card {{ background:
  .summary {{ display: flex; gap: 1rem; margin: 0.8rem 0; flex-wrap: wrap; }}
  .stat {{ background:
  .stat .value {{ font-size: 1.5rem; font-weight: bold; color:
  .stat .label {{ font-size: 0.75rem; color:
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; margin-top: 0.5rem; }}
  th {{ text-align: left; color:
  td {{ padding: 0.4rem 0.6rem; border-bottom: 1px solid
  .warn {{ color:
  .danger {{ color:
  .ok {{ color:
  .footer {{ margin-top: 2rem; text-align: center; color:
</style>
</head>
<body>
<h1>Manufacturing IoT — Telemetry Dashboard</h1>

<div class="summary">
  <div class="stat"><div class="value">{len(downtime)}</div><div class="label">Factories</div></div>
  <div class="stat"><div class="value">{len(health)}</div><div class="label">Machines</div></div>
  <div class="stat"><div class="value {'danger' if len(abnormal_machines) > 5 else 'warn' if abnormal_machines else 'ok'}">{len(abnormal_machines)}</div><div class="label">Abnormal</div></div>
  <div class="stat"><div class="value {'warn' if silent_machines else 'ok'}">{len(silent_machines)}</div><div class="label">Silent</div></div>
  <div class="stat"><div class="value {'warn' if high_scrap else 'ok'}">{len(high_scrap)}</div><div class="label">High Scrap</div></div>
  <div class="stat"><div class="value {'danger' if alerts.get('critical',0) else 'warn' if alerts.get('warnings',0) else 'ok'}">{alerts.get('total_alerts',0)}</div><div class="label">Alerts</div></div>
</div>

<div class="grid">

<div class="card">
<h2>Downtime by Factory</h2>
<table>
<tr><th>Factory</th><th>Total (min)</th><th>Avg (min)</th><th>Events</th></tr>
{_dt_rows()}
</table>
</div>

<div class="card">
<h2>Abnormal Machines (Health)</h2>
<table>
<tr><th>Machine</th><th>Max Temp</th><th>Avg Temp</th><th>Max Vib</th><th>Events</th></tr>
{_health_rows()}
</table>
{f'<p style="margin-top:0.5rem;color:#8b949e">{len(abnormal_machines)} machine(s) &gt; 85°C threshold</p>' if abnormal_machines else '<p class="ok" style="margin-top:0.5rem">All machines within normal range</p>'}
</div>

<div class="card">
<h2>Scrap Rate — High Risk</h2>
<table>
<tr><th>Machine</th><th>Rate</th><th>Scrap Units</th><th>Total Units</th></tr>
{_scrap_rows()}
</table>
{f'<p style="margin-top:0.5rem;color:#8b949e">{len(high_scrap)} machine(s) &gt; 5% threshold</p>' if high_scrap else '<p class="ok" style="margin-top:0.5rem">All scrap rates normal</p>'}
</div>

<div class="card">
<h2>Missing Telemetry</h2>
<table>
<tr><th>Machine</th><th>Status</th><th>Last Event</th><th>Hours Ago</th></tr>
{_telemetry_rows()}
</table>
{f'<p style="margin-top:0.5rem" class="warn">{len(silent_machines)} machine(s) silent or no data</p>' if silent_machines else '<p class="ok" style="margin-top:0.5rem">All machines reporting</p>'}
</div>

<div class="card" style="grid-column: 1 / -1;">
<h2>Active Alerts</h2>
<table>
<tr><th>Severity</th><th>Type</th><th>Message</th></tr>
{_alert_rows()}
</table>
{f'<p style="margin-top:0.5rem;color:#8b949e">{len(alert_list)} alert(s) — {alerts.get("critical",0)} critical, {alerts.get("warnings",0)} warning(s)</p>' if alert_list else '<p class="ok" style="margin-top:0.5rem">No active alerts</p>'}
</div>

</div>

<div class="footer">Telemetry Pipeline Dashboard — data refreshed on page load</div>
</body>
</html>"""
    return html


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path

        routes = {
            "/api/downtime": get_downtime_data,
            "/api/machines": get_machine_health_data,
            "/api/scrap": get_scrap_data,
            "/api/telemetry-status": get_telemetry_status,
            "/api/alerts": get_alerts,
        }

        if path in routes:
            data = routes[path]()
            self._json_response(data)
        elif path == "/" or path == "/index.html":
            self._html_response(_render_index())
        else:
            self.send_error(404)

    def _json_response(self, data):
        body = json.dumps(data, indent=2, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html_response(self, html: str):
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        logger.debug("%s %s", self.address_string(), fmt % args)


def serve():
    server = HTTPServer(("0.0.0.0", DASHBOARD_PORT), DashboardHandler)
    logger.info("Dashboard running at http://localhost:%d", DASHBOARD_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Dashboard stopped")
        server.server_close()


if __name__ == "__main__":
    serve()
