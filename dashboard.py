"""Local read-only NYC taxi dashboard; supports existing pipeline databases."""
import argparse
import csv
import json
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

# Infer type from original fields, including records inserted before this update.
TAXI_TYPE_SQL = """CASE
 WHEN json_type(payload, '$.lpep_pickup_datetime') IS NOT NULL THEN 'green'
 WHEN json_type(payload, '$.tpep_pickup_datetime') IS NOT NULL THEN 'yellow'
 ELSE 'unknown' END"""
WINDOW_SQL = "substr(pickup,1,14) || printf('%02d',CAST(substr(pickup,15,2) AS INTEGER)/5*5) || ':00'"


def load_zones(path):
    path = Path(path)
    if not path.is_file():
        return {}
    with path.open(newline='', encoding='utf-8-sig') as file:
        return {str(int(r['LocationID'])): (r['Zone'], r['Borough'])
                for r in csv.DictReader(file)}


def area_label(area, zones):
    if area.startswith('zone:'):
        number = area.split(':', 1)[1]
        if number in zones:
            name, borough = zones[number]
            return f'{name} ({borough}) · zone {number}'
        return f'Taxi zone {number}'
    return area


TAXI_TYPES = ('all', 'yellow', 'green', 'unknown')
PAYMENT_LABELS = {None: 'Missing payment type', 0: 'Flex Fare', 1: 'Credit card',
                  2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown code',
                  6: 'Voided trip'}


def open_readonly(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro',
                           uri=True, timeout=30)


def build_snapshot(path, zones=None, on_progress=None, month='2026-08'):
    """One bounded-memory scan per snapshot; HTTP requests reuse its results."""
    zones = zones or {}
    if len(month) != 7 or datetime.strptime(month, '%Y-%m').strftime('%Y-%m') != month:
        raise ValueError('Month must use YYYY-MM format')
    started = time.monotonic()
    buckets = {name: dict(trips=0, total=0.0, distance=0.0, first=None,
                          last=None, areas={}, windows={}, daily={}, payments={})
               for name in TAXI_TYPES}
    monthly = {name: dict(trips=0, total=0.0, distance=0.0, first=None,
                          last=None, areas={}, windows={}, daily={}, payments={})
               for name in TAXI_TYPES}
    db = open_readonly(path)
    try:
        db.execute('BEGIN')  # One consistent SQLite read snapshot.
        cursor = db.execute(f'SELECT {TAXI_TYPE_SQL},pickup,pickup_area,'
                            'total,distance,payment_type FROM trips')
        processed = 0
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            for kind, pickup, area, total, distance, payment in rows:
                minute = int(pickup[14:16]) // 5 * 5
                window = pickup[:14] + f'{minute:02d}:00'
                day = pickup[:10]
                targets = [buckets['all'], buckets[kind]]
                if pickup[:7] == month:
                    targets.extend([monthly['all'], monthly[kind]])
                for bucket in targets:
                    bucket['trips'] += 1
                    bucket['total'] += total
                    bucket['distance'] += distance
                    if bucket['first'] is None or pickup < bucket['first']:
                        bucket['first'] = pickup
                    if bucket['last'] is None or pickup > bucket['last']:
                        bucket['last'] = pickup
                    area_stats = bucket['areas'].setdefault(area, [0, 0.0])
                    area_stats[0] += 1
                    area_stats[1] += total
                    bucket['windows'][window] = bucket['windows'].get(window, 0) + 1
                    bucket['daily'][day] = bucket['daily'].get(day, 0) + 1
                    bucket['payments'][payment] = bucket['payments'].get(payment, 0) + 1
            processed += len(rows)
            if on_progress:
                on_progress(processed)
        reasons = db.execute('SELECT reason,count(*) FROM rejected GROUP BY reason '
                             'ORDER BY count(*) DESC,reason').fetchall()
        metrics = dict(db.execute('SELECT name,value FROM metrics'))
    finally:
        db.close()
    built_at = datetime.now().astimezone().isoformat(timespec='seconds')
    duration = round(time.monotonic() - started, 2)
    by_scope = {}
    for scope, selected_buckets in (('all', buckets), ('month', monthly)):
        types = {name: selected_buckets[name]['trips'] for name in TAXI_TYPES if name != 'all'}
        result = {}
        for name, bucket in selected_buckets.items():
            areas = sorted(bucket['areas'].items(), key=lambda x: (-x[1][0], x[0]))[:10]
            windows = sorted(bucket['windows'].items())[-48:]
            daily = sorted(bucket['daily'].items())[-90:]
            payments = sorted(bucket['payments'].items(),
                              key=lambda x: (x[0] is None, x[0] if x[0] is not None else 0))
            result[name] = dict(
                trips=bucket['trips'], total=bucket['total'],
                average_distance=(bucket['distance'] / bucket['trips']
                                  if bucket['trips'] else 0),
                rejected=sum(n for _, n in reasons), rejection_reasons=reasons,
                duplicates=metrics.get('duplicate_deliveries', 0),
                areas=[[area_label(a, zones), n, t] for a, (n, t) in areas],
                windows=windows, daily=daily,
                payments=[[PAYMENT_LABELS.get(code, f'Code {code}'), n]
                          for code, n in payments],
                types=types, taxi_type=name, first=bucket['first'], last=bucket['last'],
                zones_loaded=bool(zones), db=str(path), snapshot_at=built_at,
                build_seconds=duration, date_scope=scope, pickup_month=month,
                source_trips=buckets['all']['trips'],
                outside_month=buckets['all']['trips'] - monthly['all']['trips'])
        by_scope[scope] = result
    return {**by_scope['all'], 'month': by_scope['month']}


def stats(path, taxi_type='all', zones=None):
    # Compatibility helper for scripts. The HTTP server uses SnapshotCache.
    if taxi_type not in TAXI_TYPES:
        raise ValueError('Invalid taxi type')
    return build_snapshot(path, zones)[taxi_type]


class SnapshotCache:
    """Atomic snapshots. A background thread owns its SQLite monitor connection."""
    def __init__(self, path, zones=None, check_every=60, month='2026-08'):
        self.path, self.zones, self.check_every = path, zones or {}, check_every
        self.month = month
        self.lock = threading.Lock()
        self.snapshots = None
        self.building = False
        self.processed = 0
        self.error = None
        self.stopped = threading.Event()
        self.refresh_requested = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def request_refresh(self):
        # Coalesce repeated requests; never run multiple scans concurrently.
        with self.lock:
            if self.building:
                return
            self.refresh_requested.set()

    def _progress(self, processed):
        with self.lock:
            self.processed = processed
        if processed % 250000 == 0:
            print(f'Dashboard snapshot: {processed:,} trips scanned', flush=True)

    def _run(self):
        monitor = None
        version = None
        first = True
        manual = False
        try:
            monitor = open_readonly(self.path)
            while not self.stopped.is_set():
                self.refresh_requested.clear()
                current = monitor.execute('PRAGMA data_version').fetchone()[0]
                with self.lock:
                    needed = first or self.snapshots is None or current != version
                # A manual request or a source change triggers a fresh snapshot.
                if needed or manual:
                    with self.lock:
                        self.building, self.processed = True, 0
                        self.error = None
                    print('Building dashboard snapshot; source database is read-only.', flush=True)
                    try:
                        snapshots = build_snapshot(self.path, self.zones, self._progress, self.month)
                    except Exception as exc:
                        with self.lock:
                            self.error = str(exc)
                        print(f'Dashboard snapshot failed: {exc}', flush=True)
                    else:
                        with self.lock:
                            self.snapshots = snapshots
                        version = current
                        print(f"Dashboard snapshot ready: {snapshots['all']['trips']:,} "
                              f"trips in {snapshots['all']['build_seconds']:.2f}s", flush=True)
                    finally:
                        with self.lock:
                            self.building = False
                first = False
                manual = self.refresh_requested.wait(self.check_every)
        except Exception as exc:
            with self.lock:
                self.error, self.building = str(exc), False
        finally:
            if monitor is not None:
                monitor.close()

    def get(self, taxi_type, date_scope='all'):
        if taxi_type not in TAXI_TYPES:
            raise ValueError('Invalid taxi type')
        if date_scope not in ('all', 'month'):
            raise ValueError('Invalid date scope')
        with self.lock:
            state = dict(building=self.building, scanned=self.processed,
                         snapshot_error=self.error)
            if self.snapshots is None:
                return None, state
            selected = self.snapshots if date_scope == 'all' else self.snapshots['month']
            return {**selected[taxi_type], **state}, state

    def close(self):
        self.stopped.set()
        self.refresh_requested.set()
        # Current read-only scan may finish; daemon thread never delays exit.
        self.thread.join(timeout=1)


PAGE = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>NYC Taxi Analytics</title>
<style>body{font:16px system-ui;background:#0c1626;color:#e9eff9;margin:36px auto;max-width:1100px;padding:0 20px}h1{font-size:34px}.cards{display:flex;gap:16px;flex-wrap:wrap}.card{background:#192940;padding:22px;border-radius:12px;flex:1;min-width:180px}.value{font-size:28px;font-weight:700}.muted,small{color:#a9bad0}small{display:block;margin-top:8px}select{background:#192940;color:#e9eff9;border:1px solid #526880;border-radius:6px;padding:10px;font:inherit}button{background:#244366;color:#e9eff9;border:1px solid #526880;border-radius:6px;padding:10px;font:inherit;cursor:pointer}button:disabled{opacity:.5;cursor:wait}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:10px;border-bottom:1px solid #31445c}svg{width:100%;background:#192940;border-radius:12px}footer{margin:24px 0}.table-wrap{overflow:auto}</style></head><body>
<h1>NYC Taxi Analytics</h1><p class="muted">Historical trip replay · five-minute pickup windows · cached snapshot · status refreshes every 5 seconds</p>
<p><label for="type">Taxi type: </label><select id="type"><option value="all">All taxis</option><option value="yellow">Yellow</option><option value="green">Green</option><option value="unknown">Unknown type</option></select> <button id="rebuild" type="button">Refresh data</button></p>
<p id="cache-note" class="muted">Preparing dashboard data…</p>
<p><label for="scope">Pickup dates: </label><select id="scope"><option value="month">Configured month only</option><option value="all">All source dates</option></select></p>
<p id="outliers" class="muted"></p>
<p id="coverage" class="muted"></p>
<div class="cards"><div class="card">Unique trips<div id="trips" class="value">—</div><small>Selected taxi type and date scope</small></div><div class="card">Recorded total<div id="total" class="value">—</div><small>Selected taxi type and date scope</small></div><div class="card">Rejected events<div id="rejected" class="value">—</div><small>Whole database · unique rejected events</small></div><div class="card">Duplicate deliveries<div id="duplicates" class="value">—</div><small>Whole database · repeat deliveries</small></div></div>
<h2>Trip trends</h2><p><label for="interval">View: </label><select id="interval"><option value="daily">Daily trips</option><option value="windows">Five-minute windows</option></select></p><p id="chart-note" class="muted"></p><svg id="chart" viewBox="0 0 960 275" role="img" aria-label="Trip counts by pickup date or five-minute window"></svg>
<h2>Pickup areas</h2><p id="zone-note" class="muted"></p><div class="table-wrap"><table><thead><tr><th>Pickup area</th><th>Trips</th><th>Recorded total</th></tr></thead><tbody id="areas"></tbody></table></div>
<h2>Payment types</h2><p class="muted">Selected taxi type. Missing values are shown separately from code 0 (Flex Fare).</p><div class="table-wrap"><table><thead><tr><th>Payment type</th><th>Trips</th></tr></thead><tbody id="payments"></tbody></table></div>
<p id="quality" class="muted"></p>
<footer id="status" class="muted" aria-live="polite"></footer><script>
const money=x=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(x);
const number=x=>new Intl.NumberFormat('en-US').format(x);
const ns='http://www.w3.org/2000/svg';
function node(tag,attrs,text){let e=document.createElementNS(ns,tag);for(let [k,v] of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e}
let busy=false;
async function refresh(){if(busy)return;busy=true;const selected=document.getElementById('type').value;const scope=document.getElementById('scope').value;try{
let response=await fetch('/api/stats?taxi_type='+encodeURIComponent(selected)+'&date_scope='+encodeURIComponent(scope),{cache:'no-store'});let d=await response.json();if(!response.ok)throw Error(d.error||'Request failed');if(selected!==document.getElementById('type').value||scope!==document.getElementById('scope').value)return;
for(let [id,val] of Object.entries({trips:number(d.trips),total:money(d.total),rejected:number(d.rejected),duplicates:number(d.duplicates)}))document.getElementById(id).textContent=val;
document.querySelector('#scope option[value=month]').textContent=d.pickup_month+' only';
document.getElementById('outliers').textContent='Full source: '+number(d.source_trips)+' accepted trips · Outside '+d.pickup_month+': '+number(d.outside_month)+(scope==='month'?' (excluded from this view; retained in the database).':' (included in this view).');
document.getElementById('coverage').textContent='Trips in selected date scope: Yellow '+number(d.types.yellow||0)+' · Green '+number(d.types.green||0)+' · Unknown '+number(d.types.unknown||0)+(d.first?' | Selected pickup range: '+d.first+' to '+d.last:' | No trips for this selection');
document.getElementById('zone-note').textContent=d.zones_loaded?'Zone names use the TLC lookup table. Coordinate grids, when present, are approximate areas.':'Taxi zone IDs shown. Add taxi_zone_lookup.csv beside dashboard.py to display zone names. Coordinate grids are approximate areas.';
let tbody=document.getElementById('areas');tbody.replaceChildren();for(let a of d.areas){let tr=document.createElement('tr');for(let val of [a[0],number(a[1]),money(a[2])]){let td=document.createElement('td');td.textContent=val;tr.appendChild(td)}tbody.appendChild(tr)}
let interval=document.getElementById('interval').value;let series=interval==='daily'?d.daily:d.windows;
document.getElementById('chart-note').textContent=interval==='daily'?'Latest 90 populated pickup dates for this selection. Bars represent dates with trips.':'Latest 48 populated five-minute windows. Bars represent populated windows, not equal elapsed-time spacing. Timestamps use NYC local time.';
let svg=document.getElementById('chart');svg.replaceChildren();let n=series.length,max=Math.max(1,...series.map(x=>x[1]));if(!n)svg.appendChild(node('text',{x:30,y:70,fill:'#b9cce4'},'No trips for this selection'));
series.forEach((v,i)=>{let h=v[1]/max*175,x=25+i*910/n;let r=node('rect',{x,y:200-h,width:Math.max(1,910/n-3),height:h,fill:selected==='green'?'#55c791':selected==='yellow'?'#efc654':'#52b8ef'});r.appendChild(node('title',{},v[0]+' NYC local: '+number(v[1])+' trips'));svg.appendChild(r);if(i%Math.max(1,Math.ceil(n/6))===0){svg.appendChild(node('text',{x,y:226,fill:'#b9cce4','font-size':12},v[0].slice(0,10)));if(interval==='windows')svg.appendChild(node('text',{x,y:246,fill:'#b9cce4','font-size':12},v[0].slice(11,16)))}});
let payments=document.getElementById('payments');payments.replaceChildren();for(let a of d.payments){let tr=document.createElement('tr');for(let val of [a[0],number(a[1])]){let td=document.createElement('td');td.textContent=val;tr.appendChild(td)}payments.appendChild(tr)}
document.getElementById('quality').textContent='Rejection reasons (whole database): '+(d.rejection_reasons.map(x=>x[0]+': '+number(x[1])).join(' · ')||'None');
document.getElementById('cache-note').textContent='Data snapshot: '+new Date(d.snapshot_at).toLocaleString()+(d.building?' · Rebuilding: '+number(d.scanned)+' trips scanned. Showing the previous snapshot.': ' · Source changes checked every 60 seconds.')+(d.snapshot_error?' · Refresh failed: '+d.snapshot_error:'');
document.getElementById('rebuild').disabled=d.building;
document.getElementById('status').textContent='Status checked '+new Date().toLocaleTimeString()+' · Snapshot built in '+d.build_seconds+'s · '+d.db;
}catch(e){document.getElementById('status').textContent=e.message}finally{busy=false}}
document.getElementById('type').addEventListener('change',()=>{if(!busy)refresh()});
document.getElementById('interval').addEventListener('change',refresh);
document.getElementById('scope').addEventListener('change',refresh);
document.getElementById('rebuild').addEventListener('click',async()=>{document.getElementById('rebuild').disabled=true;try{let r=await fetch('/api/refresh',{method:'POST'});if(!r.ok)throw Error('Refresh request failed');document.getElementById('cache-note').textContent='Data refresh requested…';}catch(e){document.getElementById('status').textContent=e.message}finally{refresh()}});
refresh();setInterval(refresh,5000);
</script></body></html>'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='output/stream-full-202608.db')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--zones', default=str(Path(__file__).with_name('taxi_zone_lookup.csv')))
    parser.add_argument('--month', default='2026-08', help='Default pickup month, YYYY-MM')
    args = parser.parse_args()
    try:
        if len(args.month) != 7 or datetime.strptime(args.month, '%Y-%m').strftime('%Y-%m') != args.month:
            raise ValueError()
    except ValueError:
        parser.error('month must use YYYY-MM format')
    if not Path(args.db).is_file():
        parser.error(f'Database does not exist: {args.db}')
    zones = load_zones(args.zones)
    cache = SnapshotCache(args.db, zones, month=args.month)
    cache.start()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if urlsplit(self.path).path != '/api/refresh':
                self.send_error(404)
                return
            cache.request_refresh()
            body = b'{"status":"refresh_requested"}'
            self.send_response(202)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlsplit(self.path)
            code = 200
            if url.path == '/':
                body, mime = PAGE.encode(), 'text/html; charset=utf-8'
            elif url.path == '/api/stats':
                try:
                    selected = parse_qs(url.query).get('taxi_type', ['all'])[0]
                    date_scope = parse_qs(url.query).get('date_scope', ['month'])[0]
                    data, state = cache.get(selected, date_scope)
                    if data is None:
                        code = 503
                        data = {'error': (state['snapshot_error'] or
                                f"Preparing snapshot: {state['scanned']:,} trips scanned. Please wait.")}
                    body = json.dumps(data, allow_nan=False).encode()
                except ValueError as exc:
                    code, body = 400, json.dumps({'error': str(exc)}).encode()
                except sqlite3.Error:
                    code, body = 500, json.dumps({'error': 'Unable to read database; check schema and path.'}).encode()
                mime = 'application/json'
            else:
                self.send_error(404)
                return
            self.send_response(code)
            self.send_header('Content-Type', mime)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    print(f'http://localhost:{args.port}', flush=True)
    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        cache.close()


if __name__ == '__main__':
    main()
