/**
 * @section static:properties
 */

export class DashboardPageService {
  private static readonly HTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Polymarket Snapshot Dashboard</title>
    <style>
      :root { color-scheme: light; --bg:#f3efe4; --panel:#fffdf8; --ink:#17202a; --muted:#5c6773; --accent:#0f766e; --warn:#d97706; --danger:#b91c1c; --line:#d9d2c1; }
      * { box-sizing: border-box; }
      body { margin:0; font-family: "IBM Plex Sans", "Avenir Next", sans-serif; background: radial-gradient(circle at top left, #fff8e8, #f3efe4 55%); color:var(--ink); }
      main { padding:24px; }
      header { display:flex; justify-content:space-between; gap:16px; align-items:end; margin-bottom:20px; }
      h1 { margin:0; font-size:28px; letter-spacing:-0.04em; }
      .meta { color:var(--muted); font-size:14px; }
      .windows { display:grid; gap:22px; }
      .window-section { display:grid; gap:12px; }
      .window-header { display:flex; justify-content:space-between; align-items:end; gap:16px; padding-bottom:10px; border-bottom:2px solid #d8cfbd; }
      .window-title { margin:0; font-size:18px; letter-spacing:0.04em; text-transform:uppercase; }
      .window-copy { color:var(--muted); font-size:13px; }
      .grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(290px, 1fr)); gap:16px; }
      .card { background:var(--panel); border:1px solid var(--line); border-left:8px solid var(--accent); border-radius:18px; padding:18px; box-shadow:0 12px 30px rgba(23,32,42,0.08); }
      .card.is-stale { border-left-color:var(--warn); }
      .card.is-empty { border-left-color:var(--danger); }
      .pair { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; }
      .pair strong { font-size:20px; letter-spacing:-0.03em; }
      .badge { border-radius:999px; padding:6px 10px; font-size:12px; font-weight:700; letter-spacing:0.08em; }
      .badge.up { background:#dcfce7; color:#166534; }
      .badge.down { background:#fee2e2; color:#991b1b; }
      .badge.unknown { background:#e5e7eb; color:#374151; }
      .badge.disconnected { background:#fff7ed; color:#9a3412; }
      .slug { font-family:"IBM Plex Mono", monospace; font-size:12px; color:var(--muted); overflow-wrap:anywhere; margin-bottom:12px; min-height:34px; }
      .hero { display:grid; grid-template-columns:repeat(2, minmax(0, 1fr)); gap:10px; margin-bottom:12px; }
      .metric { background:#f8f4ea; border-radius:14px; padding:10px 12px; }
      .metric span { display:block; font-size:11px; text-transform:uppercase; letter-spacing:0.08em; color:var(--muted); margin-bottom:4px; }
      .metric strong { font-size:22px; }
      .table { display:grid; grid-template-columns:repeat(2, minmax(0, 1fr)); gap:8px; }
      .row { display:flex; justify-content:space-between; gap:8px; font-size:14px; padding:6px 0; border-bottom:1px solid #efe7d7; }
      .row:last-child { border-bottom:none; }
      .row span:first-child { color:var(--muted); }
      .footer { margin-top:12px; display:flex; justify-content:space-between; gap:12px; font-size:12px; color:var(--muted); }
    </style>
  </head>
  <body>
    <main>
      <header>
        <div>
          <h1>Snapshot Dashboard</h1>
          <div class="meta">Compare Polymarket UI against the last stored snapshot per asset/window.</div>
        </div>
        <div class="meta" id="dashboard-meta">Loading...</div>
      </header>
      <section class="windows" id="dashboard-grid"></section>
    </main>
    <script>
      const dashboardGrid = document.getElementById("dashboard-grid");
      const dashboardMeta = document.getElementById("dashboard-meta");
      let latestDashboardPayload = null;
      let lastSuccessfulRefreshAt = null;
      let isDisconnected = false;
      const ASSET_DECIMALS = { btc: 2, eth: 2, sol: 3, xrp: 5 };
      const formatAssetValue = (asset, value) => {
        if (value === null || value === undefined) {
          return "—";
        }
        return Number(value).toLocaleString("en-US", {
          minimumFractionDigits: 0,
          maximumFractionDigits: ASSET_DECIMALS[asset] ?? 2,
        });
      };
      const formatContractValue = (value) => {
        if (value === null || value === undefined) {
          return "—";
        }
        return Number(value).toLocaleString("en-US", {
          minimumFractionDigits: 0,
          maximumFractionDigits: 4,
        });
      };
      const formatAge = (value) => value === null ? "no snapshot" : value + " ms";
      const readClientSideSnapshotAge = (widget) => {
        const generatedAt = widget.latestSnapshot ? widget.latestSnapshot.generatedAt : null;
        return generatedAt === null ? null : Date.now() - generatedAt;
      };
      const renderWidget = (widget) => {
        const latestSnapshot = widget.latestSnapshot;
        const latestSnapshotAgeMs = readClientSideSnapshotAge(widget);
        const isStale = isDisconnected || latestSnapshotAgeMs === null || latestSnapshotAgeMs > 10000;
        const cardStateClass = widget.market ? (isStale ? "is-stale" : "") : "is-empty";
        const badgeClass = isDisconnected ? "disconnected" : widget.marketDirection === "UP" ? "up" : widget.marketDirection === "DOWN" ? "down" : "unknown";
        const badgeLabel = isDisconnected ? "DISCONNECTED" : widget.marketDirection;
        return '<article class="card ' + cardStateClass + '">' +
          '<div class="pair"><strong>' + widget.asset.toUpperCase() + ' / ' + widget.window + '</strong><span class="badge ' + badgeClass + '">' + badgeLabel + '</span></div>' +
          '<div class="slug">' + (widget.market ? widget.market.slug : 'No stored market found') + '</div>' +
          '<div class="hero">' +
            '<div class="metric"><span>Price To Beat</span><strong>' + formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.priceToBeat : null) + '</strong></div>' +
            '<div class="metric"><span>Chainlink</span><strong>' + formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.chainlinkPrice : null) + '</strong></div>' +
          '</div>' +
          '<div class="table">' +
            '<div><div class="row"><span>UP</span><span>' + formatContractValue(latestSnapshot ? latestSnapshot.upPrice : null) + '</span></div><div class="row"><span>DOWN</span><span>' + formatContractValue(latestSnapshot ? latestSnapshot.downPrice : null) + '</span></div><div class="row"><span>Binance</span><span>' + formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.binancePrice : null) + '</span></div><div class="row"><span>Coinbase</span><span>' + formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.coinbasePrice : null) + '</span></div></div>' +
            '<div><div class="row"><span>Kraken</span><span>' + formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.krakenPrice : null) + '</span></div><div class="row"><span>OKX</span><span>' + formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.okxPrice : null) + '</span></div><div class="row"><span>Start</span><span>' + (widget.market ? new Date(widget.market.marketStart).toLocaleTimeString() : '—') + '</span></div><div class="row"><span>End</span><span>' + (widget.market ? new Date(widget.market.marketEnd).toLocaleTimeString() : '—') + '</span></div></div>' +
          '</div>' +
          '<div class="footer"><span>Last snapshot age: ' + formatAge(latestSnapshotAgeMs) + '</span><span>' + (widget.market ? widget.snapshotCount + ' snapshots' : '') + '</span><span>' + (latestSnapshot ? new Date(latestSnapshot.generatedAt).toLocaleTimeString() : '') + '</span></div>' +
        '</article>';
      };
      const renderWindowSection = (windowLabel, widgets) => {
        const copy = windowLabel === "5m" ? "Fast market checks and short-range validation." : "Longer window monitoring for slower market turnover.";
        return '<section class="window-section"><div class="window-header"><div><h2 class="window-title">' + windowLabel + '</h2><div class="window-copy">' + copy + '</div></div><div class="window-copy">' + widgets.length + ' widgets</div></div><div class="grid">' + widgets.map(renderWidget).join('') + '</div></section>';
      };
      const renderDashboard = (payload) => {
        const metaSuffix = isDisconnected ? 'Disconnected from service' : 'Updated ' + new Date(payload.generatedAt).toLocaleTimeString();
        const heartbeat = lastSuccessfulRefreshAt ? ' · last success ' + new Date(lastSuccessfulRefreshAt).toLocaleTimeString() : '';
        dashboardMeta.textContent = metaSuffix + heartbeat;
        const widgets5m = payload.widgets.filter((widget) => widget.window === "5m");
        const widgets15m = payload.widgets.filter((widget) => widget.window === "15m");
        dashboardGrid.innerHTML = renderWindowSection("5m", widgets5m) + renderWindowSection("15m", widgets15m);
      };
      const refreshDashboard = async () => {
        try {
          const response = await fetch('/dashboard/state', { cache: 'no-store' });
          const payload = await response.json();
          latestDashboardPayload = payload;
          lastSuccessfulRefreshAt = Date.now();
          isDisconnected = false;
          renderDashboard(payload);
        } catch {
          isDisconnected = true;
          if (latestDashboardPayload) {
            renderDashboard(latestDashboardPayload);
          } else {
            dashboardMeta.textContent = 'Disconnected from service';
          }
        }
      };
      refreshDashboard();
      setInterval(() => {
        if (latestDashboardPayload) {
          renderDashboard(latestDashboardPayload);
        }
        void refreshDashboard();
      }, 1000);
    </script>
  </body>
</html>`;

  /**
   * @section public:methods
   */

  public renderPage(): string {
    return DashboardPageService.HTML;
  }
}
