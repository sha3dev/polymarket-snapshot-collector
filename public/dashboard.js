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
  return Number(value).toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: ASSET_DECIMALS[asset] ?? 2 });
};

const formatContractValue = (value) => {
  if (value === null || value === undefined) {
    return "—";
  }
  return Number(value).toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 4 });
};

const formatAge = (value) => (value === null ? "no snapshot" : `${value} ms`);
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
  const marketSlug = widget.market ? widget.market.slug : "No stored market found";
  const marketStart = widget.market ? new Date(widget.market.marketStart).toLocaleTimeString() : "—";
  const marketEnd = widget.market ? new Date(widget.market.marketEnd).toLocaleTimeString() : "—";
  const snapshotCount = widget.market ? `${widget.snapshotCount} snapshots` : "";
  const latestSnapshotTime = latestSnapshot ? new Date(latestSnapshot.generatedAt).toLocaleTimeString() : "";
  return `<article class="card ${cardStateClass}">
    <div class="pair"><strong>${widget.asset.toUpperCase()} / ${widget.window}</strong><span class="badge ${badgeClass}">${badgeLabel}</span></div>
    <div class="slug">${marketSlug}</div>
    <div class="hero">
      <div class="metric"><span>Price To Beat</span><strong>${formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.priceToBeat : null)}</strong></div>
      <div class="metric"><span>Chainlink</span><strong>${formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.chainlinkPrice : null)}</strong></div>
    </div>
    <div class="table">
      <div><div class="row"><span>UP</span><span>${formatContractValue(latestSnapshot ? latestSnapshot.upPrice : null)}</span></div><div class="row"><span>DOWN</span><span>${formatContractValue(latestSnapshot ? latestSnapshot.downPrice : null)}</span></div><div class="row"><span>Binance</span><span>${formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.binancePrice : null)}</span></div><div class="row"><span>Coinbase</span><span>${formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.coinbasePrice : null)}</span></div></div>
      <div><div class="row"><span>Kraken</span><span>${formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.krakenPrice : null)}</span></div><div class="row"><span>OKX</span><span>${formatAssetValue(widget.asset, latestSnapshot ? latestSnapshot.okxPrice : null)}</span></div><div class="row"><span>Start</span><span>${marketStart}</span></div><div class="row"><span>End</span><span>${marketEnd}</span></div></div>
    </div>
    <div class="footer"><span>Last snapshot age: ${formatAge(latestSnapshotAgeMs)}</span><span>${snapshotCount}</span><span>${latestSnapshotTime}</span></div>
  </article>`;
};

const renderWindowSection = (windowLabel, widgets) => {
  const copy = windowLabel === "5m" ? "Fast market checks and short-range validation." : "Longer window monitoring for slower market turnover.";
  return `<section class="window-section"><div class="window-header"><div><h2 class="window-title">${windowLabel}</h2><div class="window-copy">${copy}</div></div><div class="window-copy">${widgets.length} widgets</div></div><div class="grid">${widgets.map(renderWidget).join("")}</div></section>`;
};

const renderDashboard = (payload) => {
  const metaSuffix = isDisconnected ? "Disconnected from service" : `Updated ${new Date(payload.generatedAt).toLocaleTimeString()}`;
  const heartbeat = lastSuccessfulRefreshAt ? ` · last success ${new Date(lastSuccessfulRefreshAt).toLocaleTimeString()}` : "";
  dashboardMeta.textContent = `${metaSuffix}${heartbeat}`;
  const widgets5m = payload.widgets.filter((widget) => widget.window === "5m");
  const widgets15m = payload.widgets.filter((widget) => widget.window === "15m");
  dashboardGrid.innerHTML = renderWindowSection("5m", widgets5m) + renderWindowSection("15m", widgets15m);
};

const refreshDashboard = async () => {
  try {
    const response = await fetch("/dashboard/state", { cache: "no-store" });
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
      dashboardMeta.textContent = "Disconnected from service";
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
