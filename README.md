# Stock Data Intelligence Dashboard

A mini financial data platform built with FastAPI. It fetches real OHLCV stock data from yfinance, computes derived metrics (daily return, 7-day MA, volatility), exposes a REST API, and serves an interactive HTML/Chart.js dashboard.

---
## Live Link [Important : Kindly Wait for 1min to load as it deploy on free server]

 Render : https://stock-data-dashboard-system.onrender.com
 
 Direct Dashboard Load: https://stock-data-dashboard-system.onrender.com/static/index.html
 
## Features

- **Data ingestion** from yfinance with retry/backoff
- **Computed metrics**: daily return, 7-day moving average, volatility score
- **REST API** (FastAPI + Swagger UI at `/docs`)
- **Interactive dashboard** — price charts, time-range filters, stock comparison, gainers/losers table
- **TTL cache** (300s) on all read endpoints
- **SQLite** (default) or **PostgreSQL** via env var

---
| |
|:---:|
| ![Dashboard](StocksDashboard.jpeg){width=30%,height=30%} |
| **Dashboard** |


## Quick Start

### 1. Clone and install

```bash
git clone <your-repo-url>
cd stock-data-dashboard
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env — see Environment Variables below
```

### 3. Run the server

```bash
uvicorn main:app --reload
```

The app starts at **http://localhost:8000**.

- Dashboard: http://localhost:8000/
- Swagger UI: http://localhost:8000/docs

On startup, data ingestion runs automatically in the background for the symbols listed in `SYMBOLS` (if `INGEST_ON_STARTUP=true`).

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./stocks.db` | SQLAlchemy connection string. Use `postgresql://user:pass@host/db` for PostgreSQL. |
| `SYMBOLS` | `INFY,TCS,RELIANCE,HDFCBANK,WIPRO` | Comma-separated list of NSE/BSE ticker symbols to ingest. |
| `INGEST_ON_STARTUP` | `true` | Set to `false` to skip automatic ingestion on startup. |
| `ALLOWED_ORIGINS` | `*` | Comma-separated CORS origins. Restrict in production (e.g. `https://yourdomain.com`). |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/companies` | List all available companies |
| `GET` | `/data/{symbol}?days=30` | Last N days of OHLCV + metrics for a symbol |
| `GET` | `/summary/{symbol}` | 52-week high/low, avg close, latest close |
| `GET` | `/compare?symbol1=X&symbol2=Y&days=30` | Side-by-side comparison + Pearson correlation |
| `GET` | `/gainers-losers?days=1&top_n=5` | Top gainers and losers by avg daily return |
| `POST` | `/ingest` | Manually trigger ingestion (body: `{"symbols": ["INFY", "TCS"]}`) |
| `GET` | `/docs` | Swagger UI |

---

## Triggering Data Ingestion

**Automatic** (on startup): set `INGEST_ON_STARTUP=true` in `.env`.

**Manual** via API:

```bash
# Ingest default symbols (from SYMBOLS env var)
curl -X POST http://localhost:8000/ingest

# Ingest specific symbols
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"symbols": ["INFY", "TCS", "RELIANCE"]}'
```

---

## Running Tests

```bash
pytest tests/ -v
```

The test suite uses an in-memory SQLite database — no external services required.

---

## Project Structure

```
.
├── main.py              # FastAPI app entry point
├── requirements.txt
├── .env.example
├── app/
│   ├── models.py        # SQLAlchemy ORM model
│   ├── database.py      # Engine, session, init_db
│   ├── schemas.py       # Pydantic response models
│   ├── processor.py     # DataProcessor (clean, enrich, metrics)
│   ├── ingestion.py     # IngestionService (yfinance + retry)
│   ├── repository.py    # StockRepository (DB queries)
│   ├── routes.py        # All API endpoints
│   └── cache.py         # TTLCache wrapper
├── static/
│   └── index.html       # Frontend dashboard
└── tests/
    ├── conftest.py
    ├── test_processor_clean.py
    ├── test_processor_enrich.py
    ├── test_properties.py       # hypothesis property-based tests
    ├── test_api_integration.py
    └── test_cache.py
```

---

## Key Design Decisions

- **Volatility score** (rolling 7-day std dev of daily returns) added as a custom metric beyond the assignment requirements.
- **Pearson correlation** between two stocks' daily returns exposed via `/compare`.
- **Cache invalidation** on ingestion ensures fresh data is served after each ingest cycle.
- **StaticPool** used in tests so all sessions share the same in-memory SQLite connection.

---

## Planned Features

These are features planned for future development:

### Data & Analytics
- **Real-time price streaming** — WebSocket-based live price updates instead of polling
- **Candlestick charts** — OHLC candlestick view alongside the current line chart
- **Technical indicators** — RSI, MACD, Bollinger Bands overlaid on price charts
- **Volume chart** — bar chart below price chart showing daily trading volume
- **Sector-wise grouping** — group companies by sector (IT, Banking, Energy, etc.) in the sidebar

### Predictions & ML
- **Multiple regression models** — compare Linear, Ridge, and LSTM predictions side by side
- **Confidence intervals** — show upper/lower prediction bands on the forecast chart
- **Backtesting** — evaluate how well past predictions matched actual prices
- **Anomaly detection** — flag unusual price spikes or drops automatically

### User Experience
- **Watchlist** — let users pin favourite stocks for quick access
- **Dark/light theme toggle** — switch between dark and light UI modes
- **Export to CSV** — download historical data for any symbol
- **Price alerts** — set threshold alerts that notify via browser notification
- **Responsive mobile layout** — fully optimised layout for phones and tablets

### Infrastructure
- **PostgreSQL migration guide** — step-by-step switch from SQLite to Postgres for production
- **Docker support** — `Dockerfile` and `docker-compose.yml` for one-command local setup
- **Scheduled auto-refresh** — cron-based ingestion to keep data up to date without manual triggers
- **Authentication** — basic API key or OAuth2 login to protect the dashboard
