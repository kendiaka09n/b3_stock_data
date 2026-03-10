# B3 Stock Data — Delta Lake Lakehouse

A data engineering project that evolves a simple stock ingestion script
into a production-grade Delta Lake lakehouse, deployed from local Docker
to Azure Databricks with Terraform.

## What it does

Downloads daily OHLCV data for 8 major B3 (Brazilian stock exchange) tickers
via `yfinance`, stores it as Parquet, and progressively builds a full lakehouse
architecture on top.

**Tickers tracked:** PETR4, VALE3, BBAS3, BBDC4, ITUB4, ITSA4, ABEV3, IBOVESPA

---

## Architecture

### Phase 1 — Local (Docker)
```
yfinance → Python ingestion → Parquet → MinIO
                                           ↓
                             Spark → Delta tables
                                           ↓
                                  JupyterLab queries
```

### Phase 2 — Cloud (Azure)
```
yfinance → Python ingestion → Parquet → ADLS Gen2
                                           ↓
                              Databricks → Delta tables
                                           ↓
                                  Databricks SQL
```

---

## Tech Stack

| Layer | Local | Cloud |
|---|---|---|
| Storage | MinIO | Azure ADLS Gen2 |
| Table format | Delta Lake | Delta Lake |
| Compute | Apache Spark | Azure Databricks |
| Query | DuckDB / JupyterLab | Databricks SQL |
| IaC | — | Terraform |
| Orchestration | Docker Compose | Databricks Jobs |

---

## Project Status

See [ROADMAP.md](ROADMAP.md) for detailed phases and progress.

- [x] Phase 1.1 — Parquet ingestion with data quality checks
- [ ] Phase 1.2 — MinIO + Delta Lake (local object storage)
- [ ] Phase 1.3 — Spark + JupyterLab
- [ ] Phase 1.4 — Delta features (time travel, schema evolution, OPTIMIZE)
- [ ] Phase 1.5 — Full pipeline automation
- [ ] Phase 2.x — Terraform + Azure + Databricks

---

## Getting Started

### Requirements
- Docker + Docker Compose
- [uv](https://github.com/astral-sh/uv)

### Setup

```bash
# Clone the repo
git clone https://github.com/your-username/b3_stock_data.git
cd b3_stock_data

# Copy and fill environment variables
cp .env.example .env

# Run ingestion locally
uv run python ingestion/b3_stocks.py
```

### Environment Variables

See [.env.example](.env.example) for all required variables.

```env
DATA_INICIAL=2020-01-01
DATA_FINAL=2025-12-31
ACAO=PETR4.SA,VALE3.SA,BBAS3.SA,...
```

---

## Project Structure

```
b3_stock_data/
├── ingestion/
│   └── b3_stocks.py      ← data extraction (yfinance → Parquet)
├── notebooks/            ← exploratory analysis (Phase 1.3+)
├── terraform/            ← Azure infrastructure as code (Phase 2+)
├── guides/               ← learning notes per phase
├── docker/
│   └── Dockerfile
├── docker-compose.yaml
├── pyproject.toml
└── .env.example
```

---

## Data Quality

Each ingestion run:
- Enforces OHLCV column types (float64/int64)
- Validates row count before and after write
- Logs nulls per column
- Adds `ticker` and `ingested_at` metadata columns
- Skips tickers with no data instead of failing the pipeline
