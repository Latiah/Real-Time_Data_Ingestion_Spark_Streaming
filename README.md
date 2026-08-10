# Real-Time Data Ingestion with Spark and PostgreSQL

This project simulates e-commerce activity, processes arriving CSV files with Spark Structured Streaming, and persists validated events in PostgreSQL.

## Deliverables

- `src/data_generator.py`: atomic CSV batch generator
- `src/spark_streaming_to_postgres.py`: typed, cleaned Spark streaming job
- `sql/postgres_setup.sql`: PostgreSQL table, constraints, and indexes
- `docs/`: architecture, setup, test, and performance documentation

## Prerequisites

- Python 3.9+
- OpenJDK 17+
- PostgreSQL 13+
- Apache Spark 3.4+

Install the Python dependencies:

```bash
python -m pip install -r requirements.txt
```

See the following documents for setup, testing, and project details:

- [docs/user_guide.md](docs/user_guide.md): complete setup and run sequence
- [docs/test_cases.md](docs/test_cases.md): manual validation plan
- [docs/performance_metrics.md](docs/performance_metrics.md): latency and throughput measurements
- [docs/project_overview.md](docs/project_overview.md): architecture and data flow
