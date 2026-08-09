# Real-Time Data Ingestion with Spark and PostgreSQL

This project simulates e-commerce activity, processes arriving CSV files with Spark Structured Streaming, and persists validated events in PostgreSQL.

## Deliverables

- `src/data_generator.py`: atomic CSV batch generator
- `src/spark_streaming_to_postgres.py`: typed, cleaned Spark streaming job
- `sql/postgres_setup.sql`: PostgreSQL table, constraints, and indexes
- `docs/`: architecture, setup, test, and performance documentation

The architecture diagram source is [docs/system_architecture.dot](docs/system_architecture.dot). Render it with Graphviz using `dot -Tpng docs/system_architecture.dot -o docs/system_architecture.png`.

See [docs/user_guide.md](docs/user_guide.md) for the complete setup and run sequence.
