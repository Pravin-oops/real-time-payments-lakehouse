.PHONY: up down restart logs ps clean

## Start all services
up:
	cp -n .env.example .env || true
	docker compose up -d --build

## Stop all services
down:
	docker compose down

## Restart a single service  (make restart svc=streamlit)
restart:
	docker compose restart $(svc)

## Tail all logs
logs:
	docker compose logs -f --tail=50

## Status
ps:
	docker compose ps

## Destroy everything including volumes
clean:
	docker compose down -v --remove-orphans

## Run dbt manually
dbt-run:
	docker exec dbt dbt run --profiles-dir /dbt/profiles --project-dir /dbt

dbt-test:
	docker exec dbt dbt test --profiles-dir /dbt/profiles --project-dir /dbt

## Trigger Airflow DAG manually
trigger-dag:
	docker exec airflow-webserver airflow dags trigger medallion_pipeline

## Show service URLs
urls:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════╗"
	@echo "║  SERVICE              URL                               ║"
	@echo "╠══════════════════════════════════════════════════════════╣"
	@echo "║  Streamlit Dashboard  http://localhost:8501             ║"
	@echo "║  Airflow UI           http://localhost:8090             ║"
	@echo "║  Spark Master UI      http://localhost:8094             ║"
	@echo "║  Kafka UI             http://localhost:8091             ║"
	@echo "║  Schema Registry      http://localhost:8093             ║"
	@echo "║  pgAdmin              http://localhost:8092             ║"
	@echo "║  Postgres Analytics   localhost:5434                    ║"
	@echo "║  Postgres Meta        localhost:5433                    ║"
	@echo "╚══════════════════════════════════════════════════════════╝"
	@echo ""