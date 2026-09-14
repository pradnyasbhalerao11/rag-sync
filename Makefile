REPO    ?= ../vuejs-docs
PATHSPEC?= *.md
TOPIC   ?= doc.changes
PARTS   ?= 6
MVN     ?= mvn

.PHONY: up down topic audit extract export publish consumer test stats reset logs clean db-init db-index db-reset psql ollama-check count

up:
	docker compose up -d
	@echo "waiting for kafka..."
	@until docker exec rag-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
		--bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
	@echo "waiting for postgres..."
	@until docker exec rag-postgres pg_isready -U ragsync -d ragsync >/dev/null 2>&1; \
		do sleep 2; done
	@echo "kafka is up. UI at http://localhost:8081"
	@echo "postgres is up on localhost:5432"

# --- week 3: embeddings and vector store ----------------------------------

db-init:
	docker exec -i rag-postgres psql -U ragsync -d ragsync \
		< ingest-consumer/src/main/resources/db/schema.sql
	@echo "schema applied"

db-index:
	@echo "building HNSW index (after bulk load, not before)..."
	docker exec rag-postgres psql -U ragsync -d ragsync -c \
		"CREATE INDEX IF NOT EXISTS chunks_embedding_idx ON chunks USING hnsw (embedding vector_cosine_ops);"

db-reset:
	docker exec rag-postgres psql -U ragsync -d ragsync -c "TRUNCATE chunks;"
	@echo "chunks table emptied"

psql:
	docker exec -it rag-postgres psql -U ragsync -d ragsync

ollama-check:
	@curl -s http://localhost:11434/api/embed \
		-d '{"model":"nomic-embed-text","input":["hello world"]}' \
		| python3 -c "import json,sys; e=json.load(sys.stdin)['embeddings'][0]; \
		print(f'ollama ok: {len(e)} dimensions, first 3: {e[:3]}')"

count:
	@docker exec rag-postgres psql -U ragsync -d ragsync -t -c \
		"SELECT count(*) AS chunks, count(DISTINCT doc_id) AS docs, \
		 count(DISTINCT embedding_model) AS models FROM chunks;"

down:
	docker compose down -v

topic:
	docker exec rag-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
		--bootstrap-server localhost:9092 \
		--topic $(TOPIC) --partitions $(PARTS) --replication-factor 1
	docker exec rag-kafka /opt/kafka/bin/kafka-topics.sh --describe \
		--bootstrap-server localhost:9092 --topic $(TOPIC)

audit:
	python3 tools/audit_corpus.py --repo $(REPO) --pathspec '$(PATHSPEC)'

extract:
	python3 tools/extract_events.py --repo $(REPO) --pathspec '$(PATHSPEC)' \
		--out data/events.jsonl

export:
	python3 tools/export_blobs.py --repo $(REPO) --events data/events.jsonl \
		--out data/blobs

publish:
	python3 tools/publish_events.py --file data/events.jsonl --topic $(TOPIC)

consumer:
	cd ingest-consumer && $(MVN) spring-boot:run

test:
	cd ingest-consumer && $(MVN) test

stats:
	@curl -s localhost:8080/stats | python3 -m json.tool

reset:
	@curl -s -X POST localhost:8080/stats/reset && echo " reset"

logs:
	docker compose logs -f kafka

clean:
	rm -rf data/events.jsonl data/blobs
