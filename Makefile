REPO    ?= ../spring-boot-docs
PATHSPEC?= *.md
TOPIC   ?= doc.changes
PARTS   ?= 6
MVN     ?= mvn

.PHONY: up down topic extract publish stats reset logs clean

up:
	docker compose up -d
	@echo "waiting for kafka..."
	@until docker exec rag-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
		--bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
	@echo "kafka is up. UI at http://localhost:8081"

down:
	docker compose down -v

topic:
	docker exec rag-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
		--bootstrap-server localhost:9092 \
		--topic $(TOPIC) --partitions $(PARTS) --replication-factor 1
	docker exec rag-kafka /opt/kafka/bin/kafka-topics.sh --describe \
		--bootstrap-server localhost:9092 --topic $(TOPIC)

extract:
	python3 tools/extract_events.py --repo $(REPO) --pathspec '$(PATHSPEC)' \
		--out data/events.jsonl

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
	rm -f data/events.jsonl
