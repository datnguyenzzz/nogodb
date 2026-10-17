.PHONY: fmt docker-test test-repeat

TIMEOUT ?= 20s
LOG_FILE ?= target/cargo_test_stress.log

fmt:
	@cargo fmt

docker-test:
	@docker build -t query_engine_linux .
	@docker run --rm query_engine_linux

test-repeat:
	@echo "Running cargo test 50 times with $(TIMEOUT) timeout to check for flakiness/deadlocks..."
	@mkdir -p target
	@for i in $$(seq 1 50); do \
		timeout $(TIMEOUT) cargo test --lib -- --nocapture > $(LOG_FILE) 2>&1; \
		status=$$?; \
		if [ $$status -eq 124 ]; then \
			echo ""; \
			echo "=================================================="; \
			echo "🚨 DEADLOCK / TIMEOUT DETECTED at Run $$i (exceeded $(TIMEOUT))!"; \
			echo "=================================================="; \
			python3 scripts/find_hung_test.py $(LOG_FILE); \
			echo "=================================================="; \
			echo "Tail of log before timeout:"; \
			tail -n 15 $(LOG_FILE); \
			echo "=================================================="; \
			exit 1; \
		elif [ $$status -ne 0 ]; then \
			echo "FAILED with exit code $$status!"; \
			echo "=================================================="; \
			cat $(LOG_FILE); \
			echo "=================================================="; \
			exit 1; \
		else \
			printf "Run %02d/50: PASSED\n" $$i; \
		fi \
	done; \
	rm -f $(LOG_FILE); \
	echo "All 50 test runs completed with 100% success (zero deadlocks, zero flakiness)!"
