# Makefile

.PHONY: run test lint format proto clean

run:
	poetry run python src/main.py --pattern=sample_data/*.tsv --dry

test:
	poetry run pytest

lint:
	poetry run flake8 src/ tests/

format:
	poetry run black src/ tests/

proto:
	poetry run python -m grpc_tools.protoc -I=src/memc_load --python_out=src/memc_load src/memc_load/appsinstalled.proto

clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
