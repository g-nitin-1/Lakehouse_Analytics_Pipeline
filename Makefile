.PHONY: install lint test smoke smoke-spark dashboard run

install:
	python -m pip install -e .[dev]

lint:
	ruff check .

test:
	pytest -q

smoke:
	python -m lakehouse_pipeline.cli run --input data/sample/orders.csv --output output

smoke-spark:
	python -m lakehouse_pipeline.cli run --input data/sample/orders.csv --output output --engine spark

dashboard:
	streamlit run streamlit_app.py

run: smoke
