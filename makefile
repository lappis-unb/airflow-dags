build-testing-img:
	docker rmi airflow-testing-lappis:latest
	docker build -t airflow-testing-lappis:latest .

run-pytest:
	make build-testing-img
	docker run airflow-testing-lappis:latest "pytest"

run-linters:
	make build-testing-img
	docker run airflow-testing-lappis:latest "black ."
	docker run airflow-testing-lappis:latest "ruff ."

run-static-analyzer:
	make build-testing-img
	docker run airflow-testing-lappis:latest "mypy ."

lint:
	black .
	ruff check --fix --unsafe-fixes .