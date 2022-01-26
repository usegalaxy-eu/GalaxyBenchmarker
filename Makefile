run:
	docker-compose up -d
	docker-compose logs -f benchmarker

run_sudo:
	sudo -E docker-compose up -d
	sudo docker-compose logs -f benchmarker

lint: lint_isort lint_black

lint_isort:
	poetry run isort galaxy_benchmarker

lint_black:
	poetry run black --target-version py310 galaxy_benchmarker
