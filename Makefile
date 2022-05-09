run:
	docker-compose up -d
	docker-compose logs -f benchmarker

run_sudo:
	sudo -E docker-compose up -d
	sudo docker-compose logs -f benchmarker

.PHONY: run_cron
run_cron:
	/usr/local/bin/docker-compose up -d

stop:
	echo "Gracefull shutdown. Benchmarker has 30 seconds to save results"
	docker-compose stop -t 30 benchmarker

stop_sudo:
	echo "Gracefull shutdown. Benchmarker has 30 seconds to save results"
	sudo docker-compose stop -t 30 benchmarker

build:
	docker-compose build

build_sudo:
	sudo docker-compose build

lint: lint_isort lint_black

lint_isort:
	poetry run isort galaxy_benchmarker

lint_black:
	poetry run black --target-version py310 galaxy_benchmarker

lint_mypy:
	poetry run mypy galaxy_benchmarker
