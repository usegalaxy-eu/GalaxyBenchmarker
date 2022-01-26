run:
	docker-compose up -d
	docker-compose logs -f benchmarker

run_sudo:
	sudo -E docker-compose up -d
	sudo docker-compose logs -f benchmarker
