# Cross-platform Makefile for the NYC Taxi lakehouse.
# On Windows: works from PowerShell, cmd, or Git Bash (Git Bash recommended).
# On Linux/Mac: works from any shell.

ifeq ($(OS),Windows_NT)
    BASH := "C:/Program Files/Git/bin/bash.exe"
else
    BASH := bash
endif

.DEFAULT_GOAL := help

help:
	@echo Targets:
	@echo   make up            - bring up Minikube, install infra, run producer + Spark
	@echo   make down          - remove Helm releases + namespace (keep Minikube cluster)
	@echo   make down-hard     - same as down + delete the Minikube cluster
	@echo   make query         - open a spark-sql shell against the lakehouse
	@echo   make images        - (re)build and load producer + spark-job images
	@echo   make status        - show pod / job / svc / pvc status in the data namespace
	@echo   make logs-producer - tail producer logs
	@echo   make logs-spark    - tail Spark streaming logs

up:
	$(BASH) scripts/deploy.sh

down:
	$(BASH) scripts/teardown.sh

down-hard:
	$(BASH) scripts/teardown.sh --hard

query:
	$(BASH) scripts/query.sh

images:
	$(BASH) scripts/load-images.sh

status:
	kubectl -n data get pods,jobs,svc,pvc

logs-producer:
	kubectl -n data logs -l app=producer --tail=100 --follow

logs-spark:
	kubectl -n data logs -l app=spark-streaming --tail=100 --follow

.PHONY: help up down down-hard query images status logs-producer logs-spark
