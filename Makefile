build:
	@mvn package
	@docker build -t fdblucene-perf .
