ENV_FILE ?= $(if $(wildcard .env),.env,.env.example)
COMPOSE := ./scripts/compose.sh

.PHONY: deps up down clean migrations migration-down test race integration integration-race distributed verify-infra verify-sqs-iam verify-aws-sqs-iam verify-isolated failpoints smoke

deps:
	ENV_FILE=$(ENV_FILE) $(COMPOSE) up -d --wait postgres keycloak localstack

up:
	ENV_FILE=$(ENV_FILE) $(COMPOSE) up --build -d --wait

down:
	ENV_FILE=$(ENV_FILE) $(COMPOSE) down --remove-orphans

clean:
	ENV_FILE=$(ENV_FILE) $(COMPOSE) down --volumes --remove-orphans

migrations:
	ENV_FILE=$(ENV_FILE) ./scripts/migrate.sh up

migration-down:
	ENV_FILE=$(ENV_FILE) ./scripts/migrate.sh down

test:
	go test ./...

race:
	go test -race ./...

integration: deps migrations
	go test -tags=integration ./...

integration-race: deps migrations
	go test -race -tags=integration ./...

distributed:
	ENV_FILE=$(ENV_FILE) ./scripts/distributed.sh

verify-infra:
	ENV_FILE=$(ENV_FILE) ./scripts/verify-local-infra.sh

verify-sqs-iam:
	ENV_FILE=$(ENV_FILE) ./scripts/verify-sqs-iam.sh

verify-aws-sqs-iam:
	AWS_PROFILE=$${AWS_PROFILE:-wager-free-tier} ./scripts/verify-aws-sqs-iam.sh

verify-isolated:
	./scripts/verify-isolated-compose.sh

failpoints:
	ENV_FILE=$(ENV_FILE) ./scripts/failure-scenarios.sh deterministic-failpoints

smoke:
	ENV_FILE=$(ENV_FILE) ./scripts/verify-distributed-scenarios.sh
