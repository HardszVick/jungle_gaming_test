# Wager Service

Serviço financeiro em Go para carteiras e operações de aposta, desenhado para
preservar idempotência e integridade sob concorrência, reentrega e falhas. O
PostgreSQL é a autoridade financeira; HTTP e SQS são transportes at-least-once;
Keycloak emite as identidades locais e MiniStack aplica as policies SQS locais.

O binário `cmd/service` reúne API HTTP, autenticação OIDC, persistência pgx,
consumidor SQS, inbox, outbox e worker de referências sob lifecycle Uber Fx. O
Compose provisiona as dependências, aplica migrations e executa três réplicas da
aplicação. As garantias distribuídas são exercitadas com PostgreSQL, Keycloak e
LocalStack reais em Testcontainers e com MiniStack no gate Compose.

## Pré-requisitos

- Go `1.25.0`, conforme `go.mod` e `Dockerfile`, para os comandos Go normais. Se
  Go não estiver instalado no host, use o wrapper Docker da seção de testes.
- Docker Engine recente, com o plugin `docker compose`.
- `make`, `curl`, `jq` e `base64` para os roteiros documentados.
- AWS CLI não é necessária no host: os exemplos usam `awslocal` no MiniStack e
  o gate AWS usa a imagem `amazon/aws-cli`.
- Portas livres, por padrão: PostgreSQL `5432`, Keycloak `8081` e MiniStack
  `4566`. As portas HTTP das réplicas da aplicação são publicadas de forma
  dinâmica pelo Compose.

Confira as ferramentas:

```bash
go version
docker version
docker compose version
make --version
curl --version
jq --version
```

## Configuração

O wrapper `scripts/compose.sh` usa `.env` quando o arquivo existe e, caso
contrário, usa `.env.example`. Um checkout limpo usa MiniStack 1.5.26 com
`AUTH=true`, sem token externo e com enforcement das policies IAM. Para
personalizar o ambiente local:

```bash
cp .env.example .env
```

Os segredos de `.env.example` são apenas para uma estação isolada. Troque todos
fora desse contexto e não versione `.env`. MiniStack identifica o principal pela
access key e aplica policies, mas não valida o secret nem a assinatura SigV4. Por
isso `make verify-aws-sqs-iam` continua sendo a prova completa de autenticação e
enforcement contra SQS real, com profile local e permissões temporárias limitadas.

### Variáveis do Compose

| Variável | Padrão de exemplo | Finalidade |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `wager-service` | Nome do projeto Compose. |
| `POSTGRES_ADMIN_USER`, `POSTGRES_ADMIN_PASSWORD` | valores locais | Administração e bootstrap do PostgreSQL. |
| `POSTGRES_HOST_PORT` | `5432` | Porta PostgreSQL no host. |
| `APP_DATABASE_MIGRATION_USER`, `APP_DATABASE_MIGRATION_PASSWORD` | `wager_migration`, segredo local | Role proprietária do banco e dos objetos versionados, usada somente por migrations. |
| `APP_DATABASE_USER`, `APP_DATABASE_PASSWORD`, `APP_DATABASE_NAME` | `wager_app`, segredo local, `wager` | Role runtime sem ownership nem `CREATE` no schema e banco da aplicação. |
| `DATABASE_URL`, `MIGRATION_DATABASE_URL` | não definidas | URLs opcionais de um PostgreSQL externo. A primeira usa a role runtime; a segunda usa a role proprietária e deve incluir `options=-c%20app.application_role%3D<runtime_role>`. |
| `APP_HTTP_PORT`, `APP_METRICS_PORT` | `8080`, `9090` | Portas internas da aplicação. |
| `APP_REPLICAS` | `3` | Réplicas iniciadas pelo Compose. |
| `COMPONENTS` | `http,sqs-consumer,outbox,pending-reference` | Componentes do processo. |
| `SHUTDOWN_TIMEOUT` | `30s` | Prazo de encerramento ordenado. |
| `KEYCLOAK_*` | valores locais | Banco, administrador, portas interna/externa e URL pública do Keycloak. |
| `OIDC_ISSUER_URL`, `OIDC_AUDIENCE` | realm `wager`, `backend-api` | Emissor e audiência aceitos pela API. |
| `PROVIDER_A_CLIENT_SECRET`, `PROVIDER_B_CLIENT_SECRET`, `INTERNAL_SERVICE_CLIENT_SECRET` | segredos locais | Credenciais `client_credentials`. Somente letras, dígitos, `.`, `_` e `-` são aceitos pelo importador. |
| `AWS_REGION`, `AWS_ACCOUNT_ID` | `us-east-1`, `000000000000` | Região e conta emuladas. |
| `LOCALSTACK_HOST_PORT` | `4566` | Porta do emulator SQS/IAM no host; o nome é preservado por compatibilidade dos scripts. |
| `MINISTACK_IMAGE` | `ministackorg/ministack:1.5.26` com digest fixo | Emulator local gratuito com autorização habilitada pelo Compose. |
| `SQS_INPUT_QUEUE_NAME`, `SQS_DLQ_NAME`, `SQS_OUTPUT_QUEUE_NAME` | nomes abaixo | Filas de entrada, DLQ e eventos. |
| `SQS_VISIBILITY_TIMEOUT` | `60` | Visibility timeout, em segundos. |
| `SQS_WAIT_TIME_SECONDS` | `20` | Long polling, em segundos. |
| `SQS_REDRIVE_MAX_RECEIVE_COUNT` | `5` | Recebimentos antes do redrive para a DLQ. |

O loader Go também aceita as variáveis abaixo, com estes defaults:

| Grupo | Variáveis e defaults |
|---|---|
| HTTP | `HTTP_ADDR=:8080`, `INTERNAL_HTTP_ADDR=:9090` |
| AWS/SQS | `AWS_ENDPOINT_URL=http://localstack:4566`, `SQS_TRANSACTIONS_QUEUE=wager-transactions.fifo`, `SQS_TRANSACTIONS_DLQ=wager-transactions-dlq.fifo`, `SQS_EVENTS_QUEUE=wager-events.fifo` |
| Consumidor | `SQS_CONSUMER_BATCH_SIZE=10`, `SQS_CONSUMER_POLL_INTERVAL=20s`, `SQS_CONSUMER_LEASE_DURATION=60s` |
| Outbox | `OUTBOX_BATCH_SIZE=100`, `OUTBOX_POLL_INTERVAL=500ms`, `OUTBOX_LEASE_DURATION=30s` |
| Referências | `PENDING_REFERENCE_BATCH_SIZE=100`, `PENDING_REFERENCE_POLL_INTERVAL=1s`, `PENDING_REFERENCE_LEASE_DURATION=30s` |
| Retry SQL | `TRANSACTION_RETRY_MAX_ATTEMPTS=3`, `TRANSACTION_RETRY_BASE_DELAY=50ms`, `TRANSACTION_RETRY_MAX_DELAY=1s` |
| Retry de referência | `REFERENCE_MAX_ATTEMPTS=12`, `REFERENCE_RETRY_BASE_DELAY=1s`, `REFERENCE_RETRY_MAX_DELAY=5m`, `REFERENCE_TTL=24h` |

O bootstrap do MiniStack cria usuários `wager-runtime` e `wager-producer`,
policies separadas e arquivos de profiles em dois volumes Docker distintos. A
aplicação monta somente `ministack-runtime-credentials`, como
`/run/secrets/aws/credentials`, e usa o profile `runtime`; ela não recebe o
volume nem o arquivo do produtor. Comandos de produção executados dentro do
container do emulator usam o profile `producer` em
`/var/lib/localstack/producer-credentials/credentials`. As access keys geradas
não são gravadas no checkout. `make verify-sqs-iam` exige sucesso das operações
permitidas e `AccessDenied` nas ações cruzadas. `make verify-aws-sqs-iam` oferece
a prova adicional na AWS real com assinatura SigV4, sessões STS de 15 minutos e
filas `wager-test-*`, removidas automaticamente.

O Compose traduz as variáveis de conveniência de `.env` para os nomes lidos pelo
serviço: `APP_METRICS_PORT` vira `INTERNAL_HTTP_ADDR`, o endpoint local vira
`AWS_ENDPOINT_URL` e os três nomes de fila viram `SQS_TRANSACTIONS_QUEUE`,
`SQS_TRANSACTIONS_DLQ` e `SQS_EVENTS_QUEUE`.

O bootstrap cria duas roles PostgreSQL distintas. `wager_migration` é owner do
banco e executa migrations. `wager_app` recebe somente `CONNECT`, `USAGE`,
leitura e o DML concedido pelas migrations. A aplicação nunca deve usar a URL da
role proprietária.

Para usar um PostgreSQL externo, inclusive Supabase, defina as URLs completas no
`.env`. O Compose usa essas URLs diretamente em vez de montar a conexão com as
variáveis `APP_DATABASE_*`:

```dotenv
DATABASE_URL=postgresql://runtime_user:password@db.example.com:5432/postgres?sslmode=require
MIGRATION_DATABASE_URL=postgresql://migration_user:password@db.example.com:5432/postgres?sslmode=require&options=-c%20app.application_role%3Druntime_user
```

Antes de iniciar o Compose com PostgreSQL externo, preprovisione o banco, o
schema e as duas roles. A role de migration deve ser proprietária do schema e
capaz de criar/alterar os objetos; a role runtime referida por
`app.application_role` deve existir e conseguir conectar ao banco. As migrations
usam essa opção de conexão para conceder os privilégios mínimos à role runtime.
Codifique caracteres especiais do usuário e da senha para URL.

Se `MIGRATION_DATABASE_URL` não for definida, as migrations usam
`DATABASE_URL`. Uma única URL é suportada, mas é uma escolha intencionalmente
privilegiada: o processo da aplicação recebe a mesma identidade capaz de alterar
o schema. Use URLs separadas para preservar o modelo de menor privilégio.

### Execução direta

Fora do Compose, `DATABASE_URL` deve apontar para a role runtime. Processos com
o componente `http` também exigem `OIDC_ISSUER_URL` e `OIDC_AUDIENCE`. Migrations
usam outra `DATABASE_URL`, apontando para a role proprietária:

```bash
DATABASE_URL='postgres://wager_migration:...@127.0.0.1:5432/wager?sslmode=disable&options=-c%20app.application_role%3Dwager_app' \
MIGRATIONS_DIR='./migrations' \
go run ./cmd/service migrate up

DATABASE_URL='postgres://wager_app:...@127.0.0.1:5432/wager?sslmode=disable' \
OIDC_ISSUER_URL='http://keycloak.localhost:8081/realms/wager' \
OIDC_AUDIENCE='backend-api' \
AWS_REGION='us-east-1' \
AWS_ENDPOINT_URL='http://127.0.0.1:4566' \
AWS_PROFILE='runtime' \
AWS_SHARED_CREDENTIALS_FILE='/caminho/credentials' \
go run ./cmd/service
```

O serviço usa a credential provider chain padrão do AWS SDK v2. Profiles e
arquivo compartilhado são opcionais: variáveis `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, web identity e providers
gerenciados suportados pelo SDK também podem fornecer a identidade.
`AWS_ENDPOINT_URL` altera somente o endpoint, não a credential chain.

## Inicialização local

### Somente dependências

Suba PostgreSQL, Keycloak e MiniStack, sem tentar construir a aplicação:

```bash
make deps
```

Valide filas e os três tokens OIDC provisionados:

```bash
make verify-infra
```

Inspecione ou encerre as dependências:

```bash
./scripts/compose.sh ps
make down
```

`make clean` também remove os volumes e **apaga todos os dados locais**:

```bash
make clean
```

### Stack completa

```bash
make up
./scripts/compose.sh ps
```

`make up` constrói a imagem, aguarda PostgreSQL, Keycloak e MiniStack, executa
`service migrate up` e inicia três réplicas por padrão. Cada réplica executa os
componentes listados em `COMPONENTS`. Como não há balanceador local, cada porta
HTTP é publicada dinamicamente; consulte-a com `compose port --index` conforme a
seção da API.

## Filas SQS

O init do MiniStack cria:

| Fila | Retenção | Visibility/long poll | Uso |
|---|---:|---:|---|
| `wager-transactions.fifo` | 4 dias | `60s` / `20s` por padrão | Entrada de comandos. Redrive após `SQS_REDRIVE_MAX_RECEIVE_COUNT` recebimentos, default `5`. |
| `wager-transactions-dlq.fifo` | 14 dias | defaults do broker | Mensagens de entrada esgotadas ou permanentemente inválidas. |
| `wager-events.fifo` | 4 dias | `60s` / `20s` por padrão | Eventos publicados pela outbox. |

Todas são FIFO, com `ContentBasedDeduplication=false`. O produtor deve informar:

- entrada: `MessageGroupId=walletId` e
  `MessageDeduplicationId=messageId`;
- eventos: `MessageGroupId=aggregateId` e
  `MessageDeduplicationId=eventId`.

A entrada exige `playerId` e `walletId` em formato UUID. Em cada lote recebido,
o consumidor processa mensagens do mesmo `MessageGroupId` sequencialmente, na
ordem entregue pelo SQS, e processa grupos diferentes em paralelo. Se um item
não for confirmado e removido, o grupo para e os itens posteriores já recebidos
são liberados com visibilidade zero. Eles não ultrapassam o item em retry/DLQ.

A deduplicação FIFO é apenas otimização e tem janela limitada. A correção depende
da idempotência no PostgreSQL, da inbox e do `eventId` estável. O provisionamento
local cria usuários e policies IAM inline separados para `runtime` e `producer`.
O Compose usa MiniStack com `AUTH=true`, e `make verify-sqs-iam` comprova
localmente as permissões positivas e negativas. `make verify-aws-sqs-iam` repete
a prova contra SQS real, incluindo autenticação SigV4. O teste inicia a aplicação
com credenciais runtime temporárias, usa uma identidade producer distinta e
confirma que ações cruzadas não alteram transações, inbox, ledger ou outbox.

Consulte atributos e mensagens na DLQ:

```bash
INPUT_URL="$(./scripts/compose.sh exec -T localstack awslocal sqs get-queue-url \
  --queue-name wager-transactions.fifo --query QueueUrl --output text)"
DLQ_URL="$(./scripts/compose.sh exec -T localstack awslocal sqs get-queue-url \
  --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)"
./scripts/compose.sh exec -T localstack awslocal sqs get-queue-attributes \
  --queue-url "$INPUT_URL" \
  --attribute-names All

./scripts/compose.sh exec -T localstack awslocal sqs receive-message \
  --queue-url "$DLQ_URL" \
  --attribute-names All --message-attribute-names All
```

## Migrations

O catálogo possui três versões, cada uma com arquivos `up` e `down`:

- `000001_initial_schema`: cria carteiras, transações, ledger append-only, inbox,
  outbox, índices, constraints e trigger de imutabilidade;
- `000002_sql_hardening`: endurece referências e `OPENING`, restringe moedas a
  `AUD`, `BRL`, `CAD`, `CHF`, `CNY`, `EUR`, `GBP`, `MXN`, `NZD` e `USD`, e
  aplica os grants mínimos da role runtime;
- `000003_financial_invariants`: valida o vínculo entre transação, carteira,
  jogador e moeda, preservando snapshots de entradas rejeitadas, valida por
  trigger a semântica do ledger, protege snapshots e estados terminais e limita
  `UPDATE` da role runtime às colunas operacionais.

O runtime carrega o mesmo catálogo e recusa schema ausente, atrasado, adiantado
ou sem objetos essenciais.

### Runner versionado

O subcomando `service migrate` descobre pares `*.up.sql`/`*.down.sql`, serializa
execuções com advisory lock transacional e registra versão, nome e horário em
`schema_migrations`. `up` é idempotente para versões já aplicadas; `down` reverte
uma versão por chamada e remove `schema_migrations` quando não resta versão.

```bash
make migrations
make migration-down
make migrations
```

`down` reverte somente a maior versão aplicada. Partindo da versão 3, o ciclo
`up -> down -> up` reverte e reaplica a `000003`; três chamadas `down` removem
todo o schema. Use rollback somente em banco descartável. Para inspecionar a
versão aplicada:

```bash
set -a
. "$(test -f .env && printf '%s' .env || printf '%s' .env.example)"
set +a
./scripts/compose.sh exec -T -e PGPASSWORD="$APP_DATABASE_PASSWORD" postgres \
  psql -U "$APP_DATABASE_USER" -d "$APP_DATABASE_NAME" \
  -c 'TABLE schema_migrations;'
```

## Autenticação Keycloak

O realm local é `wager`, com audiência `backend-api`:

| Cliente | Claim/role | Uso |
|---|---|---|
| `provider-a` | `provider_id=provider-a`, `wager:read`, `wager:write` | Operações e consultas do provedor A. |
| `provider-b` | `provider_id=provider-b`, `wager:read`, `wager:write` | Operações e consultas do provedor B. |
| `internal-service` | `wallet:internal`, sem `provider_id` | Carteiras, ledger, reconciliação e consulta interna. |

Obtenha tokens por `client_credentials`:

```bash
set -a
. "$(test -f .env && printf '%s' .env || printf '%s' .env.example)"
set +a
TOKEN_URL="http://127.0.0.1:${KEYCLOAK_HOST_PORT}/realms/wager/protocol/openid-connect/token"

PROVIDER_A_TOKEN="$(curl --fail --silent --show-error \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode client_id=provider-a \
  --data-urlencode "client_secret=$PROVIDER_A_CLIENT_SECRET" \
  "$TOKEN_URL" | jq -er .access_token)"

PROVIDER_B_TOKEN="$(curl --fail --silent --show-error \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode client_id=provider-b \
  --data-urlencode "client_secret=$PROVIDER_B_CLIENT_SECRET" \
  "$TOKEN_URL" | jq -er .access_token)"

INTERNAL_TOKEN="$(curl --fail --silent --show-error \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode client_id=internal-service \
  --data-urlencode "client_secret=$INTERNAL_SERVICE_CLIENT_SECRET" \
  "$TOKEN_URL" | jq -er .access_token)"
```

`make verify-infra` também verifica `aud`, scopes, roles e `provider_id` nos
tokens. A API valida assinatura, issuer, audience, expiração e permissões usando
discovery OIDC e o cache JWKS de `go-oidc`.

## API HTTP

Dinheiro sempre é JSON com string decimal e moeda, nunca número de ponto
flutuante. O decoder rejeita campos desconhecidos, valores JSON adicionais e
corpos acima de 1 MiB.

Para uma réplica Compose, descubra a porta publicada e defina a base:

```bash
APP1_ADDR="$(./scripts/compose.sh port --index 1 app 8080)"
API_URL="http://127.0.0.1:${APP1_ADDR##*:}"
```

Em execução direta, o default é `API_URL=http://127.0.0.1:8080`.

### Saúde e métricas

```bash
curl --fail-with-body "$API_URL/health/live"
curl --fail-with-body "$API_URL/health/ready"
./scripts/compose.sh exec -T app \
  curl --fail --silent --show-error http://127.0.0.1:9090/metrics
```

- `GET /health/live` é público e independe de PostgreSQL, SQS e IdP.
- `GET /health/ready` é público e deve indicar PostgreSQL ou SQS degradado sem
  expor credenciais.
- `GET /metrics` fica na interface interna, que o Compose não publica no host.

### Abrir carteira

`POST /wallets`, token interno, resposta esperada `201`:

```bash
PLAYER_ID='11111111-1111-4111-8111-111111111111'
WALLET_RESPONSE="$(curl --fail-with-body --silent --show-error \
  -X POST "$API_URL/wallets" \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "playerId": "'$PLAYER_ID'",
    "initialBalance": {"amount": "100.00", "currency": "BRL"}
  }')"
printf '%s\n' "$WALLET_RESPONSE" | jq .
WALLET_ID="$(printf '%s' "$WALLET_RESPONSE" | jq -er .id)"
```

Saldo positivo cria carteira, `OPENING`, ledger e eventos no mesmo commit. Saldo
`0.00` cria somente a carteira. A resposta usa `id`; o campo legado `walletId`
não é emitido:

```json
{
  "id": "uuid-v7",
  "playerId": "11111111-1111-4111-8111-111111111111",
  "balance": {"amount": "100.00", "currency": "BRL"},
  "version": 1,
  "createdAt": "2026-10-10T12:00:00Z",
  "updatedAt": "2026-10-10T12:00:00Z"
}
```

### Ler carteira, ledger e reconciliação

Todos exigem o token interno:

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  "$API_URL/wallets/$WALLET_ID"

curl --fail-with-body \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  "$API_URL/wallets/$WALLET_ID/ledger?limit=50"

CURSOR='<nextCursor-retornado>'
curl --fail-with-body \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  --get --data-urlencode limit=50 --data-urlencode "cursor=$CURSOR" \
  "$API_URL/wallets/$WALLET_ID/ledger"

curl --fail-with-body -X POST \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  "$API_URL/wallets/$WALLET_ID/reconciliation"
```

O ledger retorna `entries` com `ledgerEntryId`, `walletId`, `transactionId`,
`direction`, `money`, `balanceBefore`, `balanceAfter` e `createdAt`, além de
`nextCursor` quando houver próxima página. A ordenação é descendente e o cursor
base64url representa `(created_at,id)`. A reconciliação retorna exatamente
`walletId`, `storedBalance`, `calculatedBalance`, `difference`, `consistent` e
`checkedEntries`, sem corrigir dados. Os nomes legados `walletBalance`,
`ledgerBalance` e `entryCount` não são emitidos.

### Enviar transações

`POST /wagering/transactions` exige `Idempotency-Key`, token do provedor e
`providerId` igual ao claim. Exemplo `BET`:

O hash SHA-256 de idempotência usa JSON dos campos de negócio com chaves
lexicograficamente ordenadas, inclusive `amount` antes de `currency` em
`money`. A chave de idempotência e metadados de transporte não entram no hash;
HTTP e SQS constroem o mesmo comando normalizado.

```bash
IDEMPOTENCY_KEY='idem-bet-001'
TRANSACTION_RESPONSE="$(curl --fail-with-body --silent --show-error \
  -X POST "$API_URL/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_A_TOKEN" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -d "{
    \"providerId\": \"provider-a\",
    \"externalTransactionId\": \"ext-bet-001\",
    \"playerId\": \"$PLAYER_ID\",
    \"walletId\": \"$WALLET_ID\",
    \"roundId\": \"round-1\",
    \"gameId\": \"game-1\",
    \"kind\": \"BET\",
    \"money\": {\"amount\": \"80.00\", \"currency\": \"BRL\"}
  }")"
printf '%s\n' "$TRANSACTION_RESPONSE" | jq .
TRANSACTION_ID="$(printf '%s' "$TRANSACTION_RESPONSE" | jq -er .transactionId)"
```

Troque `kind` por `WIN` para crédito. `LOSS` exige `money.amount="0.00"` e não
altera saldo, versão ou ledger. `OPENING` não é aceito externamente.

`REFUND` e `ROLLBACK` exigem `referenceExternalTransactionId` e valor integral:

```bash
curl --fail-with-body -i -X POST "$API_URL/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_A_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: idem-refund-001' \
  -d "{
    \"providerId\": \"provider-a\",
    \"externalTransactionId\": \"ext-refund-001\",
    \"playerId\": \"$PLAYER_ID\",
    \"walletId\": \"$WALLET_ID\",
    \"roundId\": \"round-1\",
    \"gameId\": \"game-1\",
    \"kind\": \"REFUND\",
    \"money\": {\"amount\": \"80.00\", \"currency\": \"BRL\"},
    \"referenceExternalTransactionId\": \"ext-bet-001\"
  }"
```

Resultados: `200` para processado/replay, `202` para
`PENDING_REFERENCE`, `400` para transporte inválido, `401`, `403`, `404`, `409`
para conflito/rejeição síncrona e `503` com `Retry-After` para infraestrutura
transitória. Replay equivalente deve retornar o saldo histórico original e
`idempotentReplay: true`.

A resposta do `POST` contém `transactionId`, identidades de negócio, `kind`,
`money`, `status`, timestamps e `idempotentReplay`. Quando `status=PROCESSED`, o
saldo histórico aparece em `balance`; o campo legado `resultBalance` não é
emitido. Quando `status=REJECTED` ou `FAILED`, `balance` é omitido e
`failureCode` é informado. Consultas `GET` usam o mesmo DTO, mas omitem
`idempotentReplay`.

### Consultar transações

Consulta interna pelo UUID:

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  "$API_URL/wagering/transactions/$TRANSACTION_ID"
```

Consulta externa pela identidade do próprio provedor:

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $PROVIDER_A_TOKEN" \
  "$API_URL/providers/provider-a/wagering/transactions/ext-bet-001"
```

Um token `provider-b` nesse caminho deve receber `403`, sem confirmar se a
transação existe.

## Exemplo SQS

O envelope é estrito: aceita somente `messageId`, `type`, `occurredAt` e `data`.
`eventType` e `version` são campos desconhecidos e tornam a mensagem inválida.
`occurredAt` deve ser RFC 3339 UTC terminado em `Z`; `data` converge para o mesmo
comando canônico usado pelo HTTP. Tanto `data.playerId` quanto `data.walletId`
devem ser UUIDs.

```bash
MESSAGE_ID='018f0000-0000-7000-8000-000000000001'
MESSAGE_BODY="$(jq -nc \
  --arg messageId "$MESSAGE_ID" \
  --arg walletId "$WALLET_ID" \
  --arg playerId "$PLAYER_ID" \
  '{
    messageId: $messageId,
    type: "WagerTransactionRequested",
    occurredAt: "2026-10-10T12:00:00Z",
    data: {
      idempotencyKey: "idem-sqs-001",
      providerId: "provider-a",
      externalTransactionId: "ext-sqs-001",
      playerId: $playerId,
      walletId: $walletId,
      roundId: "round-2",
      gameId: "game-1",
      kind: "WIN",
      money: {amount: "25.00", currency: "BRL"}
    }
  }')"

INPUT_QUEUE_URL="$(./scripts/compose.sh exec -T localstack awslocal sqs get-queue-url \
  --queue-name wager-transactions.fifo --query QueueUrl --output text)"

./scripts/compose.sh exec -T \
  -e AWS_PROFILE=producer \
  -e AWS_SHARED_CREDENTIALS_FILE=/var/lib/localstack/producer-credentials/credentials \
  localstack aws --endpoint-url=http://localhost:4566 sqs send-message \
  --queue-url "$INPUT_QUEUE_URL" \
  --message-body "$MESSAGE_BODY" \
  --message-group-id "$WALLET_ID" \
  --message-deduplication-id "$MESSAGE_ID"
```

O consumidor calcula o hash do envelope canônico, confirma inbox, transação,
ledger aplicável e outbox no mesmo commit e só então remove a mensagem.
Mensagens do mesmo grupo são tratadas sequencialmente; grupos distintos podem
avançar em paralelo. Após commit e delete bem-sucedidos, o consumidor registra em
nível `INFO` `SQS message processed and deleted`, com IDs seguros de correlação,
mensagem, transação, carteira e provedor, sem payload financeiro.
O profile do exemplo comprova a autorização somente quando o broker aplica
enforcement IAM efetivo.

## Testes e qualidade

Os comandos Go no host são a forma primária:

```bash
gofmt -w $(find . -type f -name '*.go')
go mod tidy
go mod verify
test -z "$(find . -type f -name '*.go' -exec gofmt -l {} +)"
go vet ./...
go build ./...
go test ./... -count=1
go test -race ./... -count=1
go test -tags=integration ./... -count=1
go test -race -tags=integration ./... -count=1
```

Com a build tag `integration`, Testcontainers inicia automaticamente containers
efêmeros de PostgreSQL 16, LocalStack 3.8.1 e Keycloak 25.0.6. Não é necessário
executar `make deps`: o `TestMain` de `integration/full_stack_test.go` compartilha
uma infraestrutura durante o pacote, cria schemas e filas isolados por cenário e
encerra os containers ao final. Testes de integração em outros pacotes iniciam
somente a dependência necessária. O Docker daemon precisa estar acessível.

`make integration` e `make integration-race` também funcionam, mas seus alvos
primeiro sobem as dependências Compose e aplicam migrations; em seguida, a suíte
ainda cria seus próprios Testcontainers. Para uma execução isolada e sem
infraestrutura duplicada, prefira os comandos `go test -tags=integration` acima.

### Go via Docker

Quando Go não estiver instalado no host Linux, use exatamente este wrapper. A
rede do host e `TESTCONTAINERS_HOST_OVERRIDE` são necessários para o processo Go
alcançar as portas publicadas pelos containers irmãos; montar o socket Docker
concede ao container controle equivalente sobre o daemon.

```bash
go_docker() {
  docker run --rm --network host \
    -e TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1 \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$PWD:/workspace" \
    -w /workspace \
    golang:1.25.0 go "$@"
}

go_docker mod verify
go_docker test ./... -count=1
go_docker vet ./...
go_docker build ./...
go_docker test -race ./... -count=1
go_docker test -tags=integration ./... -count=1
go_docker test -race -tags=integration ./... -count=1
```

`gofmt` e `go mod tidy` alteram arquivos, por isso execute-os em um checkout no
qual seja possível revisar o diff. No wrapper Docker, os equivalentes são:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace golang:1.25.0 \
  sh -ec 'gofmt -w $(find . -type f -name "*.go")'
go_docker mod tidy
```

### Evidência histórica de verificação

Matriz executada em 2026-10-12, antes de Community 3.8.1 tornar-se o default do
Compose:

- formatação, `go mod tidy`, `go mod verify`, vet, build, unitários e race
  passaram com `golang:1.25.0` em container;
- `AWS_IAM_FULL_SUITE=1 make verify-aws-sqs-iam` passou; o pacote `integration`
  levou `65.772s` e executou o teste de enforcement AWS sem skip;
- `AWS_IAM_FULL_SUITE=1 AWS_IAM_RACE=1 make verify-aws-sqs-iam` passou; o pacote
  `integration` levou `77.320s`;
- `make verify-isolated` construiu uma stack com volumes vazios, aplicou as
  migrations 1 e 2, validou roles/profiles e permissões PostgreSQL, executou o
  smoke distribuído e removeu o projeto isolado em `157s`;
- o smoke confirmou três réplicas, disputa `100/80/80`, HTTP/SQS, inbox/outbox,
  `SIGTERM`, restart persistente, saldo `25.00`, três lançamentos e sete eventos.

Uma validação adicional em 2026-10-12, já com LocalStack Community 3.8.1 como
default, passou na integração (`66.717s`), integração com race (`81.500s`) e no
`make verify-isolated` (`142s`).

O fechamento da auditoria em 2026-10-12 repetiu formatação, tidy, módulos, vet,
build, unitários e race com `golang:1.25.0`. A integração completa passou em
`77.895s`, a integração com race em `84.001s` e `make verify-isolated` em `138s`,
agora aplicando e verificando o schema 3 em volumes vazios. O gate AWS IAM não
foi reexecutado nessa rodada; sua última evidência continua sendo a execução real
descrita acima.

O fechamento das evidências físicas em 2026-10-12 executou `SIGKILL` pré-commit
e outage/restart de PostgreSQL durante a transação aberta em `40.448s`. O gate
AWS IAM real voltou a passar, com o teste `integration` em `42.761s`. Depois das
mudanças, módulos, vet, build, unitários e race passaram; a integração completa
passou em `71.125s`, a integração com race em `71.071s` e o Compose isolado em
`133s`. Scripts, configuração Compose, workflow e OpenSpec estrito também foram
validados.

O fechamento das lacunas remanescentes em 2026-10-12 moveu todos os timestamps
operacionais da outbox para o relógio PostgreSQL e adotou MiniStack com
autorização local efetiva. Formatação, tidy, módulos, vet, build, unitários e race
passaram; a integração completa levou `69.043s`, a integração com race `75.544s`
e o Compose isolado `132s`, agora incluindo permissões SQS positivas e negativas.
O gate AWS real também passou novamente, com `integration` em `42.427s`.

Ambiente: Linux `7.2.9-2-cachyos` `x86_64`, Docker Engine `29.9.0`, Docker
Compose `5.6.0`, PostgreSQL 16.4, Keycloak 25.0.6, LocalStack Pro 2026.3.0 e AWS
SQS em `us-east-1`. Naquela execução, o token LocalStack `freemium` não aplicou
enforcement; essa limitação local não substitui nem invalida a prova executada na
AWS real.

Essas durações são evidência histórica, não estimativas. Hoje o Compose usa
MiniStack 1.5.26 com autorização por policy habilitada. O gate AWS real permanece
a prova de autenticação SigV4 e enforcement no serviço gerenciado.

Outros alvos úteis:

| Nível | Comando | Cobertura |
|---|---|---|
| Infra Compose | `make deps && make verify-infra` | Filas/redrive e claims dos três clientes Keycloak. |
| IAM SQS local | `make verify-sqs-iam` | Ações permitidas e negativas para profiles locais no MiniStack com `AUTH=true`. |
| IAM SQS AWS | `make verify-aws-sqs-iam` | Sessões STS temporárias, ações positivas/negativas e ausência de efeito financeiro; requer profile `wager-free-tier` e a policy em `infra/aws/`. |
| Gate isolado | `make verify-isolated` | Volumes vazios, migrations, três réplicas, IAM SQS positivo/negativo, permissões PostgreSQL e smoke; remove somente o projeto criado pelo gate. |
| Stack local | `make up` | Build, migration e três réplicas completas. |
| Réplicas | `make distributed` | Exige no mínimo três containers `app`. |
| Falhas determinísticas | `./scripts/failure-scenarios.sh deterministic-failpoints` | Janelas commit/delete e publish/mark na suíte Testcontainers. |
| Falhas físicas financeiras | `go test -tags=integration ./integration -run '^(TestProcessSIGKILLBeforeCommitRollsBackAndRetries|TestPostgresOutageDuringOpenFinancialTransactionRollsBackAndRetries)$' -count=1` | `SIGKILL` pré-commit e outage real do PostgreSQL durante transação aberta, com rollback e retry. |
| Smoke distribuído | `make smoke` | Três portas, HTTP/OIDC, concorrência 100/80/80, SQS/inbox, outbox, SIGTERM, restart e reconciliação SQL/HTTP. |

### Verificações desta atualização documental

```bash
./scripts/compose.sh config --quiet
bash -n scripts/failure-scenarios.sh
bash -n scripts/verify-aws-sqs-iam.sh
bash -n scripts/verify-isolated-compose.sh
./scripts/failure-scenarios.sh --help
```

## Três réplicas e falhas

O Compose declara três réplicas e o script abaixo força `--scale app=3`:

```bash
./scripts/failure-scenarios.sh up-three
./scripts/failure-scenarios.sh status
```

Simulações disponíveis, preservando volumes:

```bash
# Crash abrupto de uma réplica; restaure-a depois com up-three.
./scripts/failure-scenarios.sh kill-one

# SIGTERM para observar graceful shutdown; restaure depois com up-three.
./scripts/failure-scenarios.sh term-one

# Indisponibilidade temporária e recuperação de dependências.
./scripts/failure-scenarios.sh outage postgres 15
./scripts/failure-scenarios.sh outage localstack 15
./scripts/failure-scenarios.sh outage keycloak 15

# Reinício de todos os containers, mantendo dados/filas persistidos em volumes.
./scripts/failure-scenarios.sh full-restart
```

Esses comandos automatizam a falha operacional, mas não fazem asserções
financeiras. As janelas exatas usam seams não configuráveis no binário normal e
são verificadas pela suíte de integração:

```bash
./scripts/failure-scenarios.sh deterministic-failpoints
```

Os seams provam rollback intermediário e as janelas commit/delete e publish/mark
de forma determinística. Além deles, `integration/physical_failure_test.go`
mantém a transação financeira aberta numa fronteira pré-commit exclusiva da
build `integration`, confirma-a externamente em `pg_stat_activity` e mata o
subprocesso com `SIGKILL`. Outro teste para e reinicia um PostgreSQL
Testcontainers dedicado durante a mesma fronteira. Ambos exigem rollback
integral e retry único por uma nova instância; nenhum failpoint é incluído no
binário normal.

O smoke Compose com asserções usa identificadores exclusivos, preserva volumes e
deixa a stack em execução ao terminar:

```bash
make smoke
```

Ele exige exatamente três réplicas, chama liveness/readiness em cada porta,
obtém tokens reais, abre uma carteira, disputa duas apostas de `80.00`, envia uma
vitória por SQS, aguarda inbox/outbox, compara SQL com reconciliação HTTP, envia
`SIGTERM` a uma réplica e reinicia a stack inteira sem apagar o estado. Os seams
determinísticos continuam na suíte de integração porque não são expostos na
imagem normal.

O roteiro de asserções, SQL, filas e failpoints necessários está em
[docs/test-plan.md](docs/test-plan.md).

## Referências

- [Arquitetura e decisões](ARCHITECTURE.md)
- [Plano de testes distribuídos e recuperação](docs/test-plan.md)
- [Migration inicial](migrations/000001_initial_schema.up.sql)
- [Hardening SQL e grants](migrations/000002_sql_hardening.up.sql)
- [Invariantes financeiras e snapshots](migrations/000003_financial_invariants.up.sql)
- [Provisionamento das filas](infra/localstack/init/10-provision-sqs.sh)
- [Realm local](infra/keycloak/realm.json)
