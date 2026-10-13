# Plano de testes distribuídos e de falhas

## Objetivo e estado

Este documento descreve a suíte implementada para idempotência, concorrência,
atomicidade, retomada e isolamento, além do roteiro operacional com três
containers Compose. `integration/full_stack_test.go` usa infraestrutura real e
três instâncias da aplicação com pools independentes dentro do mesmo processo de
teste. `scripts/failure-scenarios.sh` complementa a suíte manipulando os três
processos reais do Compose. `integration/physical_failure_test.go` adiciona
`SIGKILL` de subprocesso e outage de um PostgreSQL dedicado em uma fronteira
pré-commit observada externamente.

## Execução automatizada

A suíte Testcontainers exige somente Go 1.25 e acesso ao Docker daemon; ela
inicia e encerra PostgreSQL, LocalStack e Keycloak automaticamente. Cada cenário
recebe schema, filas e identificadores exclusivos.

```bash
go test -tags=integration ./... -count=1
go test -race -tags=integration ./... -count=1
```

Se Go não existir no host Linux, use o wrapper Docker documentado no README. Não
é necessário executar `make deps`; o alvo `make integration` é operacional, mas
sobe Compose antes de a suíte criar seus próprios containers.

Cobertura principal:

| Teste | Cobertura |
|---|---|
| `TestMigrationsConstraintsLedgerImmutabilityAndAtomicRollback` | up/down/up, constraints, trigger e rollback sem parcial. |
| `TestFinancialInvariantsRejectDirectSQLViolations` | Vínculo carteira/jogador/moeda, semântica do ledger e snapshots imutáveis no schema 3. |
| `TestFinancialInvariantsMigrationRuntimeRoleRoundTrip` | Grants por coluna e ciclo reversível `3 -> 2 -> 3`. |
| `TestLoadMigrationsOrdersAndPairsFiles` | Ordenação por versão, pares `up`/`down` e maior versão do catálogo. |
| `TestLoadMigrationsRequiresBothDirections` | Rejeição de versão sem uma das direções. |
| `TestLoadMigrationCatalogRejectsMismatchedPairNames` | Rejeição de nomes divergentes no mesmo número de versão. |
| `TestStartupRequiresCurrentMigrationVersionAndEssentialObjects` | Recusa schema ausente, atrasado, adiantado, objeto incompleto, nome incorreto e histórico incompleto; aceita somente toda a história atual. |
| `TestPendingReferenceWinRemainsRehydratableAcrossMigrationDownAndUp` | Downgrade converte `WIN` pendente em rejeição reidratável sem referência e o upgrade preserva os dados. |
| `TestMigrationOwnerAndRuntimePermissions` | Roles externas distintas, opção `app.application_role`, grants runtime, catálogo completo e negativas SQLSTATE `42501`. |
| `TestRealKeycloakAuthenticationAndAuthorization` | tokens reais, ausência/invalidez/expiração, role interna e isolamento de provider. |
| `TestFiftyParallelIdenticalDeliveriesAcrossHTTPAndSQS` | 25 HTTP + 25 SQS, replay histórico e uma aplicação financeira. |
| `TestDistributedWalletConcurrencyAndIndependentLocks` | duas apostas de 80 e ausência de lock global entre carteiras. |
| `TestConsumerCrashAfterCommitBeforeDeleteRedeliversWithoutDuplicate` | reentrega na janela commit/delete. |
| `TestOutboxCompetingPublishersCrashBoundariesAndUnavailableSQS` | claims concorrentes, duas janelas de crash e recuperação de SQS. |
| `TestPendingRefundRollbackResolutionTTLAndFinalReconciliation` | referências antecipadas, refund, rollback, TTL, eventos e reconciliação. |
| `TestFxRealLifecycleStartsStopsAndReleasesResources` | startup/shutdown Fx com recursos reais. |
| `TestThreeServiceProcessesSerializeConcurrentBetsAndPersistAcrossRestart` | três processos reais, contenção 100/80/80 e persistência após restart. |
| `TestInvalidMessageIsPhysicallyRedrivenAndReceivedFromDLQ` | redrive físico de envelope inválido até a DLQ real. |
| `TestPermanentPendingFailureLifecycle` | `FAILED`, rollback recuperável, concorrência, replay e ausência de efeito financeiro. |
| `TestConsumerRetriesCommitFailureAndDeletesDurableFailedReplay` | Não apaga a entrada após commit falho; apaga depois de resultado durável. |
| `TestConsumerPostCommitInterruptionReplaysFailedWithoutMovement` | Reentrega após interrupção pós-commit sem movimento duplicado. |
| `TestClassifyProcessingError` | Domínio, transitório, permanente, cancelamento e fallback conservador. |
| `TestAWSSDKConfigUsesEnvironmentCredentialChain` | Access key, secret e session token pela chain padrão. |
| `TestAWSSDKConfigUsesSharedCredentialsProfile` | Profile e arquivo compartilhado de credenciais. |
| `TestAWSSDKConfigAcceptsAlternativeProvider` | Provider alternativo sem credencial estática obrigatória. |
| `TestAWSSDKConfigReportsMissingCredentials` | Ausência de credencial observável. |
| `TestAWSSQSLeastPrivilegeHasNoUnauthorizedFinancialEffects` | SQS real, aplicação runtime, produtor separado, negativas cruzadas e contagens financeiras inalteradas. |
| `TestWalletContractsAndCorrelation` | Campos HTTP oficiais e ausência de aliases legados. |
| `TestTransactionStatusesReplayAndFailureCode` | `balance`, replay e códigos de falha no POST. |
| `TestTransactionQueries` | DTO de consulta sem `idempotentReplay` nem `resultBalance`. |
| `TestTask117WalletOpeningPositiveZeroAndConcurrentDuplicate` | Task 11.7: abertura positiva/zero, efeitos atômicos e uma vencedora entre duplicatas concorrentes. |
| `TestTask117PendingReferenceContinuesAfterApplicationRestart` | Task 11.7: referência pendente continua após restart e converge sem duplicação. |
| `TestTask117RealHTTPIdempotencyConflictsAndProviderScopedReplay` | Task 11.7: HTTP/OIDC real, replay por provider e conflitos sem novos efeitos. |
| `TestConsumerProcessesSameMessageGroupInBatchSequentially` | Ordem sequencial dentro do mesmo `MessageGroupId`. |
| `TestConsumerStopsMessageGroupAfterTransientFailure`, `TestConsumerStopsMessageGroupAfterPermanentFailure` | Item posterior do grupo não ultrapassa falha e é liberado. |
| `TestConsumerProcessesDifferentMessageGroupsInBatchConcurrently` | Paralelismo entre `MessageGroupId` distintos. |
| `TestPendingWorkerRecordsTerminalResult` | Resultado terminal e latência de referência retomada. |
| `TestTranslateErrorClassifiesPostgreSQLErrors` | SQLSTATE `23505`, classe 23, serialização e deadlock. |
| `TestTranslateErrorDoesNotClassifyOtherSQLStateClassesAsConstraints` | Classes 42/22 permanecem sem classificação de constraint ou retry SQL. |
| `TestDatabaseUnitOfWorkAndRepositories` | Criação, lease, reagendamento e confirmação da outbox calculados por `CURRENT_TIMESTAMP` PostgreSQL, inclusive sob evento com relógio adiantado. |
| `TestServiceFinancialWorkflow` | Skew de réplicas não toma lease nem expira referência cedo e não regride timestamp da carteira. |
| `TestFinancialFlowRollsBackAfterIntermediateIDFailure` | Falha intermediária reverte transação, saldo, ledger e outbox; retry posterior conclui. |
| `TestProcessSIGKILLBeforeCommitRollsBackAndRetries` | `pg_stat_activity` confirma transação pré-commit aberta; `SIGKILL` reverte tudo e o retry conclui uma única vez. |
| `TestPostgresOutageDuringOpenFinancialTransactionRollsBackAndRetries` | PostgreSQL para durante a transação aberta, o erro é transitório e uma nova instância recupera sem estado parcial. |
| `TestWalletClampsChangeTimestampFromBehindReplica`, `TestWagerTransactionTransitionClampsReplicaClockSkew` | Clamp monotônico no domínio para relógio atrasado. |
| `TestConsumerLogsSuccessfulProcessingAndDeletionWithoutFinancialPayload` | Log `INFO` após commit/delete com IDs seguros e sem payload financeiro. |
| `TestTransactionRetriesUseExistingMetricWithBoundedLabels` | `wager_retries_total` usa `worker="sql-transaction"` e resultados limitados. |

Comandos focados para as garantias adicionadas:

```bash
go test ./internal/httpapi \
  -run '^(TestWalletContractsAndCorrelation|TestTransactionStatusesReplayAndFailureCode|TestTransactionQueries)$' \
  -count=1
go test ./internal/bootstrap -run '^TestLoadMigration' -count=1
go test ./internal/application -run '^TestClassifyProcessingError$' -count=1
go test ./internal/postgres -run '^TestTranslateError' -count=1
go test ./internal/domain \
  -run '^(TestWalletClampsChangeTimestampFromBehindReplica|TestWagerTransactionTransitionClampsReplicaClockSkew)$' \
  -count=1
go test ./internal/observability \
  -run '^TestTransactionRetriesUseExistingMetricWithBoundedLabels$' -count=1
go test ./internal/messaging \
  -run '^(TestAWSSDKConfigUsesEnvironmentCredentialChain|TestAWSSDKConfigUsesSharedCredentialsProfile|TestAWSSDKConfigAcceptsAlternativeProvider|TestAWSSDKConfigReportsMissingCredentials|TestConsumerRetriesCommitFailureAndDeletesDurableFailedReplay|TestConsumerPostCommitInterruptionReplaysFailedWithoutMovement|TestConsumerProcessesSameMessageGroupInBatchSequentially|TestConsumerStopsMessageGroupAfterTransientFailure|TestConsumerStopsMessageGroupAfterPermanentFailure|TestConsumerProcessesDifferentMessageGroupsInBatchConcurrently|TestConsumerLogsSuccessfulProcessingAndDeletionWithoutFinancialPayload|TestPendingWorkerRecordsTerminalResult)$' \
  -count=1
go test -tags=integration ./integration \
  -run '^(TestMigrationsConstraintsLedgerImmutabilityAndAtomicRollback|TestFinancialInvariantsRejectDirectSQLViolations|TestFinancialInvariantsMigrationRuntimeRoleRoundTrip|TestStartupRequiresCurrentMigrationVersionAndEssentialObjects|TestPendingReferenceWinRemainsRehydratableAcrossMigrationDownAndUp|TestMigrationOwnerAndRuntimePermissions|TestFinancialFlowRollsBackAfterIntermediateIDFailure|TestProcessSIGKILLBeforeCommitRollsBackAndRetries|TestPostgresOutageDuringOpenFinancialTransactionRollsBackAndRetries|TestTask117WalletOpeningPositiveZeroAndConcurrentDuplicate|TestTask117PendingReferenceContinuesAfterApplicationRestart|TestTask117RealHTTPIdempotencyConflictsAndProviderScopedReplay)$' \
  -count=1
go test -tags=integration ./internal/postgres \
  -run '^TestDatabaseUnitOfWorkAndRepositories$' -count=1
go test -tags=integration ./internal/application \
  -run '^TestServiceFinancialWorkflow$' -count=1
go test -tags=integration ./internal/application \
  -run '^TestPermanentPendingFailureLifecycle$' -count=1
```

O smoke de containers completos é executado separadamente:

```bash
make smoke
```

Ele usa as três portas publicadas pelo Compose e faz asserções HTTP, SQL e SQS
antes e depois de `SIGTERM` e reinício total com volumes preservados.

### Schema e roles PostgreSQL

`TestStartupRequiresCurrentMigrationVersionAndEssentialObjects` usa o catálogo
completo e verifica schema ausente, versão atrasada, versão futura, nome de
migration incorreto, linha histórica ausente, versão atual e objeto essencial
ausente. A validação exige a sequência completa de versões e nomes, não apenas a
maior versão.

`TestMigrationOwnerAndRuntimePermissions` conecta separadamente como owner e
runtime. O teste confirma ownership da migration, executa abertura, cinco tipos
externos, inbox, outbox, referência pendente, consultas e reconciliação pela
runtime e exige SQLSTATE `42501` para `CREATE TABLE`, `ALTER TABLE`, `UPDATE`,
`DELETE`, `TRUNCATE` do ledger e escrita em `schema_migrations`. O fixture
preprovisiona banco/schema/roles como um PostgreSQL externo e passa
`options=-c%20app.application_role%3D<runtime_role>` na conexão de migration;
sem a role existente e essa opção, os grants não são direcionados ao runtime.

`TestFinancialInvariantsRejectDirectSQLViolations` cobre a migration 3 com SQL
direto: vínculo diferido entre transação e carteira/jogador/moeda, estado/tipo,
valor e direção do ledger, identidade da carteira, identidade financeira, estado
terminal, hash da inbox e envelope da outbox. O teste de round-trip confirma que
o `down` restaura exatamente o comportamento e os grants da versão 2 e que novo
`up` reaplica constraints, triggers e grants por coluna da versão 3.

`TestPendingReferenceWinRemainsRehydratableAcrossMigrationDownAndUp` cria dados
na versão atual, executa um `down` e confirma que um `WIN` pendente vira
`REJECTED/REFERENCE_NOT_FOUND`, sem referência incompatível com a versão 1. Após
novo `up`, o mesmo registro continua reidratável.

### SQLSTATE, relógio e observabilidade

`TestTranslateErrorClassifiesPostgreSQLErrors` limita constraints à classe 23 e
mantém tratamento específico para unicidade, serialização e deadlock.
`TestTranslateErrorDoesNotClassifyOtherSQLStateClassesAsConstraints` prova que
`42501`, `42P01` e `22P02` preservam o erro original: não viram constraint nem
retry SQL. Nas fronteiras de processamento, erro desconhecido continua
transitório e não terminaliza trabalho como falha permanente.

`TestDatabaseUnitOfWorkAndRepositories` verifica que outbox calcula elegibilidade
e `claim_until` com `CURRENT_TIMESTAMP` do PostgreSQL. O trecho de skew em
`TestServiceFinancialWorkflow` prova que relógios locais 24 horas atrasado ou
adiantado não retomam lease nem expiram referência prematuramente; os testes de
domínio de carteira e transação provam clamp de timestamps atrasados para manter
`updated_at` monotônico.

O sucesso SQS é observável pelo log `INFO` `SQS message processed and deleted`
após commit e delete, com somente IDs seguros. Retry transacional incrementa
`wager_retries_total` com `worker="sql-transaction"` e `result="deadlock"` ou
`result="serialization_failure"`; razões fora da lista convergem para
`result="unknown"`.
O worker de referências também registra status/failure code terminal em
`RecordTransactionResult`, latência em `ObserveProcessingLatency` e loga somente
`transactionId` e status, sem payload financeiro.

### Falha permanente de trabalho persistido

`TestClassifyProcessingError` cobre a classificação conservadora.
`TestPermanentPendingFailureLifecycle`, com PostgreSQL real, provoca erro
permanente durante a retomada, confirma `FAILED/PERMANENT_PROCESSING_FAILURE`,
limpeza de agendamento/claim, saldo, versão, ledger e outbox inalterados, replay
estável após restart, rollback recuperável e convergência entre duas instâncias.

Os testes de transporte do consumidor usam broker e processor falsos para provar
a ordem de delete/retry. Eles não substituem um cenário end-to-end com SQS real e
commit SQL real de `FAILED`.

## Evidência operacional

Além das asserções embutidas nos testes, registre em uma execução manual com
Compose, sem secrets:

- project/run ID, IDs de réplica e timestamps monotônicos;
- request/message/transaction/wallet/event IDs;
- status HTTP, estado/failure code e quantidade de tentativas;
- snapshots SQL antes/depois e atributos/contagens das filas;
- instante de entrada/saída do trecho sob lock para medir sobreposição;
- ID do container interrompido e comando de falha executado.

Asserções devem consultar PostgreSQL diretamente além da resposta HTTP. Uma
resposta isolada não prova atomicidade nem ausência de duplicação.

## Seams determinísticos

Os seams são callbacks injetados diretamente pelos testes e ficam `nil` no
grafo normal; não há variável de ambiente capaz de ativá-los em produção.

| Seam | Posição exata | Teste |
|---|---|---|
| `ConsumerOptions.AfterCommitBeforeDelete` | Depois do commit de inbox/domínio/outbox, antes de `DeleteMessage`. | Interrompe shutdown, aguarda visibility e observa segunda entrega. |
| `OutboxOptions.BeforePublish` | Depois do claim persistido, antes de `SendMessage`. | Deixa lease expirar para o segundo publisher. |
| `OutboxOptions.AfterPublishBeforeMark` | Depois de `SendMessage`, antes de `published_at`. | Observa duas tentativas com envelope e `eventId` idênticos. |

Rollback pré-commit é induzido por violação de constraint dentro de transação
real. Claims de referência são exercitados diretamente com dois owners e lease,
sem callback de pausa específico.

Execute apenas os testes de fronteira:

```bash
./scripts/failure-scenarios.sh deterministic-failpoints
```

## Oráculos comuns

Depois de cada cenário, verificar:

```sql
-- Uma identidade financeira por provedor.
SELECT provider_id, external_transaction_id, count(*)
FROM wager_transactions
WHERE provider_id IS NOT NULL
GROUP BY provider_id, external_transaction_id
HAVING count(*) <> 1;

-- No máximo um ledger por carteira/transação.
SELECT wallet_id, transaction_id, count(*)
FROM wallet_ledger_entries
GROUP BY wallet_id, transaction_id
HAVING count(*) <> 1;

-- Saldo nunca negativo.
SELECT id, balance_minor FROM wallets WHERE balance_minor < 0;

-- Equação de cada lançamento.
SELECT id
FROM wallet_ledger_entries
WHERE (direction = 'CREDIT' AND balance_after_minor <> balance_before_minor + amount_minor)
   OR (direction = 'DEBIT' AND balance_after_minor <> balance_before_minor - amount_minor);

-- Outbox pendente sem claim válido após o prazo de convergência.
SELECT event_id, attempt_count, claimed_by, claim_until
FROM outbox_events
WHERE published_at IS NULL;
```

As quatro primeiras consultas devem retornar zero linhas. A última deve convergir
para zero depois da recuperação, salvo cenário que mantém SQS indisponível.

Reconciliação final de uma carteira também deve provar:

```sql
SELECT
  w.id,
  w.balance_minor AS stored_minor,
  COALESCE(sum(CASE l.direction
    WHEN 'CREDIT' THEN l.amount_minor
    WHEN 'DEBIT' THEN -l.amount_minor
  END), 0) AS calculated_minor,
  count(l.id) AS entry_count
FROM wallets w
LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
WHERE w.id = '<wallet-uuid>'
GROUP BY w.id, w.balance_minor;
```

`stored_minor` deve ser igual a `calculated_minor` e ao resultado do endpoint de
reconciliação.

## Cenário 1: cinquenta duplicatas HTTP/SQS

**Preparação:** carteira `100.00 BRL`; um comando `BET 25.00`, uma chave de
idempotência e uma identidade externa. Produzir exatamente o mesmo negócio por
25 requests HTTP e 25 mensagens SQS em paralelo, distribuídos entre as três
réplicas. Transporte não entra no hash canônico.

**Asserções automatizadas:**

- uma linha em `wager_transactions` para a identidade externa;
- estado `PROCESSED`, um ledger de débito e saldo `75.00`;
- replays HTTP informam `idempotentReplay=true` e saldo histórico `75.00`;
- 25 inbox concluídas, uma por `messageId`, sem reaplicar o efeito;
- as 25 respostas HTTP são `200`, sem conflito para conteúdo equivalente;
- exatamente um `WagerTransactionProcessed` e um `WalletBalanceChanged` para a
  aposta duplicada;
- reconciliação final consistente.

Esse cenário está automatizado. Repeti-lo após `full-restart` continua sendo um
smoke operacional útil para os containers Compose.

## Cenário 2: duas apostas concorrentes de 80

**Preparação:** carteira `100.00 BRL`; duas `BET 80.00` com identidades e chaves
distintas. Soltar uma barreira para duas réplicas iniciarem simultaneamente.

**Asserções automatizadas:**

- exatamente uma `PROCESSED` e uma `REJECTED/INSUFFICIENT_FUNDS`;
- saldo final `20.00`, um único débito de `80.00` e reconciliação consistente;
- as instâncias usam pools independentes, provando coordenação no banco.

Contagens dos três eventos esperados na outbox são um oráculo operacional
adicional; esse teste full-stack não as consulta diretamente.

## Cenário 3: carteiras independentes

O teste abre uma transação SQL que mantém `FOR UPDATE` na carteira A e inicia uma
operação concorrente nela. Enquanto A permanece comprovadamente bloqueada, uma
terceira instância conclui a aposta na carteira B em até dois segundos. Depois do
commit do lock externo, A também conclui; ambas terminam em `90.00` e reconciliam.

## Cenário 4: rollback antes do commit

Além do rollback SQL genérico de
`TestMigrationsConstraintsLedgerImmutabilityAndAtomicRollback`, o teste
`TestFinancialFlowRollsBackAfterIntermediateIDFailure` injeta falha depois da
criação da transação e antes do ledger/outbox. Ele confirma que saldo, transação,
ledger e eventos não ficaram parciais e repete o comando com sucesso.

`TestProcessSIGKILLBeforeCommitRollsBackAndRetries` acrescenta a prova física. Um
subprocesso compilado somente na suíte de integração pausa nessa fronteira, e o
processo controlador exige uma sessão `idle in transaction` com a
`application_name` esperada em `pg_stat_activity` antes de enviar `SIGKILL`.
Outra conexão confirma ausência de saldo, transação, ledger e outbox parciais; o
mesmo comando então conclui uma única vez.

## Cenário 5: crash após commit e antes do delete SQS

**Preparação:** callback `ConsumerOptions.AfterCommitBeforeDelete` em uma mensagem
válida.

**Execução automatizada:** após sinal do seam, verificar o commit externamente e
forçar o timeout de shutdown do primeiro consumidor. Depois do visibility
timeout, um segundo consumidor recebe a mensagem.

**Asserções:**

- mensagem é entregue novamente (`ApproximateReceiveCount >= 2`);
- inbox permanece única e concluída;
- uma transação e um ledger;
- exatamente um `WagerTransactionProcessed` e um `WalletBalanceChanged` da
  operação, antes e depois da reentrega;
- saldo muda uma vez;
- reentrega é apagada e a reconciliação permanece consistente.

`kill-one` sem o seam não prova essa janela porque não sabe em que instrução o
processo estava; ele permanece útil para recuperação operacional geral.

## Cenário 6: publishers concorrentes e recuperação

**Preparação automatizada:** dois publishers e um evento isolado por subteste.

**Disputa normal:** os dois publishers entram simultaneamente numa barreira de
claim. Exatamente um obtém o evento e entra em publicação, com concorrência
máxima observada igual a um. Após sua interrupção, o outro recupera o lease,
publica o `eventId` original e converge para `published_at`.

**Antes do publish:** usar `OutboxOptions.BeforePublish`, expirar o shutdown do
primeiro publisher e aguardar `claim_until`. Outra instância deve publicar
exatamente um envelope com o `eventId` original.

**Depois do publish:** usar `OutboxOptions.AfterPublishBeforeMark`, capturar a
primeira tentativa, expirar o shutdown do publisher e aguardar o lease. A
republicação gera duas tentativas observáveis no teste; ambas têm bytes de
envelope equivalentes e o mesmo `eventId`. Downstream deduplica por esse ID.

**Destino indisponível:**

```bash
./scripts/failure-scenarios.sh outage localstack 30
```

Durante a queda, `attempt_count`/`next_attempt_at` devem avançar sem perder o
registro. Após recuperação, todas as outboxes devem ser publicadas.

## Cenário 7: referências fora de ordem

**Chegada posterior:** enviar `REFUND` antes da `BET` referenciada. Confirmar
`PENDING_REFERENCE` e evento de pendência; enviar a `BET`; o worker deve processar
o refund uma vez e restaurar o saldo.

**Referência ainda pendente:** referência em `PENDING_REFERENCE` deve reagendar a
dependente sem terminalizá-la.

**Referência malsucedida:** referência `REJECTED`/`FAILED` deve rejeitar a
dependente com `REFERENCE_NOT_ELIGIBLE`.

**Expiração:** em ambiente isolado, reduzir tentativas/TTL a valores de teste,
não criar a referência e confirmar `REJECTED/REFERENCE_NOT_FOUND`, evento de
rejeição e nenhum ledger.

Os testes de aplicação também provam que um segundo owner não obtém claim ativo.
Referência ainda pendente e referência terminal malsucedida são percorridas pela
suíte full-stack, além de chegada posterior e expiração por TTL.
Morte de um worker de referência real pode ser exercitada com `kill-one`; não há
seam dedicado para pausar exatamente depois desse claim.

## Cenário 8: DLQ e mensagens inválidas

Testes unitários verificam que envelope inválido e conflito permanente não são
apagados nem têm visibility alterada pelo consumidor, e que a métrica DLQ cresce
quando `ReceiveCount` alcança o `MaxReceiveCount` configurado. O unitário e o
teste físico usam `2` para reduzir a duração; o default Compose é `5`.
`make verify-infra` valida a policy local e
`TestInvalidMessageIsPhysicallyRedrivenAndReceivedFromDLQ` observa a mensagem na
DLQ real.

## Ordem FIFO por grupo

O envelope SQS exige `playerId` e `walletId` UUID. Em um batch, o consumidor
agrupa por `MessageGroupId`:
`TestConsumerProcessesSameMessageGroupInBatchSequentially` bloqueia a primeira
mensagem e prova que a segunda do mesmo grupo não inicia;
`TestConsumerStopsMessageGroupAfterTransientFailure` e
`TestConsumerStopsMessageGroupAfterPermanentFailure` provam que falha sem delete
interrompe o grupo e libera o receipt posterior com visibilidade zero;
`TestConsumerProcessesDifferentMessageGroupsInBatchConcurrently` bloqueia dois
grupos e prova que ambos iniciam antes da liberação.

## Cobertura da task 11.7

Os três testes `TestTask117*` completam os caminhos de aceitação: abertura de
carteira positiva e zero com efeitos exatos, disputa de doze aberturas iguais,
continuidade de referência pendente após recriar serviço/pool e idempotência HTTP
real com escopo por provider. Cada cenário consulta diretamente transações,
ledger, outbox, saldo e reconciliação.

## Cenário 9: autenticação e isolamento

- sem token, token inválido/expirado: `401`, sem SQL;
- `provider-a` operando/consultando `provider-b`: `403`, sem revelar existência;
- provider em `/wallets`, ledger ou reconciliação: `403`;
- `internal-service` em endpoints internos: permitido;
- issuer/audiência incorretos e falhas de verifier são cobertos em unitário;
- logs não contêm JWT, secret ou payload financeiro completo.

## Cenário 10: shutdown e reinício total

Durante requests e polling ativos:

```bash
./scripts/failure-scenarios.sh term-one
```

Confirmar que a réplica fica não pronta, para polling, drena dentro de
`SHUTDOWN_TIMEOUT` e conclui ou libera o trabalho. Não pode haver perda ou efeito
financeiro duplicado.

Depois:

```bash
./scripts/failure-scenarios.sh full-restart
```

O teste Fx confirma start/stop e fechamento do pool. Persistência após reinício
dos três containers continua como roteiro operacional: confirmar carteiras,
transações, inbox/outbox, retomada de claims e reconciliação.

## Falhas de dependência

PostgreSQL:

```bash
./scripts/failure-scenarios.sh outage postgres 20
```

Readiness deve degradar; liveness deve continuar enquanto o processo responde;
HTTP retorna `503`/`Retry-After`; SQS não é deletado; nenhuma escrita parcial.
`TestPostgresOutageDuringOpenFinancialTransactionRollsBackAndRetries` automatiza
o oráculo financeiro: abre a transação na mesma fronteira pré-commit, para o
container PostgreSQL dedicado, exige erro transitório, reinicia-o e verifica por
uma nova instância que não houve estado parcial antes do retry único.

Emulator SQS local (serviço Compose `localstack`):

```bash
./scripts/failure-scenarios.sh outage localstack 20
```

Readiness degrada; outbox acumula com backoff; fatos financeiros já confirmados
permanecem; publicação converge após recuperação.

Keycloak:

```bash
./scripts/failure-scenarios.sh outage keycloak 20
```

Tokens já emitidos e chave em cache continuam validáveis até expirar a política
de cache. Readiness não deve depender do IdP nesse caso; rotação/chave
desconhecida falha fechada.

## IAM SQS

`scripts/verify-sqs-iam.sh` usa credenciais de produtor em
`/var/lib/localstack/producer-credentials/credentials` e exige sucesso de
`producer SendMessage` na entrada,
`runtime ReceiveMessage/DeleteMessage` na entrada e `runtime SendMessage` na
saída. Também exige `AccessDenied` para consumo e publicação cruzados entre os
profiles. Execute sem consumidores da aplicação concorrendo com a mensagem de
probe:

```bash
make deps
make verify-sqs-iam
```

O checkout limpo usa MiniStack 1.5.26 com `AUTH=true`. O emulator identifica os
principals pela access key e aplica as policies; o script sempre falha se uma
ação proibida for aceita. MiniStack não valida o secret ou a assinatura SigV4.

A prova completa de autenticação usa SQS real:

```bash
make verify-aws-sqs-iam
AWS_IAM_FULL_SUITE=1 make verify-aws-sqs-iam
AWS_IAM_FULL_SUITE=1 AWS_IAM_RACE=1 make verify-aws-sqs-iam
```

O profile default é `wager-free-tier`; a policy de bootstrap limitada está em
`infra/aws/wager-test-bootstrap-policy.json`. O script cria três filas FIFO
`wager-test-*`, obtém sessões STS de 15 minutos para runtime e producer, executa
chamadas permitidas e cinco negativas cruzadas e remove as filas no trap.

`TestAWSSQSLeastPrivilegeHasNoUnauthorizedFinancialEffects` processa primeiro um
comando permitido pela aplicação contra PostgreSQL real. Depois das negativas,
exige `AccessDenied` e confirma que as contagens de transação, inbox, ledger e
outbox permanecem idênticas.

## Evidência histórica de verificação

Execução de 2026-10-12, anterior ao default Community atual:

- formatação, tidy, módulos, vet, build, unitários e race passaram em
  `golang:1.25.0` via Docker;
- a integração completa com enforcement AWS passou, com `integration` em
  `65.772s`;
- a mesma matriz com race passou, com `integration` em `77.320s`;
- o teste focado AWS passou em `40.421s`;
- `make verify-isolated` passou em `157s`, incluindo volumes vazios, schema 2,
  três réplicas, profiles SQS, negativas PostgreSQL, smoke, `SIGTERM`, restart e
  reconciliação;
- todas as filas AWS temporárias e todos os containers/volumes do projeto
  Compose isolado foram removidos; a stack padrão não foi alterada.

Uma validação adicional em 2026-10-12, com o default Community 3.8.1, passou na
integração (`66.717s`), integração com race (`81.500s`) e no Compose isolado
(`142s`).

O fechamento da auditoria em 2026-10-12 repetiu formatação, tidy, módulos, vet,
build, unitários e race. A integração completa passou em `77.895s`, a integração
com race em `84.001s` e o Compose isolado em `138s`, incluindo schema 3 em banco
vazio, três réplicas, permissões PostgreSQL, smoke, `SIGTERM`, restart e
reconciliação. O gate AWS IAM real não foi reexecutado nessa rodada.

As evidências físicas finais em 2026-10-12 passaram juntas em `40.448s`: morte
real por `SIGKILL` na fronteira pré-commit e outage/restart de PostgreSQL durante
a transação aberta, ambas com rollback integral e retry único. O gate AWS IAM
real também foi reexecutado: as permissões positivas, cinco negativas e ausência
de efeitos financeiros passaram, com o pacote `integration` em `42.761s`; as
filas temporárias foram removidas pelo script. A matriz posterior passou em
formatação, tidy, módulos, vet, build, unitários e race; a integração completa
levou `71.125s`, a integração com race `71.071s` e o Compose isolado `133s`.
Scripts, configuração Compose, workflow e OpenSpec estrito também passaram.

O fechamento das lacunas remanescentes em 2026-10-12 passou com a outbox usando
o relógio PostgreSQL e MiniStack aplicando policies no checkout limpo. A matriz
de qualidade, unitários e race passou; a integração completa levou `69.043s`, a
integração com race `75.544s` e o Compose isolado `132s`, incluindo as negativas
IAM locais. O gate AWS real voltou a passar em `42.427s` e removeu suas filas.

Ambiente: Linux `7.2.9-2-cachyos` `x86_64`, Docker Engine `29.9.0`, Compose
`5.6.0`, Go `1.25.0`, PostgreSQL 16.4, Keycloak 25.0.6, LocalStack Pro 2026.3.0
e AWS SQS `us-east-1`.

As durações acima são somente evidência histórica, não previsão de uma nova
execução. A configuração atual usa MiniStack 1.5.26 com autorização por policy;
o gate AWS real complementa a prova com validação de secret e assinatura SigV4.
