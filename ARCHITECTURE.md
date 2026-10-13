# Arquitetura

## Escopo e estado

O sistema é projetado como um único serviço Go com componentes selecionáveis,
executado em múltiplos processos. PostgreSQL mantém a verdade financeira;
Keycloak fornece identidade OAuth 2.0/OIDC; SQS FIFO transporta comandos e
eventos com semântica at-least-once.

O serviço implementa domínio, casos de uso, persistência pgx, HTTP/OIDC,
mensageria durável, composição Fx, observabilidade, migrations versionadas e o
ambiente local. A suíte com Testcontainers cobre banco, IdP, SQS, concorrência e
janelas ambíguas de recuperação. A seção [Limitações](#limitações) registra
somente fronteiras que permanecem deliberadas ou ainda não têm cobertura
end-to-end equivalente à produção.

## Componentes

A estrutura mantém domínio e aplicação independentes de transporte:

- `internal/domain`: dinheiro, carteira, transação, ledger, regras e eventos;
- `internal/application`: comandos normalizados e casos de uso compartilhados;
- `internal/postgres`: unidade de trabalho e repositórios SQL explícitos;
- `internal/httpapi`: DTOs, middleware e handlers `net/http`;
- `internal/auth`: descoberta OIDC, JWKS e autorização;
- `internal/messaging`: SQS, inbox, outbox e referências pendentes;
- `internal/observability`: logs e métricas;
- `internal/platform`: relógio e UUID v7;
- `cmd/service`: composição e comandos de processo/migration.

Um único binário reduz imagens e configuração. `COMPONENTS` habilita
`http`, `sqs-consumer`, `outbox` e `pending-reference`, permitindo processos
especializados sem compartilhar memória.

## Dinheiro e limites

`Money` usa `int64` em unidades mínimas e escala fixa de duas casas. O limite
externo não negativo é `92233720368547758.07`; internamente diferenças podem
chegar a `-92233720368547758.08`. Parsing, soma, subtração e negação detectam
overflow, sem `float32`, `float64` ou arredondamento.

Entrada externa aceita `0` ou parte inteira seguida de ponto e exatamente duas
casas. Assim, `0`, `0.00` e `25.00` são representáveis; sinal negativo, vírgula,
`NaN`, `Infinity`, notação científica, fração com outra escala e espaços são
recusados. A serialização sempre emite duas casas, inclusive `0.00`.

`Currency` aceita somente `AUD`, `BRL`, `CAD`, `CHF`, `CNY`, `EUR`, `GBP`, `MXN`,
`NZD` e `USD`. A fronteira de transporte normaliza moeda para maiúsculas antes
da validação, e a migration `000002_sql_hardening` aplica a mesma lista no
banco. Todas usam escala fixa de duas casas, independentemente dos minor units
recomendados para cada moeda. Não há conversão cambial.

PostgreSQL persiste `amount_minor`/`balance_minor` como `BIGINT` e moeda como
`CHAR(3)`. Constraints impedem saldo negativo. A aplicação também verifica
moeda e overflow, oferecendo defesa em profundidade.

## Fronteiras das transações SQL

Cada comando financeiro usa uma transação PostgreSQL `READ COMMITTED`. A mesma
`pgx.Tx` alcança todos os repositórios envolvidos. Não ocorre chamada
OIDC ou SQS enquanto a transação financeira está aberta.

| Operação | Conteúdo do commit atômico |
|---|---|
| Abertura com saldo positivo | carteira, `OPENING` processada, ledger de crédito e duas outboxes; conflito de `(player_id,currency)` aborta tudo. |
| Abertura com saldo zero | somente carteira versão 1; não há `OPENING`, ledger ou evento financeiro. |
| Comando HTTP | claim idempotente/transação, lock da carteira quando aplicável, estado terminal ou pendente, saldo/versão, ledger aplicável e outbox. |
| Comando SQS | inbox, os mesmos registros do caso de uso HTTP e conclusão da inbox; o delete SQS acontece somente depois do commit. |
| Rejeição de negócio | transação `REJECTED`, `failure_code` e outbox de rejeição; sem alteração de carteira/ledger. |
| Referência ausente | transação `PENDING_REFERENCE`, agenda/TTL e outbox de pendência; a inbox pode ser concluída. |

Falha antes do commit não deixa estado parcial. Falha depois do commit não pode
desfazer o fato financeiro; inbox/outbox tornam confirmação de transporte e
publicação retomáveis.

Claims de outbox e referência usam transações curtas separadas: selecionam um
lote com `FOR UPDATE SKIP LOCKED`, gravam `claimed_by`/`claim_until` e fazem
commit. Espera, backoff e I/O SQS ocorrem sem lock SQL aberto. A marcação de
sucesso exige que a instância ainda possua o claim.

Elegibilidade, expiração, agendamento, backoff, confirmação e deadlines de leases
usam `CURRENT_TIMESTAMP` do PostgreSQL, inclusive `next_attempt_at`,
`published_at` e `claim_until`; relógios de réplicas não podem antecipar nem
atrasar o trabalho da outbox. Timestamps produzidos no domínio continuam UTC e,
quando o relógio de uma réplica está atrasado, carteira e transação fazem clamp
para o último `updated_at`, mantendo a sequência monotônica.

Reconciliação é somente leitura em `REPEATABLE READ`, para ler carteira e
soma/contagem do ledger na mesma visão. Ela relata divergência, mas nunca corrige
saldo automaticamente.

Retries transacionais são limitados a deadlock/serialização: três tentativas por
padrão, backoff de `50ms` até `1s`, com respeito ao cancelamento. Erros de domínio
e constraints determinísticas não são repetidos. Na tradução PostgreSQL,
`23505` é conflito, os demais SQLSTATE da classe `23` são violações de
constraint, e somente `40001`/`40P01` são repetíveis pela transação. Outros
SQLSTATE, salvo o cancelamento `57014`, e erros de banco desconhecidos preservam
a causa e permanecem transitórios para as fronteiras HTTP/SQS, sem serem
convertidos em falha permanente.

## Locks e concorrência

Depois de resolver idempotência e dependências, operações que alteram ou validam
saldo fazem `SELECT ... FOR UPDATE` na linha da carteira. Sob o lock:

- o agregado é reidratado e valida associação, moeda, saldo e reversão;
- saldo e versão são atualizados com a versão lida;
- ledger e outbox são inseridos na mesma transação;
- `LOSS` valida a carteira, mas não muda saldo, versão ou ledger.

Cada operação financeira trava uma única carteira. Não há mutex de processo nem
lock global; carteiras diferentes usam linhas diferentes e podem avançar em
paralelo. Unique constraints resolvem corridas de carteira, idempotência,
identidade externa, inbox, ledger e reversão. A constraint de saldo não negativo
é a última barreira contra lost update ou erro de aplicação.

## Identidade, idempotência e hash

Operações externas são únicas por `(provider_id,idempotency_key)` e por
`(provider_id,external_transaction_id)`. O primeiro recebimento insere estado
`PENDING`. Em colisão, o hash persistido é comparado:

- mesma chave e mesmo hash: replay do resultado persistido, incluindo o saldo
  observado na conclusão original;
- mesma chave e hash diferente: conflito;
- mesma identidade externa com outra chave: conflito.

O hash é SHA-256 sobre JSON UTF-8 de um struct fechado, com chaves
lexicograficamente ordenadas nesta ordem:

1. `externalTransactionId`;
2. `gameId`;
3. `kind`;
4. `money`, com `amount` antes de `currency`;
5. `playerId`;
6. `providerId`;
7. `referenceExternalTransactionId`, omitido quando ausente;
8. `roundId`;
9. `walletId`.

Normalização: `kind` e moeda vão para maiúsculas; dinheiro sempre tem duas casas;
identificadores não podem estar vazios nem ter espaços externos e **não** são
trimados; a referência segue a mesma política. `Idempotency-Key`, `messageId`,
timestamps, correlação e metadados de transporte não entram no hash. HTTP e SQS
devem construir o mesmo `TransactionCommand`, evitando hashes diferentes para o
mesmo negócio.

## Máquina de estados

```text
PENDING ---------> PROCESSED
   |--------------> REJECTED
   |--------------> FAILED
   |
   +--> PENDING_REFERENCE --> PROCESSED
                           --> REJECTED
                           --> FAILED
```

`PROCESSED`, `REJECTED` e `FAILED` são terminais e imutáveis. Regras financeiras
definitivas produzem `REJECTED`. Indisponibilidade transitória causa rollback e
retry. `FAILED` é reservado a erro técnico determinístico classificado como
permanente durante a retomada de trabalho já persistido e usa
`PERMANENT_PROCESSING_FAILURE`; ele não representa rejeição de negócio. Erro
desconhecido é tratado como transitório e nunca terminaliza automaticamente.

Quando `ResumePendingReference` encontra erro permanente, a transação de
processamento é revertida. Uma transação curta bloqueia o registro por ID,
confirma que ele ainda está em estado não terminal, persiste `FAILED`, limpa
agendamento e claim e faz commit sem alterar carteira, versão, ledger ou outbox
de sucesso. Falha nesse commit mantém o item pendente e recuperável; concorrentes
convergem pelo row lock e pelo estado terminal idempotente.

Códigos financeiros estáveis:

| Código | Significado |
|---|---|
| `INSUFFICIENT_FUNDS` | `BET` excede o saldo. |
| `REVERSAL_INSUFFICIENT_FUNDS` | `ROLLBACK` debitador excede o saldo. |
| `CURRENCY_MISMATCH` | Moeda diverge da carteira/referência. |
| `WALLET_MISMATCH` | Referência pertence a outra carteira. |
| `REFERENCE_NOT_FOUND` | Referência não apareceu antes de tentativas/TTL. |
| `REFERENCE_PENDING` | Referência existe, mas ainda não é terminal; condição reagendável. |
| `REFERENCE_NOT_ELIGIBLE` | Tipo ou estado terminal da referência não é elegível. |
| `REFERENCE_MISMATCH` | Provedor, jogador, rodada, identidade ou valor diverge. |
| `ALREADY_REVERSED` | Efeito já foi compensado. |
| `INVALID_AMOUNT` | Política de valor/zero inválida. |
| `PERMANENT_PROCESSING_FAILURE` | Infraestrutura permanentemente malsucedida; único código de `FAILED`. |

## Operações e reversões

- `OPENING`: somente interno, crédito do saldo inicial positivo.
- `BET`: valor positivo, débito; saldo insuficiente rejeita.
- `WIN`: valor positivo, crédito.
- `LOSS`: exatamente `0.00`, declarativa, sem ledger ou alteração de versão.
- `REFUND`: somente uma `BET` processada, crédito integral.
- `ROLLBACK`: movimento oposto de `BET`, `WIN` ou `REFUND` processado.

Reversão e referência devem coincidir integralmente em provedor, jogador,
carteira, moeda, rodada e valor. A referência é resolvida por
`(providerId,referenceExternalTransactionId)`.

Uma `BET` aceita exatamente uma devolução direta, `REFUND` **ou** `ROLLBACK`. Uma
`WIN` aceita um `ROLLBACK`. Um `REFUND` aceita um `ROLLBACK`, que debita o crédito
devolvido. Não há cadeias além desse ponto. Unique index parcial sobre
`reference_transaction_id` processado, lock da carteira e leitura da referência
na transação impedem crédito duplicado entre instâncias.

## Referências pendentes

Uma reversão cuja referência não existe ou ainda não é terminal persiste como
`PENDING_REFERENCE`; ela não fica presa à mensagem ou à memória da instância.
São gravados `reference_attempts`, `reference_next_attempt_at` e
`reference_expires_at`, além do evento de pendência.

Workers disputam lotes com `FOR UPDATE SKIP LOCKED`, lease recuperável, backoff
exponencial e jitter limitado. Defaults: 12 tentativas, base `1s`, teto `5m` e
TTL `24h`. Referência processada e elegível retoma a operação; referência
`REJECTED`/`FAILED` gera `REFERENCE_NOT_ELIGIBLE`; tentativas ou TTL esgotados
geram `REFERENCE_NOT_FOUND`. Reinício ou morte do worker apenas deixa o lease
expirar para outra instância.

## Inbox, outbox e janelas de crash

### Entrada e inbox

O consumidor valida `WagerTransactionRequested`, cria o comando canônico e
insere `(consumer_name,message_id,payload_hash)`. Inbox, domínio e outbox
compartilham a transação. Delete da mensagem ocorre somente após commit.

| Janela | Consequência e recuperação |
|---|---|
| Crash antes do commit | PostgreSQL desfaz tudo; mensagem reaparece após visibility timeout. |
| Crash depois do commit e antes do delete | Mensagem reaparece; mesma inbox/hash concluída é reconhecida e não reaplica efeito. |
| Mesmo `messageId`, outro hash | Falha permanente; processamento original permanece intacto e a mensagem segue política de DLQ. |
| Reversão pendente confirmada | Inbox pode concluir e mensagem ser apagada; worker de referência assume continuidade. |

### Saída e outbox

O envelope completo e imutável é inserido no mesmo commit do fato. Publishers
fazem claim curto, publicam fora da transação e marcam sucesso condicionalmente.

| Janela | Consequência e recuperação |
|---|---|
| Crash depois do claim, antes do publish | Outro publisher assume após `claim_until`. |
| SQS indisponível | Tentativa, erro sanitizado e próximo envio são persistidos com backoff calculado sobre o relógio PostgreSQL. |
| Crash depois do publish, antes de marcar | Evento pode ser republicado com o mesmo `eventId`. Não há perda, mas pode haver duplicata. |

SQS não participa de 2PC, portanto exatamente uma publicação não é prometida.
Consumidores downstream devem deduplicar por `eventId`.

## SQS, retry e DLQ

Entrada usa `wager-transactions.fifo`, saída usa `wager-events.fifo` e falhas de
entrada esgotadas vão para `wager-transactions-dlq.fifo`. Defaults locais:
visibility `60s`, long poll `20s`, máximo de 5 recebimentos, retenção de 4 dias
nas filas ativas e 14 dias na DLQ.

O parser de entrada usa JSON estrito com quatro campos no topo:
`messageId`, `type="WagerTransactionRequested"`, `occurredAt` em RFC 3339 UTC e
`data`. `data` contém `idempotencyKey` e os mesmos campos financeiros do HTTP.
`playerId` e `walletId` devem ser UUIDs.
`eventType`, `version`, campos desconhecidos ou mais de um valor JSON são
rejeitados permanentemente e deixados para o redrive do broker.

Produtor de entrada usa `walletId` como `MessageGroupId` e `messageId` como
`MessageDeduplicationId`. Outbox usa `aggregateId` e `eventId`, respectivamente.
Agrupamento reduz disputa por carteira e deduplicação reduz reentrega, mas nenhum
dos dois substitui locks, inbox ou idempotência persistente.
Dentro de cada lote, o consumidor preserva a ordem FIFO processando
sequencialmente todas as mensagens de um mesmo `MessageGroupId`; grupos
diferentes são executados em paralelo. Qualquer item que não termine com delete
confirmado interrompe seu grupo; itens posteriores já recebidos são liberados
com visibilidade zero e não ultrapassam o item em retry/redrive.

Falha transitória altera visibilidade com backoff e mantém a mensagem. Envelope
inválido ou conflito de hash é permanente, mas ainda usa tentativas limitadas e
redrive para preservar evidência na DLQ. O bootstrap local cria usuários e
policies distintas: `runtime` recebe, altera visibilidade e apaga somente na
entrada, consulta as três filas e publica somente na saída; `producer` consulta
e publica somente na entrada. As credenciais geradas ficam em volumes Docker
separados. A aplicação monta somente o arquivo runtime em
`/run/secrets/aws/credentials`; exemplos de produtor dentro do emulator usam
`/var/lib/localstack/producer-credentials/credentials`. O Compose usa MiniStack
1.5.26 com `AUTH=true`: access keys identificam os principals e as policies são
efetivamente aplicadas. MiniStack não valida o secret ou a assinatura SigV4; a
prova completa permanece no gate SQS real, com filas temporárias e sessões STS
separadas. Ambos confirmam `AccessDenied`; o gate AWS também comprova ausência de
alteração de transação, inbox, ledger ou outbox.

## OIDC e autorização

Keycloak local oferece três clientes confidenciais em `client_credentials`.
Tokens de provedores carregam `provider_id`, scopes/roles `wager:read` e
`wager:write`; o cliente interno carrega `wallet:internal` e não tem
`provider_id`.

`go-oidc` descobre o issuer no startup e seu `RemoteKeySet` mantém cache JWKS,
refazendo a consulta quando encontra uma chave desconhecida. O verifier valida
assinatura assimétrica, `iss`, `aud` e `exp`; o middleware valida scopes/roles e
falha fechado. Readiness consulta PostgreSQL e SQS, não o IdP; uma instância nova
ainda depende do Keycloak para discovery no startup.

O `providerId` do path/corpo precisa ser igual ao claim. Provider não acessa
carteira, ledger, reconciliação nem transação de outro provider. Endpoints
internos exigem `wallet:internal`. Autenticação/autorização deve ocorrer antes de
parsing financeiro e acesso ao banco sempre que possível, evitando efeitos e
side channels. SQS usa profiles destinados a credenciais restritas e o comando
ainda passa por todas as regras de domínio.

## API, erros e códigos HTTP

`net/http` com `ServeMux` moderno aplica limite de corpo de 1 MiB,
`DisallowUnknownFields`, rejeição de JSON extra, correlação e recuperação de
panic. Erro público estável:

```json
{"code":"STABLE_CODE","message":"mensagem segura","details":{}}
```

`details` é opcional e nunca contém token, credencial, SQL, stack trace ou
payload financeiro completo.

| HTTP | Uso |
|---:|---|
| `200` | Processamento, replay e consultas. |
| `201` | Carteira criada. |
| `202` | Reversão persistida como `PENDING_REFERENCE`. |
| `400` | Header, JSON, identificador, tipo ou dinheiro inválido; não cria transação. |
| `401` | Token ausente, inválido ou expirado. |
| `403` | Scope/role insuficiente ou `provider_id` divergente. |
| `404` | Recurso visível inexistente. |
| `409` | Carteira/identidade/chave conflitante ou rejeição financeira síncrona. |
| `413` | Corpo acima do limite de 1 MiB (`REQUEST_BODY_TOO_LARGE`). |
| `500` | Panic recuperado como `INTERNAL_ERROR`, sem detalhes internos. |
| `503` | Infraestrutura transitória, com `Retry-After`. |

Rejeições persistidas retornam `status=REJECTED` e `failureCode`. Códigos de
validação de transporte são separados dos códigos financeiros acima.

## Contratos de eventos

Todos os eventos estão na versão 1 e usam este envelope:

```json
{
  "eventId": "uuid-v7",
  "eventType": "WagerTransactionProcessed",
  "aggregateId": "uuid",
  "correlationId": "string",
  "causationId": "string-opcional",
  "occurredAt": "2026-10-10T12:00:00Z",
  "version": 1,
  "data": {}
}
```

`occurredAt` é RFC 3339 UTC. O JSON completo é snapshot imutável na outbox.

### `WagerTransactionProcessed`

`aggregateId=transactionId`.

```json
{
  "transactionId": "uuid",
  "providerId": "provider-a",
  "externalTransactionId": "ext-1",
  "walletId": "uuid",
  "playerId": "11111111-1111-4111-8111-111111111111",
  "kind": "BET",
  "money": {"amount": "25.00", "currency": "BRL"},
  "resultBalance": {"amount": "75.00", "currency": "BRL"}
}
```

`providerId` e `externalTransactionId` são omitidos para `OPENING`.

### `WagerTransactionRejected`

`aggregateId=transactionId`.

```json
{
  "transactionId": "uuid",
  "providerId": "provider-a",
  "externalTransactionId": "ext-1",
  "walletId": "uuid",
  "kind": "BET",
  "failureCode": "INSUFFICIENT_FUNDS"
}
```

### `WagerTransactionPendingReference`

`aggregateId=transactionId`.

```json
{
  "transactionId": "uuid",
  "providerId": "provider-a",
  "externalTransactionId": "ext-refund-1",
  "walletId": "uuid",
  "referenceExternalTransactionId": "ext-bet-1"
}
```

### `WalletBalanceChanged`

`aggregateId=walletId`; emitido somente quando o saldo muda.

```json
{
  "walletId": "uuid",
  "transactionId": "uuid",
  "direction": "DEBIT",
  "money": {"amount": "25.00", "currency": "BRL"},
  "balanceBefore": {"amount": "100.00", "currency": "BRL"},
  "balanceAfter": {"amount": "75.00", "currency": "BRL"},
  "walletVersion": 2
}
```

Toda operação processada, inclusive `LOSS`, emite
`WagerTransactionProcessed`; somente mudanças de saldo emitem também
`WalletBalanceChanged`. Rejeição definitiva e pendência emitem seus eventos
específicos.

## Uber Fx e lifecycle

Fx compõe configuração, plataforma, persistência, aplicação, HTTP, autenticação,
SQS e observabilidade. O grafo é validável sem iniciar I/O. Startup valida toda a
configuração, abre o pool, compara a maior versão aplicada em `schema_migrations`
com a maior versão do catálogo, verifica as tabelas essenciais e inicia somente
os componentes selecionados por `COMPONENTS`. Schema ausente, atrasado,
adiantado ou incompleto impede startup.

No shutdown, sob `SHUTDOWN_TIMEOUT` (default `30s`), os componentes runtime são
solicitados em paralelo e o pool fecha depois deles pela ordem reversa dos hooks
Fx. Os servidores usam `http.Server.Shutdown`. O consumidor cancela long polling,
aguarda handlers e, se o prazo expira, cancela o trabalho e tenta visibilidade
zero para receipts ativos. Outbox e referência param novos claims, aguardam seu
loop e cancelam o trabalho no timeout. Claims não concluídos permanecem
recuperáveis por lease.

## Observabilidade

`log/slog` emite JSON e propaga somente identificadores seguros:
correlação, mensagem, transação, carteira e provedor. Tokens, credenciais,
connection strings, corpos completos e valores financeiros completos não são
logados. Erros de outbox são sanitizados antes de persistidos.

Prometheus é servido apenas no endereço interno. O registry define métricas de
baixa cardinalidade para resultado, replay, retry de workers, DLQ, conflito de
concorrência, atraso/publicação da outbox, latência e divergência de
reconciliação. Consumidor, outbox e worker de referências alimentam suas
métricas. Retry SQL transacional reutiliza
`wager_retries_total{worker="sql-transaction",result="deadlock"}` ou
`result="serialization_failure"`; qualquer razão não reconhecida é limitada a
`result="unknown"`. UUIDs e IDs externos não são labels.

O caminho SQS bem-sucedido emite o log `INFO` `SQS message processed and
deleted` somente depois de commit e delete. O registro contém os identificadores
seguros disponíveis (`correlationId`, `messageId`, `transactionId`, `walletId` e
`providerId`), sem dinheiro, payload, idempotency key ou credenciais.

Liveness só prova que o processo responde. Readiness usa timeouts independentes
para ping PostgreSQL e acesso leve às três filas; identifica componentes
degradados sem secrets. O IdP não participa da readiness, embora discovery OIDC
seja obrigatório no startup de processos com HTTP.

## Schema e ledger

IDs são UUID v7 gerados na aplicação; timestamps são UTC. Constraints cobrem
enums, saldo/versão, timestamps, origem interna/externa, hashes de 32 bytes,
campos de pendência, resultado terminal, referências e unicidades.

O runner aplica cada arquivo e a atualização de `schema_migrations` na mesma
transação, protegida por `pg_advisory_xact_lock`. `up` ignora versão já aplicada;
`down` reverte a maior versão e remove a tabela de controle quando ela fica
vazia. O startup usa o mesmo catálogo, compara sua versão mais recente com
`max(version)` aplicado, valida toda a sequência de versões e nomes do histórico
e também exige as cinco tabelas centrais.

O banco e os objetos versionados pertencem à role de migration. A role runtime
recebe `CONNECT` no banco e `USAGE` no schema, mas não ownership nem `CREATE`.
Ela pode ler e inserir nas tabelas necessárias e atualizar carteiras,
transações, inbox e outbox somente nas colunas operacionais concedidas pela
migration 3; no ledger pode somente ler e inserir. `UPDATE`, `DELETE` e
`TRUNCATE` do ledger são negados por grants, além do trigger de imutabilidade.

Em PostgreSQL externo, banco, schema e as duas roles devem existir antes do
startup. A URL de migration deve incluir a opção de conexão
`options=-c%20app.application_role%3D<runtime_role>` para que as migrations
apliquem os grants à role runtime correta. Usar uma única URL é permitido, mas
deixa o runtime com a identidade privilegiada de migration.

Ledger é append-only. Cada mudança de saldo produz no mesmo commit exatamente um
lançamento por `(wallet_id,transaction_id)`, com equação de crédito/débito
validada no banco. A migration 3 também exige que a transação esteja processada,
não seja `LOSS` e tenha a mesma carteira, moeda, valor e direção semântica do
lançamento. Um constraint trigger diferido exige que jogador e moeda coincidam
com a carteira em estados não rejeitados; `REJECTED` pode preservar os dados
originais incompatíveis que justificam a rejeição auditável. Triggers protegem
identidade de carteira, identidade financeira, estados terminais, hash da inbox
e envelope da outbox. Trigger
`BEFORE UPDATE OR DELETE` rejeita mutação do ledger e grants do usuário de
aplicação não concedem update/delete/truncate. Correção contábil normal usa nova
operação compensatória, não edição histórica.

## Testes de integração e failpoints

`integration/full_stack_test.go` inicia PostgreSQL, Keycloak e LocalStack reais
uma vez por pacote e cria schemas/filas isolados. Ele constrói três instâncias de
serviço com pools independentes para replay HTTP/SQS, contenção e paralelismo.
Testes separados exercitam pgx e o cliente SQS contra containers reais.
`integration/distributed_process_test.go` também inicia três processos reais do
binário, com pools independentes, e valida contenção financeira, persistência
após restart e redrive físico de mensagem inválida para a DLQ. O smoke
`scripts/verify-distributed-scenarios.sh` complementa esses testes através das
três portas dos containers Compose.

`integration/physical_failure_test.go`, compilado somente com a tag
`integration`, reexecuta o processo como subprocesso auxiliar. Uma barreira no
gerador de ID mantém a transação aberta antes do ledger/outbox; o teste comprova
essa sessão em `pg_stat_activity`, envia `SIGKILL` e verifica rollback completo e
retry único. O mesmo ponto permite parar e reiniciar um PostgreSQL Testcontainers
dedicado, observar erro transitório e recuperar por uma nova instância sem estado
parcial. O helper e a barreira não existem na imagem normal.

As janelas de crash são seams injetáveis, sempre `nil` no grafo de produção:

- `ConsumerOptions.AfterCommitBeforeDelete` interrompe depois do commit da
  inbox/domínio e antes de `DeleteMessage`;
- `OutboxOptions.BeforePublish` interrompe depois do claim e antes do send;
- `OutboxOptions.AfterPublishBeforeMark` interrompe depois do send e antes de
  `published_at`.

Isso permite testes determinísticos sem expor toggle de failpoint na imagem. O
teste de rollback antes do commit usa uma transação real que viola constraint e
prova ausência de estado parcial.

## Limitações

- SQS continua at-least-once. O crash publish/mark pode produzir duplicata física
  com `eventId` estável; downstream precisa deduplicar.
- MiniStack aplica policies IAM locais por access key, mas não valida secrets ou
  assinaturas SigV4. `verify-aws-sqs-iam` cobre essa autenticação na AWS real,
  com filas e sessões efêmeras e limpeza automática.
- O Compose usa `wager_migration` como proprietário e `wager_app` no runtime.
  Grants mínimos e o trigger negam DDL e mutações históricas do ledger ao
  runtime.
- Não há balanceador no Compose; as três réplicas publicam portas dinâmicas. O
  operador seleciona uma réplica com `compose port --index` ou adiciona um proxy.
- O full-stack principal usa três instâncias com pools separados dentro de um
  processo de teste; há ainda um teste com três processos reais e um smoke pelas
  três portas Compose, mas não há proxy/load balancer local nem injeção dos seams
  determinísticos na imagem normal.
- A chegada física de uma mensagem inválida à DLQ é coberta com LocalStack real.
  O tempo do teste é reduzido configurando visibility timeout e redrive próprios
  para o cenário, sem alterar os defaults locais.
- Métricas de conflito, latência e divergência estão conectadas aos handlers,
  além das métricas do consumidor, DLQ, referências, outbox e retry SQL. A
  retomada de referência registra seu resultado terminal e latência.
- A suíte prova rollback integral tanto por falha intermediária quanto por
  `SIGKILL` pré-commit e outage real do PostgreSQL durante a transação. Os
  roteiros Compose de outage continuam úteis para observação operacional, mas
  não são o oráculo automatizado dessas garantias.
- `FAILED/PERMANENT_PROCESSING_FAILURE` é alcançável somente para trabalho já
  persistido retomado pelo worker de referências. Operações HTTP síncronas não
  fazem commit intermediário para criar `FAILED`; erro técnico nelas causa
  rollback. Não existe evento de integração específico para `FAILED`.
- Não há conversão cambial, escalas diferentes de duas casas, ledger de partidas
  dobradas, tracing distribuído, dashboard ou benchmark formal.
- `migrate down` é destrutivo. Em ambiente com dados, rollback operacional
  preserva/exporta o banco em vez de remover o schema financeiro.

Os cenários e a cobertura atual estão detalhados em
[`docs/test-plan.md`](docs/test-plan.md).
