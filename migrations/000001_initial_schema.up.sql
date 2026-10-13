CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id TEXT NOT NULL,
    currency CHAR(3) NOT NULL,
    balance_minor BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT wallets_player_id_not_blank CHECK (
        player_id <> '' AND player_id = btrim(player_id)
    ),
    CONSTRAINT wallets_currency_format CHECK (
        currency::TEXT ~ '^[A-Z]{3}$'
    ),
    CONSTRAINT wallets_balance_nonnegative CHECK (balance_minor >= 0),
    CONSTRAINT wallets_version_positive CHECK (version >= 1),
    CONSTRAINT wallets_timestamps_ordered CHECK (updated_at >= created_at),
    CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency),
    CONSTRAINT uq_wallets_identity UNIQUE (id, player_id, currency)
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL,
    player_id TEXT NOT NULL,
    provider_id TEXT,
    external_transaction_id TEXT,
    idempotency_key TEXT,
    request_hash BYTEA,
    round_id TEXT,
    game_id TEXT,
    kind TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING',
    amount_minor BIGINT NOT NULL,
    amount_currency CHAR(3) NOT NULL,
    reference_external_transaction_id TEXT,
    reference_transaction_id UUID,
    reference_attempts INTEGER NOT NULL DEFAULT 0,
    reference_next_attempt_at TIMESTAMPTZ,
    reference_expires_at TIMESTAMPTZ,
    reference_claimed_by TEXT,
    reference_claim_until TIMESTAMPTZ,
    result_balance_minor BIGINT,
    result_balance_currency CHAR(3),
    failure_code TEXT,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_wager_transactions_wallet
        FOREIGN KEY (wallet_id)
        REFERENCES wallets (id)
        ON DELETE RESTRICT,
    CONSTRAINT wager_transactions_player_id_not_blank CHECK (
        player_id <> '' AND player_id = btrim(player_id)
    ),
    CONSTRAINT wager_transactions_kind_valid CHECK (
        kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')
    ),
    CONSTRAINT wager_transactions_state_valid CHECK (
        state IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')
    ),
    CONSTRAINT wager_transactions_amount_currency_format CHECK (
        amount_currency::TEXT ~ '^[A-Z]{3}$'
    ),
    CONSTRAINT wager_transactions_amount_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0)
        OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    CONSTRAINT wager_transactions_request_hash_length CHECK (
        request_hash IS NULL OR octet_length(request_hash) = 32
    ),
    CONSTRAINT wager_transactions_origin_consistent CHECK (
        (
            kind = 'OPENING'
            AND provider_id IS NULL
            AND external_transaction_id IS NULL
            AND idempotency_key IS NULL
            AND request_hash IS NULL
            AND round_id IS NULL
            AND game_id IS NULL
            AND reference_external_transaction_id IS NULL
            AND reference_transaction_id IS NULL
        )
        OR
        (
            kind <> 'OPENING'
            AND provider_id IS NOT NULL
            AND provider_id <> ''
            AND provider_id = btrim(provider_id)
            AND external_transaction_id IS NOT NULL
            AND external_transaction_id <> ''
            AND external_transaction_id = btrim(external_transaction_id)
            AND idempotency_key IS NOT NULL
            AND idempotency_key <> ''
            AND idempotency_key = btrim(idempotency_key)
            AND request_hash IS NOT NULL
            AND round_id IS NOT NULL
            AND round_id <> ''
            AND round_id = btrim(round_id)
            AND game_id IS NOT NULL
            AND game_id <> ''
            AND game_id = btrim(game_id)
        )
    ),
    CONSTRAINT wager_transactions_reference_kind_consistent CHECK (
        (
            kind IN ('REFUND', 'ROLLBACK')
            AND reference_external_transaction_id IS NOT NULL
            AND reference_external_transaction_id <> ''
            AND reference_external_transaction_id = btrim(reference_external_transaction_id)
        )
        OR
        (
            kind NOT IN ('REFUND', 'ROLLBACK')
            AND reference_external_transaction_id IS NULL
            AND reference_transaction_id IS NULL
        )
    ),
    CONSTRAINT wager_transactions_processed_reference_resolved CHECK (
        state <> 'PROCESSED'
        OR kind NOT IN ('REFUND', 'ROLLBACK')
        OR reference_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_transactions_reference_not_self CHECK (
        reference_transaction_id IS NULL OR reference_transaction_id <> id
    ),
    CONSTRAINT wager_transactions_pending_reference_fields CHECK (
        (
            state = 'PENDING_REFERENCE'
            AND kind IN ('REFUND', 'ROLLBACK')
            AND reference_next_attempt_at IS NOT NULL
            AND reference_expires_at IS NOT NULL
            AND reference_expires_at > created_at
            AND reference_next_attempt_at <= reference_expires_at
        )
        OR
        (
            state <> 'PENDING_REFERENCE'
            AND reference_next_attempt_at IS NULL
            AND reference_claimed_by IS NULL
            AND reference_claim_until IS NULL
        )
    ),
    CONSTRAINT wager_transactions_reference_attempts_nonnegative CHECK (
        reference_attempts >= 0
    ),
    CONSTRAINT wager_transactions_non_reversal_pending_fields CHECK (
        kind IN ('REFUND', 'ROLLBACK')
        OR (
            reference_attempts = 0
            AND reference_next_attempt_at IS NULL
            AND reference_expires_at IS NULL
            AND reference_claimed_by IS NULL
            AND reference_claim_until IS NULL
        )
    ),
    CONSTRAINT wager_transactions_reference_claim_consistent CHECK (
        (reference_claimed_by IS NULL) = (reference_claim_until IS NULL)
        AND (
            reference_claimed_by IS NULL
            OR (
                state = 'PENDING_REFERENCE'
                AND reference_claimed_by <> ''
                AND reference_claimed_by = btrim(reference_claimed_by)
            )
        )
    ),
    CONSTRAINT wager_transactions_result_balance_consistent CHECK (
        (result_balance_minor IS NULL) = (result_balance_currency IS NULL)
        AND (result_balance_minor IS NULL OR result_balance_minor >= 0)
        AND (
            result_balance_currency IS NULL
            OR result_balance_currency::TEXT ~ '^[A-Z]{3}$'
        )
        AND (
            result_balance_currency IS NULL
            OR result_balance_currency = amount_currency
        )
        AND (state = 'PROCESSED') = (result_balance_minor IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_failure_consistent CHECK (
        (state IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL)
        AND (failure_code IS NULL OR (failure_code <> '' AND failure_code = btrim(failure_code)))
    ),
    CONSTRAINT wager_transactions_completion_consistent CHECK (
        (state IN ('PROCESSED', 'REJECTED', 'FAILED')) = (completed_at IS NOT NULL)
        AND (completed_at IS NULL OR completed_at >= created_at)
    ),
    CONSTRAINT wager_transactions_timestamps_ordered CHECK (updated_at >= created_at),
    CONSTRAINT uq_wager_transactions_provider_idempotency
        UNIQUE (provider_id, idempotency_key),
    CONSTRAINT uq_wager_transactions_provider_external
        UNIQUE (provider_id, external_transaction_id),
    CONSTRAINT uq_wager_transactions_ledger_identity
        UNIQUE (id, wallet_id, amount_currency),
    CONSTRAINT uq_wager_transactions_reference_identity
        UNIQUE (
            id,
            provider_id,
            external_transaction_id,
            player_id,
            wallet_id,
            round_id,
            amount_minor,
            amount_currency
        ),
    CONSTRAINT fk_wager_transactions_reference_identity
        FOREIGN KEY (
            reference_transaction_id,
            provider_id,
            reference_external_transaction_id,
            player_id,
            wallet_id,
            round_id,
            amount_minor,
            amount_currency
        )
        REFERENCES wager_transactions (
            id,
            provider_id,
            external_transaction_id,
            player_id,
            wallet_id,
            round_id,
            amount_minor,
            amount_currency
        )
        ON DELETE RESTRICT
);

CREATE UNIQUE INDEX uq_wager_transactions_processed_direct_reversal
    ON wager_transactions (reference_transaction_id)
    WHERE state = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

CREATE INDEX idx_wager_transactions_wallet_created
    ON wager_transactions (wallet_id, created_at DESC, id DESC);

CREATE INDEX idx_wager_transactions_reference_external
    ON wager_transactions (provider_id, reference_external_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK');

-- Supports ordered batch selection followed by FOR UPDATE SKIP LOCKED.
CREATE INDEX idx_wager_transactions_pending_reference_claim
    ON wager_transactions (
        reference_next_attempt_at,
        reference_claim_until,
        created_at,
        id
    )
    WHERE state = 'PENDING_REFERENCE';

CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL,
    transaction_id UUID NOT NULL,
    direction TEXT NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    balance_before_minor BIGINT NOT NULL,
    balance_after_minor BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_wallet_ledger_entries_wallet
        FOREIGN KEY (wallet_id) REFERENCES wallets (id) ON DELETE RESTRICT,
    CONSTRAINT fk_wallet_ledger_entries_transaction_identity
        FOREIGN KEY (transaction_id, wallet_id, currency)
        REFERENCES wager_transactions (id, wallet_id, amount_currency)
        ON DELETE RESTRICT,
    CONSTRAINT wallet_ledger_entries_direction_valid CHECK (
        direction IN ('CREDIT', 'DEBIT')
    ),
    CONSTRAINT wallet_ledger_entries_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT wallet_ledger_entries_currency_format CHECK (
        currency::TEXT ~ '^[A-Z]{3}$'
    ),
    CONSTRAINT wallet_ledger_entries_balances_nonnegative CHECK (
        balance_before_minor >= 0 AND balance_after_minor >= 0
    ),
    CONSTRAINT wallet_ledger_entries_balance_equation CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR
        (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    ),
    CONSTRAINT uq_wallet_ledger_entries_wallet_transaction
        UNIQUE (wallet_id, transaction_id)
);

CREATE INDEX idx_wallet_ledger_entries_wallet_created
    ON wallet_ledger_entries (wallet_id, created_at DESC, id DESC);

CREATE FUNCTION prevent_wallet_ledger_entry_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'wallet ledger entries are immutable'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER wallet_ledger_entries_immutable
BEFORE UPDATE OR DELETE ON wallet_ledger_entries
FOR EACH ROW
EXECUTE FUNCTION prevent_wallet_ledger_entry_mutation();

CREATE TABLE inbox_messages (
    consumer_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    payload_hash BYTEA NOT NULL,
    transaction_id UUID,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,

    CONSTRAINT pk_inbox_messages PRIMARY KEY (consumer_name, message_id),
    CONSTRAINT fk_inbox_messages_transaction
        FOREIGN KEY (transaction_id) REFERENCES wager_transactions (id) ON DELETE RESTRICT,
    CONSTRAINT inbox_messages_consumer_name_not_blank CHECK (
        consumer_name <> '' AND consumer_name = btrim(consumer_name)
    ),
    CONSTRAINT inbox_messages_message_id_not_blank CHECK (
        message_id <> '' AND message_id = btrim(message_id)
    ),
    CONSTRAINT inbox_messages_payload_hash_length CHECK (
        octet_length(payload_hash) = 32
    ),
    CONSTRAINT inbox_messages_completion_consistent CHECK (
        (completed_at IS NULL) = (transaction_id IS NULL)
        AND (completed_at IS NULL OR completed_at >= received_at)
    )
);

CREATE INDEX idx_inbox_messages_completed
    ON inbox_messages (completed_at)
    WHERE completed_at IS NOT NULL;

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    version INTEGER NOT NULL,
    envelope JSONB NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    claimed_by TEXT,
    claim_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT outbox_events_type_valid CHECK (
        event_type IN (
            'WagerTransactionProcessed',
            'WagerTransactionRejected',
            'WagerTransactionPendingReference',
            'WalletBalanceChanged'
        )
    ),
    CONSTRAINT outbox_events_correlation_id_not_blank CHECK (
        correlation_id <> '' AND correlation_id = btrim(correlation_id)
    ),
    CONSTRAINT outbox_events_causation_id_not_blank CHECK (
        causation_id IS NULL OR (causation_id <> '' AND causation_id = btrim(causation_id))
    ),
    CONSTRAINT outbox_events_version_positive CHECK (version > 0),
    CONSTRAINT outbox_events_envelope_object CHECK (
        jsonb_typeof(envelope) = 'object'
    ),
    CONSTRAINT outbox_events_attempt_count_nonnegative CHECK (attempt_count >= 0),
    CONSTRAINT outbox_events_claim_consistent CHECK (
        (claimed_by IS NULL) = (claim_until IS NULL)
        AND (
            claimed_by IS NULL
            OR (claimed_by <> '' AND claimed_by = btrim(claimed_by))
        )
        AND (
            published_at IS NULL
            OR (claimed_by IS NULL AND claim_until IS NULL)
        )
    ),
    CONSTRAINT outbox_events_published_at_ordered CHECK (
        published_at IS NULL OR published_at >= created_at
    ),
    CONSTRAINT outbox_events_last_error_not_blank CHECK (
        last_error IS NULL OR last_error <> ''
    )
);

-- Supports ordered batch selection followed by FOR UPDATE SKIP LOCKED.
CREATE INDEX idx_outbox_events_pending_claim
    ON outbox_events (next_attempt_at, claim_until, created_at, event_id)
    WHERE published_at IS NULL;

-- PostgreSQL grants no table privileges to PUBLIC by default, but make the
-- ledger restriction explicit for installations with altered defaults.
REVOKE UPDATE, DELETE, TRUNCATE ON TABLE wallet_ledger_entries FROM PUBLIC;

-- A deployment may set app.application_role before applying this migration.
-- The role is intentionally not created here, so applying migrations needs no
-- CREATEROLE or superuser privilege. A missing role is safely ignored.
DO $$
DECLARE
    application_role TEXT := NULLIF(current_setting('app.application_role', TRUE), '');
    schema_name TEXT := current_schema();
BEGIN
    EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM PUBLIC', schema_name);
    IF application_role IS NULL OR to_regrole(application_role) IS NULL THEN
        RETURN;
    END IF;

    EXECUTE format('REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I', schema_name, application_role);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', schema_name, application_role);
    EXECUTE format(
        'GRANT SELECT, INSERT, UPDATE ON TABLE wallets, wager_transactions, inbox_messages, outbox_events TO %I',
        application_role
    );
    EXECUTE format(
        'GRANT SELECT, INSERT ON TABLE wallet_ledger_entries TO %I',
        application_role
    );
    EXECUTE format(
        'GRANT SELECT ON TABLE schema_migrations TO %I',
        application_role
    );
    EXECUTE format(
        'REVOKE UPDATE, DELETE, TRUNCATE ON TABLE wallet_ledger_entries FROM %I',
        application_role
    );
END;
$$;
