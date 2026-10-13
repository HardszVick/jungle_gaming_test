ALTER TABLE wager_transactions
    DROP CONSTRAINT fk_wager_transactions_reference_identity,
    DROP CONSTRAINT uq_wager_transactions_reference_identity,
    DROP CONSTRAINT wager_transactions_reference_kind_consistent,
    DROP CONSTRAINT wager_transactions_processed_reference_resolved,
    DROP CONSTRAINT wager_transactions_pending_reference_fields,
    DROP CONSTRAINT wager_transactions_non_reversal_pending_fields;

ALTER TABLE wager_transactions
    ADD COLUMN reversal_reference_transaction_id UUID
        GENERATED ALWAYS AS (
            CASE
                WHEN kind IN ('REFUND', 'ROLLBACK') THEN reference_transaction_id
                ELSE NULL
            END
        ) STORED,
    ADD COLUMN bet_reference_transaction_id UUID
        GENERATED ALWAYS AS (
            CASE
                WHEN kind IN ('WIN', 'REFUND') THEN reference_transaction_id
                ELSE NULL
            END
        ) STORED,
    ADD COLUMN bet_reference_kind TEXT
        GENERATED ALWAYS AS (
            CASE
                WHEN kind IN ('WIN', 'REFUND') AND reference_transaction_id IS NOT NULL
                    THEN 'BET'
                ELSE NULL
            END
        ) STORED,
    ADD CONSTRAINT wager_transactions_reference_kind_consistent CHECK (
        (
            kind IN ('REFUND', 'ROLLBACK')
            AND reference_external_transaction_id IS NOT NULL
            AND reference_external_transaction_id <> ''
            AND reference_external_transaction_id = btrim(reference_external_transaction_id)
        )
        OR
        (
            kind = 'WIN'
            AND (
                (
                    reference_external_transaction_id IS NULL
                    AND reference_transaction_id IS NULL
                )
                OR
                (
                    reference_external_transaction_id IS NOT NULL
                    AND reference_external_transaction_id <> ''
                    AND reference_external_transaction_id = btrim(reference_external_transaction_id)
                )
            )
        )
        OR
        (
            kind NOT IN ('WIN', 'REFUND', 'ROLLBACK')
            AND reference_external_transaction_id IS NULL
            AND reference_transaction_id IS NULL
        )
    ),
    ADD CONSTRAINT wager_transactions_processed_reference_resolved CHECK (
        state <> 'PROCESSED'
        OR reference_external_transaction_id IS NULL
        OR reference_transaction_id IS NOT NULL
    ),
    ADD CONSTRAINT wager_transactions_pending_reference_fields CHECK (
        (
            state = 'PENDING_REFERENCE'
            AND kind IN ('WIN', 'REFUND', 'ROLLBACK')
            AND reference_external_transaction_id IS NOT NULL
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
    ADD CONSTRAINT wager_transactions_reference_retry_fields CHECK (
        kind IN ('REFUND', 'ROLLBACK')
        OR (kind = 'WIN' AND reference_external_transaction_id IS NOT NULL)
        OR (
            reference_attempts = 0
            AND reference_next_attempt_at IS NULL
            AND reference_expires_at IS NULL
            AND reference_claimed_by IS NULL
            AND reference_claim_until IS NULL
        )
    ),
    ADD CONSTRAINT uq_wager_transactions_reference_identity
        UNIQUE (
            id,
            provider_id,
            external_transaction_id,
            player_id,
            wallet_id,
            round_id,
            amount_currency
        ),
    ADD CONSTRAINT uq_wager_transactions_reference_amount
        UNIQUE (id, amount_minor),
    ADD CONSTRAINT uq_wager_transactions_reference_kind
        UNIQUE (id, kind),
    ADD CONSTRAINT fk_wager_transactions_reference_identity
        FOREIGN KEY (
            reference_transaction_id,
            provider_id,
            reference_external_transaction_id,
            player_id,
            wallet_id,
            round_id,
            amount_currency
        )
        REFERENCES wager_transactions (
            id,
            provider_id,
            external_transaction_id,
            player_id,
            wallet_id,
            round_id,
            amount_currency
        )
        ON DELETE RESTRICT,
    ADD CONSTRAINT fk_wager_transactions_reversal_reference_amount
        FOREIGN KEY (reversal_reference_transaction_id, amount_minor)
        REFERENCES wager_transactions (id, amount_minor)
        ON DELETE RESTRICT,
    ADD CONSTRAINT fk_wager_transactions_bet_reference_kind
        FOREIGN KEY (bet_reference_transaction_id, bet_reference_kind)
        REFERENCES wager_transactions (id, kind)
        ON DELETE RESTRICT;

CREATE UNIQUE INDEX uq_wager_transactions_opening_wallet
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_opening_processed CHECK (
        kind <> 'OPENING' OR state = 'PROCESSED'
    );

ALTER TABLE wallets
    DROP CONSTRAINT wallets_currency_format,
    ADD CONSTRAINT wallets_currency_supported CHECK (
        currency::TEXT IN ('AUD', 'BRL', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'MXN', 'NZD', 'USD')
    );

ALTER TABLE wager_transactions
    DROP CONSTRAINT wager_transactions_amount_currency_format,
    ADD CONSTRAINT wager_transactions_amount_currency_supported CHECK (
        amount_currency::TEXT IN ('AUD', 'BRL', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'MXN', 'NZD', 'USD')
    ),
    DROP CONSTRAINT wager_transactions_result_balance_consistent,
    ADD CONSTRAINT wager_transactions_result_balance_consistent CHECK (
        (result_balance_minor IS NULL) = (result_balance_currency IS NULL)
        AND (result_balance_minor IS NULL OR result_balance_minor >= 0)
        AND (
            result_balance_currency IS NULL
            OR result_balance_currency::TEXT IN (
                'AUD', 'BRL', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'MXN', 'NZD', 'USD'
            )
        )
        AND (
            result_balance_currency IS NULL
            OR result_balance_currency = amount_currency
        )
        AND (state = 'PROCESSED') = (result_balance_minor IS NOT NULL)
    );

ALTER TABLE wallet_ledger_entries
    DROP CONSTRAINT wallet_ledger_entries_currency_format,
    ADD CONSTRAINT wallet_ledger_entries_currency_supported CHECK (
        currency::TEXT IN ('AUD', 'BRL', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'MXN', 'NZD', 'USD')
    );

REVOKE DELETE, TRUNCATE ON TABLE
    wallets,
    wager_transactions,
    wallet_ledger_entries,
    inbox_messages,
    outbox_events
FROM PUBLIC;
REVOKE UPDATE ON TABLE wallet_ledger_entries FROM PUBLIC;

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
        'REVOKE ALL PRIVILEGES ON TABLE schema_migrations, wallets, wager_transactions, wallet_ledger_entries, inbox_messages, outbox_events FROM %I',
        application_role
    );
    EXECUTE format(
        'GRANT SELECT, INSERT, UPDATE ON TABLE wallets, wager_transactions, inbox_messages, outbox_events TO %I',
        application_role
    );
    EXECUTE format(
        'GRANT SELECT, INSERT ON TABLE wallet_ledger_entries TO %I',
        application_role
    );
    EXECUTE format('GRANT SELECT ON TABLE schema_migrations TO %I', application_role);
END;
$$;
