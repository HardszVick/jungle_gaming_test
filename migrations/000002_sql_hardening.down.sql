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
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', schema_name, application_role);
END;
$$;

ALTER TABLE wallet_ledger_entries
    DROP CONSTRAINT wallet_ledger_entries_currency_supported,
    ADD CONSTRAINT wallet_ledger_entries_currency_format CHECK (
        currency::TEXT ~ '^[A-Z]{3}$'
    );

ALTER TABLE wager_transactions
    DROP CONSTRAINT wager_transactions_amount_currency_supported,
    ADD CONSTRAINT wager_transactions_amount_currency_format CHECK (
        amount_currency::TEXT ~ '^[A-Z]{3}$'
    ),
    DROP CONSTRAINT wager_transactions_result_balance_consistent,
    ADD CONSTRAINT wager_transactions_result_balance_consistent CHECK (
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
    );

ALTER TABLE wallets
    DROP CONSTRAINT wallets_currency_supported,
    ADD CONSTRAINT wallets_currency_format CHECK (
        currency::TEXT ~ '^[A-Z]{3}$'
    );

ALTER TABLE wager_transactions
    DROP CONSTRAINT wager_transactions_opening_processed;

DROP INDEX uq_wager_transactions_opening_wallet;

ALTER TABLE wager_transactions
    DROP CONSTRAINT fk_wager_transactions_bet_reference_kind,
    DROP CONSTRAINT fk_wager_transactions_reversal_reference_amount,
    DROP CONSTRAINT fk_wager_transactions_reference_identity,
    DROP CONSTRAINT uq_wager_transactions_reference_kind,
    DROP CONSTRAINT uq_wager_transactions_reference_amount,
    DROP CONSTRAINT uq_wager_transactions_reference_identity,
    DROP CONSTRAINT wager_transactions_reference_kind_consistent,
    DROP CONSTRAINT wager_transactions_processed_reference_resolved,
    DROP CONSTRAINT wager_transactions_pending_reference_fields,
    DROP CONSTRAINT wager_transactions_reference_retry_fields;

-- Version 000001 cannot represent a referenced WIN. Preserve terminal records
-- by removing only the association; reject pending records before restoring it.
UPDATE wager_transactions
SET state = CASE WHEN state = 'PENDING_REFERENCE' THEN 'REJECTED' ELSE state END,
    failure_code = CASE
        WHEN state = 'PENDING_REFERENCE' THEN 'REFERENCE_NOT_FOUND'
        ELSE failure_code
    END,
    completed_at = CASE
        WHEN state = 'PENDING_REFERENCE' THEN GREATEST(CURRENT_TIMESTAMP, created_at)
        ELSE completed_at
    END,
    updated_at = CASE
        WHEN state = 'PENDING_REFERENCE' THEN GREATEST(CURRENT_TIMESTAMP, created_at)
        ELSE updated_at
    END,
    reference_external_transaction_id = NULL,
    reference_transaction_id = NULL,
    reference_attempts = 0,
    reference_next_attempt_at = NULL,
    reference_expires_at = NULL,
    reference_claimed_by = NULL,
    reference_claim_until = NULL
WHERE kind = 'WIN';

ALTER TABLE wager_transactions
    DROP COLUMN bet_reference_kind,
    DROP COLUMN bet_reference_transaction_id,
    DROP COLUMN reversal_reference_transaction_id,
    ADD CONSTRAINT wager_transactions_reference_kind_consistent CHECK (
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
    ADD CONSTRAINT wager_transactions_processed_reference_resolved CHECK (
        state <> 'PROCESSED'
        OR kind NOT IN ('REFUND', 'ROLLBACK')
        OR reference_transaction_id IS NOT NULL
    ),
    ADD CONSTRAINT wager_transactions_pending_reference_fields CHECK (
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
    ADD CONSTRAINT wager_transactions_non_reversal_pending_fields CHECK (
        kind IN ('REFUND', 'ROLLBACK')
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
            amount_minor,
            amount_currency
        ),
    ADD CONSTRAINT fk_wager_transactions_reference_identity
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
        ON DELETE RESTRICT;
