CREATE FUNCTION validate_wager_transaction_wallet_identity_v3()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    transaction_state TEXT;
    transaction_player_id TEXT;
    transaction_currency CHAR(3);
    wallet_player_id TEXT;
    wallet_currency CHAR(3);
BEGIN
    SELECT transactions.state,
           transactions.player_id,
           transactions.amount_currency,
           wallets.player_id,
           wallets.currency
    INTO transaction_state,
         transaction_player_id,
         transaction_currency,
         wallet_player_id,
         wallet_currency
    FROM wager_transactions AS transactions
    JOIN wallets ON wallets.id = transactions.wallet_id
    WHERE transactions.id = NEW.id;

    IF FOUND AND transaction_state <> 'REJECTED' AND
        (transaction_player_id <> wallet_player_id OR transaction_currency <> wallet_currency) THEN
        RAISE EXCEPTION 'transaction wallet identity is inconsistent'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'wager_transactions_wallet_identity_consistent';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER wager_transactions_validate_wallet_identity_v3
AFTER INSERT OR UPDATE ON wager_transactions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_wager_transaction_wallet_identity_v3();

CREATE FUNCTION validate_wallet_ledger_entry_transaction_v3()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    transaction_state TEXT;
    transaction_kind TEXT;
    transaction_amount BIGINT;
    transaction_wallet_id UUID;
    transaction_currency CHAR(3);
    referenced_kind TEXT;
    expected_direction TEXT;
BEGIN
    SELECT transactions.state,
           transactions.kind,
           transactions.amount_minor,
           transactions.wallet_id,
           transactions.amount_currency,
           referenced.kind
    INTO transaction_state,
         transaction_kind,
         transaction_amount,
         transaction_wallet_id,
         transaction_currency,
         referenced_kind
    FROM wager_transactions AS transactions
    LEFT JOIN wager_transactions AS referenced
        ON referenced.id = transactions.reference_transaction_id
    WHERE transactions.id = NEW.transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'ledger transaction % does not exist', NEW.transaction_id
            USING ERRCODE = '23503',
                  CONSTRAINT = 'wallet_ledger_entries_transaction_consistent';
    END IF;

    IF transaction_wallet_id <> NEW.wallet_id OR transaction_currency <> NEW.currency THEN
        RAISE EXCEPTION 'ledger wallet or currency differs from transaction %', NEW.transaction_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'wallet_ledger_entries_transaction_consistent';
    END IF;

    IF transaction_state <> 'PROCESSED' THEN
        RAISE EXCEPTION 'ledger transaction % is not PROCESSED', NEW.transaction_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'wallet_ledger_entries_transaction_consistent';
    END IF;

    IF transaction_kind = 'LOSS' THEN
        RAISE EXCEPTION 'LOSS transaction % cannot have a ledger entry', NEW.transaction_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'wallet_ledger_entries_transaction_consistent';
    END IF;

    IF NEW.amount_minor <> transaction_amount THEN
        RAISE EXCEPTION 'ledger amount differs from transaction %', NEW.transaction_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'wallet_ledger_entries_transaction_consistent';
    END IF;

    expected_direction := CASE transaction_kind
        WHEN 'OPENING' THEN 'CREDIT'
        WHEN 'BET' THEN 'DEBIT'
        WHEN 'WIN' THEN 'CREDIT'
        WHEN 'REFUND' THEN 'CREDIT'
        WHEN 'ROLLBACK' THEN CASE referenced_kind
            WHEN 'BET' THEN 'CREDIT'
            WHEN 'WIN' THEN 'DEBIT'
            WHEN 'REFUND' THEN 'DEBIT'
            ELSE NULL
        END
        ELSE NULL
    END;

    IF expected_direction IS NULL OR NEW.direction <> expected_direction THEN
        RAISE EXCEPTION 'ledger direction differs from transaction % and its reference', NEW.transaction_id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'wallet_ledger_entries_transaction_consistent';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER wallet_ledger_entries_validate_transaction_v3
BEFORE INSERT ON wallet_ledger_entries
FOR EACH ROW
EXECUTE FUNCTION validate_wallet_ledger_entry_transaction_v3();

CREATE FUNCTION protect_wallet_snapshot_v3()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF (NEW.id, NEW.player_id, NEW.currency, NEW.created_at)
        IS DISTINCT FROM
       (OLD.id, OLD.player_id, OLD.currency, OLD.created_at) THEN
        RAISE EXCEPTION 'wallet identity is immutable'
            USING ERRCODE = '55000',
                  CONSTRAINT = 'wallets_identity_immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallets_protect_snapshot_v3
BEFORE UPDATE ON wallets
FOR EACH ROW
EXECUTE FUNCTION protect_wallet_snapshot_v3();

CREATE FUNCTION protect_wager_transaction_snapshot_v3()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'terminal wager transaction % is immutable', OLD.id
            USING ERRCODE = '55000',
                  CONSTRAINT = 'wager_transactions_terminal_immutable';
    END IF;

    IF (
        NEW.id,
        NEW.wallet_id,
        NEW.player_id,
        NEW.provider_id,
        NEW.external_transaction_id,
        NEW.idempotency_key,
        NEW.request_hash,
        NEW.round_id,
        NEW.game_id,
        NEW.kind,
        NEW.amount_minor,
        NEW.amount_currency,
        NEW.reference_external_transaction_id,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.id,
        OLD.wallet_id,
        OLD.player_id,
        OLD.provider_id,
        OLD.external_transaction_id,
        OLD.idempotency_key,
        OLD.request_hash,
        OLD.round_id,
        OLD.game_id,
        OLD.kind,
        OLD.amount_minor,
        OLD.amount_currency,
        OLD.reference_external_transaction_id,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'wager transaction identity is immutable'
            USING ERRCODE = '55000',
                  CONSTRAINT = 'wager_transactions_identity_immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_transactions_protect_snapshot_v3
BEFORE UPDATE ON wager_transactions
FOR EACH ROW
EXECUTE FUNCTION protect_wager_transaction_snapshot_v3();

CREATE FUNCTION protect_inbox_snapshot_v3()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF (NEW.consumer_name, NEW.message_id, NEW.payload_hash, NEW.received_at)
        IS DISTINCT FROM
       (OLD.consumer_name, OLD.message_id, OLD.payload_hash, OLD.received_at) THEN
        RAISE EXCEPTION 'inbox identity and payload hash are immutable'
            USING ERRCODE = '55000',
                  CONSTRAINT = 'inbox_messages_snapshot_immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER inbox_messages_protect_snapshot_v3
BEFORE UPDATE ON inbox_messages
FOR EACH ROW
EXECUTE FUNCTION protect_inbox_snapshot_v3();

CREATE FUNCTION protect_outbox_snapshot_v3()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        NEW.event_id,
        NEW.event_type,
        NEW.aggregate_id,
        NEW.correlation_id,
        NEW.causation_id,
        NEW.version,
        NEW.envelope,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.event_id,
        OLD.event_type,
        OLD.aggregate_id,
        OLD.correlation_id,
        OLD.causation_id,
        OLD.version,
        OLD.envelope,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'outbox identity and envelope are immutable'
            USING ERRCODE = '55000',
                  CONSTRAINT = 'outbox_events_snapshot_immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_events_protect_snapshot_v3
BEFORE UPDATE ON outbox_events
FOR EACH ROW
EXECUTE FUNCTION protect_outbox_snapshot_v3();

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
        'GRANT SELECT, INSERT ON TABLE wallets, wager_transactions, inbox_messages, outbox_events TO %I',
        application_role
    );
    EXECUTE format(
        'GRANT SELECT, INSERT ON TABLE wallet_ledger_entries TO %I',
        application_role
    );
    EXECUTE format(
        'GRANT UPDATE (balance_minor, version, updated_at) ON TABLE wallets TO %I',
        application_role
    );
    EXECUTE format(
        'GRANT UPDATE (state, reference_transaction_id, reference_attempts, reference_next_attempt_at, reference_expires_at, reference_claimed_by, reference_claim_until, result_balance_minor, result_balance_currency, failure_code, completed_at, updated_at) ON TABLE wager_transactions TO %I',
        application_role
    );
    EXECUTE format(
        'GRANT UPDATE (transaction_id, completed_at) ON TABLE inbox_messages TO %I',
        application_role
    );
    EXECUTE format(
        'GRANT UPDATE (attempt_count, next_attempt_at, claimed_by, claim_until, published_at, last_error) ON TABLE outbox_events TO %I',
        application_role
    );
    EXECUTE format('GRANT SELECT ON TABLE schema_migrations TO %I', application_role);
END;
$$;
