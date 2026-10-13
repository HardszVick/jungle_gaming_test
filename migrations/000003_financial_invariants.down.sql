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

DROP TRIGGER wager_transactions_validate_wallet_identity_v3 ON wager_transactions;
DROP FUNCTION validate_wager_transaction_wallet_identity_v3();

DROP TRIGGER outbox_events_protect_snapshot_v3 ON outbox_events;
DROP FUNCTION protect_outbox_snapshot_v3();

DROP TRIGGER inbox_messages_protect_snapshot_v3 ON inbox_messages;
DROP FUNCTION protect_inbox_snapshot_v3();

DROP TRIGGER wager_transactions_protect_snapshot_v3 ON wager_transactions;
DROP FUNCTION protect_wager_transaction_snapshot_v3();

DROP TRIGGER wallets_protect_snapshot_v3 ON wallets;
DROP FUNCTION protect_wallet_snapshot_v3();

DROP TRIGGER wallet_ledger_entries_validate_transaction_v3 ON wallet_ledger_entries;
DROP FUNCTION validate_wallet_ledger_entry_transaction_v3();
