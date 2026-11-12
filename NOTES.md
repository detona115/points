# Ledger operations in Postgres

Schema: ledger_entries(member_id, payment_id, kind, points, month), PRIMARY KEY (payment_id, kind).
The primary key is the exactly-once guarantee: a redelivered event conflicts and inserts nothing.
Award - one transaction:
  BEGIN;
  SELECT pg_advisory_xact_lock(hashtext(member_id || ':' || month));  -- serialize the member-month
  SELECT COALESCE(SUM(points),0) FROM ledger_entries WHERE member_id=$1 AND month=$5 AND kind='award';
  INSERT INTO ledger_entries(member_id, payment_id, kind, points, month)
    VALUES ($1,$2,'award', LEAST($3, $4 - used), $5)                 -- $4 is the monthly cap
    ON CONFLICT (payment_id, kind) DO NOTHING;
  COMMIT;
Reverse - one transaction:
  BEGIN;
  SELECT points FROM ledger_entries WHERE payment_id=$2 AND kind='award' FOR UPDATE;  -- none => unknown
  INSERT INTO ledger_entries(member_id, payment_id, kind, points, month)
    VALUES ($1,$2,'reversal', -awarded, $5) ON CONFLICT (payment_id, kind) DO NOTHING;
  COMMIT;
The member-month advisory lock makes read-cap-insert atomic, so the cap holds across replicas
while different members stay parallel; the PK makes both writes idempotent under redelivery.
