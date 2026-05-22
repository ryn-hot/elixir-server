ALTER TABLE acquisition_subscriptions
    ADD COLUMN tracking_started_at TIMESTAMP;

UPDATE acquisition_subscriptions
SET tracking_started_at = (
    SELECT MIN(t.updated_at)
    FROM acquisition_targets t
    WHERE t.subscription_id = acquisition_subscriptions.subscription_id
      AND t.state = 'imported'
)
WHERE tracking_started_at IS NULL
  AND EXISTS (
    SELECT 1
    FROM acquisition_targets t
    WHERE t.subscription_id = acquisition_subscriptions.subscription_id
      AND t.state = 'imported'
  );
