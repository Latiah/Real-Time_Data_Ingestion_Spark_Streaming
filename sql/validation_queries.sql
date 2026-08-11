-- Queries used to confirm the streaming pipeline is working correctly
-- and to explore the ingested data. Run these with psql or any SQL client
-- connected to the realtime_events database.

-- 1. Total events ingested so far
SELECT COUNT(*) AS total_events
FROM events;

-- 2. Events broken down by type
SELECT event_type, COUNT(*) AS event_count
FROM events
GROUP BY event_type;

-- 3. Total revenue from purchase events
SELECT SUM(total_amount) AS total_purchase_revenue
FROM events
WHERE event_type = 'purchase';

-- 4. Most active users by event count
SELECT user_id, COUNT(*) AS event_count
FROM events
GROUP BY user_id
ORDER BY event_count DESC
LIMIT 10;

-- 5. Most recently ingested records (sanity check that streaming is live)
SELECT event_id, event_type, event_timestamp, ingested_at
FROM events
ORDER BY ingested_at DESC
LIMIT 20;

-- 6. Check for any duplicate event_ids (should always return 0 rows,
--    since event_id is the PRIMARY KEY -- useful as a sanity check)
SELECT event_id, COUNT(*)
FROM events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- 7. Data quality check: any rows with a negative price or quantity
--    (should always return 0 rows given the CHECK constraints)
SELECT *
FROM events
WHERE price < 0 OR quantity < 0;

-- 8. Best-selling products by units purchased
SELECT product_id, product_name, SUM(quantity) AS units_sold
FROM events
WHERE event_type = 'purchase'
GROUP BY product_id, product_name
ORDER BY units_sold DESC
LIMIT 10;

-- 9. Average processing lag: time between when Spark ingested a record
--    (ingested_at) and when the event actually occurred (event_timestamp)
SELECT AVG(EXTRACT(EPOCH FROM (ingested_at - event_timestamp))) AS avg_lag_seconds
FROM events;
