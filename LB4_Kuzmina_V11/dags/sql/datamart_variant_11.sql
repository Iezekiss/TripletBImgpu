CREATE OR REPLACE VIEW dm_flights_11 AS
SELECT
    MONTH AS month,
    AIRLINE AS airline,
    ORIGIN AS origin_airport,
    CASE WHEN ARRIVAL_DELAY > 0 THEN 1 ELSE 0 END AS is_delayed,
    ARRIVAL_DELAY AS arrival_delay_minutes
FROM raw_flights_11;
