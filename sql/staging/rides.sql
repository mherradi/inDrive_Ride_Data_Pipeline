DROP TABLE IF EXISTS staging.rides ;
CREATE TABLE staging.rides AS
SELECT
    ride_id,
    driver_id,
    pickup_zone_id,
    dropoff_zone_id,
    distance_km,
    fare_amount,
    requested_at
FROM
    raw.rides_stream

WHERE
    distance_km > 0 
    AND fare_amount > 0
    AND dropoff_zone_id IS NOT NULL
    AND pickup_zone_id  IS NOT NULL
    AND driver_id IN (SELECT driver_id FROM staging.drivers);
