DROP TABLE IF EXISTS mart.rides ;

CREATE TABLE mart.rides AS

SELECT
    r.ride_id, 
    r.requested_at, 
    r.driver_id, 
    d.driver_name, 
    d.rating, 
    pz.zone_name       AS pickup_zone,
    dz.zone_name       AS dropoff_zone,
    r.distance_km, 
    r.fare_amount

FROM
    staging.rides as r 

LEFT JOIN
    staging.drivers d ON  r.driver_id = d.driver_id

LEFT JOIN 
    staging.zones pz ON r.pickup_zone_id = pz.zone_id

LEFT JOIN 
    staging.zones dz ON r.dropoff_zone_id = dz.zone_id;

