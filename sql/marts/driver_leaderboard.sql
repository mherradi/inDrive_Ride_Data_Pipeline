DROP TABLE IF EXISTS mart.driver_leaderboard ;

CREATE TABLE mart.driver_leaderboard AS

SELECT
    r.driver_id ,
    r.driver_name, 
    v.vehicle_type, 
    r.rating ,
    count(*) as number_rides ,
    sum(r.fare_amount) as total_revenue

FROM
    mart.rides AS r

LEFT JOIN 
    staging.vehicles v ON r.driver_id = v.driver_id

GROUP BY
    r.driver_id,
    r.driver_name,
    v.vehicle_type,
    r.rating

ORDER BY
    total_revenue DESC;
