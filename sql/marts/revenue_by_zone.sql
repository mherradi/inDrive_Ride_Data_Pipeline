DROP TABLE IF EXISTS mart.revenue_by_zones ;

CREATE TABLE mart.revenue_by_zones AS

SELECT
    pickup_zone ,
    count(*) as rides_count ,
    sum(fare_amount) as total_revenue

FROM
    mart.rides

GROUP BY
    pickup_zone

ORDER BY
    total_revenue DESC;

