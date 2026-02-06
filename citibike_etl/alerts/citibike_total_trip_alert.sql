SELECT trip_start_date, SUM(total_trips) ttl_trips
FROM citibike_dev.03_gold.daily_ride_summary
GROUP BY trip_start_date
HAVING SUM(total_trips) < 25
