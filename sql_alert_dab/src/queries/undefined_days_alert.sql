SELECT
  report_id, 
  original_receive_date, 
  count(*) cnt
FROM fda_animals_prj.dbt_dev_gold.fact_adverse_event
GROUP BY 1, 2
HAVING original_receive_date = to_date('9999-12-31')
