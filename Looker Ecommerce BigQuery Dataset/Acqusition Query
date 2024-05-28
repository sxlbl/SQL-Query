-- MAU, Session 수 Trend
SELECT format_date("%Y-%m", date(datetime_trunc(created_at, month))) as month 
  , count(distinct user_id) as mau
  , count(distinct session_id) as session_cnt
FROM `sql-study-420204.looker_ecommerce_dataset.events` 
GROUP BY 1
