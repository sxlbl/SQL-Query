-- MAU, Session 수 Trend
SELECT format_date("%Y-%m", date(datetime_trunc(created_at, month))) as month 
  , count(distinct user_id) as mau
  , count(distinct session_id) as session_cnt
FROM `sql-study-420204.looker_ecommerce_dataset.events` 
GROUP BY 1

-- mau, stk Trend
WITH daily_df as (
            -- dau
            SELECT  date(created_at) as dt
              , date(datetime_trunc(created_at, month)) as month
              , count(distinct user_id) as dau
            FROM `sql-study-420204.looker_ecommerce_dataset.events` 
            GROUP BY 1,2
            )
, monthly_df as (
                -- mau
                SELECT date(datetime_trunc(created_at, month)) as month
                  , count(distinct user_id) as mau
                FROM `sql-study-420204.looker_ecommerce_dataset.events` 
                GROUP BY 1
                )

SELECT format_date("%Y-%m", dt) as month
  , mau
  , avg(stk) as avg_stk
FROM (
    SELECT dt
      , dau
      , mau
      , round(dau/mau *100,2) as stk
    FROM daily_df as a
      LEFT OUTER JOIN monthly_df as b
      ON a.month = b.month
      )
GROUP BY 1,2
