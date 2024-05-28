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

-- session_cnt, DT Trend
WITH df as (
            SELECT month
              , session_id
              , datetime_diff(end_session_dt, start_session_dt, MILLISECOND)/1000/60 as times_diff_minute
            FROM (
                  SELECT format_date("%Y-%m", date(datetime_trunc(created_at, month))) as month 
                    , session_id
                    , min(created_at) over (partition by session_id) as start_session_dt
                    , max(created_at) over (partition by session_id) as end_session_dt
                  FROM `sql-study-420204.looker_ecommerce_dataset.events` 
                  )
            -- 임의 세션유효시간
            WHERE datetime_diff(end_session_dt, start_session_dt, MILLISECOND)/1000/60 <= 60
             )


SELECT month
  , count(distinct session_id) as session_cnt
  , avg(times_diff_minute) as avg_times_diff_minute
FROM df
GROUP BY 1
ORDER BY 1
