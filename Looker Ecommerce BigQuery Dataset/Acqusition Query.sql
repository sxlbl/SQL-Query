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

-- 신규유저 Trend
WITH df as (
            SELECT first_visit_dt
                , traffic_source
                , user_id
            FROM (
                  SELECT cast(user_id as int64) as user_id
                    , traffic_source
                    , created_at
                    , min(created_at) over (partition by cast(user_id as int64)) as first_visit_dt
                  FROM `sql-study-420204.looker_ecommerce_dataset.events` 
                  )
            WHERE created_at = first_visit_dt
            )

SELECT format_datetime("%Y-%m", datetime_trunc(first_visit_dt,month)) as month
  , traffic_source
  , count(distinct user_id) as new_users_cnt
FROM df
GROUP BY 1,2

-- 월별 new repeat 유저 cnt, 비율

WITH df as (
            SELECT *
              , case when month = first_visit_month then "new" else "repeat" end as new_or_repeat
            FROM (
                  SELECT cast(user_id as int64) as user_id
                    , format_datetime("%Y-%m", (datetime_trunc(created_at, month))) as month
                    , format_datetime("%Y-%m", first_value(datetime_trunc(created_at, month)) over (partition by cast(user_id as int64) order by datetime_trunc(created_at, month))) as first_visit_month
                    , city
                    , traffic_source
                    , event_type
                  FROM `sql-study-420204.looker_ecommerce_dataset.events` 
                  )
            )

SELECT month
  , count(distinct user_id) as user_cnt
  , count(distinct case when new_or_repeat = "new" then user_id end) as new_user
  , count(distinct case when new_or_repeat = "repeat" then user_id end) as repeat_user
FROM df
GROUP BY 1

