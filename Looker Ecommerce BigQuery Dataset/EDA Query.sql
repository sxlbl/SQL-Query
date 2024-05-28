

-- Session 시간확인
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
            WHERE datetime_diff(end_session_dt, start_session_dt, MILLISECOND)/1000/60 >= 100
             )


SELECT month
  , count(distinct session_id) as session_cnt
  , avg(times_diff_minute) as avg_times_diff_minute
  , max(times_diff_minute) as max_times_diff_minute
FROM df
GROUP BY 1
ORDER BY 1
