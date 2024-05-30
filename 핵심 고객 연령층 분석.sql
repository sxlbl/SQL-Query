WITH df as (
            SELECT *
            FROM (
                  SELECT  o.user_id
                    , ord_dt
                    , min(ord_dt) over (partition by o.user_id) as first_ord_dt
                    , order_amount
                    , 2024-age as age_range
                  FROM `sql-study-420204.sample_data.order_ltv` as o
                    LEFT OUTER JOIN `sql-study-420204.sample_data.users` as u
                    ON o.user_id= u.user_id
                   WHERE status = "completed"
                  )
            WHERE first_ord_dt between "2023-01-01" and "2023-01-31"
            )
, ddf as (
          SELECT *
            , case when date(ord_dt) = date(first_ord_dt) then "M-0"
                  when date(ord_dt) > date(first_ord_dt) and date(ord_dt) <= datetime_add(date(first_ord_dt), interval 1 month) then "M-1"
                  when date(ord_dt) > datetime_add(date(first_ord_dt), interval 1 month) 
                      and date(ord_dt) <= datetime_add(date(first_ord_dt), interval 2 month) then "M-2"
                  when date(ord_dt) > datetime_add(date(first_ord_dt), interval 2 month) 
                      and date(ord_dt) <= datetime_add(date(first_ord_dt), interval 3 month) then "M-3"
                  when date(ord_dt) > datetime_add(date(first_ord_dt), interval 3 month) 
                      and date(ord_dt) <= datetime_add(date(first_ord_dt), interval 4 month) then "M-4"
                      else "over-5m" end as retention_group
          FROM df
          )
SELECT *
  , arpu * retention as ltv
FROM (
      SELECT age_range
        , retention_group
        , count(distinct user_id) as user_cnt
        , sum(order_amount) as total_amount
        , round(sum(order_amount) / count(distinct user_id),2) as arpu
        , count(distinct user_id) / max(count(distinct user_id)) over (partition by age_range) as retention
      FROM ddf
      GROUP BY 1,2
      ORDER BY 1,2
      )
