WITH df as (
            SELECT *
              , datetime_diff(ord_dt, first_order_dt, day) as diff_day
            FROM (
                  SELECT distinct p.mem_no
                    , is_promotion
                    , ord_dt
                    , min(ord_dt) over (partition by fo.mem_no) as first_order_dt
                  FROM promotion as p
                    LEFT OUTER JOIN orders as o
                    ON p.mem_no = o.mem_no
                  )
            )

, retention as (
                  SELECT is_promotion
                    , count(distinct mem_no) as volume
                    , count(distinct case when diff_day between 1 and 7 then mem_no end) as w1_retention_cnt
                    , count(distinct case when diff_day between 8 and 14 then mem_no end) as w2_retention_cnt
                    , count(distinct case when diff_day between 15 and 21 then mem_no end) as w3_retention_cnt
                    , count(distinct case when diff_day between 22 and 28 then mem_no end) as w4_retention_cnt
                  FROM df
                  GROUP BY 1
                  )


SELECT is_promotion
  , volume
  , round(w1_retention_cnt / volume * 100 ,2) as w1_retention
  , round(w2_retention_cnt / volume * 100 ,2) as w2_retention
  , round(w3_retention_cnt / volume * 100 ,2) as w3_retention
  , round(w4_retention_cnt / volume * 100 ,2) as w4_retention
FROM retention
