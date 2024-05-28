# -----그래프
WITH df as(
            SELECT orders.mem_no    
                , orders.ord_no
                , ord_dt
                , "group"
                , case when ord_dt between "2023-06-05" and "2023-06-11" then "처치 전"
                       when ord_dt between "2023-06-12" and "2023-06-18" then "처치 후" end as period
            FROM query_27 as orders 
                LEFT JOIN query_28 as coupon_target
                ON orders.mem_no = coupon_target.mem_no
            )
            
SELECT ord_dt
    , "group"
    , count(distinct mem_no) as order_cnt
FROM df
WHERE period is not null 
    AND period = "처치 후"
    AND "group" != "CONTROL"
GROUP BY 1,2

# -----정률
WITH df as(
            SELECT orders.mem_no    
                , orders.ord_no
                , ord_dt
                , "group"
                , case when ord_dt between "2023-06-05" and "2023-06-11" then "처치 전"
                       when ord_dt between "2023-06-12" and "2023-06-18" then "처치 후" end as period
            FROM query_27 as orders 
                LEFT JOIN query_28 as coupon_target
                ON orders.mem_no = coupon_target.mem_no
            )

, daily as (
            SELECT ord_dt
                , "group"
                , count(distinct mem_no) as order_cnt
            FROM df
            WHERE period is not null 
            GROUP BY 1,2
            )

, result as (
            SELECT *
                , lag(order_cnt,6) over (partition by "group" order by ord_dt) as pre_order_cnt
            FROM daily
            ORDER BY 2,1
            )
            
SELECT *
FROM result
WHERE pre_order_cnt is not null
    AND "group" = "TEST1_정율"
    
# -----정액
WITH df as(
            SELECT orders.mem_no    
                , orders.ord_no
                , ord_dt
                , "group"
                , case when ord_dt between "2023-06-05" and "2023-06-11" then "처치 전"
                       when ord_dt between "2023-06-12" and "2023-06-18" then "처치 후" end as period
            FROM query_27 as orders 
                LEFT JOIN query_28 as coupon_target
                ON orders.mem_no = coupon_target.mem_no
            )

, daily as (
            SELECT ord_dt
                , "group"
                , count(distinct mem_no) as order_cnt
            FROM df
            WHERE period is not null 
            GROUP BY 1,2
            )

, result as (
            SELECT *
                , lag(order_cnt,6) over (partition by "group" order by ord_dt) as pre_order_cnt
            FROM daily
            ORDER BY 2,1
            )
            
SELECT *
FROM result
WHERE pre_order_cnt is not null
    AND "group" = "TEST2_정액"


# Bar차트
WITH df as(
            SELECT orders.mem_no    
                , orders.ord_no
                , ord_dt
                , "group"
                , case when ord_dt between "2023-06-05" and "2023-06-11" then "처치 전"
                       when ord_dt between "2023-06-12" and "2023-06-18" then "처치 후" end as period
            FROM query_27 as orders 
                LEFT JOIN query_28 as coupon_target
                ON orders.mem_no = coupon_target.mem_no
            )


SELECT "group"
    , period
    , count(distinct mem_no) as ord_cnt
FROM df
WHERE period is not null
GROUP BY 1,2


# 이중차분
WITH df as(
            SELECT orders.mem_no    
                , orders.ord_no
                , ord_dt
                , "group"
                , case when ord_dt between "2023-06-05" and "2023-06-11" then "처치 전"
                       when ord_dt between "2023-06-12" and "2023-06-18" then "처치 후" end as period
            FROM query_27 as orders 
                LEFT JOIN query_28 as coupon_target
                ON orders.mem_no = coupon_target.mem_no
            )
, total as (
            SELECT "group"
                , count(distinct case when period = "처치 전" then mem_no end ) as "처치 전 주문 수"
                , count(distinct case when period = "처치 후" then mem_no end ) as "처치 후 주문 수"
                , count(distinct case when period = "처치 후" then mem_no end ) - count(distinct case when period = "처치 전" then mem_no end ) as "처치 후 - 처치 전"
            FROM df
            WHERE period is not null
            GROUP BY 1
            )

SELECT *
    , case when "group" = "CONTROL" then "-" 
        else "처치 후 - 처치 전" - first_value("처치 후 - 처치 전") over () end as "이중차분"
FROM total
