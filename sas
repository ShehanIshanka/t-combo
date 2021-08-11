SELECT i.MerchantId, t.key, i.DatePeriod, i.DateRankOri, t.value
FROM
(
    SELECT h.MerchantId,
           CONCAT(h.DateRank, ":", CAST(h.OrderCount AS STRING)) AS OrderCount,
           CONCAT(h.DateRank, ":", CAST(h.SalesTotal AS STRING)) AS SalesTotal,
           CONCAT(h.DateRank, ":", CAST(h.AverageOrderValue AS STRING)) AS AverageOrderValue,
           CONCAT("", ":", CAST(( COALESCE(SUM(h.SalesTotal) OVER (PARTITION BY h.MerchantId,h.DatePeriod)/SUM(h.OrderCount) OVER (PARTITION BY h.MerchantId,h.DatePeriod),0) ) AS STRING)) AS AverageOrderValuePeriod,
           h.DateRankOri,
           h.DatePeriod
    FROM
    (
        SELECT g.MerchantId,
               g.DatePeriod,
               g.DateRank AS DateRankOri,
               CONCAT(DATE_FORMAT(g.DateRank,"dd/MM")," ",g.DayName) AS DateRank,
               SUM(g.OrderCount) AS OrderCount,
               SUM(g.SalesTotal) AS SalesTotal,
               SUM(g.AverageOrderValue) AS AverageOrderValue
        FROM
        (
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_original_query.hql,date_period:1,date_period_string1:"1|7",date_period_string2:"1|7"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_original_query.hql,date_period:8,date_period_string1:"1|7",date_period_string2:"7|14"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_duplicate_query.hql,date_period1:1,date_period2:7,date_period_string1:"1|7",date_period_string2:"7|14"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_duplicate_query.hql,date_period1:8,date_period2:-7,date_period_string1:"1|7",date_period_string2:"1|7"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_original_query.hql,date_period:1,date_period_string1:"1|30",date_period_string2:"1|30"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_original_query.hql,date_period:31,date_period_string1:"1|30",date_period_string2:"30|60"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_duplicate_query.hql,date_period1:1,date_period2:30,date_period_string1:"1|30",date_period_string2:"30|60"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_duplicate_query.hql,date_period1:31,date_period2:-30,date_period_string1:"1|30",date_period_string2:"1|30"}
        ) AS g GROUP BY g.MerchantId, g.DatePeriod, g.DateRank, g.DayName
        UNION ALL
        SELECT g.MerchantId,
               g.DatePeriod,
               g.DateRank AS DateRankOri,
               CONCAT(DATE_FORMAT(g.DateRank,"dd/MM")," ",g.DayName, " ", HOUR(g.DateRank)) AS DateRank,
               SUM(g.OrderCount) AS OrderCount,
               SUM(g.SalesTotal) AS SalesTotal,
               SUM(g.AverageOrderValue) AS AverageOrderValue
        FROM
        (
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_original_query.hql,date_period:1,date_period_string1:"1|1",date_period_string2:"1|1"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_original_query.hql,date_period:2,date_period_string1:"1|1",date_period_string2:"2|2"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_duplicate_query.hql,date_period1:1,date_period2:1,date_period_string1:"1|1",date_period_string2:"2|2"}
            UNION ALL
            {function:scripts/hive/merchant-analytics/sales-analytics/sub-queries/sales_analytics_pre_bulk_load_data_duplicate_query.hql,date_period1:2,date_period2:-1,date_period_string1:"1|1",date_period_string2:"1|1"}
        ) AS g GROUP BY g.MerchantId, g.DatePeriod, g.DateRank, g.DayName
    ) AS h
) AS i
LATERAL VIEW explode (map(
   'to', i.OrderCount,
   'ds', i.SalesTotal,
   'aov', i.AverageOrderValue,
   'aovp', i.AverageOrderValuePeriod
 )) t as key, value
DISTRIBUTE BY i.MerchantId, t.key, i.DatePeriod
SORT BY i.MerchantId, t.key, i.DatePeriod, i.DateRankOri