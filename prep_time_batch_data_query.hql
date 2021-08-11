SELECT
      resturantid AS MerchantId,
      hourBin,
      minuteBin,
      weekDay,
      SUM(orders.preTimeSec) / COUNT(orders.orderid) AS {replace:average_column_name}
FROM
(
    SELECT
        o_c.orderid AS orderid,
        resturantid,
        -- orderConfirmedTimestamp,
        -- orderPrepTimestamp,
        DATE_FORMAT(orderConfirmedTimestamp, 'u') AS weekDay,
        UNIX_TIMESTAMP(orderPrepTimestamp) - UNIX_TIMESTAMP(orderConfirmedTimestamp) AS preTimeSec,
        HOUR(orderConfirmedTimestamp) AS hourBin,
        FLOOR(MINUTE(orderConfirmedTimestamp) / 15) AS minuteBin
    FROM
    (
        SELECT
            orderid,
            createddatetime AS orderConfirmedTimestamp
        FROM hive_db_stage.fact_food_orders_log
        WHERE
          date_time >= DATE_SUB("${hiveconf:end_date}",{replace:date_shift})
          AND date_time <= "${hiveconf:end_date}"
          AND statusid = 12
    ) AS o_c
    LEFT JOIN (
        SELECT
            or_pe.orderid AS orderid,
            resturantid,
            createddatetime AS orderPrepTimestamp
        FROM
        (
            SELECT
                COALESCE(p_e.orderid, o_r.orderid) AS orderid,
                COALESCE(o_r.createddatetime, p_e.createddatetime) AS createddatetime
            FROM
            (
                SELECT
                    orderid,
                    createddatetime
                FROM hive_db_stage.fact_food_orders_log
                WHERE
                  date_time >= DATE_SUB("${hiveconf:end_date}",{replace:date_shift})
                  AND date_time <= "${hiveconf:end_date}"
                  AND statusid = 17
             ) AS p_e FULL
             OUTER JOIN (
                SELECT
                  orderid,
                  createddatetime
                FROM hive_db_stage.fact_food_orders_log
                WHERE
                  date_time >= DATE_SUB("${hiveconf:end_date}",{replace:date_shift})
                  AND date_time <= "${hiveconf:end_date}"
                  AND statusid = 18
             ) AS o_r ON p_e.orderid = o_r.orderid
        ) AS or_pe
        LEFT JOIN (
            SELECT
              orderid,
              resturantid
            FROM hive_db_stage.fact_unique_food_orders
            WHERE
              createddate >= CAST(REGEXP_REPLACE(DATE_SUB("${hiveconf:end_date}",{replace:date_shift}), "-", "") AS INT)
              AND createddate <= CAST(REGEXP_REPLACE("${hiveconf:end_date}", "-", "") AS INT)
        ) AS res_or ON or_pe.orderid = res_or.orderid
    ) AS final_or_pe ON o_c.orderid = final_or_pe.orderid
    WHERE orderPrepTimestamp IS NOT NULL
) AS orders
GROUP BY
  minuteBin,
  hourBin,
  weekDay,
  resturantid
