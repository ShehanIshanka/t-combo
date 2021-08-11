INSERT OVERWRITE TABLE hive_db_insights.average_prep_time_batch PARTITION(date_time='${hiveconf:end_date}')

SELECT
    monthly_avg.MerchantId AS MerchantId,
    monthly_avg.weekDay AS weekDay,
    monthly_avg.hourBin AS hourBin,
    monthly_avg.minuteBin AS minuteBin,
    WeeklyAveragePrepTime,
    ThreeMonthAveragePrepTime
FROM
(
    {function:../prep_time_batch_data_query.hql,average_column_name:ThreeMonthAveragePrepTime,date_shift:83}
) AS monthly_avg
JOIN
(
    {function:../prep_time_batch_data_query.hql,average_column_name:WeeklyAveragePrepTime,date_shift:6}
) AS weekly_avg