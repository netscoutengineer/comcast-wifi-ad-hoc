/* 
CREATED BY: BEN MASON 
CREATED ON: 04/16/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT BUCKETS TIME WINDOW DAILY BREAKOUT
CUBE NAME: UNIQUE CLIENT COUNTS - STRATDEV - SPEEDBOOST - BINS - DAILY
REQUESTED BY: STRATDEV (INDIRA)
*/
WITH speedboostcounts AS ( SELECT TO_DATE(cal_timestamp_day :: VARCHAR, 'YYYYMMDD') :: DATE AS DATE,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1' )
                                                     AND ( upw_download_max_throughput_kbps <  '25000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_25mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '25000' )
                                                     AND ( upw_download_max_throughput_kbps <  '50000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_50mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '100000' )
                                                     AND ( upw_download_max_throughput_kbps <  '150000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_150mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '150000' )
                                                     AND ( upw_download_max_throughput_kbps <  '200000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_200mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '200000' )
                                                     AND ( upw_download_max_throughput_kbps <  '250000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_250mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '250000' )
                                                     AND ( upw_download_max_throughput_kbps <  '300000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_300mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '300000' )
                                                     AND ( upw_download_max_throughput_kbps <  '350000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_350mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '350000' )
                                                     AND ( upw_download_max_throughput_kbps <  '400000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_400mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '400000' )
                                                     AND ( upw_download_max_throughput_kbps <  '450000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_450mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '450000' ) 
                                                     AND ( upw_download_max_throughput_kbps <  '500000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_500mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '500000' )
                                                     AND ( upw_download_max_throughput_kbps <  '600000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_600mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '600000' )
                                                     AND ( upw_download_max_throughput_kbps <  '650000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_650mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '650000' )
                                                     AND ( upw_download_max_throughput_kbps <  '750000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_750mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '750000' )
                                                     AND ( upw_download_max_throughput_kbps < '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_1000mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_1000mbps
                        FROM 
                            nba.f_user_plane_wireline f
                        -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                        WHERE 
                            cal_timestamp_day >= 20240101 
                            AND cal_timestamp_day < 20240301
                        -- ONLY XM SUBSCRIBERS   
                            AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )                  
                        GROUP BY
                            cal_timestamp_day
                        )
SELECT 
    *
FROM 
    speedboostcounts


/* 
CREATED BY: BEN MASON 
CREATED ON: 04/16/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT BUCKETS LAST n DAYS 
REQUESTED BY: STRATDEV (INDIRA)
*/
WITH speedboostcounts AS ( SELECT 
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1' )
                                                     AND ( upw_download_max_throughput_kbps <  '25000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_25mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '25000' )
                                                     AND ( upw_download_max_throughput_kbps <  '50000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_50mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '100000' )
                                                     AND ( upw_download_max_throughput_kbps <  '150000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_150mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '150000' )
                                                     AND ( upw_download_max_throughput_kbps <  '200000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_200mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '200000' )
                                                     AND ( upw_download_max_throughput_kbps <  '250000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_250mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '250000' )
                                                     AND ( upw_download_max_throughput_kbps <  '300000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_300mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '300000' )
                                                     AND ( upw_download_max_throughput_kbps <  '350000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_350mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '350000' )
                                                     AND ( upw_download_max_throughput_kbps <  '400000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_400mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '400000' )
                                                     AND ( upw_download_max_throughput_kbps <  '450000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_450mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '450000' ) 
                                                     AND ( upw_download_max_throughput_kbps <  '500000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_500mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '500000' )
                                                     AND ( upw_download_max_throughput_kbps <  '600000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_600mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '600000' )
                                                     AND ( upw_download_max_throughput_kbps <  '650000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_650mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '650000' )
                                                     AND ( upw_download_max_throughput_kbps <  '750000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_750mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '750000' )
                                                     AND ( upw_download_max_throughput_kbps < '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_1000mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_1000mbps
                        FROM 
                            nba.f_user_plane_wireline f
                        /* FILTER CONDITIONS FOR QUALIFIED STATEMENT */
                        WHERE 
                            cal_timestamp_day >= 20240101 
                            AND cal_timestamp_day < 20240301
                            -- ONLY XM SUBSCRIBERS   
                            AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )                  
                       
                        )
SELECT 
    *
FROM 
    speedboostcounts



/* 
CREATED BY: BEN MASON 
CREATED ON: 04/16/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT BUCKETS TIME WINDOW AGGREGATED 
REQUESTED BY: INDIRA
*/
WITH speedboostcounts AS ( SELECT 
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1' )
                                                     AND ( upw_download_max_throughput_kbps <  '25000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_25mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '25000' )
                                                     AND ( upw_download_max_throughput_kbps <  '50000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_50mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '100000' )
                                                     AND ( upw_download_max_throughput_kbps <  '150000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_150mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '150000' )
                                                     AND ( upw_download_max_throughput_kbps <  '200000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_200mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '200000' )
                                                     AND ( upw_download_max_throughput_kbps <  '250000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_250mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '250000' )
                                                     AND ( upw_download_max_throughput_kbps <  '300000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_300mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '300000' )
                                                     AND ( upw_download_max_throughput_kbps <  '350000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_350mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '350000' )
                                                     AND ( upw_download_max_throughput_kbps <  '400000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_400mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '400000' )
                                                     AND ( upw_download_max_throughput_kbps <  '450000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_450mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '450000' ) 
                                                     AND ( upw_download_max_throughput_kbps <  '500000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_500mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '500000' )
                                                     AND ( upw_download_max_throughput_kbps <  '600000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_600mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '600000' )
                                                     AND ( upw_download_max_throughput_kbps <  '650000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_650mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '650000' )
                                                     AND ( upw_download_max_throughput_kbps <  '750000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_750mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '750000' )
                                                     AND ( upw_download_max_throughput_kbps < '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_1000mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_1000mbps
                        FROM 
                            nba.f_user_plane_wireline f
                        -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                        WHERE 
                            cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-04-01 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC','YYYYMMDD' ) :: integer
                                AND TO_CHAR( TO_TIMESTAMP_TZ('2024-05-01 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC','YYYYMMDD' ) :: integer
                                AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-04-01 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                                AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-05-01 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') ) 
                            -- ONLY XM SUBSCRIBERS   
                            AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )                  
                         
                        )
SELECT 
    *
FROM 
    speedboostcounts


/* 
CREATED BY: BEN MASON 
CREATED ON: 04/16/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT SPEED BUCKETS LAST n DAYS 
REQUESTED BY: STRATDEV INDIRA
*/
WITH speedboostcounts AS ( SELECT 
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1' )
                                                     AND ( upw_download_max_throughput_kbps <  '25000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_25mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '25000' )
                                                     AND ( upw_download_max_throughput_kbps <  '50000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_50mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '100000' )
                                                     AND ( upw_download_max_throughput_kbps <  '150000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_150mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '150000' )
                                                     AND ( upw_download_max_throughput_kbps <  '200000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_200mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '200000' )
                                                     AND ( upw_download_max_throughput_kbps <  '250000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_250mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '250000' )
                                                     AND ( upw_download_max_throughput_kbps <  '300000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_300mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '300000' )
                                                     AND ( upw_download_max_throughput_kbps <  '350000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_350mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '350000' )
                                                     AND ( upw_download_max_throughput_kbps <  '400000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_400mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '400000' )
                                                     AND ( upw_download_max_throughput_kbps <  '450000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_450mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '450000' ) 
                                                     AND ( upw_download_max_throughput_kbps <  '500000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_500mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '500000' )
                                                     AND ( upw_download_max_throughput_kbps <  '600000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_600mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '600000' )
                                                     AND ( upw_download_max_throughput_kbps <  '650000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_650mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '650000' )
                                                     AND ( upw_download_max_throughput_kbps <  '750000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_750mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '750000' )
                                                     AND ( upw_download_max_throughput_kbps < '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_1000mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1000000' )
                                                    THEN f.subscriber_id
                                                    ELSE NULL
                                                END ) ) AS clients_gt_1000mbps
                            FROM 
                                nba.f_user_plane_wireline f
                            /* FILTER CONDITIONS FOR QUALIFIED STATEMENT */
                            WHERE 
                                cal_timestamp_time > sysdate - interval '30' DAY
                                -- ONLY XM SUBSCRIBERS   
                                AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )                  
                        
                            )
    SELECT 
        *
    FROM 
        speedboostcounts


/* 
CREATED BY: BEN MASON 
CREATED ON: 06/17/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT SPEED BUCKETS - MONTHLY 
CUBE NAME: UNIQUE CLIENT COUNTS - STRATDEV - SPEEDBOOST - BINS - MONTHLY
REQUESTED BY: STRATDEV INDIRA
*/
WITH speedboostcounts AS ( SELECT TO_DATE(cal_timestamp_month :: VARCHAR, 'YYYYMM') :: DATE AS MONTH, 
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1' )
                                                     AND ( upw_download_max_throughput_kbps <  '25000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_25mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '25000' )
                                                     AND ( upw_download_max_throughput_kbps <  '50000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_50mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '100000' )
                                                     AND ( upw_download_max_throughput_kbps <  '150000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_150mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '150000' )
                                                     AND ( upw_download_max_throughput_kbps <  '200000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_200mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '200000' )
                                                     AND ( upw_download_max_throughput_kbps <  '250000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_250mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '250000' )
                                                     AND ( upw_download_max_throughput_kbps <  '300000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_300mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '300000' )
                                                     AND ( upw_download_max_throughput_kbps <  '350000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_350mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '350000' )
                                                     AND ( upw_download_max_throughput_kbps <  '400000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_400mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '400000' )
                                                     AND ( upw_download_max_throughput_kbps <  '450000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_450mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '450000' ) 
                                                     AND ( upw_download_max_throughput_kbps <  '500000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_500mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '500000' )
                                                     AND ( upw_download_max_throughput_kbps <  '600000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_600mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '600000' )
                                                     AND ( upw_download_max_throughput_kbps <  '650000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_650mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '650000' )
                                                     AND ( upw_download_max_throughput_kbps <  '750000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_750mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '750000' )
                                                     AND ( upw_download_max_throughput_kbps < '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_1000mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_1000mbps
                        FROM 
                            nba.f_user_plane_wireline f
                        /* FILTER CONDITIONS FOR QUALIFIED STATEMENT */
                        WHERE cal_timestamp_day >= 20240101
                            AND cal_timestamp_day < 20240301
                            -- ONLY XM SUBSCRIBERS   
                            AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )                  
                        GROUP BY 
                            cal_timestamp_month
                        )
SELECT 
    *
FROM 
    speedboostcounts



/* 
CREATED BY: BEN MASON 
CREATED ON: 05/08/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS - COUNT CUMULATIVE GREATER THAN SPEEDS - TIME WINDOW AGGREGATED 
REQUESTED BY: INDIRA
*/
WITH speedboostcounts AS ( SELECT cal_timestamp_month,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '25000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_25mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '50000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_50mbps,
                            COUNT( DISTINCT ( CASE WHEN (upw_download_max_throughput_kbps >  '150000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_150mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '200000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_200mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '250000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_250mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '300000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_300mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '350000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_350mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '400000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_400mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '450000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_450mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '500000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_500mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '600000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_600mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '650000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_650mbps,
                            COUNT( DISTINCT ( CASE WHEN ( upw_download_max_throughput_kbps >  '750000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_750mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps > '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_1000mbps
                        FROM 
                            nba.f_user_plane_wireline f
                        -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                        WHERE 
                            cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-05-01 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC','YYYYMMDD' ) :: integer
                                AND TO_CHAR( TO_TIMESTAMP_TZ('2024-05-31 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC','YYYYMMDD' ) :: integer
                                AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-05-01 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                                AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-05-31 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') ) 
                            -- ONLY XM SUBSCRIBERS   
                            AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )                  
                        GROUP BY cal_timestamp_month
                        )
SELECT 
    *
FROM 
    speedboostcounts

/* 
CREATED BY: BEN MASON 
CREATED ON: 05/08/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT CUMULATIVE GREATER THAN SPEEDS  - DAILY
REQUESTED BY: STRATDEV (INDIRA)
*/
WITH speedboostcounts AS
                        ( SELECT TO_DATE(cal_timestamp_day :: VARCHAR, 'YYYYMMDD') :: DATE AS DATE,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '25000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_25mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '50000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_50mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '150000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_150mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '200000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_200mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '250000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_250mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '300000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_300mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '350000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_350mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '400000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_400mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '450000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_450mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '500000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_500mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '600000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_600mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '650000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_650mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '750000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_750mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '1000000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_1000mbps
                           FROM nba.f_user_plane_wireline f
                            -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                           WHERE cal_timestamp_day >= 20240201
                                 AND cal_timestamp_day < 20240601
                            -- ONLY XM SUBSCRIBERS
                                 AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )
                           GROUP BY cal_timestamp_day                                     
                        )
SELECT *
FROM speedboostcounts


/* 
CREATED BY: BEN MASON 
CREATED ON: 05/08/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT CUMULATIVE GREATER THAN SPEEDS  - MONTHLY
REQUESTED BY: STRATDEV (INDIRA)
*/
WITH speedboostcounts AS ( SELECT TO_DATE(cal_timestamp_month :: VARCHAR, 'YYYYMM') :: DATE AS MONTH,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '25000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_25mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '50000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_50mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '150000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_150mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '200000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_200mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '250000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_250mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '300000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_300mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '350000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_350mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '400000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_400mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '450000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_450mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '500000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_500mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '600000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_600mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '650000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_650mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '750000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_750mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '1000000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_1000mbps
                           FROM nba.f_user_plane_wireline f
                            -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                           WHERE cal_timestamp_day >= 20240101
                                 AND cal_timestamp_day < 20240301
                            -- ONLY XM SUBSCRIBERS
                                 AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )
                           GROUP BY cal_timestamp_month                                    
                             )
SELECT *
FROM speedboostcounts






/* 
CREATED BY: BEN MASON 
CREATED ON: 05/08/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS - COUNT CUMULATIVE GREATER THAN SPEEDS - AP MODEL - TIME WINDOW AGGREGATED 
REQUESTED BY: INDIRA
*/
WITH speedboostcounts AS ( SELECT cal_timestamp_month,
                                  ap_model_name,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '25000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_25mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '50000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_50mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '150000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_150mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '200000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_200mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '250000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_250mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '300000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_300mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '350000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_350mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '400000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_400mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '450000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_450mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '500000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_500mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '600000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_600mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '650000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_650mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '750000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_750mbps,
                                  COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '1000000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_1000mbps
                           FROM nba.f_user_plane_wireline f
                            -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                           WHERE cal_timestamp_day >= 20240501
                                 AND cal_timestamp_day < 20240601
                            -- ONLY XM SUBSCRIBERS
                                 AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )
                           GROUP BY cal_timestamp_month,
                                    ap_model_name )
SELECT *
FROM speedboostcounts

/* 
CREATED BY: BEN MASON 
CREATED ON: 05/07/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT BUCKETS TIME WINDOW AGGREGATED BROKEN OUT BY AP MODEL
REQUESTED BY: RUSHABH
*/
WITH speedboostcounts AS ( SELECT ap_model_name,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1' )
                                                     AND ( upw_download_max_throughput_kbps <  '25000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_25mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '25000' )
                                                     AND ( upw_download_max_throughput_kbps <  '50000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_50mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '100000' )
                                                     AND ( upw_download_max_throughput_kbps <  '150000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_150mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '150000' )
                                                     AND ( upw_download_max_throughput_kbps <  '200000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_200mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '200000' )
                                                     AND ( upw_download_max_throughput_kbps <  '250000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_250mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '250000' )
                                                     AND ( upw_download_max_throughput_kbps <  '300000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_300mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '300000' )
                                                     AND ( upw_download_max_throughput_kbps <  '350000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_350mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '350000' )
                                                     AND ( upw_download_max_throughput_kbps <  '400000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_400mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '400000' )
                                                     AND ( upw_download_max_throughput_kbps <  '450000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_450mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '450000' ) 
                                                     AND ( upw_download_max_throughput_kbps <  '500000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_500mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '500000' )
                                                     AND ( upw_download_max_throughput_kbps <  '600000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_600mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '600000' )
                                                     AND ( upw_download_max_throughput_kbps <  '650000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_650mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '650000' )
                                                     AND ( upw_download_max_throughput_kbps <  '750000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_750mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '750000' )
                                                     AND ( upw_download_max_throughput_kbps < '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_1000mbps,
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '1000000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_gt_1000mbps
                        FROM 
                            nba.f_user_plane_wireline f
                        -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                        WHERE 
                            cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-04-30 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC','YYYYMMDD' ) :: integer
                                AND TO_CHAR( TO_TIMESTAMP_TZ('2024-05-01 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC','YYYYMMDD' ) :: integer
                                AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-04-30 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                                AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-05-01 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') ) 
                            -- ONLY XM SUBSCRIBERS   
                            AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )                  
                        GROUP BY 
                            ap_model_name 
                        )
SELECT 
    *
FROM 
    speedboostcounts

----------------------------------------------------------------------------------------------------------------------
/*  EVERYTHING BELOW THIS ARE TEST QUERIES FOR SPEEDBOOST */
----------------------------------------------------------------------------------------------------------------------

WITH qe100 AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS clients_100Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_time > sysdate - interval '30' DAY
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '1000000' )
                )
SELECT clients_100Mbps
FROM qe100


WITH qe AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS clients_200Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-03-13 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC','YYYYMMDD' ) :: integer
                   AND TO_CHAR( TO_TIMESTAMP_TZ('2024-04-03 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC','YYYYMMDD' ) :: integer
                   AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-03-13 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                        AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-04-03 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') )
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '200000' )
                )
SELECT clients_200Mbps
FROM qe
ORDER BY 1 

/* Speedboost Bucket Analysis Last 30 days */
WITH qe250 AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS clients_250Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day < TO_CHAR(NOW() - interval '30' DAY, 'YYYYMMDD') :: INT
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '250000' )
                )
SELECT clients_250Mbps
FROM qe250

/* Speedboost Bucket Analysis Last 30 days */
WITH qe300 AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS clients_300Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day < TO_CHAR(NOW() - interval '30' DAY, 'YYYYMMDD') :: INT
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '300000' )
                )
SELECT clients_300Mbps
FROM qe300

/* Speedboost Bucket Analysis Last 30 days */
WITH qe400 AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS clients_400Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day < TO_CHAR(NOW() - interval '30' DAY, 'YYYYMMDD') :: INT
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '400000' )
                )
SELECT clients_400Mbps
FROM qe400



/* 
CREATED BY: BEN MASON 
CREATED ON: 07/09/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT BUCKETS TIME WINDOW AGGREGATED 
REQUESTED BY: INDIRA
*/
WITH speedboostcounts AS ( SELECT COUNT( DISTINCT ( CASE
                                                        WHEN ( upw_download_max_throughput_kbps > '200000' )
                                                        THEN f.subscriber_id
                                                        ELSE NULL
                                                    END ) ) AS clients_gt_200mbps
                           FROM nba.f_user_plane_wireline f
                        -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                           WHERE cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-07-08 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                                                    'YYYYMMDD' ) :: integer
                                 AND TO_CHAR( TO_TIMESTAMP_TZ('2024-07-09 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                              'YYYYMMDD' ) :: integer
                                 AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-07-09 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                                       AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-07-09 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') )
                            -- ONLY XM SUBSCRIBERS
                                 AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
SELECT *
FROM speedboostcounts



----------------------------------------------------------
/* Speedboost Bucket Analysis Last 30 days */
WITH qe200 AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS clients_200Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day < TO_CHAR(NOW() - interval '1' DAY, 'YYYYMMDD') :: INT
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '200000' )
                ) ,
                
	qe250 AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS clients_250Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day < TO_CHAR(NOW() - interval '1' DAY, 'YYYYMMDD') :: INT
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '250000' )
                )
SELECT clients_200Mbps AS 'clients_200Mbps'
FROM qe200
UNION ALL
SELECT clients_250Mbps AS 'clients_250Mbps'
FROM qe250


-----------------------------------------------------------

/* Speedboost Bucket Analysis Last 30 days */
WITH clients_200Mbps AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS c200,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day < TO_CHAR(NOW() - interval '1' DAY, 'YYYYMMDD') :: INT
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '200000' )
                		) ,        
	clients_250Mpbs AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.subscriber_id
                                   ELSE NULL
                               END ) ) AS c250,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day < TO_CHAR(NOW() - interval '1' DAY, 'YYYYMMDD') :: INT
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '250000' )
                )
SELECT 
	c200, 
	c250
FROM clients_250Mbps


-----------------------------------------------------------
/* EXAMPLE FROM EXPLORER */

WITH cte_0 AS ( SELECT TIME_SLICE(cal_timestamp_time at timezone 'Etc/UTC', 86400000, 'MILLISECOND') AS bucket,
                       COUNT( DISTINCT ( CASE
                                             WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                                             THEN f.subscriber_id
                                             ELSE NULL
                                         END ) ) AS m1146_f0
                FROM nba.a_user_plane_wireline_r_daily f
                WHERE cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-05-02 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                                         'YYYYMMDD' ) :: integer
                      AND TO_CHAR( TO_TIMESTAMP_TZ('2024-05-09 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                   'YYYYMMDD' ) :: integer
                      AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-05-02 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                            AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-05-09 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') )
                      AND ( upw_download_max_throughput_kbps > '250000' )
                      AND ( ( ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) ) ) )
                GROUP BY 1 )
SELECT /*+label('queryservice-18555-18988-dcf8065c-7543-4fbe-9460-dd44d8a7b9ee')*/ COALESCE(cte_0.bucket) AS 'bucket',
       cte_0.m1146_f0 AS m1146
FROM cte_0
ORDER BY bucket

---------------------------------------------------------------
/* DEVICE MODEL BREAKOUT */
/* 091124 */

/* 
CREATED BY: BEN MASON 
CREATED ON: 04/16/2024 
DATA SOURCE NAME: 
DESCRIPTION: SPEEDBOOST ANALYSIS - UNIQUE CLIENTS COUNT BUCKETS TIME WINDOW DAILY BREAKOUT
REQUESTED BY: STRATDEV (INDIRA)
*/
WITH speedboostcounts AS ( SELECT TO_DATE(cal_timestamp_day :: VARCHAR, 'YYYYMMDD') :: DATE AS DATE,
                            userequipment_manufacturer_name,
                            userequipment_model_name,                         
                            COUNT( DISTINCT ( CASE WHEN  ( upw_download_max_throughput_kbps >= '200000' )
                                                     AND ( upw_download_max_throughput_kbps <  '250000' )
                                                THEN f.subscriber_id
                                                ELSE NULL
                                            END ) ) AS clients_250mbps                           
                        FROM 
                            nba.f_user_plane_wireline f
                        -- FILTER CONDITIONS FOR QUALIFIED STATEMENT
                        WHERE 
                            cal_timestamp_day >= 20240901 
                            AND cal_timestamp_day < 20240902
                        -- ONLY XM SUBSCRIBERS   
                            AND clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' )    
                            AND userequipment_manufacturer_name IN ( 'Apple')     
                        GROUP BY
                            cal_timestamp_day,
                            userequipment_manufacturer_name,
                            userequipment_model_name
                        )
SELECT 
    *
FROM 
    speedboostcounts
ORDER BY clients_250mbps DESC