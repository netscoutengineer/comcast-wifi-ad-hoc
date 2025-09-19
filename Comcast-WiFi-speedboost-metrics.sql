
WITH cte_0 AS ( SELECT TIME_SLICE(cal_timestamp_time at timezone 'Etc/UTC', 86400000, 'MILLISECOND') AS bucket,
                       COUNT( DISTINCT ( CASE
                                             WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' ) )
                                             THEN f.clientequipment_mac_address
                                             ELSE NULL
                                         END ) ) AS m1146_f0,
                       MAX( CASE
                                WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' ) )
                                THEN f.upw_download_max_throughput_kbps
                                ELSE NULL
                            END ) AS m1061_f0
                FROM nba.a_user_plane_wireline_r_daily f
                WHERE cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-03-05 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                                         'YYYYMMDD' ) :: integer
                      AND TO_CHAR( TO_TIMESTAMP_TZ('2024-04-04 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                   'YYYYMMDD' ) :: integer
                      AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-03-05 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                            AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-04-04 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') )
                      AND ( upw_download_max_throughput_kbps > '153600' )
                GROUP BY 1 )
SELECT /*+label('queryservice-18555-18982-f847ba49-d1db-4e50-bd20-f121d5cee713')*/ COALESCE(cte_0.bucket) AS 'bucket',
       cte_0.m1146_f0 AS m1146,
       cte_0.m1061_f0 / 1000 AS m1061
FROM cte_0
ORDER BY bucket

WITH cte_0 AS ( SELECT TIME_SLICE(cal_timestamp_time at timezone 'Etc/UTC', 3600000, 'MILLISECOND') AS bucket,
                       COUNT( DISTINCT ( CASE
                                             WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' ) )
                                             THEN f.clientequipment_mac_address
                                             ELSE NULL
                                         END ) ) AS m1146_f0,
                       MAX( CASE
                                WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' ) )
                                THEN f.upw_download_max_throughput_kbps
                                ELSE NULL
                            END ) AS m1061_f0
                FROM nba.a_user_plane_wireline_r_1hr f
                WHERE cal_timestamp_hour BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-04-03 15:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                                          'YYYYMMDDHH' ) :: integer
                      AND TO_CHAR( TO_TIMESTAMP_TZ('2024-04-04 15:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                   'YYYYMMDDHH' ) :: integer
                      AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-04-03 15:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                            AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-04-04 15:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') )
                      AND ( upw_download_max_throughput_kbps > '150000' )
                GROUP BY 1 )
SELECT /*+label('queryservice-18555-18982-e545e555-4c81-4457-b5bf-5660a59a5e50')*/ COALESCE(cte_0.bucket) AS 'bucket',
       cte_0.m1146_f0 AS m1146,
       cte_0.m1061_f0 / 1000 AS m1061
FROM cte_0
ORDER BY bucket





WITH cte_0 AS ( SELECT f.cal_timestamp_day AS 'bucket',
                       COUNT( DISTINCT ( CASE
                                             WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' ) )
                                             THEN f.clientequipment_mac_address
                                             ELSE NULL
                                         END ) ) AS m1146_f0,
                       MAX(f.upw_download_max_throughput_kbps) AS m1061_f0
                FROM nba.a_user_plane_wireline_r_daily f
                WHERE cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-03-28 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                                         'YYYYMMDD' ) :: integer
                      AND TO_CHAR( TO_TIMESTAMP_TZ('2024-04-04 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                   'YYYYMMDD' ) :: integer
                      AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-03-28 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                            AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-04-04 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') )
                      AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' ) )
                GROUP BY 1 )
SELECT /*+label('queryservice-18555-18984-91dfaa89-dcd5-4b63-a863-3518995569c9')*/ COALESCE(cte_0.bucket) AS 'bucket',
       cte_0.m1146_f0 AS m1146,
       cte_0.m1061_f0 / 1000 AS m1061
FROM cte_0
WHERE ( cte_0.m1061_f0 / 1000 >= 25 AND cte_0.m1061_f0 / 1000 < 250 )
ORDER BY bucket
LIMIT 10


----- Comcast SpeedBoost analysis Ben Mason 040424 -----

WITH qe AS ( SELECT 
			 COUNT( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.clientequipment_mac_address
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


----- Comcast SpeedBoost analysis Ben Mason 040424 -----

WITH qetput200  AS ( SELECT 
			 COUNT ( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.clientequipment_mac_address
                                   ELSE NULL
                               END ) ) AS clients_200Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-03-16 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                                      'YYYYMMDD' ) :: integer
                   AND TO_CHAR( TO_TIMESTAMP_TZ('2024-04-15 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                'YYYYMMDD' ) :: integer
                   AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-03-16 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                         AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-04-15 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') )
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '200000' )
                )
                ,

WITH qetput250  AS ( SELECT 
			 COUNT ( DISTINCT ( CASE WHEN ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd' , 'modcon' ) )
                                   THEN f.clientequipment_mac_address
                                   ELSE NULL
                               END ) ) AS clients_200Mbps,
             MAX(f.upw_download_max_throughput_kbps)/1000 AS tput
             FROM nba.f_user_plane_wireline f
             WHERE cal_timestamp_day BETWEEN TO_CHAR( TO_TIMESTAMP_TZ('2024-02-13 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                                      'YYYYMMDD' ) :: integer
                   AND TO_CHAR( TO_TIMESTAMP_TZ('2024-03-05 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') at TIME zone 'Etc/UTC',
                                'YYYYMMDD' ) :: integer
                   AND ( cal_timestamp_time >= TO_TIMESTAMP_TZ('2024-02-13 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ')
                         AND cal_timestamp_time < TO_TIMESTAMP_TZ('2024-03-05 00:00:00 Etc/UTC', 'YYYY-MM-DD HH:MI:SS TZ') )
                   AND ( clientequipment_subscriber_type IN ( 'mod', 'modaka', 'modhsd', 'modcon' ) )
                   AND ( upw_download_max_throughput_kbps > '250000' )
                	)        
SELECT qetput250.clients                
FROM 
ORDER BY 1 




