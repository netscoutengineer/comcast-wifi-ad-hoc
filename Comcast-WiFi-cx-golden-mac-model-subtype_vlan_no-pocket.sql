/* 
CREATED BY: BEN MASON 
CREATED ON: 10/31/2024 
DESCRIPTION: GOLDEN QUERY - COMBINED CX METRICS BY AP MAC, AP MODEL, SUB TYPE, VLAN
CLIENT COUNT
TCP-LAT % GOOD SESSIONS
TCP-RTX % GOOD SESSIONS (TCP METHOD)
TCP-RTX % GOOD SESSIONS (ROW METHOD)
AVG CLIENT LATENCY
AVG DL THROUGHPUT (DLC)
TCP COUNT
ROW COUNT (RTX)
NO POCKET SESSIONS
DL/UL BYTES (DLC)
DL/UL TIME (DLC)
*/

WITH basedata AS 
                (  SELECT  ap_mac_address,
                           ap_model_name,
                           CASE
                                WHEN clientequipment_subscriber_type IN ( 'mod', 'modhsd', 'modaka', 'modcon', 'now', 'nowaka' ) THEN 'XM'
                                WHEN clientequipment_subscriber_type IN ( 'hsi' ) THEN 'hsi'
                                WHEN clientequipment_subscriber_type IN ( 'wod' ) THEN 'wod'
                                ELSE NULL
                           END AS subscriber_type,
                           vlan_id,
                        /* UNIQUE CLIENT COUNT */
                           COUNT( DISTINCT ( clientequipment_mac_address ) ) AS 'client_count',
                        /* TCP SESSION COUNT OVERALL */
                           SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN upw_tcp_count 
                                    ELSE NULL 
                                END ) AS tcp_count, 
                        /* TCP LATENCY AVERAGE & % GOOD VALUES  */
                           SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_upload_tcp_latency_bucket_1_count 
                                    ELSE NULL 
                                END ) AS tcp_lat_bucket_1_count,
                           SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_upload_tcp_latency_bucket_2_count 
                                    ELSE NULL 
                                END ) AS tcp_lat_bucket_2_count,
                           SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_upload_tcp_latency_bucket_3_count 
                                    ELSE NULL 
                                END ) AS tcp_lat_bucket_3_count,
                           SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_total_upload_tcp_latency_bucket_1_usec 
                                    ELSE NULL 
                                END ) AS tcp_lat_bucket_1_usec,
                           SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_total_upload_tcp_latency_bucket_2_usec 
                                    ELSE NULL 
                                END ) AS tcp_lat_bucket_2_usec,
                           SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_total_upload_tcp_latency_bucket_3_usec 
                                    ELSE NULL 
                                END ) AS tcp_lat_bucket_3_usec,
                        /* TCP RETRANSMISSION VALUES (TCP COUNT METHOD) */
                           SUM( CASE
                                    WHEN ( upw_download_retransmitted_packets_count >= '0'
                                        AND upw_download_packets_count > '0'
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 > 4.0
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 <= 100.0
                                        AND packet_type_name IN ( 'TCP' )
                                        AND upw_download_active_millis > '0' )
                                    THEN f.upw_tcp_count 
                                    ELSE NULL 
                                END ) AS tcp_count_rtx_impacted,
                        /* TCP RETRANSMISSION VALUES (ROW COUNT METHOD) */
                           SUM( CASE
                                    WHEN ( upw_download_retransmitted_packets_count >= '0'
                                        AND upw_download_packets_count > '0'
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 > 4.0
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 <= 100.0
                                        AND packet_type_name IN ( 'TCP' ) )
                                    THEN 1 
                                    ELSE NULL 
                                END ) AS row_count_rtx_impacted,
                        /* TCP RETRANSMISSION VALUES (ROW COUNT METHOD) */
                           SUM( CASE
                                    WHEN ( upw_download_retransmitted_packets_count >= '0'
                                        AND upw_download_packets_count > '0'
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 <= 100                                        
                                        AND packet_type_name IN ( 'TCP' )
                                        AND upw_download_active_millis > '0' )
                                    THEN 1 
                                    ELSE NULL 
                                END ) AS row_count_rtx,
                        /* TCP RETRANSMISSION VALUES (ROW COUNT METHOD - NO POCKET SESSIONS) */
                           SUM( CASE
                                    WHEN ( upw_download_retransmitted_packets_count >= '0'
                                        AND upw_download_packets_count > '100'
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 > 4.0
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 <= 100.0
                                        AND packet_type_name IN ( 'TCP' ) )
                                    THEN 1
                                    ELSE NULL 
                                END ) AS row_count_rtx_impacted_no_pocket,
                        /* TCP RETRANSMISSION VALUES (ROW COUNT METHOD) - NO POCKET SESSIONS */
                           SUM( CASE
                                    WHEN ( upw_download_retransmitted_packets_count >= '0'
                                        AND upw_download_packets_count > '100'
                                        AND upw_download_bytes_count > '1000'
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 <= 100                                        
                                        AND packet_type_name IN ( 'TCP' )
                                        AND upw_download_active_millis > '0' )
                                    THEN 1 
                                    ELSE NULL 
                                END ) AS row_count_rtx_no_pocket,
                        /* THROUGHPUT DOWNLOAD VALUES */
                           SUM( CASE 
                                    WHEN ( upw_download_active_millis > '0' 
                                        AND application_name = 'DLC' )
                                    THEN upw_download_bytes_count
                                    ELSE NULL 
                                END ) AS download_bytes_count_dlc,
                           SUM( CASE 
                                    WHEN ( upw_download_active_millis > '0'
                                        AND application_name = 'DLC' )
                                    THEN upw_download_active_millis 
                                    ELSE NULL 
                                END ) AS download_active_millis_dlc,
                        /* THROUGHPUT UPLOAD VALUES */
                           SUM( CASE 
                                    WHEN ( upw_upload_active_millis > '0' 
                                        AND application_name = 'DLC' )
                                    THEN upw_upload_bytes_count
                                    ELSE NULL 
                                END ) AS upload_bytes_count_dlc,
                           SUM( CASE 
                                    WHEN ( upw_upload_active_millis > '0'
                                        AND application_name = 'DLC' )
                                    THEN upw_upload_active_millis 
                                    ELSE NULL 
                                END ) AS upload_active_millis_dlc
                    FROM  
                        f_user_plane_wireline f
                    WHERE 
                        cal_timestamp_day BETWEEN 20241006 AND 20241102
                        --AND ap_primary_wag IN ('chicago-p.area4.il.chicago')
                        AND ap_mac_address IN ('fc:91:14:cf:5d:6b')
                        -- ONLY SPECIFIC CLIENT TYPES (FROM ENRICHMENT)
                        AND clientequipment_subscriber_type IN ( 'mod', 'modhsd', 'modaka', 'modcon', 'now', 'nowaka', 'hsi', 'wod' )
                        -- IGNORE AAA PROBE TRAFFIC
                        AND device_ip_address NOT IN ('10.146.154.134', '10.146.41.231')
                    GROUP BY 
                            ap_mac_address,
                            ap_model_name,
                            subscriber_type,
                            vlan_id  
            )                                 
SELECT ap_mac_address AS 'ap_mac',
       ap_model_name AS 'ap_model',
       subscriber_type AS 'sub_type',
       vlan_id,
       client_count,
       ROUND(( ( basedata.tcp_lat_bucket_1_count + basedata.tcp_lat_bucket_2_count) / NULLIFZERO(basedata.tcp_count) ) * 100, 2 ) AS 'TCP-LAT % GOOD SESSIONS',
       ROUND(( 1 - ( basedata.tcp_count_rtx_impacted / NULLIFZERO(basedata.tcp_count) ) ) * 100, 2)  AS 'TCP-RTX % GOOD SESSIONS (TCP COUNT)',
       ROUND(( 1 - ( basedata.row_count_rtx_impacted / NULLIFZERO(basedata.row_count_rtx) ) ) * 100, 2)  AS 'TCP-RTX % GOOD SESSIONS (ROW)',
       ROUND(( 1 - ( basedata.row_count_rtx_impacted_no_pocket / NULLIFZERO(basedata.row_count_rtx_no_pocket) ) ) * 100, 2)  AS 'TCP-RTX % GOOD SESSIONS (ROW - NO POCKET)',
       ROUND(( ( basedata.tcp_lat_bucket_1_usec + basedata.tcp_lat_bucket_2_usec + basedata.tcp_lat_bucket_3_usec) / 1000 ) / NULLIFZERO(basedata.tcp_lat_bucket_1_count + basedata.tcp_lat_bucket_2_count + basedata.tcp_lat_bucket_3_count), 2) AS 'AVG CLIENT LATENCY',
       ROUND(( ( basedata.download_bytes_count_dlc * 8 ) / (basedata.download_active_millis_dlc / 1000) ) / 1000 / 1000 , 2) AS 'AVG THROUGHPUT Mbps (DLC)',
       tcp_count AS 'tcp_sessions',
       tcp_lat_bucket_3_count AS 'lat_tcp_impacted_count',
       tcp_lat_bucket_3_usec AS 'lat_tcp_impacted_usec',
       tcp_count_rtx_impacted AS 'rtx_tcp_impacted_count',
       row_count_rtx AS 'rtx_row_count',
       row_count_rtx_impacted AS 'rtx_row_impacted_count',
       row_count_rtx_impacted_no_pocket AS 'rtx_row_impacted_count_no_pocket',
       row_count_rtx_no_pocket AS 'rtx_row_count_no_pocket',
       ( (basedata.download_bytes_count_dlc) + (basedata.upload_bytes_count_dlc) )  AS 'total_bytes_dlc',
       ( (basedata.download_active_millis_dlc) + (basedata.upload_active_millis_dlc) )  AS 'total_ms_dlc',
       download_bytes_count_dlc AS 'dl_bytes_dlc',
       upload_bytes_count_dlc AS 'ul_bytes_dlc',       
       download_active_millis_dlc AS 'dl_ms_dlc',
       upload_active_millis_dlc AS 'ul_ms_dlc'
FROM basedata
ORDER BY 1 DESC
