/* 
CREATED BY: BEN MASON 
CREATED ON: 10/31/2024 
DESCRIPTION: GOLDEN QUERY - COMBINED CX METRICS BY AP MAC, AP MODEL, SUB TYPE, VLAN
TCP-LAT % GOOD SESSIONS
TCP-RTX % GOOD SESSIONS
AVG CLIENT LATENCY
AVG DL THROUGHPUT (DLC)
ALL METRICS VALIDATED AGAINST EXPLORER FOR ACCURACY
*/

WITH basedata AS 
                ( SELECT  ap_mac_address,
                          ap_model_name,
                          CASE
                            WHEN clientequipment_subscriber_type IN ( 'mod', 'modhsd', 'modaka', 'modcon', 'now', 'nowaka' ) 
                            THEN 'XM'
                            WHEN clientequipment_subscriber_type IN ( 'hsi' ) 
                            THEN 'hsi'
                            WHEN clientequipment_subscriber_type IN ( 'wod' )
                            THEN 'wod'
                            ELSE clientequipment_subscriber_type
                          END AS clientequipment_subscriber_type,
                           vlan_id,
                        /* TCP LATENCY AVERAGE & % GOOD VALUES  */
                          SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_upload_tcp_latency_bucket_1_count ELSE NULL END ) AS tcp_lat_bucket_1_count,
                          SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_upload_tcp_latency_bucket_2_count ELSE NULL END ) AS tcp_lat_bucket_2_count,
                          SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_upload_tcp_latency_bucket_3_count ELSE NULL END ) AS tcp_lat_bucket_3_count,
                          SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_total_upload_tcp_latency_bucket_1_usec ELSE NULL END ) AS tcp_lat_bucket_1_usec,
                          SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_total_upload_tcp_latency_bucket_2_usec ELSE NULL END ) AS tcp_lat_bucket_2_usec,
                          SUM( CASE 
                                    WHEN ( packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_total_upload_tcp_latency_bucket_3_usec ELSE NULL END ) AS tcp_lat_bucket_3_usec,
                        /* TCP RETRANSMISSION VALUES (TCP COUNT METHOD) */
                          SUM( CASE
                                    WHEN ( upw_download_retransmitted_packets_count >= '0'
                                        AND upw_download_packets_count > '0'
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 > 4.0
                                        AND ( upw_download_retransmitted_packets_count / upw_download_packets_count ) * 100 <= 100.0
                                        AND packet_type_name IN ( 'TCP' ) )
                                    THEN f.upw_tcp_count
                                    ELSE NULL END ) AS tcp_count_rtx_impacted,
                        /* THROUGHPUT VALUES */
                          SUM( CASE 
                                    WHEN ( upw_download_active_millis > '0' 
                                        AND application_name = 'DLC' )
                                    THEN upw_download_bytes_count
                                    ELSE NULL END ) AS download_bytes_count_dlc,
                          SUM( CASE 
                                    WHEN ( upw_download_active_millis > '0'
                                        AND application_name = 'DLC' )
                                    THEN upw_download_active_millis 
                                    ELSE NULL END ) AS download_active_millis_dlc, 
                        /* TCP SESSION COUNT OVERALL */
                          SUM( CASE 
                                    WHEN packet_type_name IN ( 'TCP' )
                                    THEN upw_tcp_count 
                                    ELSE NULL END ) AS tcp_count                            
                   FROM  f_user_plane_wireline f
                   WHERE cal_timestamp_day BETWEEN 20241006 AND 20241019
                        --AND ap_primary_wag IN ( 'chicago-p.area4.il.chicago' )
                         AND ap_mac_address IN ( '94:04:e3:f4:67:36')
                         AND clientequipment_subscriber_type IN ( 'mod', 'modhsd', 'modaka', 'now', 'nowaka', 'hsi', 'wod')                            
                    GROUP BY ap_mac_address,
                             ap_model_name,
                             clientequipment_subscriber_type,
                             vlan_id )
SELECT ap_mac_address AS 'ap_mac',
       ap_model_name AS 'ap_model',
       clientequipment_subscriber_type AS 'subtype',
       vlan_id,
       ROUND(( ( basedata.tcp_lat_bucket_1_count + basedata.tcp_lat_bucket_2_count) / NULLIFZERO(basedata.tcp_count) ) * 100, 2 ) AS 'TCP-LAT % GOOD SESSIONS',
       ROUND(( 1 - ( basedata.tcp_count_rtx_impacted / NULLIFZERO(basedata.tcp_count) ) ) * 100, 2)  AS 'TCP-RTX % GOOD SESSIONS',
       ROUND(( ( basedata.tcp_lat_bucket_1_usec + basedata.tcp_lat_bucket_2_usec + basedata.tcp_lat_bucket_3_usec) / 1000 ) / NULLIFZERO(basedata.tcp_lat_bucket_1_count + basedata.tcp_lat_bucket_2_count + basedata.tcp_lat_bucket_3_count), 2) AS 'Avg Client Latency',
       ROUND(( ( basedata.download_bytes_count_dlc * 8 ) / (basedata.download_active_millis_dlc / 1000) ) / 1000 / 1000 , 2) AS 'Avg Throughput Mbps (DLC)',
       tcp_count AS 'total_tcp_sessions'
FROM basedata
