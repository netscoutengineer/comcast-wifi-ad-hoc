/* 
CREATED BY: BEN MASON 
CREATED ON: 10/01/2024 
DATA SOURCE NAME:   
DESCRIPTION: GATHER SPEEDBOOST DATA FOR EXECUTIVE TEAM - IMEI LIST
METHOD 1 - FOR EACH IMEI/IMSI COMBINATION, REPORT # OF TCP SESSIONS AND MAX DL THROUGHPUT > 25Mbps 
ONLY ON SPEEDBOOST APs
REQUESTED BY: RAJAT, INDIRA, PETE
*/

WITH speedboost AS ( SELECT cal_timestamp_day, 
                       f.subscriber_id,
                       f.userequipment_imeisv, 
                       f.ap_cran_name,
                       f.upw_tcp_count,
                       MAX(f.upw_download_max_throughput_kbps) AS upw_download_max_throughput_kbps
                FROM nba.f_user_plane_wireline f
                WHERE -- last 45 days
                      cal_timestamp_day >= TO_CHAR(NOW() - interval '45' DAY, 'YYYYMMDD') :: INT
                      -- speedboosted APs
                      AND ap_cran_name IN ('CBR' , 'CBR2' , 'XB6', 'XB7', 'XB8', 'Outdoor')
                      -- tcp session established
                      AND upw_tcp_count > '0'
                      -- throughput gt 25mbps
                      AND upw_download_max_throughput_kbps > '25000'
                      -- executive IMEI list
                      AND ( userequipment_imeisv IN ( '35768088053505',
                                                      '35041953132173',
                                                      '35183909109490',
                                                      '35350375776648',
                                                      '35672749535531',
                                                      '35323810768226',
                                                      '35211353538927',
                                                      '35517816867158',
                                                      '35923906660868',
                                                      '35669119416950',
                                                      '35681560507779',
                                                      '35201340680721',
                                                      '35826414303884',
                                                      '35154570838686',
                                                      '35938853082025',
                                                      '35663771306502',
                                                      '35734709952531',
                                                      '35537308586159',
                                                      '35540293286997',
                                                      '35590194933574',
                                                      '35833977748640',
                                                      '35781443123886',
                                                      '35477316424104',
                                                      '35643210544648',
                                                      '35762631996939',
                                                      '35582908206628',
                                                      '35461575898814',
                                                      '35552257028031',
                                                      '35489409550990',
                                                      '35771276489014',
                                                      '35355956537120',
                                                      '35518095263064',
                                                      '35366225187340',
                                                      '35884956647046',
                                                      '35700428836993',
                                                      '35362930676965',
                                                      '35378846425468',
                                                      '35215654000587',
                                                      '35643710618464',
                                                      '35389210374563',
                                                      '35793843066749',
                                                      '35582408592054',
                                                      '35342971604340',
                                                      '35329470910194',
                                                      '35167891492569',
                                                      '35219560343159',
                                                      '35294188234741',
                                                      '35099779061545',
                                                      '35219560531904',
                                                      '35318643996761',
                                                      '35343165026859',
                                                      '35407864258767',
                                                      '35205137744096',
                                                      '35237763707196',
                                                      '35266891136821',
                                                      '35648110000089',
                                                      '35346891682077',
                                                      '35371957965152',
                                                      '35635308101229',
                                                      '35316580959110',
                                                      '35923763479960',
                                                      '35462790930486',
                                                      '35284794707306',
                                                      '35343165417038',
                                                      '35277225433419',
                                                      '35807124694194',
                                                      '35950862668085',
                                                      '35746352176393',
                                                      '35647310561977',
                                                      '35122876676340',
                                                      '35378846478636',
                                                      '35624087248617',
                                                      '35506366713513',
                                                      '35713734146817',
                                                      '35757349218551',
                                                      '35733236707788',
                                                      '35583708157282',
                                                      '35407096332157',
                                                      '35882334422424',
                                                      '35883259453313',
                                                      '35473918445822',
                                                      '35462790510444',
                                                      '35366457518287',
                                                      '35684111824338',
                                                      '35277439572369',
                                                      '35680411749270',
                                                      '35377639173590',
                                                      '35647610809553',
                                                      '35355956525403',
                                                      '35407864098865',
                                                      '35400127940493',
                                                      '35355956668024',
                                                      '35407864084668',
                                                      '35390610774446',
                                                      '35489009798583',
                                                      '35376386267661',
                                                      '35900385332798',
                                                      '35757349072562',
                                                      '35524138773121',
                                                      '35647710162612',
                                                      '35647210203916' )
                            AND ap_cran_name IS NOT NULL
                            AND upw_tcp_count > '0' )
                GROUP BY 1,
                         2,
                         3,
                         4,
                         5 )
SELECT cal_timestamp_day AS 'date',
       COALESCE(speedboost.subscriber_id) AS 'subscriber_imsi',
       COALESCE(speedboost.userequipment_imeisv) AS 'subscriber_imei',
       COALESCE(speedboost.ap_cran_name) AS 'ap_class',
       COALESCE(speedboost.upw_tcp_count) AS 'session_count',
       ROUND(speedboost.upw_download_max_throughput_kbps / 1000, 2) AS 'dl_peak_throughput_mbps'
FROM speedboost
ORDER BY 'date' DESC



/* 
CREATED BY: BEN MASON 
CREATED ON: 10/01/2024 
DATA SOURCE NAME:   
DESCRIPTION: GATHER SPEEDBOOST DATA FOR EXECUTIVE TEAM - IMEI LIST
METHOD 2 - FOR EACH IMEI/IMSI COMBINATION, REPORT # OF TCP SESSIONS ON SPEEDBOOSTED vs NON-SPEEDBOOSTED APs
ONLY ON SPEEDBOOST APs
REQUESTED BY: RAJAT, INDIRA, PETE
*/

WITH speedboost1 AS ( SELECT TO_DATE(TO_CHAR(cal_timestamp_day), 'YYYYMMDD') AS cal_timestamp_day,
                             f.subscriber_id,
                             f.userequipment_imeisv,
                             f.ap_cran_name,
                             f.upw_tcp_count,
                             MAX(f.upw_download_max_throughput_kbps) AS upw_download_max_throughput_kbps
                      FROM  f_user_plane_wireline f
                      WHERE -- last 45 days
                            cal_timestamp_day >= TO_CHAR(NOW() - interval '45' DAY, 'YYYYMMDD') :: INT
                            -- speedboosted APs
                            AND ap_cran_name IN ( 'CBR', 'CBR2', 'XB6', 'XB7', 'XB8', 'Outdoor' )
                            -- tcp session established
                            AND upw_tcp_count > '0'
                            -- executive IMEI list
                            AND userequipment_imeisv IN 
                                                    ( '35768088053505',
                                                      '35041953132173',
                                                      '35183909109490',
                                                      '35350375776648',
                                                      '35672749535531',
                                                      '35323810768226',
                                                      '35211353538927',
                                                      '35517816867158',
                                                      '35923906660868',
                                                      '35669119416950',
                                                      '35681560507779',
                                                      '35201340680721',
                                                      '35826414303884',
                                                      '35154570838686',
                                                      '35938853082025',
                                                      '35663771306502',
                                                      '35734709952531',
                                                      '35537308586159',
                                                      '35540293286997',
                                                      '35590194933574',
                                                      '35833977748640',
                                                      '35781443123886',
                                                      '35477316424104',
                                                      '35643210544648',
                                                      '35762631996939',
                                                      '35582908206628',
                                                      '35461575898814',
                                                      '35552257028031',
                                                      '35489409550990',
                                                      '35771276489014',
                                                      '35355956537120',
                                                      '35518095263064',
                                                      '35366225187340',
                                                      '35884956647046',
                                                      '35700428836993',
                                                      '35362930676965',
                                                      '35378846425468',
                                                      '35215654000587',
                                                      '35643710618464',
                                                      '35389210374563',
                                                      '35793843066749',
                                                      '35582408592054',
                                                      '35342971604340',
                                                      '35329470910194',
                                                      '35167891492569',
                                                      '35219560343159',
                                                      '35294188234741',
                                                      '35099779061545',
                                                      '35219560531904',
                                                      '35318643996761',
                                                      '35343165026859',
                                                      '35407864258767',
                                                      '35205137744096',
                                                      '35237763707196',
                                                      '35266891136821',
                                                      '35648110000089',
                                                      '35346891682077',
                                                      '35371957965152',
                                                      '35635308101229',
                                                      '35316580959110',
                                                      '35923763479960',
                                                      '35462790930486',
                                                      '35284794707306',
                                                      '35343165417038',
                                                      '35277225433419',
                                                      '35807124694194',
                                                      '35950862668085',
                                                      '35746352176393',
                                                      '35647310561977',
                                                      '35122876676340',
                                                      '35378846478636',
                                                      '35624087248617',
                                                      '35506366713513',
                                                      '35713734146817',
                                                      '35757349218551',
                                                      '35733236707788',
                                                      '35583708157282',
                                                      '35407096332157',
                                                      '35882334422424',
                                                      '35883259453313',
                                                      '35473918445822',
                                                      '35462790510444',
                                                      '35366457518287',
                                                      '35684111824338',
                                                      '35277439572369',
                                                      '35680411749270',
                                                      '35377639173590',
                                                      '35647610809553',
                                                      '35355956525403',
                                                      '35407864098865',
                                                      '35400127940493',
                                                      '35355956668024',
                                                      '35407864084668',
                                                      '35390610774446',
                                                      '35489009798583',
                                                      '35376386267661',
                                                      '35900385332798',
                                                      '35757349072562',
                                                      '35524138773121',
                                                      '35647710162612',
                                                      '35647210203916'  )
                            AND ap_cran_name IS NOT NULL
                            AND upw_tcp_count > '0' 
                      GROUP BY 1,
                               2,
                               3,
                               4,
                               5 
                    ) ,
speedboost2 AS ( SELECT TO_DATE(TO_CHAR(cal_timestamp_day), 'YYYYMMDD') AS cal_timestamp_day,
                        f.subscriber_id,
                        f.userequipment_imeisv,
                        f.ap_cran_name,
                        f.upw_tcp_count,
                        MAX(f.upw_download_max_throughput_kbps) AS upw_download_max_throughput_kbps
                 FROM   f_user_plane_wireline f
                 WHERE  -- last 45 days
                        cal_timestamp_day >= TO_CHAR(NOW() - interval '45' DAY, 'YYYYMMDD') :: INT
                        -- non-speedboosted APs
                        AND ap_cran_name NOT IN ( 'CBR', 'CBR2', 'XB6', 'XB7', 'XB8', 'Outdoor' )
                        -- tcp session established
                        AND upw_tcp_count > '0'
                        -- executive IMEI list
                        AND userequipment_imeisv IN ( '35768088053505',
                                                      '35041953132173',
                                                      '35183909109490',
                                                      '35350375776648',
                                                      '35672749535531',
                                                      '35323810768226',
                                                      '35211353538927',
                                                      '35517816867158',
                                                      '35923906660868',
                                                      '35669119416950',
                                                      '35681560507779',
                                                      '35201340680721',
                                                      '35826414303884',
                                                      '35154570838686',
                                                      '35938853082025',
                                                      '35663771306502',
                                                      '35734709952531',
                                                      '35537308586159',
                                                      '35540293286997',
                                                      '35590194933574',
                                                      '35833977748640',
                                                      '35781443123886',
                                                      '35477316424104',
                                                      '35643210544648',
                                                      '35762631996939',
                                                      '35582908206628',
                                                      '35461575898814',
                                                      '35552257028031',
                                                      '35489409550990',
                                                      '35771276489014',
                                                      '35355956537120',
                                                      '35518095263064',
                                                      '35366225187340',
                                                      '35884956647046',
                                                      '35700428836993',
                                                      '35362930676965',
                                                      '35378846425468',
                                                      '35215654000587',
                                                      '35643710618464',
                                                      '35389210374563',
                                                      '35793843066749',
                                                      '35582408592054',
                                                      '35342971604340',
                                                      '35329470910194',
                                                      '35167891492569',
                                                      '35219560343159',
                                                      '35294188234741',
                                                      '35099779061545',
                                                      '35219560531904',
                                                      '35318643996761',
                                                      '35343165026859',
                                                      '35407864258767',
                                                      '35205137744096',
                                                      '35237763707196',
                                                      '35266891136821',
                                                      '35648110000089',
                                                      '35346891682077',
                                                      '35371957965152',
                                                      '35635308101229',
                                                      '35316580959110',
                                                      '35923763479960',
                                                      '35462790930486',
                                                      '35284794707306',
                                                      '35343165417038',
                                                      '35277225433419',
                                                      '35807124694194',
                                                      '35950862668085',
                                                      '35746352176393',
                                                      '35647310561977',
                                                      '35122876676340',
                                                      '35378846478636',
                                                      '35624087248617',
                                                      '35506366713513',
                                                      '35713734146817',
                                                      '35757349218551',
                                                      '35733236707788',
                                                      '35583708157282',
                                                      '35407096332157',
                                                      '35882334422424',
                                                      '35883259453313',
                                                      '35473918445822',
                                                      '35462790510444',
                                                      '35366457518287',
                                                      '35684111824338',
                                                      '35277439572369',
                                                      '35680411749270',
                                                      '35377639173590',
                                                      '35647610809553',
                                                      '35355956525403',
                                                      '35407864098865',
                                                      '35400127940493',
                                                      '35355956668024',
                                                      '35407864084668',
                                                      '35390610774446',
                                                      '35489009798583',
                                                      '35376386267661',
                                                      '35900385332798',
                                                      '35757349072562',
                                                      '35524138773121',
                                                      '35647710162612',
                                                      '35647210203916'  )
                        AND ap_cran_name IS NOT NULL
                        AND upw_tcp_count > '0' 
                 GROUP BY 1,
                          2,
                          3,
                          4,
                          5 )
SELECT COALESCE(speedboost1.cal_timestamp_day, speedboost2.cal_timestamp_day) AS 'date',
       COALESCE(speedboost1.subscriber_id, speedboost2.subscriber_id) AS 'subscriber_imsi',
       COALESCE(speedboost1.userequipment_imeisv, speedboost2.userequipment_imeisv) AS 'subscriber_imei',
       COALESCE(speedboost1.ap_cran_name, speedboost2.ap_cran_name) AS 'ap_class',
       COALESCE(speedboost1.upw_tcp_count, speedboost2.upw_tcp_count) AS 'session_count',
       COALESCE(speedboost1.upw_download_max_throughput_kbps,speedboost2.upw_download_max_throughput_kbps) AS 'dl_peak_throughput_kbps'                 
FROM speedboost1 
     FULL OUTER JOIN speedboost2 ON speedboost1.cal_timestamp_day <=> speedboost2.cal_timestamp_day
                                AND speedboost1.subscriber_id <=> speedboost2.subscriber_id
                                AND speedboost1.userequipment_imeisv <=> speedboost2.userequipment_imeisv
ORDER BY date DESC




/* 
CREATED BY: BEN MASON 
CREATED ON: 10/01/2024 
DATA SOURCE NAME:   
DESCRIPTION: GATHER SPEEDBOOST DATA FOR EXECUTIVE TEAM - IMEI LIST
METHOD 2 - FOR EACH IMEI/IMSI COMBINATION, REPORT # OF TCP SESSIONS ON SPEEDBOOSTED vs NON-SPEEDBOOSTED APs
ONLY ON SPEEDBOOST APs
REQUESTED BY: RAJAT, INDIRA, PETE
*/

WITH speedboost AS ( SELECT 
                        SUM(f.upw_tcp_count) AS 'upw_tcp_count',
                        COUNT(DISTINCT(f.subscriber_id)) AS 'client_count'
                 FROM   f_user_plane_wireline f
                 WHERE  -- last n days
                        cal_timestamp_day >= TO_CHAR(NOW() - interval '30' DAY, 'YYYYMMDD') :: INT
                        -- XM subscribers only
                        AND clientequipment_subscriber_type IN ('mod', 'modaka', 'modhsd', 'modcon')
                        -- speedboosted APs
                        AND ap_cran_name IN ( 'CBR', 'CBR2', 'XB6', 'XB7', 'XB8', 'Outdoor' )
                        -- tcp session established
                        AND upw_tcp_count > '0'
                        AND ap_cran_name IS NOT NULL                  
                           )                       
SELECT 
       COALESCE(speedboost.upw_tcp_count) AS 'session_count',
       COALESCE(speedboost.client_count) AS 'client_count'                 
FROM speedboost 



