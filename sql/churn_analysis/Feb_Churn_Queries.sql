DROP TABLE IF EXISTS #parent_account_floor_changes;
select A.parent_account_id,A.currency_type
,MAX(CASE WHEN F1.campaignid IS NOT NULL OR F2.campaignid IS NOT NULL THEN 'Y' ELSE 'N' END) floor_change_ind
into #parent_account_floor_changes
from tmp.BO_ATR_REVENUE A
LEFT JOIN tmp."BO_FLOOR_CHANGES" F1 ON A.campaign_id=F1.campaignid AND A.business_id=F1.yelpbizids AND F2.yelpbizids <> '#NAME?'
LEFT JOIN tmp."BO_FLOOR_CHANGES" F2 ON A.campaign_id=F2.campaignid AND F2.yelpbizids='#NAME?'
GROUP BY 1,2;


DROP TABLE IF EXISTS #parent_account_lifetime_minmax_dates;
select parent_account_id,currency_type
,min((EXTRACT(year from R.revenue_start) * 100) + EXTRACT(month from R.revenue_start)) AS first_month 
,max((EXTRACT(year from R.revenue_start) * 100) + EXTRACT(month from R.revenue_start)) AS last_month
into #parent_account_lifetime_minmax_dates
from tmp.BO_ATR_REVENUE R
 group by 1,2;


DROP TABLE IF EXISTS #parent_account_segment; 
select A.parent_account_id,A.currency_type, MAX(A.segment) segment, MAX(A.signup_type) signup_type
INTO #parent_account_segment
from tmp.BO_ATR_REVENUE A
JOIN #parent_account_lifetime_minmax_dates B ON A.parent_account_id=B.parent_account_id AND A.currency_type=B.currency_type
AND (EXTRACT(year from revenue_start) * 100 + EXTRACT(month from revenue_start))=B.first_month
GROUP BY 1,2;


DROP TABLE IF EXISTS #paid_cpc_advertiser;
SELECT parent_account_id,currency_type,(EXTRACT(year from RS.revenue_start) * 100 + EXTRACT(month from RS.revenue_start)) as revenue_year_month
INTO #paid_cpc_advertiser
FROM tmp.BO_ATR_REVENUE RS
WHERE RS.program_type='Clicks Advertiser'
AND RS.revenue_start >= '2014-01-01'    
GROUP BY 1,2,3;

DROP TABLE IF EXISTS tmp.BO_ATR_REVENUE_SUMMARY;
SELECT 
 R.parent_account_id
,R.currency_type
,S.signup_type
,S.segment
,D.first_month
,D.last_month
,F.floor_change_ind 
,(EXTRACT(year from R.revenue_start) * 100 + EXTRACT(month from R.revenue_start)) as revenue_year_month
,CASE WHEN R.contract_end IS NOT NULL AND R.contract_end > '2000-02-02'
           AND R.contract_end > R.revenue_end THEN 'IN' ELSE 'OUT' 
 END AS contract_status
,CASE  
	-- WHEN COALESCE(R.contract_end - R.contract_start,0) BETWEEN 1 AND 31 THEN '1M'
	 WHEN COALESCE(R.contract_end - R.contract_start,0) BETWEEN 31 AND 95 THEN '3M'
	 WHEN COALESCE(R.contract_end - R.contract_start,0) BETWEEN 95 AND 185 THEN '6M'
	 WHEN COALESCE(R.contract_end - R.contract_start,0) > 185 THEN '12M'
         ELSE 'OTH'
 END AS contract_type

,SUM(R.revenue_net) AS revenue_net
,SUM(R.gross_revenue) AS gross_revenue
INTO tmp.BO_ATR_REVENUE_SUMMARY
from tmp.BO_ATR_REVENUE R
JOIN #parent_account_segment S ON R.parent_account_id=S.parent_account_id AND R.currency_type=S.currency_type
JOIN #parent_account_lifetime_minmax_dates D ON R.parent_account_id=D.parent_account_id AND R.currency_type=D.currency_type
JOIN #parent_account_floor_changes F ON R.parent_account_id=F.parent_account_id AND R.currency_type=F.currency_type
JOIN #paid_cpc_advertiser CPC ON CPC.parent_account_id=R.parent_account_id AND CPC.currency_type=R.currency_type
AND (EXTRACT(year from R.revenue_start) * 100 + EXTRACT(month from R.revenue_start)) = CPC.revenue_year_month

WHERE R.currency_type='USD' AND R.revenue_start >= '2014-01-01'
GROUP BY 1,2,3,4,5,6,7,8,9,10
;

-- Validation
SELECT
(EXTRACT(year from R.revenue_start) * 100 + EXTRACT(month from R.revenue_start)) as revenue_year_month
,SUM(R.revenue_net) AS revenue_net
from tmp.BO_ATR_REVENUE R
JOIN #paid_cpc_advertiser CPC ON CPC.parent_account_id=R.parent_account_id AND CPC.currency_type=R.currency_type
AND (EXTRACT(year from R.revenue_start) * 100 + EXTRACT(month from R.revenue_start)) = CPC.revenue_year_month
WHERE R.currency_type='USD' AND R.revenue_start >= '2014-01-01'
GROUP BY 1
ORDER BY 1;

SELECT
R.revenue_year_month
,SUM(R.revenue_net) AS revenue_net
FROM tmp.BO_ATR_REVENUE_SUMMARY R
GROUP BY 1
ORDER BY 1;




DROP TABLE IF EXISTS #revenue_monthly_breakup;
select 
 parent_account_id,revenue_year_month, segment, floor_change_ind
,SUM(revenue_net) AS revenue_net
,SUM(gross_revenue) AS gross_revenue
INTO #revenue_monthly_breakup
from tmp.BO_ATR_REVENUE_SUMMARY
group by 1,2,3,4;


DROP TABLE IF EXISTS #revenue_monthly_breakup_by_contract;
select 
 parent_account_id,revenue_year_month, segment, floor_change_ind
 ,contract_type,contract_status
,SUM(revenue_net) AS revenue_net
,SUM(gross_revenue) AS gross_revenue
INTO #revenue_monthly_breakup_by_contract
from tmp.BO_ATR_REVENUE_SUMMARY
group by 1,2,3,4,5,6;


DROP TABLE IF EXISTS tmp.BO_ATR_REVENUE_RETENTION;
SELECT 
TRUNC(TO_DATE((C.revenue_year_month*100)+1,'YYYYMMDD')) AS report_date
,C.revenue_year_month
,C.segment
,C.floor_change_ind
,C.parent_account_id 
,SUM(C.revenue_net) revenue_net
,MAX(N.revenue_net) AS next_revenue_net

INTO tmp.BO_ATR_REVENUE_RETENTION
FROM #revenue_monthly_breakup C

LEFT JOIN
(
  -- select revenue_year_month, parent_account_id, SUM(revenue_net) revenue_net
  -- from #revenue_monthly_breakup
  select (EXTRACT(year from R.revenue_start) * 100 + EXTRACT(month from R.revenue_start)) as revenue_year_month
  ,parent_account_id, SUM(revenue_net) revenue_net
  FROM tmp.BO_ATR_REVENUE R
  WHERE R.currency_type='USD' AND R.revenue_start >= '2014-01-01'
  group by 1,2
) N ON C.parent_account_id=N.parent_account_id 
AND TO_DATE((N.revenue_year_month*100)+1,'YYYYMMDD') - TO_DATE((C.revenue_year_month*100)+1,'YYYYMMDD') BETWEEN 1 AND 31

group by 1,2,3,4,5
order by 2,3,4,5
;


-- Revenue Retention
SELECT
report_date
,segment
,MAX(ROUND(retention_no_change,2)) AS retention_no_change
,MAX(ROUND(retention_no_change_base,2)) AS retention_no_change_base
,MAX(ROUND(retention_with_change,2)) AS retention_with_change
,MAX(ROUND(retention_with_change_base,2)) AS retention_with_change_base

FROM
(
  select 
  report_date
  ,segment
  ,SUM(CASE WHEN floor_change_ind='N' THEN next_revenue_net ELSE 0 END) as retention_no_change
  ,SUM(CASE WHEN floor_change_ind='N' THEN revenue_net ELSE 0 END) as retention_no_change_base
  ,SUM(CASE WHEN floor_change_ind='Y' THEN next_revenue_net ELSE 0 END) as retention_with_change
  ,SUM(CASE WHEN floor_change_ind='Y' THEN revenue_net ELSE 0 END) as retention_with_change_base

  FROM tmp.BO_ATR_REVENUE_RETENTION 
  WHERE report_date >= '2015-08-01'
  GROUP BY report_date,segment,floor_change_ind
) R
GROUP BY 1,2
order by 1,2
;

-- Revenue Retention Breakdown
SELECT
report_date
,segment
,COALESCE(MAX(previous_revenue_3M_no_change),0) AS previous_revenue_3M_no_change
,COALESCE(MAX(previous_revenue_6M_no_change),0) AS previous_revenue_6M_no_change
,COALESCE(MAX(previous_revenue_12M_no_change),0) AS previous_revenue_12M_no_change
,COALESCE(MAX(previous_revenue_OTH_no_change),0) AS previous_revenue_OTH_no_change

,COALESCE(MAX(previous_revenue_3M_with_change),0) AS previous_revenue_3M_with_change
,COALESCE(MAX(previous_revenue_6M_with_change),0) AS previous_revenue_6M_with_change
,COALESCE(MAX(previous_revenue_12M_with_change),0) AS previous_revenue_12M_with_change
,COALESCE(MAX(previous_revenue_OTH_with_change),0) AS previous_revenue_OTH_with_change


FROM
(
  select 
   report_date
  ,segment

  ,CASE WHEN contract_type='3M' AND contract_status='IN' AND floor_change_ind='N' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_3M_no_change
  ,CASE WHEN contract_type='6M' AND contract_status='IN' AND floor_change_ind='N' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_6M_no_change
  ,CASE WHEN contract_type='12M' AND contract_status='IN' AND floor_change_ind='N' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_12M_no_change
  ,CASE WHEN contract_type='OTH' AND contract_status='IN' AND floor_change_ind='N' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_OTH_no_change
  
  ,CASE WHEN contract_type='3M' AND contract_status='OUT' AND floor_change_ind='Y' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_3M_with_change
  ,CASE WHEN contract_type='6M' AND contract_status='OUT' AND floor_change_ind='Y' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_6M_with_change
  ,CASE WHEN contract_type='12M' AND contract_status='OUT' AND floor_change_ind='Y' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_12M_with_change
  ,CASE WHEN contract_type='OTH' AND contract_status='OUT' AND floor_change_ind='Y' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_OTH_with_change

  FROM tmp.BO_ATR_REVENUE_RETENTION 
  WHERE report_date >= '2015-08-01'
  GROUP BY report_date,segment,floor_change_ind,contract_type,contract_status
) R
GROUP BY 1,2
ORDER BY 2,1
;

SELECT
report_date
,segment
,COALESCE(MAX(previous_revenue_IN_no_change),0) AS previous_revenue_IN_no_change
,COALESCE(MAX(previous_revenue_OUT_no_change),0) AS previous_revenue_OUT_no_change
,COALESCE(MAX(previous_revenue_IN_with_change),0) AS previous_revenue_IN_with_change
,COALESCE(MAX(previous_revenue_OUT_with_change),0) AS previous_revenue_OUT_with_change
FROM
(
  select 
   report_date
  ,segment
  ,CASE WHEN contract_status='IN'  AND floor_change_ind='N' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_IN_no_change
  ,CASE WHEN contract_status='OUT'  AND floor_change_ind='N' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_OUT_no_change
  ,CASE WHEN contract_status='IN'  AND floor_change_ind='Y' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_IN_with_change
  ,CASE WHEN contract_status='OUT'  AND floor_change_ind='Y' THEN SUM(COALESCE(revenue_net,0)) END as previous_revenue_OUT_with_change
  FROM tmp.BO_ATR_REVENUE_RETENTION 
  WHERE report_date >= '2015-08-01'
  GROUP BY report_date,segment, floor_change_ind,contract_status
) R
GROUP BY 1,2
ORDER BY 2,1
;



-- Revenue Retained

 select 
   R.report_date
  ,R.segment
  ,R.floor_change_ind
  ,SUM(R.revenue_net) AS revenue_net
  ,T.revenue_reatined
  FROM tmp.BO_ATR_REVENUE_RETENTION R
  LEFT JOIN
  (
   select 
     report_date
    ,segment
    ,floor_change_ind
    ,SUM(next_revenue_net) AS revenue_reatined
  
    FROM tmp.BO_ATR_REVENUE_RETENTION 
    WHERE report_date >= '2015-08-01' 
    GROUP BY report_date,segment, floor_change_ind
  ) T ON T.report_date=R.report_date AND T.segment=R.segment AND T.floor_change_ind=R.floor_change_ind

  WHERE R.report_date >= '2015-08-01'
  GROUP BY R.report_date,R.segment, R.floor_change_ind,T.revenue_reatined
    ORDER BY 1,3,2
  ;

-- Revenue and Accounts Lost
 select 
   R.report_date
  ,R.segment
  ,R.floor_change_ind
  ,SUM(R.revenue_net) AS revenue_net
  ,COUNT(DISTINCT R.parent_account_id) AS advertiser_count
  ,T.revenue_lost
  ,T.advertiser_lost
  FROM tmp.BO_ATR_REVENUE_RETENTION R
  LEFT JOIN
  (
   select 
     report_date
    ,segment
    ,floor_change_ind
    ,SUM(revenue_net) AS revenue_lost
    ,COUNT(DISTINCT parent_account_id) AS advertiser_lost
  
    FROM tmp.BO_ATR_REVENUE_RETENTION 
    WHERE report_date >= '2015-08-01' AND next_revenue_net IS NULL
    GROUP BY report_date,segment, floor_change_ind
  ) T ON T.report_date=R.report_date AND T.segment=R.segment AND T.floor_change_ind=R.floor_change_ind

  WHERE R.report_date >= '2015-08-01'
  GROUP BY R.report_date,R.segment, R.floor_change_ind,T.revenue_lost,T.advertiser_lost
  ORDER BY 1,3,2
  ;
