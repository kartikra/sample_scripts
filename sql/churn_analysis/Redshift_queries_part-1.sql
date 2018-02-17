DROP TABLE IF EXISTS #parent_account_floor_changes;
select A.parent_account_id,A.currency_type
,MAX(CASE WHEN F1.campaignid IS NOT NULL OR F2.campaignid IS NOT NULL THEN 'Y' ELSE 'N' END) floor_change_ind
into #parent_account_floor_changes
from tmp.BO_ATR_REVENUE A
LEFT JOIN tmp."BO_FLOOR_CHANGES" F1 ON A.campaign_id=F1.campaignid AND A.business_id=F1.yelpbizids 
LEFT JOIN tmp."BO_FLOOR_CHANGES" F2 ON A.campaign_id=F2.campaignid AND A.business_id=F2.yelpbizids AND F2.yelpbizids LIKE '%NAME%'
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
 parent_account_id,segment,revenue_year_month
,first_month,last_month,floor_change_ind
,contract_status,contract_type
,SUM(revenue_net) AS revenue_net
,SUM(gross_revenue) AS gross_revenue
INTO #revenue_monthly_breakup
from tmp.BO_ATR_REVENUE_SUMMARY
group by 1,2,3,4,5,6,7,8;


DROP TABLE IF EXISTS tmp.BO_ATR_REVENUE_RETENTION;
SELECT 
TRUNC(DATEADD(month,1,TO_DATE((C.revenue_year_month*100)+1,'YYYYMMDD'))) AS report_date
,C.revenue_year_month
,C.segment
,C.floor_change_ind
,C.parent_account_id 
,C.contract_status
,C.contract_type
,SUM(C.revenue_net) revenue_net
,N.revenue_net AS next_revenue_net
INTO tmp.BO_ATR_REVENUE_RETENTION
FROM #revenue_monthly_breakup C

LEFT JOIN
(
  select revenue_year_month, parent_account_id,contract_status,contract_type,SUM(revenue_net) revenue_net
  from #revenue_monthly_breakup
  group by 1,2,3,4
) N ON C.parent_account_id=N.parent_account_id AND C.contract_status=N.contract_status AND C.contract_type=N.contract_type
AND TO_DATE((N.revenue_year_month*100)+1,'YYYYMMDD') - TO_DATE((C.revenue_year_month*100)+1,'YYYYMMDD') BETWEEN 1 AND 31

group by 1,2,3,4,5,6,7,9
order by 2,3,4,5
;


-- Revenue Retention
SELECT
report_date
,segment
,MAX(retention_no_change) AS "%_net_revenue_retained_no_changes"
,MAX(retention_with_change) AS "%_net_revenue_retained_changes"
FROM
(
  select 
  report_date
  ,segment
  ,CASE WHEN floor_change_ind='N' THEN (SUM(COALESCE(next_revenue_net,0)) / SUM(revenue_net)) * 100 END as retention_no_change
  ,CASE WHEN floor_change_ind='Y' THEN (SUM(COALESCE(next_revenue_net,0)) / SUM(revenue_net)) * 100 END as retention_with_change
  FROM tmp.BO_ATR_REVENUE_RETENTION 
  WHERE report_date >= '2015-09-01'
  GROUP BY report_date,segment,floor_change_ind
) R
GROUP BY 1,2
order by 1,2


