DROP TABLE IF EXISTS #parent_account_rank;
select parent_account_id,currency_type, revenue_year_month,EXTRACT(year from current_date) * 100 + EXTRACT(month from current_date)  as current_year_month
,rank() OVER(PARTITION BY parent_account_id order by revenue_year_month ASC) as account_rank
INTO #parent_account_rank
from
(
select parent_account_id,currency_type, (EXTRACT(year from revenue_start) * 100 + EXTRACT(month from revenue_start)) as revenue_year_month
from tmp.BO_ATR_REVENUE
where revenue_net > 0
group by 1,2,3
) T;


DROP TABLE IF EXISTS #parent_account_lifetime_minmax_dates;
select parent_account_id,currency_type, min(revenue_year_month) first_month, max(revenue_year_month) AS last_month, max(account_rank) AS max_account_rank
into #parent_account_lifetime_minmax_dates
 from #parent_account_rank
 group by 1,2;

DROP TABLE IF EXISTS #parent_account_segment; 
select A.parent_account_id,A.currency_type, MAX(A.segment) segment, MAX(A.signup_type) signup_type
INTO #parent_account_segment
from tmp.BO_ATR_REVENUE A
JOIN #parent_account_lifetime_minmax_dates B ON A.parent_account_id=B.parent_account_id AND A.currency_type=B.currency_type
AND (EXTRACT(year from revenue_start) * 100 + EXTRACT(month from revenue_start))=B.first_month
GROUP BY 1,2;


DROP TABLE IF EXISTS #parent_account_floor_changes;
select parent_account_id,currency_type
INTO #parent_account_floor_changes
from tmp.BO_ATR_REVENUE R
LEFT JOIN tmp."BO_FLOOR_CHANGES" B1 ON B1.campaignid=R.campaign_id AND B1.yelpbizids='#NAME?'
LEFT JOIN tmp."BO_FLOOR_CHANGES" B2 ON B2.campaignid=R.campaign_id AND B2.yelpbizids=R.business_id
WHERE  B2.campaignid IS NOT NULL OR B1.campaignid IS NOT NULL
GROUP BY 1,2
;

DROP TABLE IF EXISTS tmp.BO_ATR_REVENUE_SUMMARY;
SELECT 
 R.parent_account_id
,S.currency_type
,S.signup_type
,S.segment
,D.first_month
,D.last_month
,rnk.account_rank
,(EXTRACT(year from R.revenue_start) * 100 + EXTRACT(month from R.revenue_start)) as revenue_year_month
,SUM(R.revenue_net) AS revenue_net
,SUM(R.gross_revenue) AS gross_revenue
INTO tmp.BO_ATR_REVENUE_SUMMARY
from tmp.BO_ATR_REVENUE R
JOIN #parent_account_segment S ON R.parent_account_id=S.parent_account_id AND R.currency_type=S.currency_type
JOIN #parent_account_lifetime_minmax_dates D ON R.parent_account_id=D.parent_account_id AND R.currency_type=D.currency_type
JOIN #parent_account_rank rnk ON R.parent_account_id=rnk.parent_account_id AND R.currency_type=rnk.currency_type 
AND rnk.revenue_year_month=(EXTRACT(year from R.revenue_start) * 100 + EXTRACT(month from R.revenue_start)) 
GROUP BY 1,2,3,4,5,6,7,8
;

-- Revenue Mix in any given month
DROP TABLE IF EXISTS tmp.BO_ATR_REVENUE_MIX;
select revenue_year_month 
,CASE
   WHEN account_rank = 1 THEN 'M1' 
   WHEN account_rank BETWEEN 2 and 13 THEN 'Y1'
   WHEN account_rank BETWEEN 14 and 25 THEN 'Y2'
   WHEN account_rank > 24 THEN 'Legacy' 
END as account_category
,segment
,signup_type
,count(DISTINCT parent_account_id)
,SUM(revenue_net) revenue_net
,SUM(gross_revenue) gross_revenue
INTO tmp.BO_ATR_REVENUE_MIX
from tmp.BO_ATR_REVENUE_SUMMARY

group by 1,2,3,4
order by 1,3,2
;


DROP TABLE IF EXISTS #revenue_monthly_breakup;
select R.parent_account_id, R.currency_type
,CASE WHEN F.parent_account_id IS NOT NULL THEN 'Y' ELSE 'N' END AS floor_price_changed
,R.segment, R.revenue_year_month
,MAX(CASE
   WHEN account_rank = 1 THEN 'M1' 
   WHEN account_rank BETWEEN 2 and 13 THEN 'Y1'
   WHEN account_rank BETWEEN 14 and 25 THEN 'Y2'
   WHEN account_rank > 24 THEN 'Legacy' 
END) as account_category
,SUM(revenue_net) AS revenue_net
,SUM(gross_revenue) AS gross_revenue
INTO #revenue_monthly_breakup
from tmp.BO_ATR_REVENUE_SUMMARY R
LEFT JOIN #parent_account_floor_changes F ON R.parent_account_id=F.parent_account_id AND R.currency_type=F.currency_type
group by 1,2,3,4,5;



select 
M.parent_account_id 
,M.currency_type
,M.segment
,M.account_category
,M.floor_price_changed
,M.revenue_month
,SUM(revenue_net) AS revenue_net
,SUM(gross_revenue) AS gross_revenue
from #revenue_monthly_breakup M
LEFT JOIN
(
  select revenue_year_month, TO_DATE((revenue_year_month*100 + 1),'YYYYMMDD') as revenue_month
  from tmp.BO_ATR_REVENUE_SUMMARY
  where revenue_year_month >= 201401
  group by 1,2
) PREV ON M.revenue_year_month=D.revenue_year_month

where M.revenue_year_month >= 201401 AND M.currency_type='USD'
GROUP BY 1,2,3,4,5;



select 
parent_account_id 
,currency_type
,segment
,floor_price_changed
,SUM(CASE WHEN M.revenue_year_month=201401 THEN revenue_net END) as net_revenue_201401
,SUM(CASE WHEN M.revenue_year_month=201402 THEN revenue_net END) as net_revenue_201402
,SUM(CASE WHEN M.revenue_year_month=201403 THEN revenue_net END) as net_revenue_201403
,SUM(CASE WHEN M.revenue_year_month=201404 THEN revenue_net END) as net_revenue_201404
,SUM(CASE WHEN M.revenue_year_month=201405 THEN revenue_net END) as net_revenue_201405
,SUM(CASE WHEN M.revenue_year_month=201406 THEN revenue_net END) as net_revenue_201406
,SUM(CASE WHEN M.revenue_year_month=201407 THEN revenue_net END) as net_revenue_201407
,SUM(CASE WHEN M.revenue_year_month=201408 THEN revenue_net END) as net_revenue_201408
,SUM(CASE WHEN M.revenue_year_month=201409 THEN revenue_net END) as net_revenue_201409
,SUM(CASE WHEN M.revenue_year_month=201410 THEN revenue_net END) as net_revenue_201410
,SUM(CASE WHEN M.revenue_year_month=201411 THEN revenue_net END) as net_revenue_201411
,SUM(CASE WHEN M.revenue_year_month=201412 THEN revenue_net END) as net_revenue_201412
,SUM(CASE WHEN M.revenue_year_month=201501 THEN revenue_net END) as net_revenue_201501
,SUM(CASE WHEN M.revenue_year_month=201502 THEN revenue_net END) as net_revenue_201502
,SUM(CASE WHEN M.revenue_year_month=201503 THEN revenue_net END) as net_revenue_201503
,SUM(CASE WHEN M.revenue_year_month=201504 THEN revenue_net END) as net_revenue_201504
,SUM(CASE WHEN M.revenue_year_month=201505 THEN revenue_net END) as net_revenue_201505
,SUM(CASE WHEN M.revenue_year_month=201506 THEN revenue_net END) as net_revenue_201506
,SUM(CASE WHEN M.revenue_year_month=201507 THEN revenue_net END) as net_revenue_201507
,SUM(CASE WHEN M.revenue_year_month=201508 THEN revenue_net END) as net_revenue_201508
,SUM(CASE WHEN M.revenue_year_month=201509 THEN revenue_net END) as net_revenue_201509
,SUM(CASE WHEN M.revenue_year_month=201510 THEN revenue_net END) as net_revenue_201510
,SUM(CASE WHEN M.revenue_year_month=201511 THEN revenue_net END) as net_revenue_201511
,SUM(CASE WHEN M.revenue_year_month=201512 THEN revenue_net END) as net_revenue_201512
,SUM(CASE WHEN M.revenue_year_month=201601 THEN revenue_net END) as net_revenue_201601
INTO #revenue_monthly_summary
from #revenue_monthly_breakup M

LEFT JOIN
(
  select revenue_year_month, TO_DATE((revenue_year_month*100 + 1),'YYYYMMDD') as revenue_month
  from tmp.BO_ATR_REVENUE_SUMMARY
  where revenue_year_month >= 201401
  group by 1,2
) D ON M.revenue_year_month=D.revenue_year_month

where M.revenue_year_month >= 201401 AND M.currency_type='USD'

group by 1,2,3,4
order by 3,4,1
;



