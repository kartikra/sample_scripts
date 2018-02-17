DROP TABLE IF EXISTS #parent_account_rank;

select parent_account_id, revenue_year_month,EXTRACT(year from current_date) * 100 + EXTRACT(month from current_date)  as current_year_month
,rank() OVER(PARTITION BY parent_account_id order by revenue_year_month ASC) as account_rank
INTO #parent_account_rank
from
(
select parent_account_id, (EXTRACT(year from revenue_start) * 100 + EXTRACT(month from revenue_start)) as revenue_year_month
from tmp.BO_ATR_REVENUE
where revenue_net > 0
group by 1,2
) T;


DROP TABLE IF EXISTS #parent_account_lifetime_minmax_dates;
select parent_account_id,min(revenue_year_month) first_month, max(revenue_year_month) AS last_month, max(account_rank) AS max_account_rank
into #parent_account_lifetime_minmax_dates
 from #parent_account_rank
 group by 1;
 

DROP TABLE IF EXISTS #parent_account_rank_gap; 
select T1.parent_account_id, T1.account_rank, T1.revenue_year_month
,T2.account_rank as next_account_rank,T2.revenue_year_month as next_revenue_year_month
-- If within 3 months or last month within last 2 months then continue ranking system
,  CASE WHEN (T2.revenue_year_month - T1.revenue_year_month <= 3) OR ( T2.revenue_year_month - T1.revenue_year_month = 89 AND ROUND(T2.revenue_year_month/100)-ROUND(T1.revenue_year_month/100)=1 )
          OR (T1.current_year_month - T1.revenue_year_month <= 3) OR ( T1.current_year_month - T1.revenue_year_month = 89 AND ROUND(T1.current_year_month/100)-ROUND(T1.revenue_year_month/100)=1 )
      THEN 1 ELSE 0 END as continuity_ind
INTO #parent_account_rank_gap
from #parent_account_rank T1
 LEFT JOIN (select parent_account_id, account_rank, revenue_year_month   from #parent_account_rank) T2
 ON T1.parent_account_id=T2.parent_account_id AND T1.account_rank=T2.account_rank-1
;


DROP TABLE IF EXISTS #parent_account_rank_list;
SELECT T1.*
,T2.revenue_year_month as revenue_year_month_2,T2.account_rank as account_rank_2
,T3.revenue_year_month as revenue_year_month_3, T3.account_rank as account_rank_3
,T4.revenue_year_month as revenue_year_month_4, T4.account_rank as account_rank_4
,T5.revenue_year_month as revenue_year_month_5, T5.account_rank as account_rank_5
,T6.revenue_year_month as revenue_year_month_6, T6.account_rank as account_rank_6
INTO #parent_account_rank_list
FROM 
( select parent_account_id, account_rank, revenue_year_month, continuity_ind, next_account_rank
from #parent_account_rank_gap ) T1
LEFT JOIN
( select parent_account_id, account_rank, revenue_year_month, continuity_ind, next_account_rank
from #parent_account_rank_gap ) T2
 ON T1.parent_account_id=T2.parent_account_id AND T1.account_rank=T2.account_rank-1
 AND T1.continuity_ind = 0 AND T1.next_account_rank IS NOT NULL
LEFT JOIN
( select parent_account_id, account_rank, revenue_year_month, continuity_ind, next_account_rank
from #parent_account_rank_gap ) T3
 ON T2.parent_account_id=T3.parent_account_id AND T2.account_rank=T3.account_rank-1
AND T2.continuity_ind = 0 AND T2.next_account_rank IS NOT NULL
LEFT JOIN
( select parent_account_id, account_rank, revenue_year_month, continuity_ind, next_account_rank
from #parent_account_rank_gap ) T4
 ON T3.parent_account_id=T4.parent_account_id AND T3.account_rank=T4.account_rank-1
AND T3.continuity_ind = 0 AND T3.next_account_rank IS NOT NULL
LEFT JOIN
( select parent_account_id, account_rank, revenue_year_month, continuity_ind, next_account_rank
from #parent_account_rank_gap ) T5
 ON T4.parent_account_id=T5.parent_account_id AND T4.account_rank=T5.account_rank-1
AND T4.continuity_ind = 0 AND T4.next_account_rank IS NOT NULL
LEFT JOIN
( select parent_account_id, account_rank, revenue_year_month, continuity_ind, next_account_rank
from #parent_account_rank_gap ) T6
 ON T5.parent_account_id=T6.parent_account_id AND T5.account_rank=T6.account_rank-1
AND T5.continuity_ind = 0 AND T5.next_account_rank IS NOT NULL
;

DROP TABLE IF EXISTS #parent_account_rank_summary;
select b.*
,min(a.revenue_year_month) revenue_year_month,  max(a.revenue_year_month) end_revenue_year_month
,min(revenue_year_month_2) revenue_year_month_2,max(revenue_year_month_2) end_revenue_year_month_2
,min(revenue_year_month_3) revenue_year_month_3,max(revenue_year_month_3) end_revenue_year_month_3
,min(revenue_year_month_4) revenue_year_month_4,max(revenue_year_month_4) end_revenue_year_month_4
,min(revenue_year_month_5) revenue_year_month_5,max(revenue_year_month_5) end_revenue_year_month_5
,min(revenue_year_month_6) revenue_year_month_6,max(revenue_year_month_6) end_revenue_year_month_6
INTO #parent_account_rank_summary
from #parent_account_rank_list a
join #parent_account_lifetime_minmax_dates b on a.parent_account_id=b.parent_account_id
group by 1,2,3,4
;

SELECT T.*
INTO #parent_account_rank_final
FROM
(
select parent_account_id, revenue_year_month, CASE WHEN revenue_year_month_2 IS NULL THEN last_month end from #parent_account_rank_summary
UNION 
select parent_account_id, revenue_year_month_2, CASE WHEN revenue_year_month_3 IS NULL THEN last_month end from #parent_account_rank_summary where revenue_year_month_2 is not null
UNION 
select parent_account_id, revenue_year_month_3, CASE WHEN revenue_year_month_4 IS NULL THEN last_month end from #parent_account_rank_summary where revenue_year_month_3 is not null
UNION 
select parent_account_id, revenue_year_month_4, CASE WHEN revenue_year_month_5 IS NULL THEN last_month end from #parent_account_rank_summary where revenue_year_month_4 is not null
UNION 
select parent_account_id, revenue_year_month_5,  last_month from #parent_account_rank_summary where revenue_year_month_5 is not null
) T
;


DROP TABLE IF EXISTS #interim_parent_account_rank_1;
select A.*,A.account_rank as final_account_rank,  A.next_account_rank as final_next_account_rank
INTO #interim_parent_account_rank_1
from #parent_account_rank_gap A
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #interim_parent_account_rank_2;
select T2.parent_account_id,T2.account_rank, T2.revenue_year_month,T2.continuity_ind
,T2.final_account_rank - T1.final_account_rank AS final_account_rank
,CASE WHEN T2.final_next_account_rank IS NOT NULL THEN T2.final_account_rank - T1.final_account_rank + 1 ELSE NULL END AS final_next_account_rank
INTO #interim_parent_account_rank_2
from #interim_parent_account_rank_1 T1
JOIN
(
  select A.*
  from #interim_parent_account_rank_1 A
 ) T2 ON T1.parent_account_id=T2.parent_account_id AND T1.final_account_rank < T2.final_account_rank
where T1.continuity_ind=0 and T1.final_next_account_rank IS NOT NULL
;

DROP TABLE IF EXISTS #interim_parent_account_rank_1;
select T2.parent_account_id,T2.account_rank, T2.revenue_year_month,T2.continuity_ind
,T2.final_account_rank - T1.final_account_rank AS final_account_rank
,CASE WHEN T2.final_next_account_rank IS NOT NULL THEN T2.final_account_rank - T1.final_account_rank + 1 ELSE NULL END AS final_next_account_rank
-- INTO #interim_parent_account_rank_1
from #interim_parent_account_rank_2 T1
JOIN
(
  select A.*
  from #interim_parent_account_rank_2 A
 ) T2 ON T1.parent_account_id=T2.parent_account_id AND T1.final_account_rank < T2.final_account_rank
where T1.continuity_ind=0 and T1.final_next_account_rank IS NOT NULL
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------




SELECT T2.parent_account_id, T2.account_rank,T2.revenue_year_month,T2.continuity_ind,T2.final_rank,T1.*,   T2.final_rank - T1.final_rank
FROM ( SELECT A.* from #interim_parent_account_rank A ) T1
JOIN ( SELECT B.* from #interim_parent_account_rank B) T2  ON T1.parent_account_id=T2.parent_account_id AND T1.final_rank < T2.final_rank
where T1.continuity_ind=0



select * from #parent_account_rank
where parent_account_id='MdD_vXxGMRsGBuA0BRXx0g'
order by revenue_year_month
;

select * from #parent_account_rank_list
where parent_account_id='MdD_vXxGMRsGBuA0BRXx0g'
order by revenue_year_month
;

select * from #parent_account_rank_summary
where parent_account_id='MdD_vXxGMRsGBuA0BRXx0g'
order by revenue_year_month
;


select * from #parent_account_rank_final
where parent_account_id='MdD_vXxGMRsGBuA0BRXx0g'
order by revenue_year_month
;





DROP TABLE IF EXISTS #parent_account_rank;

select parent_account_id, revenue_year_month,EXTRACT(year from current_date) * 100 + EXTRACT(month from current_date)  as current_year_month
,rank() OVER(PARTITION BY parent_account_id order by revenue_year_month ASC) as account_rank
INTO #parent_account_rank
from
(
select parent_account_id, (EXTRACT(year from revenue_start) * 100 + EXTRACT(month from revenue_start)) as revenue_year_month
from tmp.BO_ATR_REVENUE
where revenue_net > 0
group by 1,2
) T;


DROP TABLE IF EXISTS #parent_account_lifetime_minmax_dates;
select parent_account_id,min(revenue_year_month) first_month, max(revenue_year_month) AS last_month, max(account_rank) AS max_account_rank
into #parent_account_lifetime_minmax_dates
 from #parent_account_rank
 group by 1;
 

DROP TABLE IF EXISTS #parent_account_rank_gap; 
select T1.parent_account_id, T1.account_rank, T1.revenue_year_month
,T2.account_rank as next_account_rank,T2.revenue_year_month as next_revenue_year_month
-- If within 3 months or last month within last 2 months then continue ranking system
,  CASE WHEN (T2.revenue_year_month - T1.revenue_year_month <= 3) OR ( T2.revenue_year_month - T1.revenue_year_month = 89 AND ROUND(T2.revenue_year_month/100)-ROUND(T1.revenue_year_month/100)=1 )
          OR (T1.current_year_month - T1.revenue_year_month <= 3) OR ( T1.current_year_month - T1.revenue_year_month = 89 AND ROUND(T1.current_year_month/100)-ROUND(T1.revenue_year_month/100)=1 )
      THEN 1 ELSE 0 END as continuity_ind
INTO #parent_account_rank_gap
from #parent_account_rank T1
 LEFT JOIN (select parent_account_id, account_rank, revenue_year_month   from #parent_account_rank) T2
 ON T1.parent_account_id=T2.parent_account_id AND T1.account_rank=T2.account_rank-1
;


DROP TABLE IF EXISTS #parent_account_rank_list;
SELECT T1.*,P.first_month, P.last_month
,T2.revenue_year_month as revenue_next_year_month,T2.account_rank as account_next_rank
INTO #parent_account_rank_list
FROM 
( select parent_account_id, account_rank, revenue_year_month, continuity_ind, next_account_rank
from #parent_account_rank_gap ) T1
LEFT JOIN
( select parent_account_id, account_rank, revenue_year_month, continuity_ind, next_account_rank
from #parent_account_rank_gap ) T2
 ON T1.parent_account_id=T2.parent_account_id AND T1.account_rank=T2.account_rank-1
 AND T1.continuity_ind = 0 AND T1.next_account_rank IS NOT NULL
JOIN #parent_account_lifetime_minmax_dates P ON T1.parent_account_id=P.parent_account_id
;


DROP TABLE IF EXISTS #parent_account_rank_list_2;
SELECT T.*
INTO #parent_account_rank_list_2
FROM
(
  select parent_account_id, MIN(first_month) AS agg_revenue_start,COALESCE(MIN(revenue_year_month),0) AS agg_revenue_end
  from #parent_account_rank_list WHERE revenue_next_year_month IS NOT NULL 
  group by parent_account_id
 
  UNION ALL
  select parent_account_id,revenue_year_month,revenue_next_year_month
  from #parent_account_rank_list  where revenue_next_year_month IS NOT NULL 

  UNION ALL
  
  select parent_account_id,COALESCE(MAX(revenue_year_month),0), MAX(last_month) 
  from #parent_account_rank_list WHERE revenue_next_year_month IS NOT NULL 
  group by parent_account_id
  
) T
-- WHERE parent_account_id='ZuwFG3BDDNFsC5mmftKU9w'
;

SELECT parent_account_id, agg_revenue_start,agg_revenue_end -- rank() OVER(PARTITION BY parent_account_id order by agg_revenue_end ASC) as start_rank
 FROM #parent_account_rank_list_2 where (agg_revenue_start=0 or agg_revenue_end=0)


DROP TABLE IF EXISTS #parent_account_rank_summary;
SELECT T1.parent_account_id, T1.agg_revenue_start, T2.agg_revenue_end
INTO #parent_account_rank_summary
FROM
( SELECT parent_account_id, agg_revenue_start,agg_revenue_end -- rank() OVER(PARTITION BY parent_account_id order by agg_revenue_end ASC) as start_rank
  FROM #parent_account_rank_list_2 
  where parent_account_id='ZuwFG3BDDNFsC5mmftKU9w'
) T1
JOIN
( SELECT parent_account_id, agg_revenue_start,agg_revenue_end,rank() OVER(PARTITION BY parent_account_id order by agg_revenue_end ASC) as start_rank
  FROM #parent_account_rank_list_2
) T2 ON T1.parent_account_id=T2.parent_account_id AND T1.start_rank=T2.start_rank-1
;
/*
select * from #parent_account_rank_summary
order by parent_account_id, agg_revenue_start
;

select parent_account_id, COUNT(agg_revenue_start)
from #parent_account_rank_summary
group by 1;

select * from #parent_account_rank
where parent_account_id='bEq62_WWgQU7m1yCzNoM6g'
order by revenue_year_month
;
select * from #parent_account_rank_list
where parent_account_id='bEq62_WWgQU7m1yCzNoM6g'
order by revenue_year_month
;
select * from #parent_account_rank_summary
where parent_account_id='bEq62_WWgQU7m1yCzNoM6g'
order by agg_revenue_start
;



select * from #parent_account_rank
where parent_account_id='ZuwFG3BDDNFsC5mmftKU9w'
order by revenue_year_month
;
select * from #parent_account_rank_list
where parent_account_id='ZuwFG3BDDNFsC5mmftKU9w'
order by revenue_year_month
;
select * from #parent_account_rank_summary
where parent_account_id='ZuwFG3BDDNFsC5mmftKU9w'
order by agg_revenue_start
;

*/




