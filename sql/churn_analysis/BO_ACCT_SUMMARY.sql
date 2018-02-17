DROP TABLE temp.`BO_ACCT_MONTHLY_SUMMARY`;
CREATE TABLE temp.`BO_ACCT_MONTHLY_SUMMARY` (
   `account_id` varchar(100) DEFAULT NULL,
   `currency_type` varchar(45) DEFAULT NULL,
   `signup_type` varchar(45) DEFAULT NULL,
   `segment` varchar(45) DEFAULT NULL,
   `revenue_year` int(11) DEFAULT NULL,
   `revenue_month` int(11) DEFAULT NULL,
   `gross_revenue_total` double DEFAULT NULL,
   `revenue_net_total` double DEFAULT NULL,

   PRIMARY KEY (`account_id`,`currency_type`,`signup_type`,`segment`,`revenue_year`,`revenue_month`),
   KEY `Index1` (`account_id`,`currency_type`,`signup_type`,`segment`,`revenue_year`),
   KEY `Index2` (`signup_type`),
   KEY `Index3` (`segment`),
   KEY `Index4` (`account_id`),
   KEY `Index5` (`currency_type`),
   KEY `Index6` (`revenue_year`),
   KEY `Index7` (`revenue_month`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;

INSERT INTO temp.`BO_ACCT_MONTHLY_SUMMARY`
 SELECT 
	 A.Account_Id
	,A.currency_type
	,A.signup_type
	,A.segment
	,A.revenue_year
	,MOD(A.revenue_month,100) AS revenue_month
	,SUM(A.gross_revenue) AS gross_revenue_monthly_total
	,SUM(A.revenue_net) AS revenue_net_monthly_total
FROM temp.`BO_ACCT_MASTER` A 
GROUP BY 1,2,3,4,5,6;

ANALYZE TABLE temp.`BO_ACCT_MONTHLY_SUMMARY`;




DROP TABLE temp.`BO_ACCT_ROLLING_SUMMARY`;

CREATE TABLE temp.`BO_ACCT_ROLLING_SUMMARY` (
   `account_id` varchar(100) DEFAULT NULL,
   `currency_type` varchar(45) DEFAULT NULL,
   `signup_type` varchar(45) DEFAULT NULL,
   `segment` varchar(45) DEFAULT NULL,
   `revenue_year` int(11) DEFAULT NULL,
   `revenue_month` int(11) DEFAULT NULL,
   `gross_revenue_M1_to_12` double DEFAULT NULL,
   `revenue_net_M1_to_12` double DEFAULT NULL,
   `gross_revenue_M13_to_24` double DEFAULT NULL,
   `revenue_net_M13_to_24` double DEFAULT NULL,
   `gross_revenue_GT_24` double DEFAULT NULL,
   `revenue_net_GT_24` double DEFAULT NULL,

   PRIMARY KEY (`account_id`,`currency_type`,`signup_type`,`segment`,`revenue_year`,`revenue_month`),
   KEY `Index1` (`account_id`,`currency_type`,`signup_type`,`segment`,`revenue_year`),
   KEY `Index2` (`signup_type`),
   KEY `Index3` (`segment`),
   KEY `Index4` (`account_id`),
   KEY `Index5` (`currency_type`),
   KEY `Index6` (`revenue_year`),
   KEY `Index7` (`revenue_month`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;


INSERT INTO temp.`BO_ACCT_ROLLING_SUMMARY`
SELECT 
 A.account_id
,A.currency_type
,A.signup_type
,A.segment
,A.revenue_year
,A.revenue_month

,COALESCE(SUM(CASE WHEN ((B.revenue_year*100)+B.revenue_month) - ((A.revenue_year*100)+A.revenue_month) between 0 and 99 THEN B.gross_revenue_total END),0) As gross_revenue_M1_to_12
,COALESCE(SUM(CASE WHEN ((B.revenue_year*100)+B.revenue_month) - ((A.revenue_year*100)+A.revenue_month) between 0 and 99 THEN B.revenue_net_total END),0) As revenue_net_M1_to_12

,COALESCE(SUM(CASE WHEN ((B.revenue_year*100)+B.revenue_month) - ((A.revenue_year*100)+A.revenue_month) between 100 and 199 THEN B.gross_revenue_total END),0) As gross_revenue_net_M13_to_24
,COALESCE(SUM(CASE WHEN ((B.revenue_year*100)+B.revenue_month) - ((A.revenue_year*100)+A.revenue_month) between 100 and 199 THEN B.revenue_net_total END),0) As revenue_net_M13_to_24

,COALESCE(SUM(CASE WHEN ((B.revenue_year*100)+B.revenue_month) - ((A.revenue_year*100)+A.revenue_month) > 199 THEN B.gross_revenue_total END),0) As gross_revenue_net_GT_24
,COALESCE(SUM(CASE WHEN ((B.revenue_year*100)+B.revenue_month) - ((A.revenue_year*100)+A.revenue_month) > 199 THEN B.revenue_net_total END),0) As revenue_net_GT_24

FROM temp.`BO_ACCT_MONTHLY_SUMMARY` A
JOIN temp.`BO_ACCT_MONTHLY_SUMMARY` B  ON A.Account_Id=B.Account_Id 
    AND A.currency_type=B.currency_type 
    -- AND A.segment=B.segment AND A.signup_type=B.signup_type
    AND ((B.revenue_year*100)+B.revenue_month) >= ((A.revenue_year*100)+A.revenue_month)
    
WHERE ((A.revenue_year*100)+A.revenue_month) <= 201401

GROUP BY  1,2,3,4,5,6;

ANALYZE TABLE temp.`BO_ACCT_ROLLING_SUMMARY`;



DROP TABLE temp.`BO_ACCT_YEARLY_SUMMARY`;
CREATE TABLE temp.`BO_ACCT_YEARLY_SUMMARY` (
   `account_id` varchar(100) DEFAULT NULL,
   `currency_type` varchar(45) DEFAULT NULL,
   `signup_type` varchar(45) DEFAULT NULL,
   `segment` varchar(45) DEFAULT NULL,
   `revenue_year` int(11) DEFAULT NULL,
   `Enroll_Month` int(11) DEFAULT NULL,
   `total_calendar_months` int(11) DEFAULT NULL,
   `gross_revenue_total` double DEFAULT NULL,
   `revenue_net_total` double DEFAULT NULL,



   PRIMARY KEY (`account_id`,`currency_type`,`signup_type`,`segment`,`revenue_year`),
   KEY `Index1` (`Enroll_Month`),
   KEY `Index2` (`signup_type`),
   KEY `Index3` (`segment`),
   KEY `Index4` (`account_id`),
   KEY `Index5` (`currency_type`)
   KEY `Index6` (`revenue_year`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;


INSERT INTO temp.`BO_ACCT_YEARLY_SUMMARY`
SELECT 
	 A.Account_Id
	,A.currency_type
	,A.signup_type
	,A.segment
	,A.revenue_year
	,A.Enroll_Month
	,MAX(total_calendar_months) AS total_calendar_months
	,SUM(A.gross_revenue) AS gross_revenue_total
	,SUM(A.revenue_net) AS revenue_net_total
         
FROM temp.`BO_ACCT_MASTER` A    
GROUP BY 1,2,3,4,5,6;


ANALYZE TABLE temp.`BO_ACCT_YEARLY_SUMMARY`;



DROP TABLE temp.`BO_ACCT_SUMMARY`;

CREATE TABLE temp.`BO_ACCT_SUMMARY` (
   `account_id` varchar(100) DEFAULT NULL,
   `currency_type` varchar(45) DEFAULT NULL,
   `signup_type` varchar(45) DEFAULT NULL,
   `segment` varchar(45) DEFAULT NULL,
   `revenue_year` int(11) DEFAULT NULL,
   `revenue_month` int(11) DEFAULT NULL,
   `gross_revenue_monthly` double DEFAULT NULL,
   `revenue_net_monthly` double DEFAULT NULL,
   `prior_year_gross_revenue_monthly` double DEFAULT NULL,
   `prior_year_revenue_net_monthly` double DEFAULT NULL,
   `Enroll_Month` int(11) DEFAULT NULL,
   `curr_year_total_months` int(11) DEFAULT NULL,
   `curr_year_gross_revenue` double DEFAULT NULL,
   `curr_year_revenue_net` double DEFAULT NULL,
   `prior_year_total_months` int(11) DEFAULT NULL,
   `prior_year_gross_revenue` double DEFAULT NULL,
   `prior_year_revenue_net` double DEFAULT NULL,

   PRIMARY KEY (`account_id`,`currency_type`,`signup_type`,`segment`,`revenue_year`,`revenue_month`),
   KEY `Index1` (`account_id`,`currency_type`,`signup_type`,`segment`,`revenue_year`),
   KEY `Index2` (`signup_type`),
   KEY `Index3` (`segment`),
   KEY `Index4` (`account_id`),
   KEY `Index5` (`currency_type`),
   KEY `Index6` (`revenue_year`),
   KEY `Index7` (`revenue_month`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;


INSERT INTO temp.`BO_ACCT_SUMMARY` 
 SELECT 
	 A.Account_Id
	,A.currency_type
	,A.signup_type
	,A.segment
	,A.revenue_year
	,A.revenue_month
	,A.gross_revenue_total AS gross_revenue_monthly
	,A.revenue_net_total AS revenue_net_monthly
	,COALESCE(SUM(B.gross_revenue_total),0) AS prior_year_gross_revenue_monthly
	,COALESCE(SUM(B.revenue_net_total),0) AS prior_year_revenue_net_monthly

	,C.Enroll_Month
	,COALESCE(C.total_calendar_months,0) AS curr_year_total_months
	,COALESCE(C.gross_revenue_total,0) AS curr_year_gross_revenue
	,COALESCE(C.revenue_net_total,0)AS curr_year_revenue_net

	,COALESCE(P.total_calendar_months,0) AS prior_year_total_months
	,COALESCE(P.gross_revenue_total,0) AS prior_year_gross_revenue
	,COALESCE(P.revenue_net_total,0)AS prior_year_revenue_net

    FROM temp.`BO_ACCT_MONTHLY_SUMMARY` A    
     
    LEFT JOIN temp.`BO_ACCT_MONTHLY_SUMMARY` B  ON A.Account_Id=B.Account_Id 
    AND A.currency_type=B.currency_type AND A.signup_type=B.signup_type
    AND A.segment=B.segment AND A.revenue_year=B.revenue_year+1
    AND A.revenue_month = B.revenue_month

    JOIN temp.`BO_ACCT_YEARLY_SUMMARY` C ON A.Account_Id=C.Account_Id 
    AND A.currency_type=C.currency_type AND A.signup_type=C.signup_type
    AND A.segment=C.segment AND A.revenue_year=C.revenue_year

    LEFT JOIN temp.`BO_ACCT_YEARLY_SUMMARY` P ON A.Account_Id=P.Account_Id 
    AND A.currency_type=P.currency_type AND A.signup_type=P.signup_type
    AND A.segment=P.segment AND A.revenue_year=P.revenue_year+1

   GROUP BY 1,2,3,4,5,6,7,8;

   
ANALYZE TABLE temp.`BO_ACCT_SUMMARY`;



