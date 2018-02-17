DROP TABLE temp.`BO_ACCT_MASTER`;

CREATE TABLE temp.`BO_ACCT_MASTER` (
   `revenue_year` int(11) DEFAULT NULL,
   `advertiser_id` varchar(100) DEFAULT NULL,
   `account_id` varchar(100) DEFAULT NULL,
   `currency_type` varchar(45) DEFAULT NULL,
   `revenue_month` int(11) DEFAULT NULL,
   `signup_type` varchar(45) DEFAULT NULL,
   `segment` varchar(45) DEFAULT NULL,
   `Enroll_Month` int(11) DEFAULT NULL,
   `Last_Revenue_Month` int(11) DEFAULT NULL,
   `total_calendar_months` int(11) DEFAULT NULL,
   `revenue_net` double DEFAULT NULL,
   `gross_revenue` double DEFAULT NULL,
   PRIMARY KEY (`account_id`,`currency_type`,`revenue_month`,`signup_type`,`segment`),
   KEY `Index1` (`Enroll_Month`),
   KEY `Index2` (`Last_Revenue_Month`),
   KEY `Index3` (`signup_type`),
   KEY `Index4` (`segment`),
   KEY `Index5` (`account_id`),
   KEY `Index6` (`currency_type`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;

INSERT INTO temp.`BO_ACCT_MASTER`
SELECT 
 B.Adv_Revenue_Year
,A.advertiser_id
,A.Parent_Account_Id AS Account_Id
,A.currency_type
,EXTRACT(YEAR_MONTH FROM A.revenue_start) 

,C.signup_type     -- self serve vs rep
,C.segment         -- national, local, franchise, mid market

,B.Enroll_Month
,B.Last_Revenue_Month
,(B.Max_Calendar_Month - B.Adv_Min_Month_of_Year + 1) As total_calendar_months

,SUM(A.revenue_net) AS revenue_net
,SUM(A.gross_revenue) AS gross_revenue

FROM temp.BO_ADV_SALES_DATA A
JOIN temp.BO_PARENT_ADV_REV_DATE B ON A.Parent_Account_Id=B.Parent_Account_Id 
AND A.currency_type=B.currency_type
AND EXTRACT(YEAR FROM A.revenue_start)=B.Adv_Revenue_Year

JOIN temp.`BO_PARENT_ADV_SEGMENT` C ON A.Parent_Account_Id=C.Parent_Account_Id 
AND A.currency_type=C.currency_type AND C.status_ind='I'

GROUP BY 1,2,3,4,5,6,7,8,9,10;

ANALYZE TABLE temp.`BO_ACCT_MASTER`;

