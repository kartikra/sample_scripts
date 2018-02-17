CREATE TABLE temp.BO_ADV_MM&Natl_Admin_Acct_Map LIKE temp.`AB_November_Q4_MM&Natl_Admin_Acct_Map;

CREATE TABLE temp.`BO_ADV_SALES_DATA` (
   `advertiser_id` varchar(100) DEFAULT NULL,
   `currency_type` varchar(45) DEFAULT NULL,
   `Parent_Account_Id` varchar(100) DEFAULT NULL,
   `signup_type` varchar(45) DEFAULT NULL,
   `segment` varchar(45) DEFAULT NULL,
   `revenue_start` date DEFAULT NULL,
   `revenue_end` date DEFAULT NULL,
   `promotion_code` varchar(45) DEFAULT NULL,
   `contract_start` date DEFAULT NULL,
   `contract_end` date DEFAULT NULL,
   `contract_type` varchar(10) DEFAULT NULL,
   `revenue_net` double DEFAULT NULL,
   `gross_revenue` double DEFAULT NULL,
   KEY `PK_Index` (`advertiser_id`,`currency_type`),
   KEY `Index1` (`Parent_Account_Id`),
   KEY `Index2` (`revenue_start`,`revenue_end`),
   KEY `Index3` (`signup_type`),
   KEY `Index4` (`segment`),
   KEY `Index5` (`promotion_code`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;


INSERT INTO temp.BO_ADV_SALES_DATA
SELECT 
a.advertiser_id
,a.currency_type
,CASE WHEN 												
   b.Parent_Account_ID IS NULL OR LOCATE('FMSA',b.Parent_Account_ID) > 0 OR a.account_business_model LIKE 'Franchisee%' THEN a.advertiser_id												
   ELSE b.Parent_Account_ID END	As Parent_Account_Id											
,a.signup_type
,CASE  WHEN (a.self_serve_cpc LIKE 'TRUE%' OR a.signup_type = 'Web Sale') THEN 'Self-Serve'
	   WHEN a.account_business_model LIKE 'Franchisee%' THEN 'Franchise'
	   WHEN a.account_level LIKE 'National%' THEN 'National'
	   WHEN a.account_level LIKE 'Mid-Market%' THEN 'Mid-Market'
	   ELSE 'Local' 
END AS segment
,COALESCE(a.revenue_start,'') AS revenue_start
,COALESCE(a.revenue_end,'') AS revenue_end
,COALESCE(a.promotion_code,'') AS promotion_code
,a.contract_start
,a.contract_end
 ,CASE  
	 WHEN COALESCE(DATEDIFF(a.contract_end,a.contract_start),0) = 0 THEN 'M2M'
	 WHEN COALESCE(DATEDIFF(a.contract_end,a.contract_start),0) BETWEEN 1 AND 95 THEN '3M'
	 WHEN COALESCE(DATEDIFF(a.contract_end,a.contract_start),0) BETWEEN 95 AND 185 THEN '6M'
	 WHEN COALESCE(DATEDIFF(a.contract_end,a.contract_start),0) > 185 THEN 'Y'
         ELSE 'UNK'
END AS contract_type	
,a.revenue_net
,a.gross_revenue

FROM temp.ATR_filtered a
 LEFT JOIN  temp.`BO_ADV_MM&Natl_Admin_Acct_Map` b  ON a.advertiser_id = b.advertiser_id
 
WHERE a.signup_type IN ('Sales Ops','Web Sale') AND revenue_start >= '2015-01-01';



ANALYZE TABLE temp.BO_ADV_SALES_DATA;

