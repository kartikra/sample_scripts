DROP TABLE temp.`BO_ADV_REV_DATE`;
CREATE TABLE temp.`BO_ADV_REV_DATE` (
   `advertiser_id` varchar(100) NOT NULL,
   `currency_type` varchar(45) NOT NULL,
   `Enroll_Month` int(11) DEFAULT NULL,
   `Last_Revenue_Month` int(11) DEFAULT NULL,
   `Adv_Revenue_Year` int(11) DEFAULT NULL,
   `Adv_Min_Month_of_Year` int(11) DEFAULT NULL,
   `Adv_Max_Month_of_Year` int(11) DEFAULT NULL,
   `Max_Calendar_Month` int(11) DEFAULT NULL,
   PRIMARY KEY (`advertiser_id`,`currency_type`,`Adv_Revenue_Year`),
   KEY `Index1`(`advertiser_id`,`currency_type`),
   KEY `Index2`(`Adv_Revenue_Year`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;


INSERT INTO temp.`BO_ADV_REV_DATE`
SELECT 
 A.advertiser_id
,A.currency_type 
,B.Enroll_Month
,B.Last_Revenue_Month
,EXTRACT(YEAR FROM A.revenue_start) As Adv_Revenue_Year
,MIN(EXTRACT(YEAR_MONTH FROM A.revenue_start)) AS Adv_Min_Month_of_Year
,MAX(EXTRACT(YEAR_MONTH FROM A.revenue_start)) AS Adv_Max_Month_of_Year
,(EXTRACT(YEAR FROM A.revenue_start)*100 + 12) As Max_Calendar_Month

FROM temp.BO_ADV_SALES_DATA A

JOIN
(
select 
 advertiser_id
,currency_type
,MIN(EXTRACT(YEAR_MONTH FROM revenue_start)) AS Enroll_Month
,MAX(EXTRACT(YEAR_MONTH FROM revenue_start)) AS Last_Revenue_Month
from temp.BO_ADV_SALES_DATA
GROUP BY 1,2
) B ON A.advertiser_id=B.advertiser_id AND A.currency_type=B.currency_type

GROUP BY A.advertiser_id,A.currency_type,B.Enroll_Month,B.Last_Revenue_Month, 
EXTRACT(YEAR FROM A.revenue_start)
;


CREATE TABLE temp.`BO_PARENT_ADV_NET_PAID_DATE` (
   `Parent_Account_Id` varchar(100) NOT NULL,
   `currency_type` varchar(45) NOT NULL,
   `Net_Paid_Month` int(11) DEFAULT NULL,
   PRIMARY KEY (`Parent_Account_Id`,`currency_type`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;


INSERT INTO temp.`BO_PARENT_ADV_NET_PAID_DATE`
SELECT 
A.Parent_Account_Id
,A.currency_type
,MIN(EXTRACT(YEAR_MONTH FROM A.revenue_start))
FROM temp.BO_ADV_SALES_DATA A
WHERE A.revenue_net > 0
GROUP BY 1,2;


DROP TABLE temp.`BO_PARENT_ADV_REV_DATE`;

CREATE TABLE temp.`BO_PARENT_ADV_REV_DATE` (
   `Parent_Account_Id` varchar(100) NOT NULL,
   `currency_type` varchar(45) NOT NULL,
   `Enroll_Month` int(11) DEFAULT NULL,
   `Last_Revenue_Month` int(11) DEFAULT NULL,
   `Adv_Revenue_Year` int(11) DEFAULT NULL,
   `Adv_Min_Month_of_Year` int(11) DEFAULT NULL,
   `Adv_Max_Month_of_Year` int(11) DEFAULT NULL,
   `Max_Calendar_Month` int(11) DEFAULT NULL,
   PRIMARY KEY (`Parent_Account_Id`,`currency_type`,`Adv_Revenue_Year`),
   KEY `Index1`(`Parent_Account_Id`,`currency_type`),
   KEY `Index2`(`Adv_Revenue_Year`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;


INSERT INTO temp.`BO_PARENT_ADV_REV_DATE`
SELECT 
A.Parent_Account_Id
,A.currency_type 
,B.Enroll_Month
,B.Last_Revenue_Month
,EXTRACT(YEAR FROM A.revenue_start) As Adv_Revenue_Year
,MIN(EXTRACT(YEAR_MONTH FROM A.revenue_start)) AS Adv_Min_Month_of_Year
,MAX(EXTRACT(YEAR_MONTH FROM A.revenue_start)) AS Adv_Max_Month_of_Year
,(EXTRACT(YEAR FROM A.revenue_start)*100 + 12) As Max_Calendar_Month

FROM temp.BO_ADV_SALES_DATA A

JOIN
(
select 
 Parent_Account_Id
,currency_type
,MIN(EXTRACT(YEAR_MONTH FROM revenue_start)) AS Enroll_Month
,MAX(EXTRACT(YEAR_MONTH FROM revenue_start)) AS Last_Revenue_Month
from temp.BO_ADV_SALES_DATA
GROUP BY 1,2
) B ON A.Parent_Account_Id=B.Parent_Account_Id AND A.currency_type=B.currency_type

GROUP BY A.Parent_Account_Id,A.currency_type,B.Enroll_Month,B.Last_Revenue_Month, 
EXTRACT(YEAR FROM A.revenue_start)
;

DROP TABLE temp.`BO_PARENT_ADV_SEGMENT`;

CREATE TABLE temp.`BO_PARENT_ADV_SEGMENT` (
   `Parent_Account_Id` varchar(100) NOT NULL,
   `currency_type` varchar(45) NOT NULL,
   `status_ind` varchar(45) DEFAULT NULL,
   `signup_type` varchar(45) DEFAULT NULL,
   `segment` varchar(45) DEFAULT NULL,
 PRIMARY KEY (`Parent_Account_Id`,`currency_type`,`status_ind`),
   KEY `Index1` (`signup_type`),
   KEY `Index2` (`segment`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
;



INSERT INTO temp.`BO_PARENT_ADV_SEGMENT`
select 
 A.Parent_Account_Id
,A.currency_type
,'I'
,MAX(A.signup_type)
,MAX(A.segment)
from temp.BO_ADV_SALES_DATA A
JOIN temp.`BO_PARENT_ADV_REV_DATE` B ON A.Parent_Account_Id=B.Parent_Account_Id AND A.currency_type=B.currency_type
WHERE EXTRACT(YEAR_MONTH FROM A.revenue_start)= B.Enroll_Month
GROUP BY 1,2,3;


INSERT INTO temp.`BO_PARENT_ADV_SEGMENT`
select 
 A.Parent_Account_Id
,A.currency_type
,'F'
,MAX(A.signup_type)
,MAX(A.segment)
from temp.BO_ADV_SALES_DATA A
JOIN temp.`BO_PARENT_ADV_REV_DATE` B ON A.Parent_Account_Id=B.Parent_Account_Id AND A.currency_type=B.currency_type
WHERE EXTRACT(YEAR_MONTH FROM A.revenue_start)= B.Last_Revenue_Month
GROUP BY 1,2,3;


ANALYZE TABLE temp.`BO_PARENT_ADV_SEGMENT`;

