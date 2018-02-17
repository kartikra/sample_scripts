DROP TABLE IF EXISTS #sd_payment_account_groupings;
SELECT T.*
INTO #sd_payment_account_groupings
FROM
(  SELECT
  a.id as business_id,a.cus_id AS business_cus_id, a.payment_account_id, c.cus_id as payment_cus_id,
  COUNT(a.id) OVER(PARTITION BY a.payment_account_id)  AS bizzes_grouped_beneath_payment_account

  FROM business_payment_account_fact a
  JOIN business_payment_account_flags_dimension b
  ON a.business_payment_account_flags_id = b.id
  JOIN payment_account_dimension c
  ON a.payment_account_id = c.id
  JOIN payment_account_flags_dimension d
  ON c.payment_account_flags_id = d.id
  WHERE b.is_inactive <> 'Inactive'
  AND d.is_inactive <> 'Inactive'
  AND d.account_type NOT IN('User','PlatformPartner')
  AND d.account_level NOT IN('PartnerAccount','AgencyAccount','InternalAccount','Internal')
  AND c.name <> 'YP CPC Admin'
) T
WHERE T.bizzes_grouped_beneath_payment_account > 1;


DROP TABLE IF EXISTS #sd_payment_account_grouped_orphans;
SELECT a.yelp_business_id AS business_id,a.id,	a.name,b.payment_account_id, b.bizzes_grouped_beneath_payment_account
INTO #sd_payment_account_grouped_orphans
FROM  smi.sfdc_account_fact a
JOIN #sd_payment_account_groupings b
 ON a.yelp_business_id = b.business_id
 WHERE a.parent_id IS NULL;



DROP TABLE IF EXISTS #bd_sfdc_account_groupings;										
-- 1m 6s										
SELECT										
a.name,										
COUNT(a.yelp_business_id) AS appearances										
INTO #bd_sfdc_account_groupings										
FROM smi.sfdc_account_fact a										
JOIN business_dimension b										
ON a.yelp_business_id = b.cus_id										
JOIN business_flags_dimension c										
ON b.business_flags_id = c.id										
WHERE c.is_closed <> 'Closed'										
AND c.is_migrated <> 'Migrated'										
AND c.is_inactive <> 'Inactive'										
AND c.is_removed_from_search <> 'RemovedFromSearch'										
GROUP BY a.name										
HAVING COUNT(a.yelp_business_id) >=10;										
										
DROP TABLE IF EXISTS #bd_sfdc_account_grouped_orphans;										
SELECT a.yelp_business_id AS business_id,										
a.id,										
a.name,										
MAX(b.appearances) AS appearances										
INTO #bd_sfdc_account_grouped_orphans										
FROM smi.sfdc_account_fact a										
JOIN #bd_sfdc_account_groupings b										
ON a.name = b.name										
WHERE a.parent_id IS NULL										
GROUP BY 1,2,3;										
										
SELECT COUNT(*) FROM #bd_sfdc_account_grouped_orphans;										


DROP TABLE IF EXISTS #bd_chain_detection_orphans;									
SELECT a.business_id,									
b.id,									
b.name,									
MAX(a.chain_confidence) AS chain_confidence,									
MAX(a.biz_in_chain_confidence) AS biz_in_chain_confidence									
INTO #bd_chain_detection_orphans									
FROM tmp.biz_in_chain_20150812_v2 a									
JOIN smi.sfdc_account_fact b									
ON a.business_id = b.yelp_business_id									
WHERE b.parent_id IS NULL									
GROUP BY 1,2,3;	
																	
SELECT COUNT(*) FROM #bd_chain_detection_orphans;									


										
-- 4,382,245 rows in 5.55s										
DROP TABLE IF EXISTS #bd_any_orphan_detection;										
SELECT										
a.business_id										
INTO #bd_any_orphan_detection										
FROM #bd_chain_detection_orphans a										
UNION										
SELECT										
b.business_id										
FROM #bd_payment_account_grouped_orphans b										
UNION SELECT										
c.business_id										
FROM #bd_sfdc_account_grouped_orphans c;										
										
SELECT COUNT(*) FROM #bd_any_orphan_detection;



DROP TABLE IF EXISTS #bd_business_photos;
SELECT a.id,													
a.cus_id AS business_id,													
COUNT(b.id) AS active_photos													
INTO #bd_business_photos													
FROM business_dimension a													
JOIN business_photo_fact b													
ON a.id = b.business_id													
JOIN business_photo_flags_dimension c													
ON b.business_photo_flags_id = c.id													
WHERE c.is_inactive <> 'Inactive'													
AND c.is_completed <> 'NotCompleted'													
GROUP BY 1,2;													


DROP TABLE IF EXISTS #bd_business_quick_tips;													
-- 39.13s													
SELECT a.id,													
a.cus_id AS business_id,													
COUNT(b.id) AS active_quick_tips													
INTO #bd_business_quick_tips													
FROM business_dimension a													
JOIN quick_tip_fact b													
ON a.id = b.business_id													
JOIN quick_tip_flags_dimension c													
ON b.quick_tip_flags_id = c.id													
WHERE c.is_inactive <> 'Inactive'													
GROUP BY 1,2;													
													

DROP TABLE IF EXISTS #bd_business_check_ins;
-- 5.67s													
SELECT a.id,													
a.cus_id AS business_id,													
COUNT(b.id) AS active_check_ins													
INTO #bd_business_check_ins													
FROM business_dimension a													
JOIN business_check_in_fact b													
ON a.id = b.business_id													
JOIN business_check_in_flags_dimension c													
ON b.check_in_flags_id = c.id													
WHERE c.is_inactive <> 'Inactive'													
GROUP BY 1,2;													


DROP TABLE IF EXISTS #bd_business_payment_account;													
SELECT 													
b.cus_id AS business_id,													
MIN(e.cus_id) AS payment_account_id													
INTO #bd_business_payment_account													
FROM business_dimension b													
JOIN business_payment_account_fact c													
ON b.id = c.business_id													
JOIN business_payment_account_flags_dimension d													
ON c.business_payment_account_flags_id = d.id													
JOIN payment_account_dimension e													
ON c.payment_account_id = e.id													
JOIN payment_account_flags_dimension f													
ON e.payment_account_flags_id = f.id													
WHERE d.is_inactive <> 'Inactive'													
AND f.is_inactive <> 'Inactive'													
AND f.account_type NOT IN('User','PlatformPartner')													
AND f.account_level NOT IN('PartnerAccount','AgencyAccount','InternalAccount','Internal')													
AND e.name <> 'YP CPC Admin'													
GROUP BY 1;													
										

------------------------------------------------------

DROP TABLE IF EXISTS #bd_any_orphan_detection_all_data;																
-- 4,382,222 rows in 41.73s																
SELECT																
a.business_id,																
e.name,																
e.address1 AS address,																
g.city,																
g.state,																
g.postal_code AS zip_code,																
g.country,																
e.phone,																
e.url AS website,																
CASE WHEN f.business_type IN('NonAdvertiser','UnclaimedAdvertiser') THEN 'NotClaimed' ELSE f.business_type END AS advertiser_status,																
f.is_closed,																
f.is_removed_from_search,																
e.review_count AS recommended_reviews,																
e.filtered_review_count AS not_recommended_reviews,																
CASE WHEN h.active_photos IS NULL THEN 0 ELSE h.active_photos END AS photos,																
CASE WHEN j.active_quick_tips IS NULL THEN 0 ELSE j.active_quick_tips END AS quick_tips,																
CASE WHEN k.active_check_ins IS NULL THEN 0 ELSE k.active_check_ins END AS check_ins,																
CASE WHEN b.chain_confidence IS NOT NULL THEN 1 ELSE 0 END AS chain_detector,																
CASE WHEN c.bizzes_grouped_beneath_payment_account IS NOT NULL THEN 1 ELSE 0 END AS payment_account,																
CASE WHEN d.appearances IS NOT NULL THEN 1 ELSE 0 END AS sfdc_repeat_name,																
MAX(b.chain_confidence) AS chain_confidence,																
MAX(b.biz_in_chain_confidence) AS biz_in_chain_confidence,																
MAX(c.bizzes_grouped_beneath_payment_account) AS bizzes_grouped_beneath_payment_account,																
MAX(c.payment_account_id) AS parent_payment_account_id,																
MAX(d.appearances) AS sfdc_name_appearances																
INTO #bd_any_orphan_detection_all_data																
FROM #bd_any_orphan_detection a																
JOIN business_dimension e																
ON a.business_id = e.cus_id																
JOIN business_flags_dimension f																
ON e.business_flags_id = f.id																
JOIN location_dimension g																
ON e.location_current_id = g.id																
LEFT JOIN #bd_business_photos h																
ON e.id = h.id																
LEFT JOIN #bd_business_quick_tips j																
ON e.id = j.id																
LEFT JOIN #bd_business_check_ins k																
ON e.id = k.id																
LEFT JOIN #bd_chain_detection_orphans b																
ON a.business_id = b.business_id																
LEFT JOIN #bd_payment_account_grouped_orphans c																
ON a.business_id = c.business_id																
LEFT JOIN #bd_sfdc_account_grouped_orphans d																
ON a.business_id = d.business_id																
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20;

SELECT COUNT(*) FROM #bd_any_orphan_detection_all_data;					



DROP TABLE IF EXISTS  #bd_any_orphan_detection_all_data_transform_parent_payment_account_id;										
-- 4,382,222 rows in 9.86s										
SELECT a.business_id,										
a.name,										
a.address,										
a.city,										
a.state,										
a.zip_code,										
a.country,										
a.phone,										
a.website,										
a.advertiser_status,										
a.is_closed,										
a.is_removed_from_search,										
a.recommended_reviews,										
a.not_recommended_reviews,										
a.photos,										
a.quick_tips,										
a.check_ins,										
a.chain_detector,										
a.payment_account,										
a.sfdc_repeat_name,										
a.chain_confidence,										
a.biz_in_chain_confidence,										
a.bizzes_grouped_beneath_payment_account,										
b.cus_id AS parent_payment_account_id,										
a.sfdc_name_appearances										
INTO #bd_any_orphan_detection_all_data_transform_parent_payment_account_id										
FROM #bd_any_orphan_detection_all_data a										
LEFT JOIN payment_account_dimension b										
ON a.parent_payment_account_id = b.id										
;
select count(*) from #bd_any_orphan_detection_all_data_transform_parent_payment_account_id;


DROP TABLE IF EXISTS #bd_any_orphan_detection_all_data_append_categories_and_payment_account;																
-- 4,364,671 rows in 1m 4s																
SELECT a.business_id,																
a.name,																
a.address,																
a.city,																
a.state,																
a.zip_code,																
a.country,																
a.phone,																
a.website,																
b.payment_account_id,																
a.advertiser_status,																
a.is_closed,																
a.is_removed_from_search,																
a.recommended_reviews,																
a.not_recommended_reviews,																
a.photos,																
a.quick_tips,																
a.check_ins,																
a.chain_detector,																
a.payment_account,																
a.sfdc_repeat_name,																
a.chain_confidence,																
a.biz_in_chain_confidence,																
a.bizzes_grouped_beneath_payment_account,																
a.parent_payment_account_id,																
a.sfdc_name_appearances,																
MIN(CASE WHEN d.name IS NULL THEN 'None' WHEN d.name IN('Art Galleries','Arts & Crafts','Bridal','Cards & Stationery','Cosmetics & Beauty Supply','Flowers','Flowers & Gifts','Jewelry','Mattresses','Musical Instruments & Teachers','Mattresses') THEN 'Shopping' ELSE d.name END) AS parent_category,																
MIN(CASE WHEN e.name IS NULL THEN 'None' ELSE e.name END) AS primary_category																
INTO #bd_any_orphan_detection_all_data_append_categories_and_payment_account																
FROM #bd_any_orphan_detection_all_data_transform_parent_payment_account_id a																
LEFT JOIN #bd_business_payment_account b																
ON a.business_id = b.business_id																
LEFT JOIN smi.sfdc_account_fact c																
ON a.business_id = c.yelp_business_id																
LEFT JOIN smi.sfdc_country_category_fact d																
ON c.rate_card_category = d.id																
LEFT JOIN smi.sfdc_country_category_fact e																
ON c.primary_category = e.id																
WHERE (c.is_deleted = 'false' OR c.is_deleted IS NULL)																
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26;																
																	

select count(*) from #bd_any_orphan_detection_all_data_append_categories_and_payment_account;		

