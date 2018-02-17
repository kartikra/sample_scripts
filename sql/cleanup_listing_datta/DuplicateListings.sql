-- Get max score from algorithm that tags likelihood of chain

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
GROUP BY 1,2,3
;

-- Count number of photos per business_id

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
GROUP BY 1,2
;


-- Count number of quick tips per business_id

DROP TABLE IF EXISTS #bd_business_quick_tips;
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
GROUP BY 1,2
;

-- Count number of  check ins per business_id

DROP TABLE IF EXISTS #bd_business_check_ins;
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
GROUP BY 1,2
;


DROP TABLE IF EXISTS #bd_admin_listing_data;
CREATE TABLE #bd_admin_listing_data
(business_id integer ,
business_cus_id varchar(22) primary key,
business_name varchar(510),
address varchar(256) ,
city varchar(128) ,
state varchar(7) ,
zip_code varchar(12),
country varchar(7),
total_listings_in_address bigint,
phone varchar(32),
website varchar(510),
advertiser_status varchar(255),
is_closed varchar(255),
is_removed_from_search varchar(255),
is_migrated varchar(255),
recommended_reviews integer,
not_recommended_reviews integer,
photos bigint,
quick_tips bigint,
check_ins bigint,
chain_detector integer
)
sortkey (address,city,state,zip_code);

INSERT INTO #bd_admin_listing_data
SELECT
 M.id as business_id
,M.cus_id as business_cus_id
,M.name as business_name
,M.address1 AS address
,L.city
,L.state
,L.postal_code AS zip_code
,L.country
,COUNT(M.id) OVER (PARTITION BY address,city,state,postal_code, country) As total_listings_in_address

,M.phone
-- ,C.category_name
,M.url AS website 
,CASE WHEN F.business_type IN('NonAdvertiser','UnclaimedAdvertiser') THEN 'NotClaimed' ELSE F.business_type END AS advertiser_status
,F.is_closed
,F.is_removed_from_search
,F.is_migrated

,M.review_count AS recommended_reviews
,M.filtered_review_count AS not_recommended_reviews

,CASE WHEN h.active_photos IS NULL THEN 0 ELSE h.active_photos END AS photos
,CASE WHEN j.active_quick_tips IS NULL THEN 0 ELSE j.active_quick_tips END AS quick_tips
,CASE WHEN k.active_check_ins IS NULL THEN 0 ELSE k.active_check_ins END AS check_ins
,CASE WHEN x.chain_confidence IS NOT NULL THEN 1 ELSE 0 END AS chain_detector

FROM business_dimension M
JOIN business_flags_dimension F ON M.business_flags_id = F.id
-- LEFT JOIN business_category_dimension BC ON BC.business_id=M.business_id 
-- LEFT JOIN category_dimension C ON C.category_id=BC.category_id

JOIN location_dimension L
ON M.location_current_id = L.id

LEFT JOIN #bd_business_photos h
ON M.id = h.id
LEFT JOIN #bd_business_quick_tips j
ON M.id = j.id
LEFT JOIN #bd_business_check_ins k
ON M.id = k.id
LEFT JOIN #bd_chain_detection_orphans x
ON M.cus_id = x.business_id
;

select count(*) from #bd_admin_listing_data;


DROP TABLE IF EXISTS #bd_admin_duplicate_data;

SELECT A.address,A.city,A.state,A.zip_code,A.country
,A.business_id, A.business_cus_id,A.business_name,A.advertiser_status
,B.business_id as dup_business_id, B.business_cus_id as dup_business_cus_id
,B.business_name as dup_business_name, B.advertiser_status as dup_advertiser_status
,A.recommended_reviews, A.not_recommended_reviews, A.photos, A.quick_tips, A.check_ins
,B.recommended_reviews AS dup_recommended_reviews, B.not_recommended_reviews AS dup_not_recommended_reviews
,B.photos AS dup_photos, B.quick_tips AS dup_quick_tips, B.check_ins  AS dup_check_ins
,CASE WHEN A.advertiser_status <> 'NotClaimed' 
	THEN
		CASE WHEN B.advertiser_status <> 'NotClaimed' 
		THEN
			'Needs additional research'
		ELSE
			CASE WHEN B.advertiser_status <> 'NotClaimed' OR B.recommended_reviews > 0 OR B.not_recommended_reviews > 0 OR B.photos > 0 OR B.quick_tips > 0 OR B.check_ins > 0 
			THEN
				'Merge content of 2nd under 1st listing'
			ELSE
				'remove 2nd listing'
			END
		END
       ELSE
		CASE WHEN B.advertiser_status <> 'NotClaimed' 
		THEN
			CASE WHEN A.not_recommended_reviews > 0 OR A.photos > 0 OR A.quick_tips > 0 OR A.check_ins > 0 
			THEN
				'Merge content of 1st under 2nd listing'
			ELSE
				'remove 1st listing'
			END
		ELSE
			CASE WHEN B.recommended_reviews > 0 OR B.not_recommended_reviews > 0 OR B.photos > 0 OR B.quick_tips > 0 OR B.check_ins > 0 
			THEN
				CASE WHEN A.not_recommended_reviews > 0 OR A.photos > 0 OR A.quick_tips > 0 OR A.check_ins > 0 
				THEN
					'Needs additional research'
				ELSE
					'remove 1st listing'
				END
			ELSE
				CASE WHEN A.not_recommended_reviews > 0 OR A.photos > 0 OR A.quick_tips > 0 OR A.check_ins > 0 
				THEN
					'remove 2nd listing'
				ELSE
					'remove either listing'
				END
			END
		END
 END  As action_required

INTO #bd_admin_duplicate_data
FROM #bd_admin_listing_data A
JOIN #bd_admin_listing_data B ON A.address=B.address AND A.city=B.city 
AND A.state=B.state AND A.zip_code=B.zip_code AND A.country=B.country AND A.business_cus_id > B.business_cus_id 
WHERE A.address IS NOT NULL AND LENGTH(TRIM(A.address)) > 2  AND A.country='US'
AND A.is_removed_from_search='InSearchResults' AND B.is_removed_from_search='InSearchResults' 
AND A.is_closed='Open' AND B.is_closed='Open'
AND A.is_migrated='Not Migrated' AND B.is_migrated='Not Migrated'
AND A.total_listings_in_address <= 5;

select count(*) from #bd_admin_duplicate_data;


select * from #bd_admin_duplicate_data limit 800000;


