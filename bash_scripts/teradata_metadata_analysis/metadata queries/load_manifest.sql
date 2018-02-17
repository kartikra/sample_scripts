GRANT select ON MY_USHAREDB.UPGRADE_MANIFEST_LOAD to PUBLIC;	

DROP TABLE MY_USHAREDB.UPGRADE_MANIFEST;
	
CREATE SET TABLE MY_USHAREDB.UPGRADE_MANIFEST,
	NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL,CHECKSUM = DEFAULT
   (slno 					INTEGER,
	tablename				VARCHAR(200) 	CHARACTER SET LATIN NOT CASESPECIFIC,
	columnname				VARCHAR(200) 	CHARACTER SET LATIN NOT CASESPECIFIC,
	chg_type				VARCHAR(30) 	CHARACTER SET LATIN NOT CASESPECIFIC,
	old_datatype			VARCHAR(50) 	CHARACTER SET LATIN NOT CASESPECIFIC,
	new_datatype			VARCHAR(50) 	CHARACTER SET LATIN NOT CASESPECIFIC,
	testing_rqrd			VARCHAR(5) 		CHARACTER SET LATIN NOT CASESPECIFIC
	)	UNIQUE PRIMARY INDEX ( slno );

GRANT select ON MY_USHAREDB.UPGRADE_MANIFEST to PUBLIC;	


UPDATE MY_USHAREDB.UPGRADE_MANIFEST_LOAD
SET tablename=TRIM(tablename)
   ,columnname=TRIM(columnname)
   ,chg_type=TRIM(chg_type)
   ,old_datatype=OREPLACE(TRIM(old_datatype),' ','')
   ,new_datatype=OREPLACE(TRIM(new_datatype),' ','')
;

UPDATE MY_USHAREDB.UPGRADE_MANIFEST_LOAD
SET tablename=TRIM(tablename)
   ,columnname=TRIM(columnname)
   ,chg_type=TRIM(chg_type)
   ,old_datatype=OREPLACE(TRIM(old_datatype),';',',')
   ,new_datatype=OREPLACE(TRIM(new_datatype),';',',')
;

	
-- ------------------------------------------------------------------------------------------------
-- to load data create manifest table  first load all records into UPGRADE_MANIFEST_LOAD table
-- then run the following query to eliminate dups and only get the most recent slno for the change
-- ------------------------------------------------------------------------------------------------

INSERT INTO MY_USHAREDB.UPGRADE_MANIFEST
(slno, tablename, columnname, chg_type, old_datatype, new_datatype, testing_rqrd)
	SELECT
		MAX(slno) AS slno,
		tablename,
		columnname,
		chg_type,
		old_datatype,
		new_datatype,
		MAX(testing_rqrd) AS testing_rqrd
	FROM MY_USHAREDB.UPGRADE_MANIFEST_LOAD AS a
	WHERE  	chg_type IN ('Column Add','Column Drop','Column Modify','Column Rename','Deprec Column', 'Deprec Table',
	'Table Add','Table Drop','View Added')
	GROUP BY 2,3,4,5,6;
