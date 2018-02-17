-- -----------------------------------------------
-- UPGRADE and Build Checkout table build script
-- -----------------------------------------------
--
-- -----------------------------------------------------------------------------------------
-- Change History
-- 11/04/2008	Jack Richter 	Created from other scripts
--						added column definition, fallback, checksum and journaling info
-- 11/05/2008	Jack Richter	modified upgrade_issues table definition
-- 11/10/2008	Jack Richter	modified upgrade_issues table definition
-- 01/15/2009	Jack Richter	added dummy table to handle dummy db's in MA
-- 04/12/2009	Jack Richter	added staging database to upgrade_issues table and enlarged runname column 
-- 06/15/2009	Jack Richter	added is_extracted, data_retained, cm_phy_owner_id to UPGRADE_ISSUES table
--								added is_extracted, data_retained, cm_phy_owner_id to UPGRADE_STGTBLINFO;
-- 08/19/2009	Jack Richter	modified for multideployment
-- 10/07/2009	Jack Richter	Added trims around column names on inserts
-- 03/26/2010	Jack Richter	added columns for new manifest validation checks to Upgrade_issues table.
--								dropped definition for UPGARDE_STGTBLINFO - table no longer used
-- 06/21/2010	Jack Richter	added upgrade_manifest ddl
--								added new variable RPT_DB
-- 06/22/2010	Jack Richter	chged upgrade_manifest table structure & added testing_rqrd column to upgrade issues table
-- 06/24/2010	Jack Richter	added sql for loading upgrade_manifest table
-- 07/19/2010	Jack Richter	changed set set to set
-- 07/23/2010	Jack Richter	corrected physical ownerids for SCAL databases
-- 08/06/2010	Jack Richter	added usage comments
-- 08/19/2010	jACK rICHTER	corrected insert statements
-- 08/23/2010	Jack Richter	added view for test patient table list
-- 								added columns for ini/items to upgrade issues table
-- 08/25/2010	Ed Kraynak		corrected DECIMAL (12,20) to DECIMAL(12,2)\
-- 09/08/2010	Jack Richter	changed format to compressible in columns in upgrade_issues table
-- 09/09/2010	Jack Richter	added base_db to upgrade_tp_tables view and altered where clause
-- 09/21/2010	Jack Richter	added grants on objects to ensure these always happen
--								added table creation for test patient upgrade_tp_tables table
--								added missing drop table statements.
-- 09/24/2010	Jack Richter	moved test pat validation tables to seperate script
-- 09/28/2010	Jack Richter	modified ushare.upgrade_columns definition and load
-- 02/14/2011	Jack Richter	republished so filenames are in synch with other scripts
-- 02/16/2011	Jack Richter	corrected HCCLNC to HCCLxx
-- 02/22/2011	Jack Richter	added logic to craete UPGRADE_OLD_STG_TBLS table
-- 02/23/2011	Ed Kraynak		in new table UPGRADE_STG_TBLS
--									added extra x missing in HCCLx_ushare
--									added ; at the end of select
--									changed databasenam in insert statement to dbname 
--									added cm_phy_owner_id to INSERT stmt
-- 02/22/2011	Jack Richter	corrected comments to note ALL deployments (including lead) should be in RPT_DB/STG_DB strings
-- 02/23/2011	Jack Richter	moved creation of UPGRADE_STG_TBLS table to after load of UPGRADE_DB_OWNER_LINK 
--									because it needs to use UPGRADE_DB_OWNER_LINK to set the staging table database name
--								Added update statement for UPGRADE_STG__TBLS
-- 03/06/2011	Jack Richter	corrected physical owner values for NCAL
-- 06/08/2011	Jack Richter	updated the cm_phy_owner_id columns from Integer to Varchar(25)
-- 01/19/2012   Tom Tang	Added quote to phy id
-- 01/19/2012	Tom Tang	Changed "View Add" to "View Added"
-- -----------------------------------------------------------------------------------------
-- To modify the script for a specific region :
--
-- 1) replace HCCLxx with name of the regions database. (ie HCCLxx with HCCLMA for MAS)
--
-- 2) replace :RPT_DB with the list of ALL reporting databases to be checked - multiple values in Califs  
--		(note: in the ROCs this should be the same value placed in :DPLY)
--		(note: in califs this should include the deployment _T, "base" and materialized view reporting databases (see below)
--	Include quotes!.... ie. 
--					Scal - 'HCCLDSC9A_RESC_T','HCCLDSC9_RESC_T', 'HCCLDSC9KP_RESC_T'
--					Ncal - 'HCCLDSC9A_WITS3_T','HCCLDSC9_WITS3_T', 'HCCLDSC9KP_WITS3_T'
--					region - 'HCCLDHI9_T'
--
--- 3) replace :STG_DB with the list of ALL staging databases to be checked- multiple values in Califs
--		(note: in the ROCs this should be the same value placed in :DPLY)
--		(note: in califs this should include the deployment _T, "base" and materialized view reporting databases (see below)
--	Include quotes!.... ie. 
--					Scal - 'HCCLDSC9A_RESC_S', 'HCCLDSC9_RESC_S', 'HCCLDSC9KP_RESC_S'
--					Ncal - 'HCCLDSC9A_WITS3_S','HCCLDSC9_WITS3_S', 'HCCLDSC9KP_WITS3_S'
--					region - 'HCCLDHI9_S'
-- Note: All the data loads (except UPGRADE_MANIFEST_LOAD table) are written for SQL*ASSISTANT
-- -----------------------------------------------------------------------------------------
-- DO NOT RUN THIS FILE AS A SCRIPT!!!  Statements must be run manually due to the exports and loads needed
-- ---------------------------------------------------------------------------------------------------------

-----------------
-- VARIABLES  ---
-----------------

-- MY_USHAREDB ---
-- MY_REPORTDB ---
-- MY_STAGEDB  ---
-- MY_USERDB   ---


-- ---------------
-- UPGRADE_TABLES
-- ---------------

-- ---------------------------------------------------------------
-- tables for migrating table structure info between WITS and PROD
-- ---------------------------------------------------------------


DROP TABLE MY_USHAREDB.KPCC_UPGRADE_TABLES;

CREATE SET TABLE MY_USHAREDB.KPCC_UPGRADE_TABLES,
	NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT
 (	dbname				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	tablename			VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	create_date			DATE FORMAT 'YYYY/MM/DD',
 	cm_phy_owner_id		VARCHAR(25) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX (dbname, tablename);

GRANT select ON MY_USHAREDB.KPCC_UPGRADE_TABLES to PUBLIC;

-- ------------------------------------------------------------------
-- SQL to extract Table structure data for USHARE table
-- Run this sql and save the results to a file (ie. upgrade_tables_<rgn>_prod.txt)
-- ------------------------------------------------------------------
INSERT INTO MY_USHAREDB.KPCC_UPGRADE_TABLES 
SELECT
		UPPER(TRIM(databasename)) dbname,
		UPPER(TRIM(tablename)) AS tablename,
		CURRENT_DATE,
		CASE
			WHEN databasename LIKE '%CO%' THEN '120140'
			WHEN databasename LIKE '%GA%' THEN '120200'
			WHEN databasename LIKE '%HI%' THEN '120130'
			WHEN databasename LIKE '%MA%' THEN '120170'
			WHEN databasename LIKE '%NW%' THEN '120190'
			WHEN databasename LIKE '%OH%' THEN '120180'
			WHEN databasename LIKE '%NC2/_%' ESCAPE '/' OR databasename LIKE '%PNCKP/_%' ESCAPE '/'    THEN '120160' 
			WHEN databasename LIKE '%NC2A/_%' ESCAPE '/'  THEN '121312' 
			WHEN databasename LIKE '%NC2B/_%' ESCAPE '/'  THEN '121320'
			WHEN databasename LIKE '%NC2C/_%' ESCAPE '/'  THEN '121318'
			WHEN databasename LIKE '%NC2D/_%' ESCAPE '/'  THEN '121314'
			WHEN databasename LIKE '%NC2E/_%' ESCAPE '/'  THEN '121316'
			WHEN databasename LIKE '%NC2F/_%' ESCAPE '/'  THEN '121322'
			WHEN databasename LIKE '%NC2G/_%' ESCAPE '/'  THEN ''
			WHEN databasename LIKE '%SC/_%' ESCAPE '/' OR databasename LIKE '%PSCKP/_%' ESCAPE '/'  THEN '120150'
			WHEN databasename LIKE '%SCA/_%' ESCAPE '/'  THEN '121212'
			WHEN databasename LIKE '%SCB/_%' ESCAPE '/'  THEN '121214'
			WHEN databasename LIKE '%SCC/_%' ESCAPE '/'  THEN '121216'
			WHEN databasename LIKE '%SCD/_%' ESCAPE '/'  THEN '121218'
			WHEN databasename LIKE '%SCE/_%' ESCAPE '/'  THEN '121220'
			WHEN databasename LIKE '%SCF/_%' ESCAPE '/'  THEN '121222'
			WHEN databasename LIKE '%SCG/_%' ESCAPE '/'  THEN '1217'
			
			WHEN 'NC' = 'MY_REGION' THEN
			CASE
				WHEN databasename = 'MY_REPORTDB' THEN  '120160' 
				WHEN databasename = 'MY_CALCDB1'  THEN '121312' 
				WHEN databasename = 'MY_CALCDB2'  THEN  '121320'
				WHEN databasename = 'MY_CALCDB3'  THEN '121318'
				WHEN databasename = 'MY_CALCDB4'  THEN '121314'
				WHEN databasename = 'MY_CALCDB5'  THEN  '121316'
				WHEN databasename = 'MY_CALCDB6'  THEN  '121322'
				WHEN databasename = 'MY_CALCDB7'  THEN  ''
			END
			
			WHEN 'SC' = 'MY_REGION' THEN
			CASE
				WHEN databasename = 'MY_REPORTDB' THEN '120150'
				WHEN databasename = 'MY_CALCDB1'  THEN '121212'
				WHEN databasename = 'MY_CALCDB2'  THEN '121214'
				WHEN databasename = 'MY_CALCDB3'  THEN  '121216'
				WHEN databasename = 'MY_CALCDB4'  THEN '121218'
				WHEN databasename = 'MY_CALCDB5'  THEN '121220'
				WHEN databasename = 'MY_CALCDB6'  THEN '121222'
				WHEN databasename = 'MY_CALCDB7'  THEN '1217'
			END
			
		END AS cm_phy_owner_id
	FROM dbc.tables
	WHERE databasename IN ('MY_REPORTDB','MY_CALCDB1','MY_CALCDB2','MY_CALCDB3','MY_CALCDB4','MY_CALCDB5','MY_CALCDB6','MY_CALCDB7'

)

	AND TRIM(tablename ) NOT LIKE ALL ('1/_%','2/_%','3/_%','BF%') ESCAPE '/';






-- ---------------
-- UPGRADE_COLUMNS
-- ---------------

DROP TABLE MY_USHAREDB.KPCC_UPGRADE_COLUMNS;

CREATE SET TABLE MY_USHAREDB.KPCC_UPGRADE_COLUMNS,
	NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT
 (	dbname					VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	tablename				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	columnname				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	columntype				VARCHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
    columnlength 			INTEGER, 
	decimaltotaldigits		INTEGER,
	decimalfractionaldigits	INTEGER,
    compressible			VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	nullable				VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
	create_date				DATE FORMAT 'YYYY/MM/DD',
	cm_phy_owner_id			VARCHAR(25) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX (dbname, tablename, columnname);

GRANT select ON MY_USHAREDB.KPCC_UPGRADE_COLUMNS to PUBLIC;


-- ---------------------------------------------------------------------------------------
-- SQL to extract column definitions structure data for USHARE table
-- Run this sql and save the results to a file (ie. upgrade_columns_<rgn>_prod.txt)
-- ---------------------------------------------------------------------------------------
INSERT INTO MY_USHAREDB.KPCC_UPGRADE_COLUMNS
SELECT	TRIM(databasename), UPPER(TRIM(tablename)), UPPER(TRIM(columnname)), TRIM(columntype), TRIM(columnlength) (INTEGER), 
		TRIM(decimaltotaldigits) (INTEGER), TRIM(decimalfractionaldigits) (INTEGER), TRIM(compressible), TRIM(nullable),
CURRENT_DATE,
CASE
			WHEN databasename LIKE '%CO%' THEN '120140'
			WHEN databasename LIKE '%GA%' THEN '120200'
			WHEN databasename LIKE '%HI%' THEN '120130'
			WHEN databasename LIKE '%MA%' THEN '120170'
			WHEN databasename LIKE '%NW%' THEN '120190'
			WHEN databasename LIKE '%OH%' THEN '120180'
			WHEN databasename LIKE '%NC2/_%' ESCAPE '/' OR databasename LIKE '%PNCKP/_%' ESCAPE '/'    THEN '120160' 
			WHEN databasename LIKE '%NC2A/_%' ESCAPE '/'  THEN '121312' 
			WHEN databasename LIKE '%NC2B/_%' ESCAPE '/'  THEN '121320'
			WHEN databasename LIKE '%NC2C/_%' ESCAPE '/'  THEN '121318'
			WHEN databasename LIKE '%NC2D/_%' ESCAPE '/'  THEN '121314'
			WHEN databasename LIKE '%NC2E/_%' ESCAPE '/'  THEN '121316'
			WHEN databasename LIKE '%NC2F/_%' ESCAPE '/'  THEN '121322'
			WHEN databasename LIKE '%NC2G/_%' ESCAPE '/'  THEN ''
			WHEN databasename LIKE '%SC/_%' ESCAPE '/' OR databasename LIKE '%PSCKP/_%' ESCAPE '/'  THEN '120150'
			WHEN databasename LIKE '%SCA/_%' ESCAPE '/'  THEN '121212'
			WHEN databasename LIKE '%SCB/_%' ESCAPE '/'  THEN '121214'
			WHEN databasename LIKE '%SCC/_%' ESCAPE '/'  THEN '121216'
			WHEN databasename LIKE '%SCD/_%' ESCAPE '/'  THEN '121218'
			WHEN databasename LIKE '%SCE/_%' ESCAPE '/'  THEN '121220'
			WHEN databasename LIKE '%SCF/_%' ESCAPE '/'  THEN '121222'
			WHEN databasename LIKE '%SCG/_%' ESCAPE '/'  THEN '1217'
			
			
			WHEN 'NC' = 'MY_REGION' THEN
			CASE
				WHEN databasename = 'MY_REPORTDB' THEN  '120160' 
				WHEN databasename = 'MY_CALCDB1'  THEN '121312' 
				WHEN databasename = 'MY_CALCDB2'  THEN  '121320'
				WHEN databasename = 'MY_CALCDB3'  THEN '121318'
				WHEN databasename = 'MY_CALCDB4'  THEN '121314'
				WHEN databasename = 'MY_CALCDB5'  THEN  '121316'
				WHEN databasename = 'MY_CALCDB6'  THEN  '121322'
				WHEN databasename = 'MY_CALCDB7'  THEN  ''
			END
			
			WHEN 'SC' = 'MY_REGION' THEN
			CASE
				WHEN databasename = 'MY_REPORTDB' THEN '120150'
				WHEN databasename = 'MY_CALCDB1'  THEN '121212'
				WHEN databasename = 'MY_CALCDB2'  THEN '121214'
				WHEN databasename = 'MY_CALCDB3'  THEN  '121216'
				WHEN databasename = 'MY_CALCDB4'  THEN '121218'
				WHEN databasename = 'MY_CALCDB5'  THEN '121220'
				WHEN databasename = 'MY_CALCDB6'  THEN '121222'
				WHEN databasename = 'MY_CALCDB7'  THEN '1217'
			END
			
		END AS cm_phy_owner_id
FROM 	dbc.COLUMNS 
WHERE databasename IN
('MY_REPORTDB','MY_CALCDB1','MY_CALCDB2','MY_CALCDB3','MY_CALCDB4','MY_CALCDB5','MY_CALCDB6','MY_CALCDB7'

)

AND		TRIM(tablename ) NOT LIKE ALL ('1/_%','2/_%','3/_%','BF%') ESCAPE '/';







-- ---------------------
-- UPGRADE_COLUMNS_VIEWS
-- ---------------------

-- -----------------------------------------------------------------------
-- Build table to hold the view columns for comparison between PROD & WITS
-- -----------------------------------------------------------------------

DROP TABLE MY_USHAREDB.KPCC_UPGRADE_COLUMNS_VIEWS;

CREATE SET TABLE MY_USHAREDB.KPCC_UPGRADE_COLUMNS_VIEWS ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT
     (tablename VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      columnname VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      create_date				DATE FORMAT 'YYYY/MM/DD')
PRIMARY INDEX ( tablename ,columnname );

GRANT select ON MY_USHAREDB.KPCC_UPGRADE_COLUMNS_VIEWS to PUBLIC;

-- ---------------------------------------------------------------------------------------
-- SQL to extract view definitions structure data for USHARE table
-- Run this sql and save the results to a file (ie. upgrade_columns_views_<rgn>_prod.txt)
-- ---------------------------------------------------------------------------------------

INSERT INTO MY_USHAREDB.KPCC_UPGRADE_COLUMNS_VIEWS
SELECT	tablename, columnname, 	CURRENT_DATE FROM dbc.columns WHERE databasename = 'MY_USERDB';








-- -------------------------			
-- UPGRADE STAGING TABLE
-- -------------------------

DROP TABLE MY_USHAREDB.KPCC_UPGRADE_STG_TBLS;

CREATE SET TABLE MY_USHAREDB.KPCC_UPGRADE_STG_TBLS,
	NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT
(	dbname				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	tablename			VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	create_date				DATE FORMAT 'YYYY/MM/DD',
 	cm_phy_owner_id		VARCHAR(25)	CHARACTER SET LATIN NOT CASESPECIFIC
)  PRIMARY INDEX (dbname, tablename);

GRANT ALL ON MY_USHAREDB.KPCC_UPGRADE_STG_TBLS to PUBLIC;

-- ---------------------------------------------------------------------------------------
-- SQL to extract staging table names for USHARE table
-- Run this sql and save the results to a file (ie. upgrade_old_stg_tbls_<rgn>.txt)
-- ---------------------------------------------------------------------------------------
INSERT INTO MY_USHAREDB.KPCC_UPGRADE_STG_TBLS	
SELECT 
	databasename, 
	tablename, 	
	CURRENT_DATE,
	CASE
		WHEN databasename LIKE '%CO%' THEN '120140'
		WHEN databasename LIKE '%GA%' THEN '120200'
		WHEN databasename LIKE '%HI%' THEN '120130'
		WHEN databasename LIKE '%MA%' THEN '120170'
		WHEN databasename LIKE '%NW%' THEN '120190'
		WHEN databasename LIKE '%OH%' THEN '120180'
		WHEN databasename LIKE '%NC2/_%' ESCAPE '/' OR databasename LIKE '%PNCKP/_%' ESCAPE '/'    THEN '120160' 
		WHEN databasename LIKE '%NC2A/_%' ESCAPE '/'  THEN '121312' 
		WHEN databasename LIKE '%NC2B/_%' ESCAPE '/'  THEN '121314' 
		WHEN databasename LIKE '%NC2C/_%' ESCAPE '/'  THEN '121316'
		WHEN databasename LIKE '%NC2D/_%' ESCAPE '/'  THEN '121318'
		WHEN databasename LIKE '%NC2E/_%' ESCAPE '/'  THEN '121320'
		WHEN databasename LIKE '%NC2F/_%' ESCAPE '/'  THEN '121322'
		WHEN databasename LIKE '%NC2G/_%' ESCAPE '/'  THEN ''
		WHEN databasename LIKE '%SC/_%' ESCAPE '/' OR databasename LIKE '%PSCKP/_%' ESCAPE '/'  THEN '120150'
		WHEN databasename LIKE '%SCA/_%' ESCAPE '/'  THEN '121212'
		WHEN databasename LIKE '%SCB/_%' ESCAPE '/'  THEN '121214'
		WHEN databasename LIKE '%SCC/_%' ESCAPE '/'  THEN '121216'
		WHEN databasename LIKE '%SCD/_%' ESCAPE '/'  THEN '121218'
		WHEN databasename LIKE '%SCE/_%' ESCAPE '/'  THEN '121220'
		WHEN databasename LIKE '%SCF/_%' ESCAPE '/'  THEN '121222'
		WHEN databasename LIKE '%SCG/_%' ESCAPE '/'  THEN '1217'
		
		WHEN 'NC' = 'MY_REGION' THEN
		CASE
			WHEN databasename = 'MY_STAGEDB' THEN  '120160' 
			WHEN databasename = 'MY_DEPLOYDB1'  THEN '121312' 
			WHEN databasename = 'MY_DEPLOYDB2'  THEN  '121320'
			WHEN databasename = 'MY_DEPLOYDB3'  THEN '121318'
			WHEN databasename = 'MY_DEPLOYDB4'  THEN '121314'
			WHEN databasename = 'MY_DEPLOYDB5'  THEN  '121316'
			WHEN databasename = 'MY_DEPLOYDB6'  THEN  '121322'
			WHEN databasename = 'MY_DEPLOYDB7'  THEN  ''
		END
		
		WHEN 'SC' = 'MY_REGION' THEN
		CASE
			WHEN databasename = 'MY_STAGEDB' THEN '120150'
			WHEN databasename = 'MY_DEPLOYDB1'  THEN '121212'
			WHEN databasename = 'MY_DEPLOYDB2'  THEN '121214'
			WHEN databasename = 'MY_DEPLOYDB3'  THEN  '121216'
			WHEN databasename = 'MY_DEPLOYDB4'  THEN '121218'
			WHEN databasename = 'MY_DEPLOYDB5'  THEN '121220'
			WHEN databasename = 'MY_DEPLOYDB6'  THEN '121222'
			WHEN databasename = 'MY_DEPLOYDB7'  THEN '1217'
		END
		
	END AS cm_phy_owner_id  
FROM dbc.tables  
WHERE databasename IN 
('MY_STAGEDB','MY_DEPLOYDB1','MY_DEPLOYDB2','MY_DEPLOYDB3','MY_DEPLOYDB4','MY_DEPLOYDB5','MY_DEPLOYDB6','MY_DEPLOYDB7'

);




-- -----------------------------------------
-- Update to correct staging databasename.
-- ------------------------------------------

UPDATE 	MY_USHAREDB.KPCC_UPGRADE_STG_TBLS
SET 	dbname = MY_USHAREDB.KPCC_UPGRADE_DB_OWNER_LINK.stg_db
where 	MY_USHAREDB.KPCC_UPGRADE_STG_TBLS.cm_phy_owner_id = MY_USHAREDB.KPCC_UPGRADE_DB_OWNER_LINK.cm_phy_owner_id;





-- --------------------------------------			
-- UPGRADE STAGING TABLE COLUMNS
-- --------------------------------------



DROP TABLE MY_USHAREDB.KPCC_UPGRADE_STG_COLUMNS;

CREATE SET TABLE MY_USHAREDB.KPCC_UPGRADE_STG_COLUMNS,
	NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT
 (	dbname					VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	tablename				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	columnname				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	columntype				VARCHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
    columnlength 			INTEGER, 
	decimaltotaldigits		INTEGER,
	decimalfractionaldigits	INTEGER,
    compressible			VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	nullable				VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
	create_date				DATE FORMAT 'YYYY/MM/DD',
	cm_phy_owner_id			VARCHAR(25) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX (dbname, tablename, columnname);

GRANT select ON MY_USHAREDB.KPCC_UPGRADE_STG_COLUMNS to PUBLIC;

-- ---------------------------------------------------------------------------------------
-- SQL to extract column definitions structure data for USHARE table
-- Run this sql and save the results to a file (ie. upgrade_columns_<rgn>_prod.txt)
-- ---------------------------------------------------------------------------------------
INSERT INTO MY_USHAREDB.KPCC_UPGRADE_STG_COLUMNS
SELECT	TRIM(databasename), UPPER(TRIM(tablename)), UPPER(TRIM(columnname)), TRIM(columntype), TRIM(columnlength) (INTEGER), 
		TRIM(decimaltotaldigits) (INTEGER), TRIM(decimalfractionaldigits) (INTEGER), TRIM(compressible), TRIM(nullable),
CURRENT_DATE,
	CASE
			WHEN databasename LIKE '%CO%' THEN '120140'
			WHEN databasename LIKE '%GA%' THEN '120200'
			WHEN databasename LIKE '%HI%' THEN '120130'
			WHEN databasename LIKE '%MA%' THEN '120170'
			WHEN databasename LIKE '%NW%' THEN '120190'
			WHEN databasename LIKE '%OH%' THEN '120180'
			WHEN databasename LIKE '%NC2/_%' ESCAPE '/' OR databasename LIKE '%PNCKP/_%' ESCAPE '/'    THEN '120160' 
			WHEN databasename LIKE '%NC2A/_%' ESCAPE '/'  THEN '121312' 
			WHEN databasename LIKE '%NC2B/_%' ESCAPE '/'  THEN '121320'
			WHEN databasename LIKE '%NC2C/_%' ESCAPE '/'  THEN '121318'
			WHEN databasename LIKE '%NC2D/_%' ESCAPE '/'  THEN '121314'
			WHEN databasename LIKE '%NC2E/_%' ESCAPE '/'  THEN '121316'
			WHEN databasename LIKE '%NC2F/_%' ESCAPE '/'  THEN '121322'
			WHEN databasename LIKE '%NC2G/_%' ESCAPE '/'  THEN ''
			WHEN databasename LIKE '%SC/_%' ESCAPE '/' OR databasename LIKE '%PSCKP/_%' ESCAPE '/'  THEN '120150'
			WHEN databasename LIKE '%SCA/_%' ESCAPE '/'  THEN '121212'
			WHEN databasename LIKE '%SCB/_%' ESCAPE '/'  THEN '121214'
			WHEN databasename LIKE '%SCC/_%' ESCAPE '/'  THEN '121216'
			WHEN databasename LIKE '%SCD/_%' ESCAPE '/'  THEN '121218'
			WHEN databasename LIKE '%SCE/_%' ESCAPE '/'  THEN '121220'
			WHEN databasename LIKE '%SCF/_%' ESCAPE '/'  THEN '121222'
			WHEN databasename LIKE '%SCG/_%' ESCAPE '/'  THEN '1217'
			
			
	WHEN 'NC' = 'MY_REGION' THEN
		CASE
			WHEN databasename = 'MY_STAGEDB' THEN  '120160' 
			WHEN databasename = 'MY_DEPLOYDB1'  THEN '121312' 
			WHEN databasename = 'MY_DEPLOYDB2'  THEN  '121320'
			WHEN databasename = 'MY_DEPLOYDB3'  THEN '121318'
			WHEN databasename = 'MY_DEPLOYDB4'  THEN '121314'
			WHEN databasename = 'MY_DEPLOYDB5'  THEN  '121316'
			WHEN databasename = 'MY_DEPLOYDB6'  THEN  '121322'
			WHEN databasename = 'MY_DEPLOYDB7'  THEN  ''
		END
		
	WHEN 'SC' = 'MY_REGION' THEN
		CASE
			WHEN databasename = 'MY_STAGEDB' THEN '120150'
			WHEN databasename = 'MY_DEPLOYDB1'  THEN '121212'
			WHEN databasename = 'MY_DEPLOYDB2'  THEN '121214'
			WHEN databasename = 'MY_DEPLOYDB3'  THEN  '121216'
			WHEN databasename = 'MY_DEPLOYDB4'  THEN '121218'
			WHEN databasename = 'MY_DEPLOYDB5'  THEN '121220'
			WHEN databasename = 'MY_DEPLOYDB6'  THEN '121222'
			WHEN databasename = 'MY_DEPLOYDB7'  THEN '1217'
		END
			
	END AS cm_phy_owner_id
FROM 	dbc.COLUMNS 
WHERE databasename IN
('MY_STAGEDB','MY_DEPLOYDB1','MY_DEPLOYDB2','MY_DEPLOYDB3','MY_DEPLOYDB4','MY_DEPLOYDB5','MY_DEPLOYDB6','MY_DEPLOYDB7'
);

