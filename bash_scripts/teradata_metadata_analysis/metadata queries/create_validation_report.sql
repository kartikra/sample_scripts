-- --------------------------- 
-- UPGRADE Checkout/Validation 
-- --------------------------- 
-- ----------------------------------------------------------------------------------------- 
-- Change History 
-- 06/26/2008	Jack Richter	Added check for column adds and drops to _T validation 
--								Rewrote trailing decimal check to use teradata Trailing function 
--								Added exclusion of text and note columns 
-- 07/02/2008	Jack Richter	Added check for column adds/drops to _S validation 
--								Added check for HCCLxx views against USHARE to ensu re 
--								Any custom or derived columns match. 
--								Corrected check of columns with CF datatype 
-- 07/10/2008	Jack Richter	Fixed queries for checking view columns to check only tables in common 
-- 07/17/2008	Jack Richter	Modified to handle job divided tables in _S to _T comparisons 
-- 07/22/2008	Jack Richter	Changed CF datatype from varchar to char 
--								added exclusion of test/work tables in _S checkout section 
--								corrected comments and msg on _S/_T missing/extra column checks 
-- 07/23/2008	Jack Richter	Added logic to use job divided flag setting when checking _S tables. 
-- 08/07/2008	Jack Richter	added escape logic for underscores '_' 
-- 10/16/2008	Jack Richter	chgd checks to insert to an error table. 
-- 								added error numbers 
--								added additional checks on HCCLPxxx views 
-- 10/28/2008	Jack Richter	added section for compass compare and changed ushare table structure 
--								added formatting 'zzzzzz' to eliminate commas in column sizes 
--								added logging info to table create. 
--								fixed perf issue on check 53 
-- 10/29/2008	Jack Richter	added create_dttm to log table 
--								added inserts at start of each section to record start times 
--								added join to get is_preserved_yn for check 101 
-- 10/30/2008	Jack Richter	fixed issues in errors 51-55 pointed out by epic - handling owner 9001 
--								fixed issues in errors 101, 102 - they now handle job divided tables. 
-- 11/03/2008	Jack Richter	added logic for job divided tables to checks 103-108 
--								added breakout of column differences to multiple error numbers 
-- 11/04/2008	Jack Richter	attempted to convert to macro but it failed due to lack of access to DBC tables from macro 
--								added dbname to issues table so that multi-depoloyments could write all errors to one table. 
--								added dbname (_T reporting table owner) to all inserts. 
--								changed to handle multiple deployment utilization - seperated out table creates to sep script. 
--								added ck of cm_phys_owner_id for tables in errors 6,7,56-58 
--								added ck/save of table is_preserved_yn value for table in errors 6-9,52,53,56-58,102-108 
-- 11/05/2008	Jack Richter	fixed col order in errors 56-58 
--								added case wrapper on is_preserved_yn in errors 103 and 108 
--								chgd for merge of def and fmt columns in upgrade_issues table 
--								added check for work tables to be cleaned up -errors 901-903 
--								added on_demand flag reporting to errors 54,901-903 
-- 11/05/2008	Jack Richter	tested and corrected exclusions on new errors 901-903 
--								replaced hard coded dbname with value from dbc tables 
--								chgd check of dbname to use "in" instead of "=" to allow checking multi builds or deployments in one run 
--								added logic to exclude tables and columns with table/column extracted flag set to N 
-- 11/06/2008	Jack Richter	added error check 904 for job-division flag setting checks 
--								updated instructions for customizing script for a region 
--								updated exclusion list for additional work/temp table. 
-- 11/10/2008	Jack Richter	finshied check of all errors 
--								added colon (:) variables throughout the script and tested 
--								corrected numerous logic errors. 
--								added check of table kind in dbc.tables 
--								added assumptions to comment block below 
--								added Run Name to all inserts. 
-- 11/11/2008	Jack Richter	corrected spelling of DELETE on first sql stmt 
--								changed all current_timestamp to current_timestamp(0) 
--								error 4 - added UPPER(TRIM()) around tablenames based on false errors in MAS 
--								errors < 0 added logging of end time for each error to aid in perf analysis 
--								error 103 - fixed aliasing errors and incorrect columnnames 
--								error 52 - change to ignore 'U% tables 
-- 11/12/2008	Jack Richter	error 301 - removed t1.tablename = t2.tablename join condition 
--								error 103 - revised logic to use not exists rather than minus 
--								error 103 - chgd variable from COL_TBL to TAB_TBL 
-- 11/13/2008	Jack Richter	added ablitiy to specify errors to skip 
--								added formatting for columnlenght > 1000 so no commas appear 
-- 11/14/2008	Jack Richter	error 105 - removed extra parenthesis 
-- 11/17/2008	Jack Richter	error 6-9 - changed joins to clarity tables to inner joins 
--								added gathering of stats on ushare tables at start of script 
--								error 6-9 runtime reduced from 97/120 mins to 17 seconds 
--								error 103 - removed t1.tablename = t2.tablename join condition 
-- 11/24/2008	Jack Richter	added stats at end and additional select for count by errors at end of report 
--								error 105 - fixed errors 
-- 12/01/2008	Jack Richter	error 101 - add job divided flag to data returned 
--								error 102 - add code to handle clarity_tdl_age job division, added is_job_divided column to query 
--								error 103 - add code to handle clarity_tdl_age job division, added is_job_divided column to query 
--								error 105 to 107 - removed runname from subquery select, added is_job_divided column to query 
--								corrected column name in 'get detail of issues found' sql statement at end of script 
--								fixed incorrect instructions in comments for modifications 
-- 12/02/2008	Jack Richter	all errors - updated tablename exclusion list for SP07% and %_BK 
--								error 6 to 9 - corrected insert column name order - moved runname to position 3 from 5 
--								excluded tables ending in underscore or _BK or _BKUP 
--								error 101 - added code to eliminate tables where job division flag does not match _S table structures 
--								error 102 - fixed alias of table - was tbl should have been t1  changed to handle tables with incorrect job-divide setting 
--								error 103 - fixed typo changed colx. to cols., added comma after is_preserved in subquery, 
--								changed to inner join to ignore tables with incorrect job-divided settings 
--								corrected insert column name order - moved is_preserved to correct position 
--								error 901 to 904 - added check for tables ending in underscore 
--								error 903 - added is_jobdivided column 
--								error 904 - added on_demand column and fixed is_jobdivided column, fixed case statement on inner join - missing_1 
-- 12/02/2008(2) Jack Richter	error 904 - corrected double semicolon.  Updated comments on logic 
-- 12/03/2008	Jack Richter	error 101 - added column 4 to group by 
--								error 54 - changed tablename to table_name in exclusion list 
--								error 4 - fixed exclude of ('%_') to be ('%/_') 
-- 12/04/2008	Jack Richter	modified all exclusion lists - were excluding valid tables. 
-- 12/05/2008	Jack Richter 	error 54 - change table_name in the insert colum777ns  to tablename 
--								error 104 - changed join to HCCLSC_RESC.CLARITY_TBL to use same logic as previously modified logic in other queries. 
-- 01/05/2009	Jack Richter	moved gather stats from start of script to occur after each major section - to keep stats fresh. 
--								error 3,4,5-  changed to use minus and corrected alias error in 3 
--								error 102 & 104 - added comment ...cannot eliminate false-positives until Compass/_S job division issues resolved. 
--								changed to return minimum false-postives 
--								error 902 & 903 - changed column populated from tablename to stg_table 
-- 01/06/2009	Jack Richter	error 202 & 301 - changed to use minus to reduce runtime.  Tested with multi-deployment 
-- 01/07/2009	Jack Richter	error 103 - changed logic to eliminate tables not in _T and handle job division 
--								error 205 & 206 reunumbers to 20 and 21 and moved to first section 
-- 01/16/2009	Jack Richter	error 1-40 - removed unnecessary upper/TRIM - now done as part of ushare table load 
-- 01/27/2009	Jack Richter	Error 53 - fixed t1.tablename in select to t1.table_name 
--								error 104 - fixed typo ustg.staging_tbl chgd to stg.staging_tbl, 
--								all - added check for work tables XXX_ and BAK_ 
-- 01/28/2009	Jack Richter	fixed typo ANDF chgd to AND, .tablensme chgd to .tablename 
-- 02/16/2009	Jack Richter	error 454,202,301 - added chk for databasename is not null 
--								error 301 - corrected variable from USER_VIEW to EPIC_VIEWS 
-- 03/10/2009	Jack Richter	added error -99 for checks of ushare upgrade table count checks 
-- 03/19/2009	Jack Richter	added error 000 to store validation run parameters 
-- 03/20/2009	Jack Richter	errors 901 to 903 - added creator name and createtime to error message info 
--								errors 0 to -99 now writes user view value to dbname field 
--								chgd table exclusion lists to user NOT LIKE ALL vs multiple NOT LIKE stmts (shrunk script by 50%) 
--								error 105 - moved exclusion of work tables into inner query and added subquery...runtime signficantly reduced 
--								error 3 - chgd to use ushare.upgrade_columns instead of ushare.upgrade_tables 
-- 03/23/2009	Jack Richter	errors 200-399 - reworked...were not checking all correct conditions. 
-- 03/25/2009	Jack Richter	added select at the end to get dbchanges for Clarity Leads 
-- 04/09/2009	Jack Richter	error 201-205 - finished rework and tuning 
-- 04/10/2009	Jack Richter	Updated assumptions to reflect structure of databases in multi-deployment 
--								errors 1-305 reworked for revised assumptions and to fix errors. 
-- 04/13/2009	Jack Richter	fixed typos found in testing 
--								changed delete at start of script to only delete records with the same runname as specified. 
--								error 306 - new error check added 
--								error 306- 999 reworked for revised assumptions and to fix errors. 
--								added duration by error to runtimes reported at end of script 
--								added msg to screen as each error check starts. 
--								dropped error 205 - not a reasonable check Epic views will not match views in USHARE 
--								error 904 - chged to return all staging tables for table with incorrect job division so resolution could be determined 
-- 04/21/2009	Jack Richter	error 103 - removed extra tb1 on end of table name - added missing alias t1 
--								error 104 - chgd stg_tbl to stg_table in insert column list 
--								error 105 - chgd a.stg_db to stg_db in insert column list 
-- 04/23/2009	Jack Richter	error summary - chngd to handle restarts for calculating error runtimes. 
--								chgd to use only one staging database so no quotes needed on any paramter. 
--								script can now only handle one ROC database or one Deployment per run. 
-- 05/04/2009	Jack Richter	error 53 - removed check of count in HCCLSC_RESC.CLARITY_TBL and removed unnecessary upper/trim commands 
--									modified for perf improvments (3:28 to 1:30 in testing) 
--								error 54 - added check of count in HCCLSC_RESC.CLARITY_TBL 
--								error 103 - chgd MINUS to NOT EXISTS to improve response time (4:25 to 0:25 in testing) 
--								error 104 - reworked from/where clause for performance and to handle changes in Compass  
--								errors 101 thru 108, 901,902 - correct join to HCCLSC_RESC.CLARITY_TBL for changes in Compass 
-- 05/05/2009	Jack Richter	errors 105-108 - corrected join to t3 to use tbl.table_name from t2.tablename 
-- 05/06/2009	Jack Richter	error 52 - fixed so only checks if table exists in database and uses not exists instead of minus for performance 
--								error 101 - added join to clarity tbl to get on_demand, is_preserved and job_divided settings 
--								all errors - chgd from handling 10 way job division to handling up to 20 way job division 
-- 06/02/2009	Jack Richter	error 52 - fixed errors in sql. 
-- 06/08/2009	Jack Richter	error 302 - added exclusion of derived tables 
--								error 304 - added exclusion of derived columns 
--								error 904 - added info on job division to error message 
--								added stg_db to select of error detail 
-- 06/10/2009	Jack Richter	error 53  - chgd text HCCLSC_RESC.CLARITY_COL to : COL_TBL 
--								error 101 - chgd alias from CT to CTB - ct is a reserved word 
-- 06/15/2009	Jack Richter		added   to all comment lines to fix column name issue  
--								fixed build of UPGRADE_STGTBLINFO - was not catching tables with job-divided = n but were job divided 
--								multiple - added %/_OLD and %/_OLD2 to list of work tables 
--								multiple - chgd dt9 to dtl in table names - error was causing access_ tables to show as errors. 
--								removed create_dttm from detail report 
--								error 55 & 101 - add issue_comments - from the dictionary to tell you  if the table has been replaced by another table. 
--								error 101 - added is_extracted, data_retained and cm_phy_owner_id columns to data returned  
--								error 104 - chg to only report if _S has an _T table - previously if the _T table was missing all columns for the _S table listed here 
--								error 108 - added error message to start with "Warning only - "  - this is the column formatting error msg 
--								error 903 - moved tablename to stg_table column in upgrade-issues table 
--								error 904 - totally redid error  - new message is "Table has job divided tables but is set to job_divided = N" 
--								error 905 - added error - "Table has job divided tables but base table also found" 
-- 06/16/2009	Jack Richter	removed hardcoded HCCLMA values
--								error 904,905 - added comma after err_msg
--								error 905 - added quotes around : STG_DB
-- 06/20/2009	Jack Richter	Removed duplicate insert command to UPGRADE_STGTBLINFO table
-- 08/28/2009	Jack Richter	Dropped errors 903-905.   Dropped UPGRADE_STGTBLINFO table usage
--								modified for new job-division structures in Summer09
-- 02/23/2010_1	Jack Richter	Added insert statementes after group errors so that all errors show in summary report
--								Changed hardcoded values back to variables to allow use in other regions.
-- 03/27/2010	Jack Richter	Added errors 500-510
--								added info on tables required to use script
-- 03/27/2010 v2				corrected table_comment to issue_comment
--								error 510 - changed outer join to inner join
-- 06/02/2010	Jack Richter	updated and verified tests 1-10 for multi-deployment
-- 06/18/2010	jack Richter	updated and verified tests 100-202 for multi-deployment
--								removed checks for job division - no longer appliciable in sum09 & dropped checks 904 & 905
-- 06/21/2010	Jack Richter	updated tests 202-399 for  multi-deployment
-- 06/22/2010	Jack Richter	updated tests 50-58 for multi-deployment
-- 06/24/2010	Jack Richter	updated tests for 500-999 for multi-deployment
--								added sql for custom reports by error range.
-- 07/02/2010	Jack Richter	Added variable replacement list to comments section
--								corrected log messages to all be consistent on columns populated
--								corrected variable user_views in numerous spots
--								removed variables tab_tbl and col_tbl
--								error 2-9 - corrected errors
--								error 902 - rewrote from scratch
-- 07/05/2010	Jack Richter	added logic for new table UPGRADE_DB_OWNER_LINK
--								added new variable ENV
-- 07/19/2010	Jack Richter	error 53 - added check of phy ownere not equal to 9001
-- 07/20/2010	Jack Richter	removed insert statements at start of each error section.
-- 								error 54 - removed 9001 from subquery on phy owner id
-- 07/23/2010	Ed Kraynak		error 506,509,902,903 - removed hardwired NCAL values and put in variables where needed
--								error 52,101,903 - corrected owner_id to cm_phy_owner_id
-- 07/23/2010	Jack Richter	error 54 - corrected last case statemtn to have from = 'Y' to <> 'Y'
-- 								error 55 - added new warning message
-- 07/27/2010	Jack Richter	error 512 - removed reference to mfst_chgtype and changes select of chg.dbname to c1.databasename
-- 08/05/2010	Ed Kraynak		error 52 - removed extra character at beginning of line
--								error 509 & 903 - added at end of statement
--								error 512 -  reduced GROUP BY by 1.  Last # s/b 12
--								error 902 - added ) at the end.
-- 08/09/2010	Jack Richter	error 3 - changed = dbname to in (rpt_db)
--								error 201,203,204,502,503 - chgd from minus to not exists in subquery to allow return of databasename
--								error 203 - added inner join to remove false positives
--								error 303,304 - chgd inner join & select to return databasename of table/column missing		
-- 08/10/2010	Jack Richter	error 54,201,202,203,204,205,301,302,303,304 - updated/added tables to ignore those that are view only in epic layer		
--								error 55 - changed to ignore table PAT_CVG_BEN_OT
--								error 56-59 - corrected insert column list changed a.cm_phy_owner_id to cm_phy_owner_id
--								error 101 - changed logic to not report missing staging table if table is deprecated, added cm_phy_owner_id to insert
--								error 102 - added erroring_dbname to the insert
--								error 303 - corrected errornumber in check of skip errors and corrected join clauses					
--								error 304 - added databasename to not exists sql statement where clause
-- 08/12/2010	Jack Richter	error 2,3,4,5,6,7,8,9 - corrected physical owner id list in where clauses 
--									 - note: errors 1`-10 must be run for all deployments as well as lead
-- 08/13/2010	Jack Richter	error 54 - changed t1.tablename to t1.table_name
--								error 101 - changed t1.deprecated_yn to tbl.deprecated_yn 
--								error 202 - removed incorrect line
--								error 303 - corrected EXIST to EXISTS
--								error 304 - corrected quotes and = in subqueries to in ('HCCLDSC9A_RESC_T')
--								error 503 - removed extra ) at end of stmt, corrected second not exists clause
--								error 902 - chgd skip error from 903 or 902, corrected insert column list
-- 08/20/2010	Jack Richter	error 5 - changed to not exists and added erroring_dbname to insert
-- 08/23/2010	Jack Richter	error 104 - chenage to exclude work tables and staging only tables.
--								errors 2,6,7,8 - corrected so not dependent on table/column being in dictionary
--								errors 5-9 - changed exclusion list of deletes/update tables
--								error 58 - added error
--								error 52-55, 101-104 - added table ini to returned values
--								error 56,57,105-108 = added table ini and col format ini/item to returned values
--								corrected exclusion lists for update/delete/2011/2011/BKP
-- <version emailed>
-- 08/24/2010	Ed Kraynak		corrected comments for NCAL databases to have correct name.
-- 08/25/2010	Ed Kraynak		error 5 - removed extra FROM and changed table_name to tablename
--								error 101 - removed the letter "A" sticking in the left hand margin., removed left ( prior to TRIM keyword,
-- 								error 102 - corrected erroring_db in insert to erroring_dbname
--								error 103 - added 9 to group by
--								error 104 - changed t1.tablename to t2.tablename, removed left ( prior to TRIM keyword, added 9 in group by
--								error 105 - changed format_in to format_ini
--								error 304 - made EXIST in WHERE statement EXISTS
-- <version emailed>
-- <following fixes from OH/SCAL validations runs>
-- 08/25/2010	Jack Richter	error 54, 58, 201-204, 301-304 - added table V_ROI_STATUS_HISTORY to the exclusion list
-- 09/08/2010	Jack Richter	error 505 - added inner join to DB LINK table to get databasename for columns table join
--								error 9 & 108 - changed from format difference to compressible difference
--								removed collect stats statements
-- 09/15/2010	Ed Kraynak		Error 104 - added ,9 to group by
-- <version emailed>
-- 09/27/2010	Jack Richter	removed references to TP and CRPK validations
-- 09/28/2010	Jack Richter	error 5-9 - added missing ) and fixed missing E in spelling of compressible.
-- 09/29/2010	Ed Kraynak		error 105 replaced T_COMPRESST with T_COMPRESS
--								error 505 changes :UPGRADE to HCCLSC_USHARE
-- <version emailed>
-- 02/14/2011	Jack Richter	chged error completion msg to new format with error count in text
--								error 105-108 - corrected from error not in (0) to error not in (:SKIP_ERRORS)
--								error 105 - added check of skip errors
-- 02/15/2011	Jack Richter	replace UPGR_CPRK with UPGRADE (17 replacements)
--								error 7 - fixed . to , and removed extra )
--								error 105-108 - chged spelling of compressiable to compressible
-- 02/21/2011	Jack Richter	error 50,51 - added checks for staging table between WITS & PROD
-- 02/22/2011	Jack Richter	error 50,51 - corrected physical owner checks
-- 02/23/2011	Jack Richter	error 50 - added staging databasename to the error detail and added join to UPGRADE_DB_OWNER_LINK table to get this.
--								error 51 - changed subquery to use HCCLSC_USHARE.UPGRADE_DB_OWNER_LINK dbol (so it doesn't error all tables in all deployments).
-- 				v2				error 50 - added logic to exclude known table drops per the manifest
--								error 51 - added logic to exclude known table adds per the manifest
-- 02/24/2011	Jack Richter	error 51 - changed order of from clauses to resolve invalid reference error.
--				v2				error 54, 58, 201-204, 301-304 - added table CR_REMAP_CIDS to the exclusion list
--								error 58 - added exclusion for backfill tables like BF%
-- 03/05/2011	Jack Richter	error 55 - added comment about CR_ tables
--								error 501,502,503,504 - added full list of temp/work tables to ignore to where clause
--								error 508,511 - added 'Warning Only' to start of error messages
--								error 2,3,4,5,6-9 - chgd from 'HCCLDSC9A_RESC_T' to HCCLDSC9A_RESC_T
--								error 201-204, 301-304 - removed tables that are unioned all in the Epic layer to a new view name.
--								status msgs - changed to include deployment in the where clause
--								error 512 - corrected space in datatype definition that was causing false positives
--								error 2 - added check of physical owner in error 2
--								error 2,4 - removed 9001 from physical owner list.
--								error 512 - removed space between digits & fractional digits
-- 03/06/2011	Jack Richter	corrected physical owner info for NCAL
-- 03/06/2011	v2				error 52 - removed 9001 from physical owners to check
-- 03/06/2011	Ed Kraynak (v3)	error 5 - chgd "AND" to "WHERE" after line dbc.tables t1
-- 								error 52 - rmvd  "WHERE t.databasename = CASE WHEN t1.cm_phy_owner_id = 9001 THEN t.databasename else coalesce(link.rpt_db,'') END..."	
--									- chgd "AND NOT EXISTS (" to "WHERE NOT EXISTS (" and changed from left outer to inner join
-- 03/12/2011					error 512 - split into 2 errors
--									- chged error message
--									- chged subquery join from inner to outer and added where clauses
--									- added where clause to remove no testing needed items
--								error 513 - created
--								error 509 - added where clause to remove no testing needed items
-- 03/24/2011	Jack Richter	error 501 - removed 9001 from physical owner list.
-- 04/28/2011	Jack Richter 	error 103 - chgd subquery to user stg dbs instead of reporting dbs string
-- 05/02/2011	Jack Richter	--compared to current script from Ed Kraynak and verified all changes accounted for
--								error 300-399 - updated exclusion lists to now take into account _UPGR views
-- 05/02/2011	v2				-- compared to SCAL validation and made the following changes
--								MAJOR REWRITE to incorporate lessons from SCAL IU7+ upgrade
--								errors 50-999 affected
-- 05/12/2011	Jack Richter	Continued Rewrite & Testing
-- 05/25/2011					errors 51, 52 rewritten and renumbered to 109 and 110 respectively
-- 12/14/2011   Tom Tang	Modify error 110, typo "AND t1.tablename not like 'TOKEN%'"
--				Added 86 error for newly added tables
---01/03/2012   Tom Tang	Added 87 error for newly added EPIC views.
-- 02/18/2012	Tom Tang Added update error 80 for null column data type 
-- ----------------------------------------------------------------------------------------- 
-- The following tables must exist and be populated before running this script.  
-- If any of these tables are missing or not correctly propulated, the validation will create invalid errors or just fail to run.
--
--	HCCLSC_USHARE.UPGRADE_COLUMNS			contains the column info from PROD (for wits testing) or from WITS (post upgrade testing)
--	HCCLSC_USHARE.UPGRADE_TABLES			contains the table names from PROD (for wits testing) or from WITS (post upgrade testing)
--	HCCLSC_USHARE.UPGRADE_COLUMNS_VIEWS	contains the view name & view column names from PROD (for wits testing) or from WITS (post upgrade testing)
--	HCCLSC_USHARE.UPGRADE_MANIFEST		contains the table and column change info from the upgrade manifest spreadsheet
-- 	HCCLSC_USHARE.UPGRADE_ISSUES			table should be empty - it will be populated by this script.
-- ----------------------------------------------------------------------------------------- 
-- 
-- To modify the script for a specific region : 
-- ============================================ 
--
-- 1) replace  with a name for this validation run. This will appear as the dbname for the timing error messages. 
--	No quotes!.... ie Hawaii WITS 20090225 
-- 
-- 2) replace HCCLSC_USHARE with ushare database where upgrade_xxx tables exist - only one value may be sepecified. 
--	No quotes!.... ie. HCCLHI_USHARE 
-- 
-- 3) replace 'HCCLDSC9A_RESC_T' with the list of reporting databases to be checked this deployment - multiple values in Califs
--		(note: in the ROCs this should be the same value placed in HCCLDSC9A_RESC_T)
--		(note: in califs this should include the  materialized view reporting databases
--	Include quotes!.... ie. 
--					Scal Wits - 'HCCLDSC9A_RESC_T','HCCLDSC9KP_RESC_T'
--					Scal Prod - 'HCCLPSCA_T', 'HCCLPSCKP_T'
--					Ncal Wits - 'HCCLDNC9A_T', 'HCCLDNC9KP_T'
--					Ncal Prod - 'HCCLPNC2A_T','HCCLPNCKP_T'
--					region - 'HCCLDHI9_T'
--
-- 5) replace 'HCCLDSC9A_RESC_S' with the list of staging databases to be checked this deployment - multiple values in Califs
--		(note: in the ROCs this should be the same value placed in :DPLY)
--		(note: in califs this should include materialized view reporting databases
--	Include quotes!.... ie. 
--					Scal - 'HCCLDSC9A_RESC_S','HCCLDSC9KP_RESC_S'
--					Ncal - 'HCCLDNC9A_S', 'HCCLDNC9KP_S'
--					region - 'HCCLDHI9_S'
--		
-- 6) replace HCCLDSC9_RESC with the database layer where the Epic views reside - only one value may be sepecified. 
--	No quotes!.... ie HCCLPHI 
-- 
-- 7) replace HCCLSC_RESC with the database layer where the user views reside - only one value may be sepecified. 
--	No quotes!.... ie HCCLHI 
-- 
-- 8) replace HCCLDSC9A_RESC_T with the name of the or deployment reporting table database this validation is for.  
--	No quotes!.... ie HCCLPHI_T,  HCCLPSC9A_RESC_T, HCCLDNC9A_T, etc..
--

-- 9) replace 121212 with the physical owner of the deployment
--	No quotes! ... ie 120312
--		CO	120140
--		GA	120200
--		HI	120130
--		MA	120170
--		NW	120190
--		OH	120180
--		NC	120160 (LEAD AND MATERIALIZED VIEWS - do not use)
--		NCA	121312
--		NCB	121320
--		NCC	121318
--		NCD	121314
--		NCE	121316
--		NCF	121322
--		SC	120150	(lEAD AND MATERIALIZED VIEWS - do not use)
--		SCA	121212
--		SCB	121214
--		SCC	121216
--		SCD	121218
--		SCE	121220
--		SCF	121222
--
-- 10) replace WITS with environment you are validating.  Valid values are WITS or PROD.
--	No quotes! ... ie WITS -or- PROD

-- 11)replace 0 with the error numbers to skip.  you must at least skip error 0. 
--	No Quotes!.... ie 0,901,902,903,904 
--
-- 12)replace 'HCCLDSC9_RESC_T' - Lead (in Califs) or regional reporting database name (in ROCs)
--	Include quotes!.... ie. 
--					Scal - 'HCCLDSC9_RESC_T'
--					Ncal - 'HCCLDNC9_T',
--					ROC  - 'HCCLDHI_T'
--
-- 13)replace 'HCCLDSC9_RESC_S' - Lead (in Califs) or regional staging database name (in ROCs)
--	Include quotes!.... ie. 
--					Scal - 'HCCLDSC9_RESC_ST'
--					Ncal - 'HCCLDNC9_S',
--					ROC  - 'HCCLDHI_S'
-- ------------------------------------------------------------------------------------------ 
-- Assumptions: 
-- ============ 
-- 1) Only one HCCLxx User view layer exists in a region 
-- 2) Only one HCCLPxx Epic view layer exists in a region 
-- 3) Only one HCCLPxx_T Reporting tables layer exists in a region 
-- 4) Only one HCCLPxx_S Staging   tables layer exists in a region 
-- 5) At the start of the script the UPGRADE_ISSUES table will have all rows for the same run-name deleted 
-- 6) This script is not designed for Clarity Business users to run and should only be run with agreement from CTC CLARITY Upgrade support teams. 
-- ---------------------------------------------------------------------------------------- 
-- SCAL and NCAL DATABASE STRUCTURES 
-- ================================== 
-- KP VIEW LAYER						  HCCLSC 
-- EPIC VIEW LAYER 		HCCLPSC[A-F] 	& HCCLPSC 
-- RPT TABLE LAYER		HCCLPSC[A-F]_T 	& HCCLPSC_T 
-- STAGING TABLE LAYER	HCCLPSC[A-F]_S	& HCCLPSC_S 
-- ACCESS LOG EXTRACT	HCCLPSCCC[A-F]_S  
-- MATERIALIZED VIEWS	HCCLPSCKP_T 
-- ----------------------------------------------------------------------------------------- 
-- Errors/Checks performed: 
-- ======================== 
-- --------------------------------------- 
-- 000			Run Parameters used 
-- -01 - -99 - 	Validation Run Statistical Information 
-- 001 - 049 - 	Reporting table checks against USHARE 
-- 050 - 099 - 	Reporting (_T) tables checks against Clarity Compass 
-- 100 - 199 - 	Reporting (_T) tables compared to ETL Staging (_S) tables 
-- 200 - 299 - 	HCCLPxx (EPic) view comparison to USHARE and _T 
-- 300 - 399 - 	HCCLxx  (KP)   view comparison to USHARE and HCCLPxx and _T 
-- 500 - 599 - 	Reporting (_T) tables and HCCLxx viewss compared to the Upgrade Manifest
-- 900 - 999 -  Work/Temp Tables
-- --------------------------------------- 
-- Detail by Error message: 
-- --------------------------------------- 
-- Validation Run Statistical Info 
-- --------------------------------------- 
--  0			Run Parameters 
--	-1 to -90 	Runtimes per error 
-- 	-99			Necessary USHARE tables are empty 
-- --------------------------------------- 
-- Base Tables & Views to USHARE Checks 
-- Note: When running in WITS, USHARE contains the structures as they exist PROD. 
--	 	 When running in PROD, USHARE contains the structures as they exist WITS. 
-- --------------------------------------- 
-- 2. 	Column in USHARE but not in _T database 	(column drop) 
-- 3. 	Column in _T but not in USHARE list	 		(column add) 
-- 4. 	Table in USHARE list but not in _T table 	(table drop) 
-- 5. 	Table in _T but not in USHARE list 			(table add) 
-- 6.	HCCL - USHARE - Column Datatype difference. (column modify - datatype)  
-- 7.	HCCL - USHARE - Column Size difference. 	(column modify - size)  
-- 8.	HCCL - USHARE - Column nullable difference. (column modify - nullability)  
-- 9.	HCCL - USHARE - Column compressible difference. 	(column modify - compressible)
-- ----------------------------------------------------------  
-- Checks for MA DUMMY tables - only appear in MAS version  
-- ----------------------------------------------------------  
-- 32. 	Column in USHARE but not in _T database  
-- 33. 	Column in _T but not in USHARE list  
-- 34. 	Table in USHARE list but not in _T table  
-- 35. 	Table in _T but not in USHARE list  
-- 36.	HCCL - USHARE - Column Datatype difference.  
-- 37.	HCCL - USHARE - Column Size difference.  
-- 38.	HCCL - USHARE - Column nullable difference.  
-- 39.	HCCL - USHARE - Column format difference.  
-- ---------------------------------------  
-- Base Tables to COMPASS Checks  
-- ---------------------------------------  
-- 50.	Staging Table in USHARE list but not in staging database (missing table)
-- 51.	Table in staging database but not in USHARE staging list (extra table)
-- 52. 	Column in COMPASS but not in _T database  
-- 53. 	Column in _T but not in COMPASS  
-- 54. 	Table in COMPASS but not in _T  
-- 55. 	Table in _T but not in COMPASS  
-- 56.	HCCL - COMPASS - Column Datatype difference.  
-- 57.	HCCL - COMPASS - Column Size difference.  
-- 58.	Table in COMPASS but not in staging databases
-- -------------------------------------------- 
-- Base Tables to Upgrade Manifest Checks
-- --------------------------------------------
-- 75. Column Drop in database but not in Manifest
-- 76. Column Add in database but not in Manifest
-- 77. Table Drop in database but not in Manifest
-- 78. Table Add in database but not in Manifest
-- 79. Column Modify in database but not in Manifest
-- 80. Table Add in manifest but table not in database
-- 81. Table Dropped in manifest but table still in database
-- 82. Column Add in manifest but column not in database
-- 83. Column Drop in manifest but column still in database
-- 84. Column Modify in manifest but not in database
-- 86. Table add in manifest, but not in database
-- 87. EPIC view add in manifest, but not in epic view layer
-- 88. Column modify in manifest, but table not existing in database
-- ---------------------------------------  
-- Staging Tables (_S) Checks  
-- ---------------------------------------  
-- 101. _T Reporting table found but no _S Staging table exists  
-- 102.	_S Staging table found but no _T table exists  
-- 103.	_T Reporting table has column not found in _S table  
-- 104.	_S Staging table has column not found in _T table  
-- 105.	_S to _T - Column Datatype difference.  
-- 106.	_S to _T - Column Size difference.  
-- 107.	_S to _T - Column nullable difference.  
-- 108.	_S to _T - Column format difference.  
-- ---------------------------------------  
-- EPIC View layer (HCCLPxx) Checks  
-- ---------------------------------------  
-- 201.	Table does not appear to have a matching HCCLPxx view.  
-- 202.	HCCLPxx view does not appear to have a matching _T table.  
-- 203.	Column in base table but not in HCCLPxx view.  
-- 204.	Column in HCCLPxx view but not in base table.  
-- ---------------------------------------  
-- KP (USER) View layer (HCCLxx) Checks  
-- ---------------------------------------  
-- 301.	Table does not appear to have a matching HCCLxx view.  
-- 302.	HCCLxx view does not appear to have a matching _T table. 
-- 303.	Column in base table but not in HCCLxx view. 
-- 304.	Column in HCCLxx view but not in base table.			(derived column added?) 
-- 305.	Column in USHARE list for view but not in HCCLxx View. 	(derived column dropped) 
-- 306. Column in HCCLxx view but not in USHARE list for View. 	(derived column added) 
-- --------------------------------------- 
-- All layers 
-- --------------------------------------- 
-- 901.	Work-Temp table found in _T 
-- 902.	Work-Temp table found in _S 
-- 903.	Work-Temp table found in HCCLPxx 
-- ----------------------------------------------------------------------------------------- 

-- 	 'MY_ENV' Variable Replacement List
--	==========================
--	MY_RUNNAME runname,
--  MY_USHAREDB as ushare_db
--  MY_ENV env,
--  MY_USERDB user_db,
--  MY_EPICDB epic_db,
--  'MY_REPORT_DB','MY_MATVIEW_DB' rpt_dbs,
--  MY_REPORT_DB,MY_LEAD_REPORTDB rpt_dbs_withoutquotes,
--  'MY_STAGE_DB' stg_dbs,
--  'MY_LEAD_REPORTDB' as lead_rpt_db,
--  'MY_LEAD_STAGEDB' as lead_stgt_db,
--  'MY_DEPLOY_NAME' dply_name,
--   MY_OWNER_ID phy_owner,
--   0' skip_errs;
--
--
--  MY_RUNNAME  ---
--  MY_USHAREDB ---
--  MY_ENV      ---
--  MY_USERDB   ---
--  MY_EPICDB ---
--  MY_REPORT_DB ---
--  MY_MATVIEW_DB --
--  MY_STAGE_DB  ---
--  MY_LEAD_REPORTDB ---
--  MY_LEAD_STAGEDB  ---
--  MY_DEPLOY_NAME  ---
--  MY_OWNER_ID  ---
-- -----------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------- 
DROP TABLE MY_USHAREDB.UPGRADE_DBC_TABLES;
CREATE SET TABLE MY_USHAREDB.UPGRADE_DBC_TABLES,
	NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT
 (	databasename				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	tablename			VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	TableKind CHAR(1) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC NOT NULL,
	CreatorName CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	CreateTimeStamp TIMESTAMP(0))
PRIMARY INDEX (databasename, tablename);

INSERT INTO MY_USHAREDB.UPGRADE_DBC_TABLES
SELECT databasename, tablename, tablekind,CreatorName,CreateTimeStamp
from dbc.tables t2
where t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB','MY_STAGE_DB','MY_USERDB',
				'MY_EPICDB','MY_LEAD_REPORTDB','MY_LEAD_STAGEDB')
;
	
DROP TABLE MY_USHAREDB.UPGRADE_DBC_COLUMNS;	
CREATE SET TABLE MY_USHAREDB.UPGRADE_DBC_COLUMNS,
	     NO BEFORE JOURNAL,
	     NO AFTER JOURNAL,
	     CHECKSUM = DEFAULT
	     (
	    DatabaseName CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      TableName CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      ColumnName CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      ColumnFormat CHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ColumnTitle VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SPParameterType CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      ColumnType CHAR(2) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      ColumnUDTName CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      ColumnLength INTEGER,
      DefaultValue VARCHAR(1024) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Nullable CHAR(1) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      CommentString VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DecimalTotalDigits SMALLINT,
      DecimalFractionalDigits SMALLINT,
      ColumnId SMALLINT,
      UpperCaseFlag CHAR(1) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      Compressible CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      CompressValue INTEGER,
      ColumnConstraint VARCHAR(8192) CHARACTER SET LATIN NOT CASESPECIFIC,
      ConstraintCount SMALLINT,
      CreatorName CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      CreateTimeStamp TIMESTAMP(0),
      LastAlterName CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      LastAlterTimeStamp TIMESTAMP(0),
      CharType SMALLINT,
      IdColType CHAR(2) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      AccessCount INTEGER,
      LastAccessTimeStamp TIMESTAMP(0),
      CompressValueList VARCHAR(8192) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TimeDimension CHAR(1) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      VTCheckType CHAR(1) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      TTCheckType CHAR(1) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      ConstraintId BYTE(4),
      ArrayColNumberOfDimensions BYTEINT,
      ArrayColScope VARCHAR(3200) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      ArrayColElementType CHAR(2) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      ArrayColElementUdtName CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC)
	PRIMARY INDEX ( DatabaseName, TableName, ColumnName );	
	
INSERT INTO MY_USHAREDB.UPGRADE_DBC_COLUMNS
SELECT *
from DBC.COLUMNS t2
where t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB','MY_STAGE_DB','MY_USERDB',
				'MY_EPICDB','MY_LEAD_REPORTDB','MY_LEAD_STAGEDB')
;



COLLECT STATISTICS MY_USHAREDB.upgrade_columns COLUMN TABLENAME;
COLLECT STATISTICS MY_USHAREDB.upgrade_columns COLUMN (TABLENAME,COLUMNNAME,CM_PHY_OWNER_ID);
COLLECT STATISTICS MY_USHAREDB.upgrade_columns COLUMN (TABLENAME,COLUMNNAME);
COLLECT STATISTICS MY_USHAREDB.upgrade_columns COLUMN (COLUMNNAME,CM_PHY_OWNER_ID);
COLLECT STATISTICS MY_USHAREDB.upgrade_columns COLUMN (TABLENAME,CM_PHY_OWNER_ID);
COLLECT STATISTICS MY_USHAREDB.upgrade_columns COLUMN (DBNAME,TABLENAME);
COLLECT STATISTICS MY_USHAREDB.upgrade_columns COLUMN COLUMNNAME;
COLLECT STATISTICS MY_USHAREDB.upgrade_columns COLUMN CM_PHY_OWNER_ID;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_TABLES COLUMN DATABASENAME;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_TABLES COLUMN TABLEKIND;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_TABLES COLUMN TABLENAME;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_TABLES COLUMN (DATABASENAME ,TABLENAME);
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_TABLES COLUMN (CREATORNAME);
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_COLUMNS COLUMN (DATABASENAME ,TABLENAME);
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_COLUMNS COLUMN (TABLENAME ,COLUMNNAME);
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_COLUMNS COLUMN COLUMNNAME;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DBC_COLUMNS COLUMN TABLENAME;

COLLECT STATISTICS MY_USHAREDB.UPGRADE_MANIFEST_LOAD COLUMN TABLENAME;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_ISSUES COLUMN ERR_NO;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_ISSUES COLUMN (ERR_NO,DBNAME);
COLLECT STATISTICS MY_USHAREDB.UPGRADE_ISSUES COLUMN DBNAME;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_TABLES COLUMN CM_PHY_OWNER_ID;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_TABLES COLUMN TABLENAME;

COLLECT STATISTICS MY_USHAREDB.UPGRADE_MANIFEST_LOAD COLUMN CHG_TYPE;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_MANIFEST_LOAD COLUMN (TABLENAME,COLUMNNAME ,NEW_DATATYPE ,TESTING_RQRD);
COLLECT STATISTICS MY_USHAREDB.UPGRADE_MANIFEST_LOAD COLUMN NEW_DATATYPE;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_MANIFEST_LOAD COLUMN COLUMNNAME;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_MANIFEST_LOAD COLUMN (TABLENAME,COLUMNNAME);
COLLECT STATISTICS MY_USHAREDB.UPGRADE_MANIFEST_LOAD COLUMN TESTING_RQRD;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DB_OWNER_LINK COLUMN ENV;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DB_OWNER_LINK COLUMN STG_DB;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DB_OWNER_LINK COLUMN RPT_DB;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_DB_OWNER_LINK COLUMN (CM_PHY_OWNER_ID,ENV);
COLLECT STATISTICS MY_USHAREDB.UPGRADE_MANIFEST COLUMN CHG_TYPE;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_STG_TBLS COLUMN TABLENAME;
COLLECT STATISTICS MY_USHAREDB.UPGRADE_STG_TBLS COLUMN CM_PHY_OWNER_ID;
COLLECT STATISTICS MY_USHAREDB.upgrade_columns_views COLUMN TABLENAME;
COLLECT STATISTICS MY_USHAREDB.upgrade_columns_views COLUMN COLUMNNAME;

COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN TABLE_ID;
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN (COLUMN_NAME,TABLE_ID,IS_EXTRACTED_YN);
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN (TABLE_ID,IS_EXTRACTED_YN);
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN (TABLE_ID,CM_PHY_OWNER_ID);
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN (COL_DESCRIPTOR,CM_PHY_OWNER_ID);
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN (TABLE_ID,IS_EXTRACTED_YN,CM_PHY_OWNER_ID);
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN (COLUMN_NAME,TABLE_ID,CM_PHY_OWNER_ID);
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN CM_PHY_OWNER_ID;
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN IS_EXTRACTED_YN;
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_TBL COLUMN IS_EXTRACTED_YN;
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_TBL COLUMN CHRONICLES_MF;
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_TBL COLUMN (TABLE_NAME,EXTRACT_FILENAME,DEPENDENT_INI ,CHRONICLES_MF);
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_TBL COLUMN (TABLE_NAME,CM_PHY_OWNER_ID);
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_TBL COLUMN CM_PHY_OWNER_ID;
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN COLUMN_NAME;
COLLECT STATISTICS MY_LEAD_REPORTDB.CLARITY_COL COLUMN COL_DESCRIPTOR;


-- ********************************** Start of Validation - Parameter and reference data check  ********************************************** 
-- DIAGNOSTIC HELPSTATS ON FOR SESSION;

-- Report out the replacement variable settings used:

select 	'MY_RUNNAME' as runname,
		'MY_ENV' as env,
		'MY_USHAREDB' as ushare_db, 
		'MY_USERDB' as user_db,
		'MY_EPICDB' as epic_db;
select		
		'MY_REPORT_DB' || ',' || 'MY_MATVIEW_DB' as rpt_dbs,
		'MY_STAGE_DB' as stg_dbs,
		'MY_LEAD_REPORTDB' as lead_rpt_db,
		'MY_LEAD_STAGEDB' as lead_stgt_db;
select
		'MY_DEPLOY_NAME' as dply_name,
		'MY_OWNER_ID' as phy_owner,
		'0' as skip_errs;

DELETE FROM MY_USHAREDB.upgrade_issues 
-----WHERE dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
;

-- --------------------------------------------- 
-- write load parameters to the validation table 
-- ---------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, dbname, runname, create_dttm) VALUES 
(0, '2. Parameters - deployment name = MY_REPORT_DB', 'MY_REPORT_DB', 'MY_RUNNAME', CURRENT_TIMESTAMP(0));
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, dbname, runname, create_dttm) VALUES 
(0, '3. Parameters - epic_views  = MY_EPICDB', 'MY_REPORT_DB','MY_RUNNAME', CURRENT_TIMESTAMP(0));
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, dbname, runname, create_dttm) VALUES 
(0, '4. Parameters - MY_USERDB.CLARITY_TBL = MY_USERDB.CLARITY_TBL', 'MY_REPORT_DB','MY_RUNNAME', CURRENT_TIMESTAMP(0));
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, dbname, runname, create_dttm) VALUES 
(0, '5. Parameters - clarity_col = MY_USERDB.CLARITY_COL', 'MY_REPORT_DB','MY_RUNNAME', CURRENT_TIMESTAMP(0));

-- ------------------------------------------------ 
-- verify if necessary USHARE tables are populated 
-- ------------------------------------------------ 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, dbname, runname, create_dttm) 
SELECT -99,'UPGRADE_COLUMNS table is empty', 'MY_REPORT_DB','MY_RUNNAME', CURRENT_TIMESTAMP(0)
FROM 	(SELECT COUNT(*) recs FROM MY_USHAREDB.upgrade_columns) AS b
WHERE 	b.recs = 0;

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, dbname, runname, create_dttm) 
SELECT -99,'UPGRADE_TABLES table is empty', 'MY_REPORT_DB','MY_RUNNAME', CURRENT_TIMESTAMP(0)
FROM 	(SELECT COUNT(*) recs FROM MY_USHAREDB.upgrade_tables) AS b
WHERE 	b.recs = 0;

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, dbname, runname, create_dttm) 
SELECT -99,'UPGRADE_COLUMNS_VIEWS table is empty', 'MY_REPORT_DB','MY_RUNNAME', CURRENT_TIMESTAMP(0)
FROM 	(SELECT COUNT(*) recs FROM MY_USHAREDB.upgrade_columns_views) AS b
WHERE 	b.recs = 0;

-- ********************************** Start of Reporting (_T) Table Validation ********************************************** 
-- ************************************************************************************************************************** 
-- Run this section after the tables have been built by the DBAs and after the DBAs fix any table errors 
-- ************************************************************************************************************************** 

-- --------------------------------------------------------- 
-- Check for columns missing from HCCL database 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT 	2 ,	'Column in USHARE table but not in reporting (_T) database' msg, 'MY_RUNNAME',
		'MY_REPORT_DB', c1.dbname, c1.tablename, c1.columnname
FROM 	MY_USHAREDB.upgrade_columns c1
		INNER JOIN MY_USHAREDB.UPGRADE_DBC_TABLES t2
		ON c1.tablename = t2.tablename
		AND t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
		AND t2.tablekind in ('T','V')
		AND c1.cm_phy_owner_id IN ('MY_OWNER_ID') 
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
		AND	2 NOT IN (0)
		-- ---------------------------- 
		-- exclude test and work tables 
		-- ---------------------------- 
		AND	c1.tablename NOT LIKE 'BF%'
WHERE	NOT EXISTS (
			SELECT 1
			FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
			WHERE 	c2.tablename = t2.tablename
			AND		c2.columnname = c1.columnname
			AND 	c2.databasename = t2.databasename)
GROUP BY 1,2,3,4,5,6,7;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-2, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM 	MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no = 2;
	
-- --------------------------------------------------------- 
-- Check for extra columns in HCCL database 
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT 	3,'Column in reporting database but not in USHARE list' msg,
		'MY_RUNNAME', 'MY_REPORT_DB', c1.databasename, t1.tablename, UPPER(TRIM(c1.columnname)) colname
FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c1
		-- ------------------------------------------------ 
		-- only check tables if in USHARE list and database 
		-- ------------------------------------------------ 
		INNER JOIN (
			SELECT dbname, tablename 
			FROM MY_USHAREDB.upgrade_columns 
			WHERE cm_phy_owner_id IN ('MY_OWNER_ID', '9001') 
			GROUP BY 1,2) AS t1
		ON UPPER(TRIM(c1.tablename)) = t1.tablename
		AND UPPER(TRIM(c1.databasename)) in ('MY_REPORT_DB','MY_MATVIEW_DB')
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
		AND 3 NOT IN (0)		
		-- ------------------------------------------------ 
		-- check tbl/col not in ushare list
		-- ------------------------------------------------ 		
		AND NOT EXISTS (
			SELECT 	1
			FROM 	MY_USHAREDB.upgrade_columns c3
			WHERE 	c3.tablename = c1.tablename
			AND		c3.columnname = c1.columnname
			AND 	c3.cm_phy_owner_id IN ('MY_OWNER_ID', '9001') );

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-3, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  3;

-- --------------------------------------------------------- 
-- Check for tables that all expected tables are built 
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, tablename)
SELECT 	4, 'Table in USHARE list but not in reporting database' msg, 'MY_RUNNAME', 'MY_REPORT_DB', t1.tablename
FROM	MY_USHAREDB.UPGRADE_TABLES t1
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
WHERE	4 NOT IN (0)
AND		t1.tablename NOT LIKE ALL ('UTL%','UPD%')
AND		t1.cm_phy_owner_id IN ('MY_OWNER_ID') 
MINUS
SELECT 	4, 'Table in USHARE list but not in reporting database' msg, 'MY_RUNNAME', 'MY_REPORT_DB', t2.tablename
FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t2
WHERE 	UPPER(TRIM(t2.databasename)) in ('MY_REPORT_DB','MY_MATVIEW_DB')
AND 	t2.tablekind IN ('V','T');

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-4, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB','MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  4;

-- --------------------------------------------------------- 
-- Check for tables that exist and should not 
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename)
SELECT 5, 'Table in reporting database but not in USHARE list' msg,	'MY_RUNNAME', 'MY_REPORT_DB', t1.databasename, UPPER(TRIM(t1.tablename))
FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t1
WHERE	UPPER(TRIM(t1.databasename)) in ('MY_REPORT_DB','MY_MATVIEW_DB')
AND		t1.tablekind IN ('T','V')
		------------------------------------------------------
		-- ignore the crpk backup tables from the last upgrade
		-- ---------------------------------------------------
AND		t1.tablename not like 'CRPK%'  
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
AND		5 NOT IN (0)
AND	NOT EXISTS
		(SELECT 1 
		FROM	MY_USHAREDB.UPGRADE_TABLES t2
		WHERE	t2.cm_phy_owner_id IN ('MY_OWNER_ID', '9001')
		AND		t2.tablename = t1.tablename);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-5, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  5;

-- --------------------------------------------------------- 
-- Check all PROD columns definitions match MY_ENV. 
-- --------------------------------------------------------- 
-- Note: not all tables have a view with the same name. 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues
	(err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname, ushare_or_cmps_or_S_def, hccl_or_T_def, ushare_or_cmps_or_S_cmprs,
	hccl_or_T_cmprs, is_preserved, on_demand)
SELECT  a.err_no, a.change_type AS error_msg, 'MY_RUNNAME', 'MY_REPORT_DB', a.databasename, a.tablename, a.columnname, a.ushare_def, a.hccl_def,
		a.ushare_compressible,a.hccl_compressible, a.is_preserved, on_demand
FROM (SELECT
		t2.databasename,
		c1.tablename,
		c1.columnname,
		CASE WHEN col.is_preserved_yn = 'Y' OR tbl.is_preserved_yn = 'Y' THEN 'Y' ELSE NULL END AS is_preserved,
		CASE WHEN tbl.load_frequency = 'ON DEMAND' THEN 'Y' ELSE NULL END AS on_demand,
		"USHARE_DTYPE"||' '||"USHARE_SIZE"||' '||"USHARE_NULLABLE" AS "USHARE_DEF",
		"HCCL_DTYPE"||' '||"HCCL_SIZE"||' '||"HCCL_NULLABLE" AS "HCCL_DEF",
		CASE WHEN c1.columntype = 'CV' THEN 'VARCHAR'
			WHEN c1.columntype = 'CF'  THEN 'VARCHAR'
			WHEN c1.columntype = 'CO' THEN 'VARCHAR'
			WHEN c1.columntype = 'I' THEN 'INTEGER'
			WHEN c1.columntype = 'D' THEN 'DECIMAL'
			WHEN c1.columntype = 'DA' THEN 'DATE'
			WHEN c1.columntype = 'F' THEN 'FLOAT'
			WHEN c1.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c1.columntype,'')
		END AS "USHARE_DTYPE",
		CASE WHEN c1.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c1.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c1.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c1.columntype = 'D' THEN '('||TRIM(c1.decimaltotaldigits (FORMAT 'zzzzzzz'))||', '||TRIM(c1.decimalfractionaldigits)||')'
			ELSE COALESCE(c1.columntype,'')
		END AS "USHARE_SIZE",
		CASE WHEN c1.nullable = 'Y' THEN 'NULL' ELSE 'NOT NULL' END AS "USHARE_NULLABLE",
		TRIM(c1.compressible) AS "USHARE_COMPRESSIBLE",
		CASE WHEN c2.columntype = 'CV' THEN 'VARCHAR'
			WHEN c2.columntype = 'CF'    THEN 'VARCHAR'
			WHEN c2.columntype = 'CO' THEN 'VARCHAR'
			WHEN c2.columntype = 'I' THEN 'INTEGER'
			WHEN c2.columntype = 'D' THEN 'DECIMAL'
			WHEN c2.columntype = 'DA' THEN 'DATE'
			WHEN c2.columntype = 'F' THEN 'FLOAT'
			WHEN c2.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c2.columntype,'')
		END AS "HCCL_DTYPE",
		CASE WHEN c2.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c2.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c2.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c2.columntype = 'D' THEN '('||TRIM(c2.decimaltotaldigits (FORMAT 'zzzzzzz'))||', '||TRIM(c2.decimalfractionaldigits)||')'
			ELSE COALESCE(c2.columntype, '')
		END AS "HCCL_SIZE",
		CASE WHEN c2.nullable = 'Y' THEN 'NULL' ELSE 'NOT NULL' END  AS "HCCL_NULLABLE",
		TRIM(c2.compressible) AS "HCCL_COMPRESSIBLE",
		CASE WHEN "USHARE_DTYPE" <> "HCCL_DTYPE" THEN 'HCCL-USHARE Column Datatype difference.'
			WHEN "USHARE_SIZE" <> "HCCL_SIZE" THEN 'HCCL-USHARE Column Size difference.'
			WHEN "USHARE_NULLABLE" <> "HCCL_NULLABLE" THEN 'HCCL-USHARE Column nullable difference.'
			WHEN "USHARE_COMPRESSIBLE" <> "HCCL_COMPRESSIBLE" THEN 'HCCL-USHARE Column compressible difference.'
			ELSE NULL
		END AS "CHANGE_TYPE",
		CASE WHEN "USHARE_DTYPE" <> "HCCL_DTYPE" THEN 6
			WHEN "USHARE_SIZE" <> "HCCL_SIZE" THEN 7
			WHEN "USHARE_NULLABLE" <> "HCCL_NULLABLE" THEN 8
			WHEN "USHARE_COMPRESSIBLE" <> "HCCL_COMPRESSIBLE" THEN 9
			ELSE NULL
		END AS "ERR_NO"
    FROM 	MY_USHAREDB.upgrade_columns c1
			-- --------------------------------------------------------------------- 
			-- join to clarity compass tables to get preserved and on-demand flags 
			-- --------------------------------------------------------------------- 
			LEFT OUTER JOIN MY_USERDB.CLARITY_TBL tbl
				ON UPPER(TRIM(tbl.table_name)) = c1.tablename
				AND tbl.cm_phy_owner_id = 'MY_OWNER_ID'
				AND tbl.is_extracted_yn = 'Y'
				AND tbl.cm_phy_owner_id = c1.cm_phy_owner_id		
				AND c1.cm_phy_owner_id = 'MY_OWNER_ID'
			LEFT OUTER JOIN MY_USERDB.CLARITY_COL col
				ON col.TABLE_ID = tbl.TABLE_ID
				AND col.cm_phy_owner_id = tbl.cm_phy_owner_id
				AND UPPER(TRIM(col.column_name)) = c1.columnname
				AND col.cm_phy_owner_id = 'MY_OWNER_ID'
				AND c1.cm_phy_owner_id = 'MY_OWNER_ID' 
				AND col.is_extracted_yn = 'Y'
			-- -------------------------------------------------------------------- 
			-- only get comparison for table columns not view columns 
			-- -------------------------------------------------------------------- 
			INNER JOIN MY_USHAREDB.UPGRADE_DBC_TABLES t2
				ON t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
				AND	c1.tablename = t2.tablename
				AND c1.cm_phy_owner_id = 'MY_OWNER_ID'
				AND t2.tablekind in ('T','V')
			-- -------------------------------------------------------------------- 
			-- get table.columns that are in both new and old release 
			-- and if there is a difference in nullability, data type or data size 
			-- return it - this is a column change. 
			-- -------------------------------------------------------------------- 
			INNER JOIN MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
				ON  c1.tablename = c2.tablename
				AND c1.columnname = c2.columnname
				AND UPPER(TRIM(c2.databasename)) in ('MY_REPORT_DB','MY_MATVIEW_DB')
				AND ((TRIM(c1.columntype) <> TRIM(TRIM(c2.columntype)) AND 6 NOT IN (0))
					OR (ZEROIFNULL(TRIM(c1.columnlength)) <> ZEROIFNULL(TRIM(c2.columnlength)) AND 7 NOT IN (0))
					OR (COALESCE(TRIM(c1.nullable),'Y') <> COALESCE(TRIM(c2.nullable),'Y') AND 8 NOT IN (0))
					OR (ZEROIFNULL(TRIM(c1.decimaltotaldigits)) <> ZEROIFNULL(TRIM(c2.decimaltotaldigits)) AND 7 NOT IN (0))
					OR (ZEROIFNULL(TRIM(c1.decimalfractionaldigits)) <> ZEROIFNULL(TRIM(c2.decimalfractionaldigits)) AND 7 NOT IN (0))
					OR (COALESCE(TRIM(c1.compressible),'') <> COALESCE(TRIM(c2.compressible),'')) AND 9 NOT IN (0))        
				-- ---------------------------- 
				-- exclude test and work tables 
				-- ---------------------------- 
				AND TRIM(c1.tablename) NOT LIKE ALL ('BF%','%/_DELETE', '%/_DELETE/_CT','%/_UPDATE/_CT','%/_UPDATE', 'UPD%','UTL%') ESCAPE '/' 
	) AS a;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-6, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  6;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-7, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  7;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-8, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  8;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-9, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  9;

-- ********************************** End of Reporting (_T) Table Validation ******************************************************** 
-- ********************************** Start of Reporting _T) Table Validation to Compass ******************************************** 

-- -----------------------------------------------------------------
-- Check for columns missing from reporting database but in COMPASS 
-- -----------------------------------------------------------------
 
INSERT INTO MY_USHAREDB.UPGRADE_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname,
		is_preserved, on_demand, is_extracted, data_retained, is_deprecated, cm_phy_owner_id, issue_comment, tbl_ini)
SELECT 	52 ,'Column in COMPASS but not in reporting database' msg,
		'MY_RUNNAME', 'MY_REPORT_DB','MY_REPORT_DB', UPPER(TRIM(t1.table_name)), UPPER(TRIM(c1.column_name)),
		CASE WHEN c1.is_preserved_yn = 'Y' THEN 'Y' ELSE NULL END AS is_preserved,
		CASE WHEN t1.load_frequency = 'ON DEMAND'  THEN 'Y' ELSE NULL END AS on_demand,
		c1.is_extracted_yn AS is_extracted,
		CASE WHEN t1.data_retained_yn = 'Y' THEN 'Y' ELSE NULL END AS data_retained,
		CASE WHEN c1.deprecated_yn = 'Y' THEN 'Y' ELSE NULL END AS is_deprecated,
		c1.cm_phy_owner_id,
		CASE 
			WHEN c1.deprecated_yn = 'Y' AND c1.is_extracted_yn = 'N'THEN 'Warning Only - Deprecated column that is not extracted'
			WHEN c1.deprecated_yn = 'Y' AND c1.is_extracted_yn = 'Y' THEN 'Column deprecated but flagged as extracted - error'
			WHEN mt.tablename is not null THEN 'Column Dropped per Upgrade Manifest'
		END AS issue_comment,
		COALESCE(NULLIF(t1.chronicles_mf,'N/A'), NULLIF(t1.dependent_ini,'N/A'), SUBSTR(t1.extract_filename, 1,3)) AS tbl_ini
FROM 	MY_USERDB.CLARITY_COL c1
		INNER JOIN MY_USERDB.CLARITY_TBL t1
			ON t1.table_id = c1.table_id
			AND t1.cm_phy_owner_id = c1.cm_phy_owner_id
			AND c1.cm_phy_owner_id = 'MY_OWNER_ID'
			AND c1.is_extracted_yn = 'Y'
			AND t1.is_extracted_yn = 'Y'
			-- ------------------------------------- 
			-- check if this error should be skipped 
			-- ------------------------------------- 
			AND 52 NOT IN (0)
			AND t1.table_name NOT LIKE 'BF%'		
			-- -------------------------------------------------------------------------- 
			-- only get columns where the table exists in the compass and in the database 
			-- -------------------------------------------------------------------------- 
			AND EXISTS (
				SELECT	1
				FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t 
				WHERE	TRIM(t.tablename) = TRIM(t1.table_name)
				AND UPPER(TRIM(t.databasename)) IN ('MY_REPORT_DB','MY_MATVIEW_DB','MY_LEAD_REPORTDB') 
				AND t.tablekind IN ('V','T'))
			AND UPPER(TRIM(t1.table_name))||'.'||UPPER(TRIM(c1.column_name)) 
				NOT IN (
				'OR_BLOCKNAMES.CM_LOG_OWNER_ID',
				'OR_BLOCKNAMES.CM_PHY_OWNER_ID',
				'OR_CASE_APPTS.CM_LOG_OWNER_ID',
				'OR_CASE_APPTS.CM_PHY_OWNER_ID',
				'OR_LOG_PNLCNT_CMTS.CM_LOG_OWNER_ID',
				'OR_LOG_PNLCNT_CMTS.CM_PHY_OWNER_ID',
				'OR_LOG_PNLCNT_INFO.CM_LOG_OWNER_ID',
				'OR_LOG_PNLCNT_INFO.CM_PHY_OWNER_ID')
		-- ---------------------------------------------------------------------------
		-- Identify if Column is supposed to be a Column drop.  They will be in the 
		-- ushare list when running validation in MY_ENV but not in the database
		-- ---------------------------------------------------------------------------
		LEFT OUTER JOIN (
			SELECT 	tablename, columnname 
			FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD
			WHERE 	chg_type = 'Column Drop' 
			AND 	(testing_Rqrd IS NULL OR testing_rqrd = 'Y')
			GROUP BY 1,2) AS mt
		ON mt.tablename = t1.table_name
		AND mt.columnname = c1.column_name
		AND c1.cm_phy_owner_id = 'MY_OWNER_ID'
WHERE 	NOT EXISTS (
		SELECT 	1
		FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS t3
		WHERE	t3.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB','MY_LEAD_REPORTDB') 
		AND		t3.tablename = t1.table_name
		AND		TRIM(t3.columnname) = TRIM(c1.column_name)
		AND 	c1.table_id = t1.table_id
		AND	    t1.table_name = t3.tablename) ;
		
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-52, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  52;

-- --------------------------------------------------------- 
-- Check for columns in reporting database but not in Compass 
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.UPGRADE_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname,
		is_preserved, on_demand, is_extracted, data_retained, is_deprecated, cm_phy_owner_id, issue_comment, tbl_ini)
SELECT 53,'Column in reporting database but not in COMPASS' msg, 
		'MY_RUNNAME', 'MY_REPORT_DB', TRIM(c1.databasename), t1.table_name, UPPER(TRIM(c1.columnname)),
		CASE WHEN t1.is_preserved_yn = 'Y' THEN 'Y' ELSE NULL END AS is_preserved,
		CASE WHEN t1.load_frequency = 'ON DEMAND'  THEN 'Y' ELSE NULL END AS on_demand,
		t1.is_extracted_yn AS is_extracted,
		t1.data_retained_yn AS data_retained,
		CASE WHEN t1.deprecated_yn = 'Y' THEN 'Y' ELSE NULL END AS is_deprecated,
		t1.cm_phy_owner_id,
		CASE 
			WHEN t1.deprecated_yn = 'Y' AND COALESCE(t1.is_extracted_yn,'N') = 'N' THEN 'Warning Only - Deprecated column that is not extracted'
			WHEN t1.deprecated_yn = 'Y' AND t1.is_extracted_yn = 'Y' THEN 'Column deprecated but flagged as extracted - error'
			WHEN c1.tablename like 'CRPK%' THEN 'Post-Upgrade Compass backup (table).'
			WHEN mt.tablename is not null THEN 'Column Drop per the Upgrade Manifest.'
		END AS issue_comment,
		COALESCE(NULLIF(t1.chronicles_mf,'N/A'), NULLIF(t1.dependent_ini,'N/A'), SUBSTR(t1.extract_filename, 1,3)) AS tbl_ini	
FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c1
		-- ---------------------------------------- 
		-- get only tables that exist in the compass 
		-- ----------------------------------------- 
		INNER JOIN MY_USERDB.CLARITY_TBL t1
		ON	TRIM(c1.tablename) =  TRIM(t1.table_name)
		AND UPPER(TRIM(c1.databasename)) IN ('MY_REPORT_DB','MY_MATVIEW_DB','MY_LEAD_REPORTDB') 
		AND t1.cm_phy_owner_id = 'MY_OWNER_ID'
		AND t1.is_extracted_yn = 'Y'
		AND t1.table_name NOT LIKE ALL ('BF%','UTL%','UPD%','%/_UPDATE'',%/_UPGRADE/_CT','%/_DELETE','%/_DELETE/_CT','%/_ERROR1','%/_ERROR2') ESCAPE '/' 
		-- ---------------------------------------------------- 
		-- left outer join to identify columns missing in table 
		-- ----------------------------------------------------	 
		LEFT OUTER JOIN MY_USERDB.CLARITY_COL c2
		ON t1.table_id = c2.table_id
		AND t1.cm_phy_owner_id = c2.cm_phy_owner_id		
		AND TRIM(c1.columnname) = TRIM(c2.column_name)
		-- ---------------------------------------------------------------------------
		-- Identify if Column is supposed to be a Column drop.  They will be in the 
		-- ushare list when running validation in MY_ENV but not in the database.
		-- ---------------------------------------------------------------------------
		LEFT OUTER JOIN (
			SELECT tablename, columnname 
			FROM MY_USHAREDB.UPGRADE_MANIFEST_LOAD 
			WHERE chg_type = 'Column Drop' 
			AND (testing_Rqrd IS NULL OR testing_rqrd = 'Y')			
			GROUP BY 1,2) AS mt
		ON mt.tablename = c1.tablename AND mt.columnname = c1.columnname
WHERE 	c2.table_id IS NULL
AND 	53 NOT IN (0);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-53, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM 	MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  53;

-- --------------------------------------------------------- 
-- Check that all tables in Compass exist in the database 
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.UPGRADE_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, 
		is_preserved, on_demand, is_extracted, data_retained, is_deprecated, cm_phy_owner_id, issue_comment, tbl_ini)
SELECT 	54, 'Table in COMPASS but not in reporting databases.' msg,
		'MY_RUNNAME', 'MY_REPORT_DB','MY_REPORT_DB', UPPER(TRIM(t1.table_name)) tablename,
		CASE WHEN t1.is_preserved_yn = 'Y' THEN 'Y' ELSE NULL END AS is_preserved,
		CASE WHEN t1.load_frequency = 'ON DEMAND'  THEN 'Y' ELSE NULL END AS on_demand,
		t1.is_extracted_yn is_extracted,
		CASE WHEN t1.data_retained_yn = 'Y' THEN 'Y' ELSE NULL END AS data_retained,
		CASE WHEN t1.deprecated_yn = 'Y' THEN 'Y' ELSE NULL END AS is_deprecated,
		t1.cm_phy_owner_id,
		CASE 
			WHEN mt.chg_type = 'Table Drop' and mt.recs = 2 then 'Table changed to view in Epic (middle) view layer per upgrade manifest'
			WHEN mt.chg_type = 'Table Drop' AND mt.recs = 1 then 'Table dropped with upgrade per upgrade manifest'
			WHEN t1.deprecated_yn = 'Y' THEN 'Warning Only - Deprecated table but flagged for extract' 
			WHEN t1.load_frequency = 'ON DEMAND' THEN 'Warning Only - On-Demand table'
			WHEN COALESCE(t1.deprecated_yn,'') <> 'Y' AND COALESCE(t1.load_frequency,'') <> 'ON DEMAND'
				THEN 'Error - table is not deprecated, not on-demand and not owned by 9001.'
		END AS issue_comment,
		COALESCE(NULLIF(t1.chronicles_mf,'N/A'), NULLIF(t1.dependent_ini,'N/A'), SUBSTR(t1.extract_filename, 1,3)) AS tbl_ini
FROM	MY_USERDB.CLARITY_TBL t1
		-- ---------------------------------------------------------------------------
		-- Identify if table is supposed to be a table drop.  They will be in the 
		-- ushare list when running validation in MY_ENV but not in the database
		-- ---------------------------------------------------------------------------
		LEFT OUTER JOIN (
			SELECT 	tablename, case when chg_type = 'View Added' then 'Table Drop' end as chg_type, count(*) as recs
			FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD
			WHERE 	chg_type in ('Table Drop' , 'View Added')
			AND 	(testing_Rqrd IS NULL OR testing_rqrd = 'Y')
			GROUP BY 1,2) AS mt
		ON mt.tablename = t1.table_name
		AND	t1.cm_phy_owner_id = 'MY_OWNER_ID'
		AND	t1.is_extracted_yn = 'Y'
		-- ----------------------------------
		-- and not in the reporting database
		-- ----------------------------------
WHERE	NOT EXISTS ( 
			SELECT 1 
			FROM MY_USHAREDB.UPGRADE_DBC_TABLES t2
			WHERE t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB', 'MY_LEAD_REPORTDB')		
			AND TRIM(t2.tablename) = TRIM(t1.table_name)
			AND t2.tablekind in ('T','V')
			AND t1.cm_phy_owner_id = 'MY_OWNER_ID'
			AND t1.is_extracted_yn = 'Y')
		-- ------------------------------------
		-- only get records for this deployment
		-- ------------------------------------
AND		t1.cm_phy_owner_id = 'MY_OWNER_ID'
		-- --------------------------------------------------------------------
		-- only get records where the table data is extracted from chronicles
		-- --------------------------------------------------------------------
AND		t1.is_extracted_yn = 'Y'
		-- --------------------------------------------------------------------------
		-- skip any staging tables - they should not exist in the reporting database
		-- --------------------------------------------------------------------------
AND		TRIM(t1.table_name) NOT LIKE ALL ('BF%','UTL%','UPD%','%/_DELETE','%/_UPDATE','%/_DELETE_CT','%/_UPDATE_CT','UPD%','UTL%') ESCAPE '/'
		-- ----------------------------------------------------------------------
		-- ignore tables that are only created as views in the middle/epic layer
		-- ----------------------------------------------------------------------
AND		TRIM(t1.table_name) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
		'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS','CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
		'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
		'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
		'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
		'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
		'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_REQUESTER_CREATION', 'V_ZC_CANCEL_REASON','V_ROI_STATUS_HISTORY'
		,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

);
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-54, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  54;
	
-- --------------------------------------------------------- 
-- Check for tables that exist in database but not in Compass
-- CR_ tables will show up in this error.  This is a warning only.  
-- These tables may or may not be present.
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.UPGRADE_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, 
		is_preserved, on_demand, is_extracted, data_retained, is_deprecated, cm_phy_owner_id, issue_comment, tbl_ini)
SELECT 	55, 'Table in reporting database but should not be per COMPASS' msg,
		'MY_RUNNAME', 'MY_REPORT_DB', TRIM(t1.databasename), UPPER(TRIM(t1.tablename)) tablename,
		t2.is_preserved_yn ,
		CASE WHEN t2.load_frequency = 'ON DEMAND'  THEN 'Y' ELSE NULL END AS on_demand,
		t2.is_extracted_yn ,
		t2.data_retained_yn,
		t2.deprecated_yn,
		t2.cm_phy_owner_id,
		CASE 
			WHEN t2.table_name IS NULL THEN 'Table Not found in MY_USERDB.CLARITY_TBL (Compass).' 
			WHEN COALESCE(t2.cm_phy_owner_id, t3.cm_phy_owner_id)  = '9001'   THEN 'Warning Only - Table owner = 9001 - should not exist.' 
			WHEN t2.load_frequency = 'ON DEMAND' THEN 'Warning Only - On-Demand Table.'
			WHEN COALESCE(t2.is_extracted_yn,'N') = 'N'  AND t2.deprecated_yn = 'Y'	THEN 'Warning Only - Deprecated table and flagged as no extrect'
			WHEN COALESCE(t2.is_extracted_yn,'N') = 'Y'  AND t2.deprecated_yn = 'Y'	THEN 'Warning Only - Deprecated table flagged for extract.'
			WHEN mt.chg_type = 'Table Drop' and mt.recs = 1 then 'Table Drop per Upgrade Manifest.'
			WHEN mt.chg_type = 'Table Drop' and mt.recs = 2 then 'Table converted to View per Upgrade Manifest'
			ELSE t2.table_introduction 
		END ,
		COALESCE(NULLIF(t2.chronicles_mf,'N/A'), NULLIF(t2.dependent_ini,'N/A'), SUBSTR(t2.extract_filename, 1,3)) AS tbl_ini
FROM	MY_USHAREDB.UPGRADE_DB_OWNER_LINK link
		-- only report on tables for this deployment
		INNER JOIN MY_USHAREDB.UPGRADE_DBC_TABLES t1
			ON 	link.cm_phy_owner_id = 'MY_OWNER_ID'
			AND link.env = 'MY_ENV'
			AND t1.databasename = link.rpt_db
			AND t1.tablekind in ('T','V')
			-- ---------------------------- 
			-- exclude backfill tables
			-- ---------------------------- 
			AND t1.tablename NOT LIKE 'BF%' 
			-- -------------------------------------------
			-- exclude derived and compass backup tables
			-- ------------------------------------------- 
			AND t1.tablename NOT LIKE ALL ('PAT_CVG_BEN_OT%', 'CRPK%')		
		-- ---------------------------------------------
		-- get compass data if it exists for deployment 
		-- ---------------------------------------------
		LEFT OUTER JOIN MY_USERDB.CLARITY_TBL t2
			ON TRIM(t2.table_name) = TRIM(t1.tablename)
			AND t2.cm_phy_owner_id = 'MY_OWNER_ID'
		-- --------------------------------------------
		-- get compass data if it exists for owner 9001
		-- --------------------------------------------
		LEFT OUTER JOIN MY_USERDB.CLARITY_TBL t3
			ON TRIM(t3.table_name) = TRIM(t1.tablename)
			AND t3.cm_phy_owner_id = '9001'
		-- ---------------------------------------------------------------------------
		-- Identify if table is supposed to be a table drop.  They will be in the 
		-- ushare list when running validation in MY_ENV but not in the database
		-- ---------------------------------------------------------------------------
		LEFT OUTER JOIN (
			SELECT 	tablename, case when chg_type = 'View Added' then 'Table Drop' end as chg_type, count(*) as recs
			FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD
			WHERE 	chg_type in ('Table Drop', 'View Added')
			AND 	(testing_Rqrd IS NULL OR testing_rqrd = 'Y')
			GROUP BY 1,2) AS mt
		ON mt.tablename = t1.tablename
WHERE	-- -------------------------------------------
		-- If tablenames are both null then table not in compass
		-- -------------------------------------------
		((t2.table_name IS NULL AND t3.table_name is NULL)
		-- -------------------------------------------------------------------------------------------
		-- or orly table owner is 9001 so return a warning table should logically not exist in the database
		-- --------------------------------------------------------------------------------------------
		OR  (t3.table_name is not null and t2.table_name is null))
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
AND 	55 NOT IN (0)	;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-55, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  55;

-- --------------------------------------------------------- 
-- Check all PROD columns definitions match MY_ENV. 
-- ---------------------------------------------- 
-- Note: not all tables have a view with the same name. 
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.UPGRADE_issues (
	err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname, ushare_or_cmps_or_S_def, hccl_or_T_def, 	
	is_preserved, on_demand, is_extracted, data_retained, is_deprecated, cm_phy_owner_id, tbl_ini, col_fmt_ini, col_fmt_item)
SELECT 	a.err_no, a.change_type AS error_msg, 
		'MY_RUNNAME', 'MY_REPORT_DB', a.err_db, a.tablename, a.columnname, a.compass_def, a.hccl_def, 
		a.is_preserved_yn, a.on_demand, a.is_extracted, a.data_retained, a.deprecated_yn, a.cm_phy_owner_id, 
		a.tbl_ini, a.format_ini, a.format_item
FROM (
	SELECT	
		UPPER(TRIM(t1.table_name)) AS tablename,
		UPPER(TRIM(c1.column_name)) AS columnname,
		UPPER(TRIM(c2.databasename)) AS err_db, 
		CASE WHEN c1.is_preserved_yn = 'Y' THEN 'Y'
			WHEN t1.is_preserved_yn = 'Y' THEN 'Y'
			ELSE NULL
		END AS IS_PRESERVED_YN,
		CASE WHEN t1.load_frequency = 'ON DEMAND'  THEN 'Y' ELSE NULL END AS on_demand,
		CASE WHEN c1.is_extracted_yn = 'Y' THEN 'Y'  ELSE NULL END AS is_extracted,
		CASE WHEN t1.data_retained_yn = 'Y' THEN 'Y' ELSE NULL END AS data_retained,		
		c1.cm_phy_owner_id,		
		"COMPASS_DTYPE"||' '||"COMPASS_SIZE" AS "COMPASS_DEF",
		"HCCL_DTYPE"||' '||"HCCL_SIZE" AS "HCCL_DEF",
		CASE WHEN c1.data_type = 'NUMERIC' THEN 'DECIMAL'
			WHEN c1.data_type = 'DATETIME' AND c1.hour_format IN ('DATETIME 24HR INCL SECONDS', 'DATETIME 24HR')
			THEN 'TIMESTAMP'
			ELSE c1.data_type
		END AS "COMPASS_DTYPE",
		CASE 
			WHEN c1.data_type IN ('VARCHAR') THEN '('||TRIM(c1.clarity_precision)||')'
			WHEN c1.data_type IN ('INTEGER', 'FLOAT', 'DATETIME') THEN ''
			WHEN c1.data_type = 'NUMERIC' THEN '('||TRIM(c1.clarity_precision)||', '||TRIM(c1.clarity_scale)||')'
			ELSE COALESCE(c1.data_type,'')
		END AS "COMPASS_SIZE",
		CASE WHEN c2.columntype = 'CV' THEN 'VARCHAR'
			WHEN c2.columntype = 'CF' THEN 'VARCHAR'
			WHEN c2.columntype = 'CO' THEN 'VARCHAR'
			WHEN c2.columntype = 'I' THEN 'INTEGER'
			WHEN c2.columntype = 'D' THEN 'DECIMAL'
			WHEN c2.columntype = 'DA' THEN 'DATETIME'
			WHEN c2.columntype = 'F' THEN 'FLOAT'
			WHEN c2.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c2.columntype,'')
		END AS "HCCL_DTYPE",
		CASE WHEN c2.columntype IN ('CO', 'CV', 'CF') THEN '('||(TRIM(c2.columnlength (FORMAT 'zzzzzzz')))||')'
			WHEN c2.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c2.columntype = 'D' THEN '('||(TRIM(c2.decimaltotaldigits (FORMAT 'zzzzzzz')))||', '||TRIM(c2.decimalfractionaldigits)||')'
			ELSE COALESCE(c2.columntype, '')
		END AS "HCCL_SIZE",
		CASE WHEN "COMPASS_DTYPE" <> "HCCL_DTYPE" 	THEN 'Database to Compass column datatype difference.'
			WHEN "COMPASS_SIZE" <> "HCCL_SIZE" 		THEN 'Database to Compass column size difference.'
			ELSE NULL
		END AS "CHANGE_TYPE",
		CASE WHEN "COMPASS_DTYPE" <> "HCCL_DTYPE" THEN 56
			WHEN "COMPASS_SIZE" <> "HCCL_SIZE" THEN 57
			ELSE 0
		END AS "ERR_NO",
		c1.deprecated_yn,
		COALESCE(NULLIF(t1.chronicles_mf,'N/A'), NULLIF(t1.dependent_ini,'N/A'), SUBSTR(t1.extract_filename, 1,3)) AS tbl_ini,
		c1.format_ini,
		c1.format_item
	FROM	
		MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
		-- ---------------------------------- 
		-- get clarity compass defintions 
		-- ---------------------------------- 
		INNER JOIN MY_USERDB.CLARITY_TBL t1
			ON  TRIM(t1.table_name)  = TRIM(c2.tablename)
			AND t1.cm_phy_owner_id = 'MY_OWNER_ID'
			AND t1.is_extracted_yn = 'Y'
			AND t1.table_name NOT LIKE ALL ('BF%','V/_%') ESCAPE '/'
			AND c2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB','MY_LEAD_REPORTDB')
		INNER JOIN MY_USERDB.CLARITY_COL c1 
			ON t1.table_id = c1.table_id
			AND TRIM(c2.columnname) = TRIM(c1.column_name)
			AND t1.cm_phy_owner_id  = c1.cm_phy_owner_id
			AND c1.is_extracted_yn = 'Y'
	WHERE 	"CHANGE_TYPE" IS NOT NULL
	AND 	"ERR_NO" NOT IN (0)
	) AS a
	-- ---------------------------------------------------------------------------
	-- Identify if column is supposed to be a column modify. 
	-- ---------------------------------------------------------------------------
	LEFT OUTER JOIN (
		SELECT 	tablename, columnname, 'Column Modified to datatype '||TRIM(new_datatype)||' per upgrade manifest' chg_comment
		FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD
		WHERE 	chg_type = 'Column Modify'
		AND 	(testing_Rqrd IS NULL OR testing_rqrd = 'Y')
		GROUP BY 1,2,3) AS mt
	ON TRIM(a.tablename) = TRIM(mt.tablename)
	AND TRIM(a.columnname) = TRIM(mt.columnname);
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-56, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  56;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-57, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  57;
	
-- --------------------------------------------------------- 
-- Check that all tables in Compass exist in the database 
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.UPGRADE_issues (err_no, err_msg, runname, dbname, tablename, 
		is_preserved, on_demand, is_extracted, data_retained, is_deprecated, cm_phy_owner_id, issue_comment, tbl_ini)
SELECT 	58, 'Table in COMPASS but not in staging databases.' msg,
		'MY_RUNNAME', 'MY_REPORT_DB', UPPER(TRIM(t1.table_name)) tablename,
		CASE WHEN t1.is_preserved_yn = 'Y' THEN 'Y' ELSE NULL END AS is_preserved,
		CASE WHEN t1.load_frequency = 'ON DEMAND'  THEN 'Y' ELSE NULL END AS on_demand,
		t1.is_extracted_yn is_extracted,
		CASE WHEN t1.data_retained_yn = 'Y' THEN 'Y' ELSE NULL END AS data_retained,
		CASE WHEN t1.deprecated_yn = 'Y' THEN 'Y' ELSE NULL END AS is_deprecated,
		t1.cm_phy_owner_id,
		CASE 
			WHEN mt.chg_type = 'Table Drop' and mt.recs = 2 THEN 'Table changed to View per upgrade manifest'
			WHEN mt.chg_type = 'Table Drop' and mt.recs = 1 THEN 'Table dropped per upgrade manifest'
			WHEN mt.chg_type = 'Table Add' THEN 'Table added per upgrade manifest'
			WHEN t1.deprecated_yn = 'Y' THEN 'Warning Only - Deprecated table but flagged for extract' 
			WHEN t1.load_frequency = 'ON DEMAND' THEN 'Warning Only - On-Demand table'
			WHEN COALESCE(t1.deprecated_yn,'') <> 'Y' AND COALESCE(t1.load_frequency,'') <> 'ON DEMAND'
				THEN 'Error - table is not deprecated AND not on-demand.'
		END AS issue_comment,
		COALESCE(NULLIF(t1.chronicles_mf,'N/A'), NULLIF(t1.dependent_ini,'N/A'), SUBSTR(t1.extract_filename, 1,3)) AS tbl_ini
FROM	MY_USERDB.CLARITY_TBL t1
		-- ---------------------------------------------------------------------------
		-- Identify if column is supposed to be a column modify. 
		-- ---------------------------------------------------------------------------
		LEFT OUTER JOIN (
			SELECT 	tablename,  case when chg_type = 'View Added' then 'Table Drop' end as chg_type, count(*) as recs
			FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD
			WHERE 	chg_type in ('Table Drop', 'View Added','Table Add')
			AND 	(testing_Rqrd IS NULL OR testing_rqrd = 'Y')
			GROUP BY 1,2) AS mt
		ON TRIM(t1.table_name) = TRIM(mt.tablename)

WHERE	t1.is_extracted_yn = 'Y'
AND		t1.cm_phy_owner_id = 'MY_OWNER_ID'
AND NOT EXISTS ( 
			SELECT 1 
			FROM MY_USHAREDB.UPGRADE_DBC_TABLES t2
			WHERE t2.databasename IN ('MY_STAGE_DB', 'MY_LEAD_STAGEDB') 
			AND TRIM(t2.tablename) = TRIM(t1.table_name)
			AND t2.tablekind in ('T','V')
			AND t1.is_extracted_yn = 'Y'
			AND t1.cm_phy_owner_id = 'MY_OWNER_ID')
		-- ----------------------------------------------------------------------
		-- ignore tables that are only created as views in the middle/epic layer
		-- ----------------------------------------------------------------------
AND 	TRIM(t1.table_name) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
		'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS','CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
		'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
		'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
		'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
		'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
		'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_REQUESTER_CREATION', 'V_ROI_STATUS_HISTORY','V_ZC_CANCEL_REASON'
	,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'
)	
AND 	TRIM(t1.table_name) NOT LIKE 'BF%'
AND 	t1.is_extracted_yn = 'Y'
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
AND 	58 NOT IN (0);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-58, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no = 58;
		
-- ********************************** End of Reporting (_T) Table to Compass Validation *********************************** 

-- ********************** Start of structure validation to manifest and vice-versa ******************************
	
-- -------------------------------------------
-- Check for columns drops not in the manifest
-- -------------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT 	75, 'Column Drop in database but not in Manifest' msg, 
		'MY_RUNNAME', 'MY_REPORT_DB','MY_REPORT_DB', c1.tablename, c1.columnname
FROM  	MY_USHAREDB.upgrade_columns c1
WHERE	c1.cm_phy_owner_id = 'MY_OWNER_ID'
		-- -----------------------------------------------------
		-- only check for table/columns valid for the deployment
		-- -----------------------------------------------------
		AND EXISTS (
			SELECT 	1
			FROM 	MY_USERDB.CLARITY_COL col
			WHERE 	TRIM(c1.tablename)||'__'||TRIM(c1.columnname) = col.col_descriptor
			AND 	c1.cm_phy_owner_id = col.cm_phy_owner_id
			AND		c1.cm_phy_owner_id = 'MY_OWNER_ID'
			AND		col.cm_phy_owner_id = 'MY_OWNER_ID')
		-- ----------------------------------------------
		-- only report on tables in the physical database
		-- ----------------------------------------------
		AND EXISTS (
			SELECT	1
			FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t2
			WHERE	t2.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND 	t2.tablename = c1.tablename  
			AND 	c1.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE',	'%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/')
		-- -----------------------------------------------------
		-- column in ushare list but not in db - denotes a column drop
		-- -----------------------------------------------------
		AND NOT EXISTS (
			SELECT	1
			FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
			WHERE	c2.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND		c2.tablename = c1.tablename
			AND 	c2.columnname = c1.columnname
			AND		c1.cm_phy_owner_id = 'MY_OWNER_ID')
		-- -----------------------------------------------------
		-- column drop not found in manifest
		-- -----------------------------------------------------			
		AND NOT EXISTS (
			SELECT 	1 
			FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD mt 
			WHERE 	mt.chg_type = 'Column Drop'
			AND 	mt.tablename = c1.tablename
			AND		mt.columnname = c1.columnname
			AND		c1.cm_phy_owner_id = 'MY_OWNER_ID'
			AND 	(mt.testing_Rqrd IS NULL OR mt.testing_rqrd = 'Y'));
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-75, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  75;

-- -----------------------------------------
-- Check for column adds not in the manifest
-- -----------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, 
		tablename, columnname)
SELECT 	76, 'Column Add in database but not in Manifest' msg, 
		'MY_RUNNAME', 'MY_REPORT_DB', c1.databasename, c1.tablename, c1.columnname
FROM 	(select databasename, tablename, columnname
		FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS 
		WHERE	databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
		) as c1
		-- -----------------------------------------------------
		-- only check for table/columns valid for the deployment
		-- -----------------------------------------------------
WHERE EXISTS (
			SELECT 	1
			FROM 	MY_USERDB.CLARITY_COL col
			WHERE 	TRIM(c1.tablename)||'__'||TRIM(c1.columnname) = col.col_descriptor
			AND		col.cm_phy_owner_id = 'MY_OWNER_ID')
		-- ------------------------------------------------ 
		-- only check tables if in USHARE list and database 
		-- ------------------------------------------------ 
		AND EXISTS (
			SELECT 	1 
			FROM 	MY_USHAREDB.upgrade_columns AS ush
			WHERE	ush.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
			AND		UPPER(TRIM(c1.tablename)) = ush.tablename	 
			AND 	ush.cm_phy_owner_id = 'MY_OWNER_ID')
		-- -------------------------------------------------------------
		-- table in ushare but column not in database ie.column add	
		-- -------------------------------------------------------------
		AND NOT EXISTS 
			(SELECT 1 
			FROM 	MY_USHAREDB.upgrade_columns c2 
			WHERE 	c1.tablename = c2.tablename 
			AND 	c1.columnname = c2.columnname
			AND		c2.cm_phy_owner_id = 'MY_OWNER_ID')
		-- -------------------------------------------------------------
		-- column add is not in the manifest
		-- -------------------------------------------------------------
		AND NOT EXISTS
			(SELECT 1
			FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD mt
			WHERE 	mt.chg_type = 'Column Add'
			AND		mt.tablename = c1.tablename
			AND		mt.columnname = c1.columnname
			AND 	(mt.testing_Rqrd IS NULL OR mt.testing_rqrd = 'Y'));

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-76, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  76;

-- --------------------------------------
-- Check for tables drops not in manifest
-- --------------------------------------

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename)
SELECT 	77, 'Table Drop in database but not in Manifest' msg, 
	'MY_RUNNAME', 'MY_REPORT_DB', 'MY_REPORT_DB', TRIM(t2.tablename)
FROM 	MY_USHAREDB.upgrade_tables AS t2 
WHERE	t2.cm_phy_owner_id = 'MY_OWNER_ID'
		-- ---------------------------- 
		-- exclude test and work tables 
		-- ---------------------------- 
AND		t2.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'		
		-- -----------------------------------------------------
		-- table in USHARE but not in database - ie table drop
		-- -----------------------------------------------------
AND		NOT EXISTS (
			SELECT	1
			FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES tbl
			WHERE	tbl.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND 	tbl.tablename = t2.tablename
			AND		t2.cm_phy_owner_id = 'MY_OWNER_ID')
		-- ---------------------------------
		-- table drop is not in the manifest	
		-- ---------------------------------
AND		NOT EXISTS (
			SELECT 1 
			FROM	MY_USHAREDB.UPGRADE_MANIFEST_LOAD mt 
			WHERE 	mt.chg_type  in ('Table Drop')
			AND 	mt.tablename = t2.tablename
			AND 	(mt.testing_Rqrd IS NULL OR mt.testing_rqrd = 'Y'));

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-77, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  77;
	
-- -------------------------------------
-- Check for tables adds not in manifest
-- -------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename)
SELECT 	78, 'Table Add in database but not in Manifest' msg, 
	'MY_RUNNAME', 'MY_REPORT_DB', t1.databasename, t1.tablename
FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t1
WHERE t1.databasename IN ('MY_REPORT_DB')
	-- ---------------------------- 
	-- exclude test and work tables 
	-- ---------------------------- 
AND	t1.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
AND	t1.tablekind in ('V','T')
	-- -----------------------------------------------------
	-- table in database but not in Ushare table- ie table add
	-- -----------------------------------------------------
AND NOT EXISTS (
		SELECT	1
		FROM 	MY_USHAREDB.upgrade_tables tbl
		WHERE	tbl.cm_phy_owner_id = 'MY_OWNER_ID'
		AND 	tbl.tablename = t1.tablename)
	-- -------------------------
	-- table add not in manifest
	-- -------------------------
AND	NOT EXISTS (
			SELECT	1
			FROM	MY_USHAREDB.UPGRADE_MANIFEST_LOAD mt 
			WHERE	mt.tablename = t1.tablename
			AND		mt.chg_type in ('Table Add')
			AND		(mt.testing_Rqrd IS NULL OR mt.testing_rqrd = 'Y'));

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-78, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  78;
	
-- -----------------------------------------------------------------
-- Check for column Modifies in the database but not in the manifest
-- -----------------------------------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, 
		dbname, erroring_dbname, tablename, columnname, 
		ushare_or_cmps_or_S_def,  mfst_def,  hccl_or_T_def,testing_rqrd )
SELECT  79,  'Column Modify in database but not in Manifest' msg, 
		'MY_RUNNAME', 'MY_REPORT_DB', a.rpt_db, a.tablename, a.columnname, 
		a.ushare_def, chg.new_datatype AS mfst_def, a.db_def,chg.testing_rqrd
FROM	(SELECT	
		c2.databasename as rpt_db,
		c1.tablename,
		c1.columnname,
		"DB_DTYPE"||''||"DB_SIZE" AS "DB_DEF",
		"USHARE_DTYPE"||''||"USHARE_SIZE" AS "USHARE_DEF",
		CASE WHEN c2.columntype = 'CV' THEN 'VARCHAR'
			WHEN c2.columntype = 'CF'    THEN 'VARCHAR'
			WHEN c2.columntype = 'CO'    THEN 'VARCHAR'
			WHEN c2.columntype = 'I' THEN 'INTEGER'
			WHEN c2.columntype = 'D' THEN 'DECIMAL'
			WHEN c2.columntype = 'DA' THEN 'DATE'
			WHEN c2.columntype = 'F' THEN 'FLOAT'
			WHEN c2.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c2.columntype,'')
		END AS "DB_DTYPE",
		CASE WHEN c2.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c2.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c2.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c2.columntype = 'D' THEN '('||TRIM(c2.decimaltotaldigits (FORMAT 'zzzzzzz'))||','||TRIM(c2.decimalfractionaldigits)||')'
			ELSE COALESCE(c2.columntype, '')
		END AS "DB_SIZE",
		CASE WHEN c1.columntype = 'CV' THEN 'VARCHAR'
			WHEN c1.columntype = 'CF'    THEN 'VARCHAR'
			WHEN c1.columntype = 'CO'    THEN 'VARCHAR'
			WHEN c1.columntype = 'I' THEN 'INTEGER'
			WHEN c1.columntype = 'D' THEN 'DECIMAL'
			WHEN c1.columntype = 'DA' THEN 'DATE'
			WHEN c1.columntype = 'F' THEN 'FLOAT'
			WHEN c1.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c1.columntype,'')
		END AS "USHARE_DTYPE",
		CASE WHEN c1.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c1.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c1.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c1.columntype = 'D' THEN '('||TRIM(c1.decimaltotaldigits (FORMAT 'zzzzzzz'))||','||TRIM(c1.decimalfractionaldigits)||')'
			ELSE COALESCE(c1.columntype, '')
		END AS "USHARE_SIZE"
	FROM 	MY_USHAREDB.upgrade_columns c1
			-- --------------------------------------------------------------------- 
			-- join to clarity compass tables to get preserved and on-demand flags 
			-- --------------------------------------------------------------------- 
			INNER JOIN MY_USERDB.CLARITY_TBL tbl
			ON UPPER(TRIM(tbl.table_name)) = c1.tablename
			AND tbl.is_extracted_yn = 'Y'
			AND c1.cm_phy_owner_id = 'MY_OWNER_ID'
			AND c1.cm_phy_owner_id = tbl.cm_phy_owner_id
			-- -------------------------------------------------------
			-- only get table/columns where they exist in the compass
			-- -------------------------------------------------------
			AND EXISTS (
				SELECT 	1
				FROM	MY_USERDB.CLARITY_COL col
				WHERE 	col.TABLE_ID = tbl.TABLE_ID
				AND 	col.cm_phy_owner_id = tbl.cm_phy_owner_id
				AND 	col.cm_phy_owner_id = 'MY_OWNER_ID'
				AND 	UPPER(TRIM(col.column_name)) = c1.columnname
				AND 	col.is_extracted_yn = 'Y')
			-- -------------------------------------------------------------------- 
			-- only get comparison for table columns not view columns 
			-- -------------------------------------------------------------------- 
			AND EXISTS (
				SELECT	1
				FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t2
				WHERE	t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
				AND 	c1.tablename = t2.tablename
				AND 	c1.cm_phy_owner_id = 'MY_OWNER_ID'	
				AND 	t2.tablekind = 'T') 
				-- ---------------------------- 
				-- exclude test and work tables 
				-- ---------------------------- 
			AND	c1.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
			-- -------------------------------------------------------------------- 
			-- get column changes
			-- get table.columns that are in both new and old release 
			-- and if there is a difference in nullability, data type or data size 
			-- return it - this is a column change. 
			-- -------------------------------------------------------------------- 
			INNER JOIN MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
			ON  c2.tablename = c1.tablename
			AND c2.columnname = c1.columnname
			AND c2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND c1.cm_phy_owner_id = 'MY_OWNER_ID'	
			AND ((TRIM(c1.columntype) <> TRIM(TRIM(c2.columntype)))
				OR (ZEROIFNULL(TRIM(c1.columnlength)) <> ZEROIFNULL(TRIM(c2.columnlength)) )
				OR (ZEROIFNULL(TRIM(c1.decimaltotaldigits)) <> ZEROIFNULL(TRIM(c2.decimaltotaldigits))) 
				OR (ZEROIFNULL(TRIM(c1.decimalfractionaldigits)) <> ZEROIFNULL(TRIM(c2.decimalfractionaldigits))))
		WHERE 	"USHARE_DEF" <> "DB_DEF"
		) AS a  -- changes in the database
		-- -------------------------------------------------------------------------
		-- join to get manifest information and to determine if change exists.
		-- -------------------------------------------------------------------------
		LEFT OUTER JOIN MY_USHAREDB.UPGRADE_MANIFEST_LOAD chg
				ON 		chg.tablename = a.tablename
				AND 	chg.columnname = a.columnname
				AND 	chg.chg_type IN ('Column Modify', 'Col Modify')
-- -------------------------------------------------------------------------
-- database change not found in manifest but ushare <> database
-- -------------------------------------------------------------------------
WHERE chg.tablename IS NULL 
-- Manifest change different from database change
OR chg.new_datatype <> a.db_def
GROUP BY 1,2,3,4,5,6,7,8,9,10,11;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-79, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  79;

-- -----------------------------------------------------------------
-- Check for column Modifies in the manifest but not in the database
-- -----------------------------------------------------------------
	
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, 
		dbname, erroring_dbname, tablename, columnname, 
		ushare_or_cmps_or_S_def,  mfst_def,  hccl_or_T_def , testing_rqrd)
SELECT  80,  'Column Modify in Manifest but not in database' msg, 
		'MY_RUNNAME', 'MY_REPORT_DB', a.rpt_db, chg.tablename, chg.columnname, 
		a.ushare_def, chg.new_datatype AS mfst_def, a.db_def, chg.testing_rqrd
FROM	(Select * From MY_USHAREDB.UPGRADE_MANIFEST_LOAD chg
		where  	chg.chg_type IN ('Column Modify', 'Col Modify')) chg
		LEFT OUTER JOIN 
		(SELECT	
		c2.databasename as rpt_db,
		c1.tablename,
		c1.columnname,
		"DB_DTYPE"||''||"DB_SIZE" AS "DB_DEF",
		"USHARE_DTYPE"||''||"USHARE_SIZE" AS "USHARE_DEF",
		CASE WHEN c2.columntype = 'CV' THEN 'VARCHAR'
			WHEN c2.columntype = 'CF' THEN 'VARCHAR'
			WHEN c2.columntype = 'CO' THEN 'VARCHAR'
			WHEN c2.columntype = 'I' THEN 'INTEGER'
			WHEN c2.columntype = 'D' THEN 'DECIMAL'
			WHEN c2.columntype = 'DA' THEN 'DATE'
			WHEN c2.columntype = 'F' THEN 'FLOAT'
			WHEN c2.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c2.columntype,'')
		END AS "DB_DTYPE",
		CASE WHEN c2.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c2.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c2.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c2.columntype = 'D' THEN '('||TRIM(c2.decimaltotaldigits (FORMAT 'zzzzzzz'))||','||TRIM(c2.decimalfractionaldigits)||')'
			ELSE COALESCE(c2.columntype, '')
		END AS "DB_SIZE",
		CASE WHEN c1.columntype = 'CV' THEN 'VARCHAR'
			WHEN c1.columntype = 'CF'    THEN 'VARCHAR'
			WHEN c1.columntype = 'CO'    THEN 'VARCHAR'
			WHEN c1.columntype = 'I' THEN 'INTEGER'
			WHEN c1.columntype = 'D' THEN 'DECIMAL'
			WHEN c1.columntype = 'DA' THEN 'DATE'
			WHEN c1.columntype = 'F' THEN 'FLOAT'
			WHEN c1.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c1.columntype,'')
		END AS "USHARE_DTYPE",
		CASE WHEN c1.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c1.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c1.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c1.columntype = 'D' THEN '('||TRIM(c1.decimaltotaldigits (FORMAT 'zzzzzzz'))||','||TRIM(c1.decimalfractionaldigits)||')'
			ELSE COALESCE(c1.columntype, '')
		END AS "USHARE_SIZE"
	FROM 	MY_USHAREDB.upgrade_columns c1
			-- --------------------------------------------------------------------- 
			-- join to clarity compass tables to get preserved and on-demand flags 
			-- --------------------------------------------------------------------- 
			INNER JOIN MY_USERDB.CLARITY_TBL tbl
			ON UPPER(TRIM(tbl.table_name)) = c1.tablename
			AND tbl.is_extracted_yn = 'Y'
			AND c1.cm_phy_owner_id = 'MY_OWNER_ID'
			AND tbl.cm_phy_owner_id = 'MY_OWNER_ID'
			-- -------------------------------------------------------
			-- only get table/columns where they exist in the compass
			-- -------------------------------------------------------
			AND EXISTS (
				SELECT 	1
				FROM	MY_USERDB.CLARITY_COL col
				WHERE 	col.TABLE_ID = tbl.TABLE_ID
				AND 	col.cm_phy_owner_id = tbl.cm_phy_owner_id
				AND 	col.cm_phy_owner_id = 'MY_OWNER_ID'
				AND 	UPPER(TRIM(col.column_name)) = c1.columnname
				AND 	col.is_extracted_yn = 'Y')
			-- -------------------------------------------------------------------- 
			-- only get comparison for table columns not view columns 
			-- -------------------------------------------------------------------- 
			AND EXISTS (
				SELECT	1
				FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t2
				WHERE	t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
				AND 	c1.tablename = t2.tablename
				AND 	c1.cm_phy_owner_id = 'MY_OWNER_ID'	
				AND 	t2.tablekind = 'T') 
				-- ---------------------------- 
				-- exclude test and work tables 
				-- ---------------------------- 
			AND	c1.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
			-- -------------------------------------------------------------------- 
			-- get column changes
			-- get table.columns that are in both new and old release 
			-- and if there is a difference in nullability, data type or data size 
			-- return it - this is a column change. 
			-- -------------------------------------------------------------------- 
			INNER JOIN MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
			ON  c2.tablename = c1.tablename
			AND c2.columnname = c1.columnname
			AND c2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND c1.cm_phy_owner_id = 'MY_OWNER_ID'	
			/*AND ((TRIM(c1.columntype) <> TRIM(TRIM(c2.columntype)))
				OR (ZEROIFNULL(TRIM(c1.columnlength)) <> ZEROIFNULL(TRIM(c2.columnlength)) )
				OR (ZEROIFNULL(TRIM(c1.decimaltotaldigits)) <> ZEROIFNULL(TRIM(c2.decimaltotaldigits))) 
				OR (ZEROIFNULL(TRIM(c1.decimalfractionaldigits)) <> ZEROIFNULL(TRIM(c2.decimalfractionaldigits))))*/
		) AS a  -- changes in the database
		ON 		chg.tablename = a.tablename
		AND 	chg.columnname = a.columnname
	
		AND 	not exists (
					SELECT 1 
					from MY_USHAREDB.UPGRADE_MANIFEST_LOAD mu
					where chg.tablename = mu.tablename
					AND 	chg.columnname = mu.columnname
					AND 	chg.chg_type IN ('Column Add', 'Column Drop'))
-- -------------------------------------------------------------------------
-- manifest change not found or different from the database.
-- -------------------------------------------------------------------------
WHERE 	(a.db_def <> chg.new_datatype OR a.db_def is null)
AND 	EXISTS (
			SELECT	1
			FROM	MY_USHAREDB.UPGRADE_DBC_COLUMNS c9
			WHERE	c9.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND		c9.tablename = chg.tablename
			AND		c9.columnname = chg.columnname)
			-- -------------------------------------------------------
			-- only get table/columns where they exist in the compass
			-- --------------------------------------------------------
AND EXISTS (
		SELECT	1
		FROM	MY_USERDB.CLARITY_TBL tbl9
				INNER JOIN MY_USERDB.CLARITY_COL col9
				ON tbl9.table_id = col9.table_id
				AND tbl9.is_extracted_yn = 'Y'
				AND tbl9.cm_phy_owner_id = 'MY_OWNER_ID'
				AND	col9.is_extracted_yn = 'Y'
				AND	TRIM(tbl9.table_name) = chg.tablename
				AND TRIM(col9.column_name) = chg.columnname)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11;		


UPDATE MY_USHAREDB.UPGRADE_ISSUES
FROM 
(select 	c2.databasename as dbname, c2.tablename as tblname, c2.columnname,
			"DB_DTYPE"||''||"DB_SIZE" AS DB_DEF,
		CASE WHEN c2.columntype = 'CV' THEN 'VARCHAR'
			WHEN c2.columntype = 'CF' THEN 'VARCHAR'
			WHEN c2.columntype = 'CO' THEN 'VARCHAR'
			WHEN c2.columntype = 'I' THEN 'INTEGER'
			WHEN c2.columntype = 'D' THEN 'DECIMAL'
			WHEN c2.columntype = 'DA' THEN 'DATE'
			WHEN c2.columntype = 'F' THEN 'FLOAT'
			WHEN c2.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c2.columntype,'')
		END AS "DB_DTYPE",
		CASE WHEN c2.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c2.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c2.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c2.columntype = 'D' THEN '('||TRIM(c2.decimaltotaldigits (FORMAT 'zzzzzzz'))||','||TRIM(c2.decimalfractionaldigits)||')'
			ELSE COALESCE(c2.columntype, '')
		END AS "DB_SIZE"
from 	MY_USHAREDB.UPGRADE_ISSUES a,
		MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
where 	a.err_no=80
and	c2.tablename = a.tablename
AND 	c2.columnname = a.columnname
AND 	c2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
GROUP BY 1,2,3,4,5,6
) as a1 (dbname, tblname,columnname, db_def,DB_DTYPE, DB_SIZE)
set hccl_or_t_def= a1.db_def
where MY_USHAREDB.upgrade_issues.tablename =a1.tblname
and MY_USHAREDB.upgrade_issues.dbname=a1.dbname
AND MY_USHAREDB.upgrade_issues.columnname=A1.columnname
AND err_no=80
;

update MY_USHAREDB.upgrade_issues
from 
(select 	c1.dbname as dbname, c1.tablename as tblname, c1.columnname,
		"USHARE_DTYPE"||''||"USHARE_SIZE" AS "USHARE_DEF",
		CASE WHEN c1.columntype = 'CV' THEN 'VARCHAR'
			WHEN c1.columntype = 'CF'    THEN 'VARCHAR'
			WHEN c1.columntype = 'CO'    THEN 'VARCHAR'
			WHEN c1.columntype = 'I' THEN 'INTEGER'
			WHEN c1.columntype = 'D' THEN 'DECIMAL'
			WHEN c1.columntype = 'DA' THEN 'DATE'
			WHEN c1.columntype = 'F' THEN 'FLOAT'
			WHEN c1.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c1.columntype,'')
		END AS "USHARE_DTYPE",
		CASE WHEN c1.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c1.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c1.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c1.columntype = 'D' THEN '('||TRIM(c1.decimaltotaldigits (FORMAT 'zzzzzzz'))||','||TRIM(c1.decimalfractionaldigits)||')'
			ELSE COALESCE(c1.columntype, '')
		END AS "USHARE_SIZE"
from 	MY_USHAREDB.upgrade_issues a,
		MY_USHAREDB.upgrade_columns c1
where 	a.err_no=80
and	c1.tablename = a.tablename
AND 	c1.columnname = a.columnname
AND 	c1.dbname in ('MY_REPORT_DB','MY_MATVIEW_DB')
GROUP BY 1,2,3,4,5,6
) as a1 (dbname, tblname,columnname, db_def,DB_DTYPE, DB_SIZE)
set ushare_or_cmps_or_s_def= a1.db_def
where MY_USHAREDB.upgrade_issues.tablename =a1.tblname
and MY_USHAREDB.upgrade_issues.dbname=a1.dbname
AND MY_USHAREDB.upgrade_issues.columnname=A1.columnname
AND err_no=80
;

DELETE 
FROM MY_USHAREDB.UPGRADE_ISSUES
where hccl_or_t_def= mfst_def
and  err_no=80
;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-80, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  80;
	
-- -------------------------------------------------------------
-- Check for table drops in manifest but table still in database
-- -------------------------------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename,  testing_rqrd, slno)
SELECT	
	CASE WHEN chg.chg_type = 'Table Drop' THEN 81 ELSE 82 END AS err_no,
	CASE WHEN chg.chg_type = 'Table Drop' 
		THEN 'Table Drop in manifest but table still in database'
		ELSE 'Warning Only - Table Deprecated in manifest but table still in database'				
	END AS err_msg, 
	'MY_RUNNAME', 'MY_REPORT_DB', 'MY_REPORT_DB',TRIM(chg.tablename) tablename, chg.testing_rqrd, chg.slno
FROM	MY_USHAREDB.UPGRADE_MANIFEST_LOAD AS chg
WHERE	chg.chg_type IN  ('Table Drop','Deprec Table')
AND 	chg.tablename NOT LIKE ALL  ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
and 	(chg.testing_Rqrd IS NULL OR chg.testing_rqrd = 'Y')
AND		EXISTS (
			SELECT	1
			FROM	MY_USHAREDB.UPGRADE_DBC_TABLES c
			WHERE	c.tablename = chg.tablename
			AND 	c.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB') )
AND		NOT EXISTS (
			SELECT	1
			FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD m2
			WHERE	m2.tablename = chg.tablename
			AND		m2.chg_type = 'View Added');;
			
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-81, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  81;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-82, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  82;
	
-- ----------------------------------------------------
-- Check for column adds in manifest but not in database
-- ----------------------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (
		err_no, err_msg, 
		runname, dbname, erroring_dbname, 
		tablename, columnname,  testing_rqrd, slno)
SELECT	83, 'Column Add in manifest but column not in database', 
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_REPORT_DB',
		TRIM(m.tablename) tablename, TRIM(m.columnname) columnname, 
		m.testing_rqrd, m.slno
FROM	MY_USHAREDB.UPGRADE_MANIFEST_LOAD m
WHERE 	m.chg_type = 'Column Add'
AND	 	m.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
		-- ------------------------------------------------
		-- ensure the table exists in the physical database
		-- ------------------------------------------------
AND		EXISTS (
			SELECT 1
			FROM	MY_USHAREDB.UPGRADE_DBC_TABLES AS t2
			WHERE	m.tablename = t2.tablename
			AND 	t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND		t2.tablekind in ('T','V'))
		-- ------------------------------------------
		-- and column not found in the database table
		-- ------------------------------------------
AND		NOT EXISTS (
			SELECT 	1 
			FROM	MY_USHAREDB.UPGRADE_DBC_COLUMNS c
			WHERE	c.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND		c.tablename = m.tablename
			AND		c.columnname = m.columnname);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-83, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  83;

-- ----------------------------------------------------------------
-- Check for column drops in manifest but column still in database
-- ----------------------------------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname, testing_rqrd, slno)
SELECT	CASE WHEN m.chg_type = 'Column Drop' THEN 84 ELSE 85 END AS err_no,
		CASE WHEN m.chg_type = 'Column Drop' THEN 'Column Drop in manifest but column still in database'
			ELSE 'Warning Only - Column Deprecated in manifest but column still in database' 
		END AS err_msg ,
		'MY_RUNNAME', 'MY_REPORT_DB', c.databasename, 
		TRIM(m.tablename) tablename, 
		TRIM(m.columnname) columnname, m.testing_rqrd, m.slno
FROM 	( 
		-- ------------------------------------------
		-- get column drop/deprecs from manifest
		-- ------------------------------------------
		SELECT	tablename, columnname, chg_type, testing_rqrd, slno
		FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD 
		WHERE	chg_type IN('Column Drop','Column Deprec')
		AND 	tablename NOT LIKE ALL  ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
		GROUP BY 1,2,3,4,5) AS m
		-- ------------------------------------------
		-- join and any columns returned are errors
		-- ------------------------------------------
		INNER JOIN MY_USHAREDB.UPGRADE_DBC_COLUMNS c
		ON c.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
		AND c.tablename = m.tablename
		AND c.columnname = m.columnname
GROUP BY 1,2,3,4,5,6,7,8,9;

	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-84, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  84;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-85, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  85;
-- ----------------------------------------------------
-- Check for Table adds in manifest but not in database
-- ----------------------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (
		err_no, err_msg, 
		runname, dbname, erroring_dbname, 
		tablename,  testing_rqrd, slno)
SELECT	86, 'Table Add in manifest but table not in database', 
		'MY_RUNNAME', 'MY_REPORT_DB',  'MY_REPORT_DB',
		TRIM(m.tablename) tablename,  
		m.testing_rqrd, m.slno
from 	MY_USHAREDB.UPGRADE_MANIFEST m
where 	m.chg_type in ('Table Add')
and 	m.tablename not in 
	(select tablename  
	FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t1
	WHERE t1.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB', 'MY_LEAD_REPORTDB')
	-- ---------------------------- 
	-- exclude test and work tables 
	-- ---------------------------- 
	AND	t1.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
	AND	t1.tablekind in ('T')
	);
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-86, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  86;

-- ----------------------------------------------------
-- Check for View adds in manifest but not in database
-- ----------------------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (
		err_no, err_msg, 
		runname, dbname, erroring_dbname, 
		tablename,  testing_rqrd, slno)
SELECT	87, m.chg_type||' in manifest but view not in database', 
		'MY_RUNNAME', 'MY_EPICDB',  'MY_EPICDB', 
		TRIM(m.tablename) tablename,  
		m.testing_rqrd, m.slno
from 	MY_USHAREDB.UPGRADE_MANIFEST m
where 	m.chg_type in ('View Added')
and 	m.tablename not in 
	(select tablename  
	FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t1
	WHERE t1.databasename IN ('MY_EPICDB')
	-- ---------------------------- 
	-- exclude test and work tables 
	-- ---------------------------- 
	AND	t1.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
	AND	t1.tablekind in ('V')
	);
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-87, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_EPICDB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname = 'MY_EPICDB'
	AND 	err_no =  87;
	
-- ----------------------------------------------------
-- Check for Column modify in manifest but table not in database
-- ----------------------------------------------------
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, 
		dbname, erroring_dbname, tablename, columnname, 
		hccl_or_t_def,  mfst_def,  testing_rqrd, slno)

SELECT	88, m.chg_type||' in manifest but table not in database', 
		'MY_RUNNAME', 'MY_REPORT_DB',  'MY_REPORT_DB', 
		TRIM(m.tablename) tablename,  m.columnname,
		m.old_datatype as hccl_or_t_def, m.new_datatype AS mfst_def, 
		
		m.testing_rqrd, m.slno
		
from 	MY_USHAREDB.UPGRADE_MANIFEST m
where 	m.chg_type like ('Column%')
and 	m.tablename not in 
	(select tablename  
	FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t1
	WHERE t1.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB','MY_LEAD_REPORTDB')
	-- ---------------------------- 
	-- exclude test and work tables 
	-- ---------------------------- 
	AND	t1.tablename NOT LIKE ALL ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/'
	AND	t1.tablekind in ('T')
	);
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-88, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  88;
		
	
	
-- ********************************** Start of Staging (_S) Table Validation ********************************************** 
-- ************************************************************************************************************************
-- Run this section after the tables have been built by the DBAs and after the DBAs fix any table errors 
-- Run one last time after all issues with structures have been fixed. 
-- ************************************************************************************************************************

-- ----------------------------- 
-- Tables exist in _T but not _S 
-- ----------------------------- 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, stg_db, tablename,
		is_preserved, on_demand, is_extracted, data_retained,cm_phy_owner_id, is_deprecated, tbl_ini)
SELECT 	101, 'Reporting table found but no _S Staging table exists' msg,
		'MY_RUNNAME', 'MY_REPORT_DB',  t1.databasename, SUBSTR(TRIM(t1.databasename),1,(character(trim(t1.databasename))-2))||'_S',
		t1.tablename,
		CASE WHEN tbl.is_preserved_yn = 'Y' THEN 'Y' ELSE NULL END AS is_preserved,
		CASE WHEN tbl.load_frequency = 'ON DEMAND'  THEN 'Y' ELSE NULL END AS on_demand,
		CASE WHEN tbl.is_extracted_yn = 'Y' THEN 'Y' ELSE NULL END AS is_extracted,
		CASE WHEN tbl.data_retained_yn = 'Y' THEN 'Y' ELSE NULL END AS data_retained,
		tbl.cm_phy_owner_id,
		tbl.deprecated_yn,
		COALESCE(NULLIF(tbl.chronicles_mf,'N/A'), NULLIF(tbl.dependent_ini,'N/A'), SUBSTR(tbl.extract_filename, 1,3)) AS tbl_ini
FROM  	MY_USHAREDB.UPGRADE_DBC_TABLES t1
		-- ---------------------------------------------
		-- only get tables for the physical owner noted
		-- ---------------------------------------------
		INNER JOIN MY_USERDB.CLARITY_TBL tbl
			ON tbl.table_name = t1.tablename
			AND tbl.cm_phy_owner_id = 'MY_OWNER_ID'
			AND tbl.is_extracted_yn = 'Y'
			AND t1.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
			-- ------------------------------------- 
			-- check if this error should be skipped 
			-- ------------------------------------- 
			AND	101 NOT IN (0)
			-- ---------------------------- 
			-- exclude test and work tables and views 
			-- ---------------------------- 
			AND	t1.tablename NOT LIKE 'BF%'
			AND	t1.tablekind = 'T'
			-- --------------------------------------------------------------------------
			-- ignore tables that are materialized views and have no staging sql
			-- --------------------------------------------------------------------------
			AND TRIM(t1.tablename) NOT IN
				('OR_CASE_APPTS_REPL','OR_LOG_PCTCMT_REPL','OR_LOG_PCTINF_REPL','OR_LOG_PNL_TM_REPL','OR_SCHED_REPL','OR_TEMPLATE_REPL')
			AND NOT EXISTS (
				SELECT 	1 
				FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t2
				WHERE	t2.databasename = SUBSTR(TRIM(t1.databasename),1,(character(trim(t1.databasename))-2))||'_S'
				AND 	t2.tablename = t1.tablename);
		
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-101, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  101;

-- ------------------------------ 
-- Tables exist in _S but not _T 
-- ------------------------------ 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, stg_db, stg_table,
		is_preserved, on_demand, is_extracted, data_retained,cm_phy_owner_id, is_deprecated, tbl_ini)
SELECT 	102, 'Staging table found but no Reporting table exists' msg,
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_REPORT_DB', t1.databasename, t1.tablename,
		CASE WHEN tbl.is_preserved_yn = 'Y' THEN 'Y' ELSE NULL END AS is_preserved,
		CASE WHEN tbl.load_frequency = 'ON DEMAND'  THEN 'Y' ELSE NULL END AS on_demand,
		CASE WHEN tbl.is_extracted_yn = 'Y' THEN 'Y' ELSE NULL END AS is_extracted,
		CASE WHEN tbl.data_retained_yn = 'Y' THEN 'Y' ELSE NULL END AS data_retained,
		tbl.cm_phy_owner_id,
		tbl.deprecated_yn,
		COALESCE(NULLIF(tbl.chronicles_mf,'N/A'), NULLIF(tbl.dependent_ini,'N/A'), SUBSTR(tbl.extract_filename, 1,3)) AS tbl_ini
FROM  	MY_USHAREDB.UPGRADE_DBC_TABLES t1
		-- -------------------------------------------------
		-- only check valid Clarity Tables
		-- -------------------------------------------------
		INNER JOIN 	MY_USERDB.CLARITY_TBL tbl
		ON 	tbl.table_name = t1.tablename
		AND tbl.cm_phy_owner_id = 'MY_OWNER_ID'
		AND t1.databasename IN ('MY_STAGE_DB')
		-- -------------------------------------
		-- check if this error should be skipped 
		-- ------------------------------------- 
		AND 102 NOT IN (0)
		-- ---------------------------- 
		-- exclude test & work tables and views
		-- ---------------------------- 
		AND (TRIM(t1.tablename) NOT LIKE ALL('BF%','UPD%','UTL%','%/_ERROR1', '%/_ERROR2', '%/_DELETE','%/_UPDATE','%/_DELETE_CT','%/_UPDATE_CT') ESCAPE '/' )
		AND	t1.tablekind in ('T')
		-- -------------------------------------------------
		-- AND table not in any of the reporting databases.
		-- -------------------------------------------------
		AND NOT EXISTS (
			SELECT 	1 
			FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t2
			WHERE 	t2.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB','MY_LEAD_REPORTDB')
			AND 	t2.tablename = t1.tablename);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-102, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  102;

-- ------------------------------ 
-- columns exist in _T but not _S (missing column) 
-- LOGIC: 
-- get tables in both _S and _T databases and all associated _T columns. 
-- MINUS 
-- get tables in both _S and _T databases and all associated _S columns. 
-- ------------------------------ 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname, stg_db, stg_table, tbl_ini)
SELECT 	103, 'Reporting table has column not found in staging table.' msg,'MY_RUNNAME', 'MY_REPORT_DB', 
		t2.databasename, t2.tablename, UPPER(TRIM(t2.columnname)), t3.databasename, t2.tablename,
		COALESCE(NULLIF(tbl.chronicles_mf,'N/A'), NULLIF(tbl.dependent_ini,'N/A'), SUBSTR(tbl.extract_filename, 1,3)) AS tbl_ini
FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS t2
		-- ---------------------------------------------
		-- get dbname based on physical owner id and env
		-- ---------------------------------------------
		INNER JOIN MY_USHAREDB.UPGRADE_DB_OWNER_LINK link
			ON 	link.cm_phy_owner_id = 'MY_OWNER_ID'
			AND link.env = 'MY_ENV'
			AND link.rpt_db = t2.databasename
		-- --------------------------------------------------------------------
		-- this join ensures we only report on reporting tables that have a staging table.
		-- otherwise all columns in a table in reporting table would error
		-- --------------------------------------------------------------------
		INNER JOIN MY_USHAREDB.UPGRADE_DBC_TABLES t3
			ON t3.databasename = link.stg_db
			AND t2.databasename = link.rpt_db
			AND t3.tablename = t2.tablename
			AND t3.tablekind in ('T')
			AND 103 NOT IN (0)
		-- ------------------------------------------------
		-- only check tables that are valid in the compass
		-- ------------------------------------------------
		INNER JOIN (
			SELECT 	table_name, chronicles_mf, dependent_ini, extract_filename
			FROM 	MY_USERDB.CLARITY_TBL
			WHERE	cm_phy_owner_id in ('MY_OWNER_ID', '9001')
			AND		is_extracted_yn = 'Y'
			AND 	table_name not like 'BF%'
			GROUP BY 1,2,3,4
			) AS tbl
			ON tbl.table_name = t2.tablename
WHERE NOT EXISTS (
	SELECT 1 
	FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c
	WHERE	c.databasename = t3.databasename
	AND		c.tablename = t2.tablename
	AND 	c.columnname = t2.columnname)
GROUP BY 1,2,3,4,5,6,7,8,9,10;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-103, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  103;
	
-- --------------------------------------------------------------------------------------------------- 
-- columns exist in _S but not _T (extra column) 
-- LOGIC: 
-- get tables in both _S and _T databases (correcting for job division) and all associated _S columns. 
-- MINUS 
-- get tables in both _S and _T databases (correcting for job division) and all associated _T columns. 
-- --------------------------------------------------------------------------------------------------- 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, tablename, columnname, stg_db, stg_table, tbl_ini)
SELECT 	104, 'Staging table has column not found in Reporting Table' msg,
		'MY_RUNNAME', 'MY_REPORT_DB', t2.tablename, UPPER(TRIM(t2.columnname)), t2.databasename, t2.tablename,
		COALESCE(NULLIF(tbl.chronicles_mf,'N/A'), NULLIF(tbl.dependent_ini,'N/A'), SUBSTR(tbl.extract_filename, 1,3)) AS tbl_ini
FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS t2
		-- ---------------------------------------------
		-- get dbname based on physical owner id and env
		-- ---------------------------------------------
		INNER JOIN MY_USHAREDB.UPGRADE_DB_OWNER_LINK link
			ON 	link.cm_phy_owner_id = 'MY_OWNER_ID'
			AND link.env = 'MY_ENV'
			AND link.stg_db = t2.databasename
		-- --------------------------------------------------------------------
		-- this join ensures we only report on reporting tables that have a staging table.
		-- otherwise all columns in a table in reporting and not in staging would error
		-- --------------------------------------------------------------------
		INNER JOIN (
			SELECT	tablename
			FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES 
			WHERE	databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
			GROUP BY 1) t3
		ON t2.databasename = link.stg_db
		AND t2.tablename = t3.tablename
		AND 103 NOT IN (0)
		-- ---------------------------- 
		-- exclude test and work tables 
		-- --------------------------- 
		AND (TRIM(t2.tablename) NOT LIKE ALL('BF%','%/_ERROR1', '%/_ERROR2', '%/_DELETE','%/_UPDATE','%/_DELETE_CT','%/_UPDATE_CT') ESCAPE '/' )
		-- ------------------------------------------------
		-- only check tables that are valid in the compass
		-- ------------------------------------------------
		INNER JOIN (
			SELECT 	table_name, chronicles_mf, dependent_ini, extract_filename
			FROM 	MY_USERDB.CLARITY_TBL
			WHERE	cm_phy_owner_id in ('MY_OWNER_ID', '9001')
			AND		is_extracted_yn = 'Y'
			AND 	table_name not like 'BF%'
			GROUP BY 1,2,3,4
			) AS tbl
		ON tbl.table_name = t2.tablename
WHERE NOT EXISTS (
	SELECT 	1 
	FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c
	WHERE	c.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
	AND		c.tablename = t2.tablename
	AND 	c.columnname = t2.columnname)
GROUP BY 1,2,3,4,5,6,7,8,9;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-104, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  104;

-- ------------------------------------------------------------------------------------ 
-- Column Checks - Check for columns definition differences between  _S and _T tables 
-- ------------------------------------------------------------------------------------ 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, stg_db, stg_table, columnname, hccl_or_T_def,
		ushare_or_cmps_or_S_def, hccl_or_T_cmprs, ushare_or_cmps_or_S_cmprs, tbl_ini, col_fmt_ini, col_fmt_item)
SELECT	a.err_no, a.difference_type AS error_msg, 'MY_RUNNAME', 'MY_REPORT_DB', a.rpting_db, a.tablename, a.staging_db, a.staging_tbl, a.columnname,
		a.T_def, a.S_def,a.T_compress, a.S_compress, a.tbl_ini, a.col_fmt_ini, a.col_fmt_item
FROM (SELECT	
		CASE	WHEN "T_DTYPE" <> "S_DTYPE" THEN 105
			WHEN "T_SIZE" <> "S_SIZE" THEN 106
			WHEN "T_NULLABLE" <> "S_NULLABLE" THEN 107
			WHEN "T_COMPRESS" <> "S_COMPRESS" THEN 108
			ELSE NULL
		END AS "ERR_NO",
		CASE	WHEN "T_DTYPE" <> "S_DTYPE" 	THEN 'Staging to Reporting DB - COLUMN Datatype difference.'
			WHEN "T_SIZE" <> "S_SIZE" 			THEN 'Staging to Reporting DB - COLUMN Size difference.'
			WHEN "T_NULLABLE" <> "S_NULLABLE" 	THEN 'Staging to Reporting DB - COLUMN Nullable difference.'
			WHEN "T_COMPRESS" <> "S_COMPRESS" 	THEN 'Staging to Reporting DB - COLUMN Compressible difference.'
			ELSE NULL
		END AS "difference_type",
		UPPER(TRIM(c1.tablename)) AS Tablename,
		c1.databasename AS rpting_db,
		stg.databasename AS staging_db,
		UPPER(TRIM(stg.tablename)) AS staging_tbl,
		stg.columnname,
		"T_DTYPE"||' '||"T_SIZE"||' '||"T_NULLABLE" AS "T_DEF",
		"S_DTYPE"||' '||"S_SIZE"||' '||"S_NULLABLE" AS "S_DEF",
		TRIM(c1.compressible) AS "T_COMPRESS",
		TRIM(stg.compressible) AS "S_COMPRESS",
		CASE	WHEN c1.columntype = 'CV' THEN 'VARCHAR'
			WHEN c1.columntype = 'CF'    THEN 'VARCHAR'
			WHEN c1.columntype = 'CO'    THEN 'VARCHAR'
			WHEN c1.columntype = 'I' THEN 'INTEGER'
			WHEN c1.columntype = 'D' THEN 'DECIMAL'
			WHEN c1.columntype = 'DA' THEN 'DATE'
			WHEN c1.columntype = 'F' THEN 'FLOAT'
			WHEN c1.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c1.columntype,'')
		END AS "T_DTYPE",
		CASE	WHEN c1.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c1.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c1.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c1.columntype = 'D' THEN '('||TRIM(c1.decimaltotaldigits)||', '||TRIM(c1.decimalfractionaldigits)||')'
			ELSE COALESCE(c1.columntype,'')
		END AS "T_SIZE",
		CASE 	WHEN c1.nullable = 'Y' THEN 'NULL' ELSE 'NOT NULL' END AS "T_NULLABLE",
		CASE	WHEN stg.columntype = 'CV' THEN 'VARCHAR'
			WHEN stg.columntype = 'CF'    THEN 'VARCHAR'
			WHEN stg.columntype = 'CO'    THEN 'VARCHAR'
			WHEN stg.columntype = 'I' THEN 'INTEGER'
			WHEN stg.columntype = 'D' THEN 'DECIMAL'
			WHEN stg.columntype = 'DA' THEN 'DATE'
			WHEN stg.columntype = 'F' THEN 'FLOAT'
			WHEN stg.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(stg.columntype,'')
		END AS "S_DTYPE",
		CASE	WHEN stg.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(stg.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN stg.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN stg.columntype = 'D' THEN '('||TRIM(stg.decimaltotaldigits)||', '||TRIM(stg.decimalfractionaldigits)||')'
			ELSE COALESCE(stg.columntype, '')
		END AS "S_SIZE",
		CASE WHEN stg.nullable = 'Y' THEN 'NULL' ELSE 'NOT NULL' END  AS "S_NULLABLE",
		col.format_ini AS tbl_ini,
		col.format_ini AS col_fmt_ini,
		col.format_item AS col_fmt_item
	FROM MY_USHAREDB.UPGRADE_DBC_COLUMNS c1
		-- ---------------------------------------------
		-- get reporting dbname based on physical owner id and env
		-- ---------------------------------------------
		INNER JOIN MY_USHAREDB.UPGRADE_DB_OWNER_LINK link
			ON 	link.cm_phy_owner_id = 'MY_OWNER_ID'
			AND link.env = 'MY_ENV'
			AND link.rpt_db = c1.databasename
		-- -------------------------------------------------------------------- 
		-- get table.columns that are in both _S and _T tables 
		-- and if there is a difference in nullability, data type or data size 
		-- note it as is a column difference. 
		-- -------------------------------------------------------------------- 
		-- ------------------------------------------------------------ 
		-- get staging tables column info
		-- ------------------------------------------------------------ 
		INNER JOIN MY_USHAREDB.UPGRADE_DBC_COLUMNS stg 
				ON c1.tablename = stg.tablename
				AND c1.columnname = stg.columnname
				AND link.stg_db = stg.databasename 
				AND 105 NOT IN (0)
		INNER JOIN MY_USERDB.CLARITY_COL col
			ON 	col.cm_phy_owner_id = 'MY_OWNER_ID'
			AND col.col_descriptor = TRIM(c1.tablename)||'__'||TRIM(c1.columnname)
			AND (  (TRIM(c1.columntype) <> TRIM(stg.columntype) AND 105 NOT IN (0))
				OR (ZEROIFNULL(TRIM(c1.columnlength)) <> ZEROIFNULL(TRIM(stg.columnlength)) AND 106 NOT IN (0))
				OR (COALESCE(TRIM(c1.nullable),'Y') <> COALESCE(TRIM(stg.nullable),'Y') AND 107 NOT IN (0))
				OR (COALESCE(TRIM(c1.compressible),'Y') <> COALESCE(TRIM(stg.compressible),'Y')  AND 108 NOT IN (0))
				OR (ZEROIFNULL(TRIM(c1.decimaltotaldigits)) <> ZEROIFNULL(TRIM(stg.decimaltotaldigits)) AND 106 NOT IN (0))
				OR (ZEROIFNULL(TRIM(c1.decimalfractionaldigits)) <> ZEROIFNULL(TRIM(stg.decimalfractionaldigits)) AND 106 NOT IN (0)))
	)AS a;	
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-105, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  105;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-106, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  106;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-107, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  107;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-108, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  108;

-- --------------------------------------------------------------------------------------------
-- Error 109: Check for tables in USHARE but not in database (staging table dropped)
-- also excludes tables noted as 'table drop' in the manifest
-- --------------------------------------------------------------------------------------------

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, issue_comment)
SELECT 	109, 'Staging Table in USHARE list but not in staging database' msg, 'MY_RUNNAME', 'MY_REPORT_DB', 
		dbol.stg_db, t1.tablename, 
		CASE 
			WHEN mt.chg_type = 'Table Drop' and mt.recs = 1 then 'Table Dropped per Upgrade Manifest.' 
			WHEN mt.chg_type = 'Table Drop' and mt.recs = 2 Then 'Table converted to view per Upgrade Manifest'
		END as issue_comment
FROM	MY_USHAREDB.UPGRADE_STG_TBLS t1
		-- ----------------------------------------
		-- only include tables for this deployment.
		-- ----------------------------------------
		INNER JOIN MY_USHAREDB.UPGRADE_DB_OWNER_LINK dbol
		ON t1.cm_phy_owner_id = dbol.cm_phy_owner_id
		AND t1.cm_phy_owner_id = 'MY_OWNER_ID'
		AND dbol.env = 'MY_ENV'
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
		AND 109 NOT IN (0)
		-- --------------------
		-- skip ETL work tables
		-- --------------------
		AND TRIM(t1.tablename) NOT LIKE ALL('BF%','%/_ERROR1', '%/_ERROR2', 'Token%') ESCAPE '/' 
		-- -------------------------------
		-- table not in staging database
		-- -------------------------------
		AND NOT EXISTS (
			SELECT 	1
			FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t2
			WHERE 	t2.databasename in ('MY_STAGE_DB')
			AND 	t2.tablename = t1.tablename
			AND 	t2.tablekind IN ('T'))
		-- ---------------------------------------------------------------------------
		-- Identify if table is supposed to be a table drop.  They will be in the 
		-- ushare list when running validation in MY_ENV but not in the database
		-- ---------------------------------------------------------------------------
		LEFT OUTER JOIN  (
			select 	tablename, case when chg_type = 'View Added' then 'Table Drop' end as chg_type, count(*) as recs
			FROM 	MY_USHAREDB.UPGRADE_MANIFEST_LOAD 
			WHERE	chg_type in ('Table Drop' , 'View Added','Table Added')
			AND 	(testing_Rqrd IS NULL OR testing_rqrd = 'Y')
			GROUP BY 1,2) as mt
		ON  mt.tablename = t1.tablename
		AND t1.cm_phy_owner_id = 'MY_OWNER_ID';

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-109, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  109;

-- --------------------------------------------------------------------------------------------
-- Error 110: Check for staging tables in database but not in USHARE table (staging table added)
-- Only checks tables not in USHARE and NOT in the manifest.
-- --------------------------------------------------------------------------------------------

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, stg_db, tablename, issue_comment)
SELECT 	110, 'Table in staging database but not in USHARE staging list' msg,
		'MY_RUNNAME', 'MY_REPORT_DB', t1.databasename, t1.databasename, t1.tablename, 
		CASE WHEN mt.tablename is not null then TRIM(mt.chg_type)||' per Upgrade Manifest' END as issue_comment
FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t1
		-- ----------------------------------------
		-- only include tables for this deployment.
		-- ----------------------------------------
		INNER JOIN MY_USHAREDB.UPGRADE_DB_OWNER_LINK dbol
		ON dbol.cm_phy_owner_id = 'MY_OWNER_ID'
		AND dbol.env = 'MY_ENV'
		AND t1.databasename in ('MY_STAGE_DB')
		AND t1.tablekind IN ('T','V')
		AND	110 NOT IN (0)
		-- ---------------------------- 
		-- exclude test and work tables 
		-- ---------------------------- 
		AND TRIM(t1.tablename) NOT LIKE ALL	('BF%','%/_ERROR1','%/_ERROR2','TOKEN%') ESCAPE '/' 
		-- ---------------------------------------------------------------------------
		-- Identify if table is supposed to be a table adds.  They will not be in the 
		-- ushare list when running validation in MY_ENV but will be when run in PROD.
		-- ---------------------------------------------------------------------------
		LEFT OUTER JOIN  (
			SELECT	tablename, case when chg_type = 'View Added' then 'Table Drop' end as chg_type, count(*) as recs
			FROM	MY_USHAREDB.UPGRADE_MANIFEST_LOAD
			WHERE 	chg_type in ('Table Drop' , 'View Added','Table Added')
			GROUP BY 1,2) as mt
		ON mt.tablename = t1.tablename
		AND t1.databasename in ('MY_STAGE_DB')
		AND t1.tablekind IN ('T','V')
		AND t1.tablename not like 'TOKEN%'
WHERE	NOT EXISTS (
			SELECT	1 
			FROM	MY_USHAREDB.UPGRADE_STG_TBLS t2 
			WHERE 	t2.cm_phy_owner_id = 'MY_OWNER_ID'
			AND		t2.tablename = t1.tablename
			AND		t1.databasename in ('MY_STAGE_DB'))
GROUP BY 1,2,3,4,5,6,7,8;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-110, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no = 110;

-- --------------------------------------------------------- 
-- Check for columns missing from HCCL database 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT 	112 ,	'Column in USHARE table but not in Staging (_S) database' msg, 'MY_RUNNAME',
		'MY_STAGE_DB', c1.dbname, c1.tablename, c1.columnname
FROM 	MY_USHAREDB.upgrade_stg_columns c1
		INNER JOIN MY_USHAREDB.UPGRADE_DBC_TABLES t2
		ON c1.tablename = t2.tablename
		AND t2.databasename in ('MY_STAGE_DB')
		AND t2.tablekind in ('T','V')
		AND c1.cm_phy_owner_id IN ('MY_OWNER_ID') 
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
		AND	112 NOT IN (0)
		-- ---------------------------- 
		-- exclude test and work tables 
		-- ---------------------------- 
		AND	c1.tablename NOT LIKE 'BF%'
WHERE	NOT EXISTS (
			SELECT 1
			FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
			WHERE 	c2.tablename = t2.tablename
			AND		c2.columnname = c1.columnname
			AND 	c2.databasename = t2.databasename)
GROUP BY 1,2,3,4,5,6,7;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-112, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_STAGE_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM 	MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname = 'MY_STAGE_DB'
	AND 	err_no = 112;
	
-- --------------------------------------------------------- 
-- Check for extra columns in HCCL database 
-- --------------------------------------------------------- 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT 	113,'Column in Staging database but not in USHARE list' msg,
		'MY_RUNNAME', 'MY_STAGE_DB', c1.databasename, t1.tablename, UPPER(TRIM(c1.columnname)) colname
FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c1
		-- ------------------------------------------------ 
		-- only check tables if in USHARE list and database 
		-- ------------------------------------------------ 
		INNER JOIN (
			SELECT dbname, tablename 
			FROM MY_USHAREDB.upgrade_stg_columns 
			WHERE cm_phy_owner_id IN ('MY_OWNER_ID', '9001') 
			GROUP BY 1,2) AS t1
		ON UPPER(TRIM(c1.tablename)) = t1.tablename
		AND UPPER(TRIM(c1.databasename)) in ('MY_STAGE_DB')
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
		AND 113 NOT IN (0)		
		-- ------------------------------------------------ 
		-- check tbl/col not in ushare list
		-- ------------------------------------------------ 		
		AND NOT EXISTS (
			SELECT 	1
			FROM 	MY_USHAREDB.upgrade_stg_columns c3
			WHERE 	c3.tablename = c1.tablename
			AND		c3.columnname = c1.columnname
			AND 	c3.cm_phy_owner_id IN ('MY_OWNER_ID', '9001') );

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-113, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_STAGE_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname = 'MY_STAGE_DB'
	AND 	err_no =  113;

--- --------------------------------------------------------- 
-- Check all PROD columns definitions match PROD. 
-- --------------------------------------------------------- 
-- Note: not all tables have a view with the same name. 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues
	(err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname, ushare_or_cmps_or_S_def, hccl_or_T_def, ushare_or_cmps_or_S_cmprs,
	hccl_or_T_cmprs, is_preserved, on_demand)
SELECT  a.err_no, a.change_type AS error_msg, 'MY_RUNNAME', 'MY_STAGE_DB', a.databasename, a.tablename, a.columnname, a.ushare_def, a.hccl_def,
		a.ushare_compressible,a.hccl_compressible, a.is_preserved, on_demand
FROM (SELECT
		t2.databasename,
		c1.tablename,
		c1.columnname,
		CASE WHEN col.is_preserved_yn = 'Y' OR tbl.is_preserved_yn = 'Y' THEN 'Y' ELSE NULL END AS is_preserved,
		CASE WHEN tbl.load_frequency = 'ON DEMAND' THEN 'Y' ELSE NULL END AS on_demand,
		"USHARE_DTYPE"||' '||"USHARE_SIZE"||' '||"USHARE_NULLABLE" AS "USHARE_DEF",
		"HCCL_DTYPE"||' '||"HCCL_SIZE"||' '||"HCCL_NULLABLE" AS "HCCL_DEF",
		CASE WHEN c1.columntype = 'CV' THEN 'VARCHAR'
			WHEN c1.columntype = 'CF'  THEN 'VARCHAR'
			WHEN c1.columntype = 'CO' THEN 'VARCHAR'
			WHEN c1.columntype = 'I' THEN 'INTEGER'
			WHEN c1.columntype = 'D' THEN 'DECIMAL'
			WHEN c1.columntype = 'DA' THEN 'DATE'
			WHEN c1.columntype = 'F' THEN 'FLOAT'
			WHEN c1.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c1.columntype,'')
		END AS "USHARE_DTYPE",
		CASE WHEN c1.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c1.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c1.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c1.columntype = 'D' THEN '('||TRIM(c1.decimaltotaldigits (FORMAT 'zzzzzzz'))||', '||TRIM(c1.decimalfractionaldigits)||')'
			ELSE COALESCE(c1.columntype,'')
		END AS "USHARE_SIZE",
		CASE WHEN c1.nullable = 'Y' THEN 'NULL' ELSE 'NOT NULL' END AS "USHARE_NULLABLE",
		TRIM(c1.compressible) AS "USHARE_COMPRESSIBLE",
		CASE WHEN c2.columntype = 'CV' THEN 'VARCHAR'
			WHEN c2.columntype = 'CF'    THEN 'VARCHAR'
			WHEN c2.columntype = 'CO' THEN 'VARCHAR'
			WHEN c2.columntype = 'I' THEN 'INTEGER'
			WHEN c2.columntype = 'D' THEN 'DECIMAL'
			WHEN c2.columntype = 'DA' THEN 'DATE'
			WHEN c2.columntype = 'F' THEN 'FLOAT'
			WHEN c2.columntype = 'TS' THEN 'TIMESTAMP'
			ELSE COALESCE(c2.columntype,'')
		END AS "HCCL_DTYPE",
		CASE WHEN c2.columntype IN ('CO', 'CV', 'CF') THEN '('||TRIM(c2.columnlength (FORMAT 'zzzzzzz'))||')'
			WHEN c2.columntype IN ('I', 'DA', 'F', 'TS') THEN ''
			WHEN c2.columntype = 'D' THEN '('||TRIM(c2.decimaltotaldigits (FORMAT 'zzzzzzz'))||', '||TRIM(c2.decimalfractionaldigits)||')'
			ELSE COALESCE(c2.columntype, '')
		END AS "HCCL_SIZE",
		CASE WHEN c2.nullable = 'Y' THEN 'NULL' ELSE 'NOT NULL' END  AS "HCCL_NULLABLE",
		TRIM(c2.compressible) AS "HCCL_COMPRESSIBLE",
		CASE WHEN "USHARE_DTYPE" <> "HCCL_DTYPE" THEN 'HCCL-USHARE Column Datatype difference.'
			WHEN "USHARE_SIZE" <> "HCCL_SIZE" THEN 'HCCL-USHARE Column Size difference.'
			WHEN "USHARE_NULLABLE" <> "HCCL_NULLABLE" THEN 'HCCL-USHARE Column nullable difference.'
			WHEN "USHARE_COMPRESSIBLE" <> "HCCL_COMPRESSIBLE" THEN 'HCCL-USHARE Column compressible difference.'
			ELSE NULL
		END AS "CHANGE_TYPE",
		CASE WHEN "USHARE_DTYPE" <> "HCCL_DTYPE" THEN 116
			WHEN "USHARE_SIZE" <> "HCCL_SIZE" THEN 117
			WHEN "USHARE_NULLABLE" <> "HCCL_NULLABLE" THEN 118
			WHEN "USHARE_COMPRESSIBLE" <> "HCCL_COMPRESSIBLE" THEN 119
			ELSE NULL
		END AS "ERR_NO"
    FROM 	MY_USHAREDB.upgrade_stg_columns c1
			-- --------------------------------------------------------------------- 
			-- join to clarity compass tables to get preserved and on-demand flags 
			-- --------------------------------------------------------------------- 
			LEFT OUTER JOIN MY_USERDB.CLARITY_TBL tbl
				ON UPPER(TRIM(tbl.table_name)) = c1.tablename
				AND tbl.cm_phy_owner_id = 'MY_OWNER_ID'
				AND tbl.is_extracted_yn = 'Y'
				AND tbl.cm_phy_owner_id = c1.cm_phy_owner_id		
				AND c1.cm_phy_owner_id = 'MY_OWNER_ID'
			LEFT OUTER JOIN MY_USERDB.CLARITY_COL col
				ON col.TABLE_ID = tbl.TABLE_ID
				AND col.cm_phy_owner_id = tbl.cm_phy_owner_id
				AND UPPER(TRIM(col.column_name)) = c1.columnname
				AND col.cm_phy_owner_id = 'MY_OWNER_ID'
				AND c1.cm_phy_owner_id = 'MY_OWNER_ID' 
				AND col.is_extracted_yn = 'Y'
			-- -------------------------------------------------------------------- 
			-- only get comparison for table columns not view columns 
			-- -------------------------------------------------------------------- 
			INNER JOIN MY_USHAREDB.UPGRADE_DBC_TABLES t2
				ON t2.databasename in ('MY_STAGE_DB')
				AND	c1.tablename = t2.tablename
				AND c1.cm_phy_owner_id = 'MY_OWNER_ID'
				AND t2.tablekind in ('T','V')
			-- -------------------------------------------------------------------- 
			-- get table.columns that are in both new and old release 
			-- and if there is a difference in nullability, data type or data size 
			-- return it - this is a column change. 
			-- -------------------------------------------------------------------- 
			INNER JOIN MY_USHAREDB.UPGRADE_DBC_COLUMNS c2
				ON  c1.tablename = c2.tablename
				AND c1.columnname = c2.columnname
				AND UPPER(TRIM(c2.databasename)) in ('MY_STAGE_DB')
				AND ((TRIM(c1.columntype) <> TRIM(TRIM(c2.columntype)) AND 6 NOT IN (0))
					OR (ZEROIFNULL(TRIM(c1.columnlength)) <> ZEROIFNULL(TRIM(c2.columnlength)) AND 117 NOT IN (0))
					OR (COALESCE(TRIM(c1.nullable),'Y') <> COALESCE(TRIM(c2.nullable),'Y') AND 118 NOT IN (0))
					OR (ZEROIFNULL(TRIM(c1.decimaltotaldigits)) <> ZEROIFNULL(TRIM(c2.decimaltotaldigits)) AND 117 NOT IN (0))
					OR (ZEROIFNULL(TRIM(c1.decimalfractionaldigits)) <> ZEROIFNULL(TRIM(c2.decimalfractionaldigits)) AND 117 NOT IN (0))
					OR (COALESCE(TRIM(c1.compressible),'') <> COALESCE(TRIM(c2.compressible),'')) AND 119 NOT IN (0))        
				-- ---------------------------- 
				-- exclude test and work tables 
				-- ---------------------------- 
				AND TRIM(c1.tablename) NOT LIKE ALL ('BF%','%/_DELETE', '%/_DELETE/_CT','%/_UPDATE/_CT','%/_UPDATE', 'UPD%','UTL%') ESCAPE '/' 
	) AS a;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-116, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_STAGE_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname = 'MY_STAGE_DB'
	AND 	err_no =  116;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-117, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_STAGE_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname = 'MY_STAGE_DB'
	AND 	err_no =  117;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-118, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_STAGE_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname = 'MY_STAGE_DB'
	AND 	err_no =  118;
	
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-119, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_STAGE_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname = 'MY_STAGE_DB'
	AND 	err_no =  119;		
-- ********************************** End of (_S) Structure Validation ********************************************** 
-- ********************************** Start of Epic View Validation ********************************************** 

-- --------------------------------------------------------- 
-- Check for tables that may be missg views 
-- --------------------------------------------------------- 
-- Note: not all tables have a view with the same name. 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename)
SELECT	201, 'Table does not appear to have a matching middle (Epic) layer view' err_msg,
		'MY_RUNNAME', 'MY_REPORT_DB', t2.databasename, t2.tablename
FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES AS t2
WHERE 	t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
AND		t2.tablekind in ('V','T')
		-- ------------------------------------------------------------
		-- only check for view it the table is valid for the deployment
		-- ------------------------------------------------------------
AND		EXISTS (
		SELECT	1
		FROM	MY_USERDB.CLARITY_TBL AS tbl
		WHERE	tbl.cm_phy_owner_id = 'MY_OWNER_ID'
		AND 	tbl.is_extracted_yn = 'Y'
		AND		tbl.table_name not like ALL ('V/_%','BF%') ESCAPE '/'
		AND	 	tbl.table_name = t2.tablename
		AND		t2.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB'))
	-- ------------------------------------- 
	-- check if this error should be skipped  
	-- ------------------------------------- 
	AND 201 NOT IN (0)
	-- ---------------------------- 
	-- exclude derived views 
	-- ---------------------------- 
	AND TRIM(t2.tablename) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
		'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS', 'CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
		'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
		'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
		'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
		'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
		'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_REQUESTER_CREATION', 'V_ROI_STATUS_HISTORY','V_ZC_CANCEL_REASON'
		,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

)
	-- ------------------------------------------------------------
	-- exclude tables that do not have views - views are union alls
	-- -------------------------------------------------------------
	AND	t2.tablename NOT LIKE ALL ('OR_BLOCK%','OR_SCHED%','OR_STFF_BLK%','OR_TEMPLATE%','OR_STAFF_BL%','OR_LOG_PNLCNT%','OR_CASE_APPTS%','OR_LOG_PANEL%')
	-- ---------------------------- 
	-- not in epic layer.
	-- ---------------------------- 
	AND NOT EXISTS (
		SELECT 	1 
		FROM	MY_USHAREDB.UPGRADE_DBC_TABLES AS t3 
		WHERE 	t3.databasename = 'MY_EPICDB' 
		AND 	t3.tablename = t2.tablename)
GROUP BY 1,2,3,4,5,6;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-201, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  201;

-- --------------------------------------------------------- 
-- Check for views that may be missing base tables 
-- Note: not all tables have a view with the same name. 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname,  erroring_dbname, tablename)
SELECT	202, 'Middle layer (Epic) view does not appear to have a matching reporting table.' err_msg,
	'MY_RUNNAME', 'MY_REPORT_DB', 'MY_EPICDB', t2.tablename
FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t2
WHERE t2.databasename = 'MY_EPICDB'
	-- ------------------------------------------------------------
	-- only check for table if the table is valid for the deployment
	-- ------------------------------------------------------------
AND EXISTS (
		SELECT 1 
		FROM 	MY_USERDB.CLARITY_TBL AS tbl
		WHERE	tbl.cm_phy_owner_id = 'MY_OWNER_ID'
		AND		tbl.is_extracted_yn = 'Y'
		AND		tbl.table_name not like all ('V/_%','BF%') escape '/'
		AND		t2.tablename = tbl.table_name
		AND		t2.databasename = 'MY_EPICDB') 
	-- ------------------------------------- 
	-- check if this error should be skipped  
	-- ------------------------------------- 
	AND 202 NOT IN (0)
	-- ----------------------------------------------------------------------
	-- ignore tables that are only created as views in the middle/epic layer
	-- ----------------------------------------------------------------------
	AND TRIM(t2.tablename) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
	'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS', 'CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
	'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
	'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
	'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
	'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
	'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_REQUESTER_CREATION', 'V_ROI_STATUS_HISTORY','V_ZC_CANCEL_REASON'
	,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

)
	-- ------------------------------------------------------------
	-- exclude tables that do not have views - views are union alls
	-- -------------------------------------------------------------
	AND	t2.tablename NOT LIKE ALL (
		'OR_BLOCKUTIL%','OR_SCHED%','OR_STFF_BLK%','OR_TEMPLATE%','OR_STAFF_BL%','OR_LOG_PNLCNT%','OR_CASE_APPTS%','OR_LOG_PANEL%')					
	AND NOT EXISTS (
		SELECT 1
		FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t3
		WHERE 	t3.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB', 'MY_LEAD_REPORTDB')
		AND 	t3.tablename = t2.tablename);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-202, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  202;

-- --------------------------------------------------------- 
-- Check for tables that have more columns than associated view 
-- view needs recompile or redefinition 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT 	203, 'Column in reporting table but not in Middle (Epic) layer view.' AS msg,
	'MY_RUNNAME', 'MY_REPORT_DB', trim(c2.databasename), trim(c2.tablename), trim(c2.columnname)
FROM	MY_USHAREDB.UPGRADE_DBC_COLUMNS AS c2
	-- ------------------------------------------------------------
	-- only check for table if the table is valid for the deployment
	-- ------------------------------------------------------------
	INNER JOIN (
		SELECT 	table_name
		FROM 	MY_USERDB.CLARITY_TBL
		WHERE	cm_phy_owner_id = 'MY_OWNER_ID'
		AND		is_extracted_yn = 'Y'
		AND		table_name not like all ('V/_%','BF%') escape '/'
		) AS tbl
	ON 	tbl.table_name = c2.tablename
	AND c2.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB') 
	-- -----------------------------------------------------------------------
	-- Only get tables where the table exists in the reporting and epic layers
	-- -----------------------------------------------------------------------
	INNER JOIN MY_USHAREDB.UPGRADE_DBC_TABLES t1
	ON t1.databasename = 'MY_EPICDB'
	AND t1.tablename = c2.tablename
	AND c2.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB') 
	-- ------------------------------------- 
	-- check if this error should be skipped  
	-- ------------------------------------- 
	AND 203 NOT IN (0)
	-- ---------------------------- 
	-- exclude derived views 
	-- ---------------------------- 
	AND TRIM(c2.tablename) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
	'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS', 'CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
	'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
	'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
	'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
	'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
	'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_REQUESTER_CREATION', 'V_ROI_STATUS_HISTORY', 'V_ZC_CANCEL_REASON'
	
	,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

)		
	-- ------------------------------------------------------------
	-- exclude tables that do not have views - views are union alls
	-- -------------------------------------------------------------
	AND	c2.tablename NOT LIKE ALL ('OR_BLOCKUTIL%','OR_SCHED%','OR_STFF_BLK%','OR_TEMPLATE%','OR_STAFF_BL%','OR_LOG_PNLCNT%','OR_CASE_APPTS%','OR_LOG_PANEL%')
	-- ---------------------------- 
	-- exclude test and work tables 
	-- ---------------------------- 
	AND c2.tablename not like ALL ('V/_%','BF%') ESCAPE '/'
WHERE NOT EXISTS (
	SELECT 	1 
	FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c 
	WHERE 	c.databasename = 'MY_EPICDB'
	AND 	c.tablename = c2.tablename 
	AND 	c.columnname = c2.columnname)
;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-203, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  203;

-- --------------------------------------------------------- 
-- Check for views that have columns not in the associated table 
-- view --may-- need recompile or redefinition or table may be incorrect 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT 	204, 'Column in Middle (Epic) layer view  but not in reporting table(s).' AS msg,
		'MY_RUNNAME', 'MY_REPORT_DB', c.databasename, UPPER(TRIM(c.tablename)) AS tbl, UPPER(TRIM(c.columnname)) AS col
FROM	MY_USHAREDB.UPGRADE_DBC_COLUMNS AS c
WHERE 	c.databasename = 'MY_EPICDB'
		-- ------------------------------------------------------------
		-- only check for table ir the table is valid for the deployment
		-- ------------------------------------------------------------
		AND EXISTS (
			SELECT	1
			FROM	MY_USERDB.CLARITY_TBL tbl
			WHERE	tbl.table_name = c.tablename
			AND 	tbl.cm_phy_owner_id = 'MY_OWNER_ID')
		-- ------------------------------------------------------------
		-- only check for table ir the table is also in the reporting database
		-- ------------------------------------------------------------
		AND EXISTS (
			SELECT	1
			FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t
			WHERE 	t.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND 	t.tablename = c.tablename)
		-- ------------------------------------- 
		-- check if this error should be skipped  
		-- ------------------------------------- 
		AND 204 NOT IN (0)
		-- ------------------------------------------------------------
		-- exclude tables that do not have views - views are union alls
		-- -------------------------------------------------------------
		AND	c.tablename NOT LIKE ALL (
			'OR_BLOCKUTIL%','OR_SCHED%','OR_STFF_BLK%','OR_TEMPLATE%','OR_STAFF_BL%','OR_LOG_PNLCNT%','OR_CASE_APPTS%','OR_LOG_PANEL%')	
		-- ---------------------------- 
		-- exclude derived views 
		-- ---------------------------- 
		AND TRIM(c.tablename) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
			'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS','CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
			'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
			'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
			'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
			'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
			'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_REQUESTER_CREATION', 'V_ROI_STATUS_HISTORY','V_ZC_CANCEL_REASON'
			,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

)	
		AND NOT EXISTS (
			SELECT 1 
			FROM MY_USHAREDB.UPGRADE_DBC_COLUMNS AS c2 
			WHERE c2.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND c2.tablename = c.tablename 
			AND c2.columnname = c.columnname);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-204, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  204;

-- ********************************** End of Epic Views Validation ***************************************************
-- ********************************** Start of User/KP View Validation  ********************************************** 
	
-- --------------------------------------------------------- 
-- Check for tables that may be missing views 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename)
SELECT 	301, 'Reporting table does not appear to have a user level matching view',
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_USERDB',  t0.tablename AS tbl
FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t0
WHERE	t0.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
		-- ------------------------------------------------------------
		-- only check for table ir the table is valid for the deployment
		-- ------------------------------------------------------------
		AND EXISTS (
			SELECT	1
			FROM	MY_USERDB.CLARITY_TBL tbl
			WHERE	tbl.table_name = t0.tablename
			AND 	tbl.cm_phy_owner_id = 'MY_OWNER_ID')		
		-- ------------------------------------- 
		-- check if this error should be skipped  
		-- ------------------------------------- 
		AND 301 NOT IN (0)
		-- ---------------------------- 
		-- exclude derived views 
		-- ---------------------------- 
		AND TRIM(tablename) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
			'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS','CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
			'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
			'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
			'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
			'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
			'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_STATUS_HISTORY','V_ROI_REQUESTER_CREATION', 'V_ZC_CANCEL_REASON'
		,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

)
		-- -------------------------------------------------------------
		-- exclude tables that do not have views - views are union alls
		-- -------------------------------------------------------------
		AND	tablename NOT LIKE ALL (
			'OR_BLOCKUTIL%','OR_SCHED%','OR_STFF_BLK%','OR_TEMPLATE%','OR_STAFF_BL%','OR_LOG_PNLCNT%','OR_CASE_APPTS%','OR_LOG_PANEL%')			
		-- ---------------------------- 
		-- exclude upgrade views 
		-- ---------------------------- 
		AND tablename NOT LIKE ALL ('BF%','UTL%','UPD%','%/_DELETE','%/_UPDATE','%/_DELETE_CT','%/_UPDATE_CT','UPD%','UTL%') ESCAPE '/'
		-- --------------------------------------------
		-- Check if table exists in user database level
		-- --------------------------------------------
		AND NOT EXISTS (
			SELECT 	1
			FROM	MY_USHAREDB.UPGRADE_DBC_TABLES AS ut
			WHERE	ut.databasename = 'MY_USERDB'
			AND		ut.tablename = t0.tablename);
		
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-301, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  301;

-- --------------------------------------------------------- 
-- Check for views that may be missing base tables 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, issue_comment)
SELECT 	302, 'User level view does not appear to have a matching reporting table.' err_msg,
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_USERDB',  t0.tablename,
		CASE 
			WHEN t0.tablename like any ('BF%','UTL%','UPD%','%/_DELETE','%/_UPDATE','%/_DELETE_CT','%/_UPDATE_CT','UPD%','UTL%')  ESCAPE '/'
			AND tbl.table_name is not null THEN 'Staging table - view should not exist' 
			WHEN tbl.table_name is null THEN 'Possible invalid view - table not found in Clarity Compass'
		END AS issue_comment
FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t0
		LEFT OUTER JOIN MY_USERDB.CLARITY_TBL tbl
			ON		tbl.table_name = t0.tablename
			AND 	tbl.cm_phy_owner_id = 'MY_OWNER_ID'
			AND		t0.databasename = 'MY_USERDB'
WHERE  t0.databasename = 'MY_USERDB'
		-- ------------------------------------- 
		-- check if this error should be skipped  
		-- ------------------------------------- 
		AND 302 NOT IN (0)
		-- ---------------------------- 
		-- exclude derived views 
		-- ---------------------------- 
		AND TRIM(t0.tablename) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
			'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS','CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
			'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
			'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
			'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
			'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
			'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT','V_ROI_STATUS_HISTORY','V_ROI_REQUESTER_CREATION', 'V_ZC_CANCEL_REASON'
			
			,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

)		
		-- ------------------------------------------------------------
		-- exclude tables that do not have views - views are union alls
		-- -------------------------------------------------------------
		AND	t0.tablename NOT LIKE ALL (
			'OR_BLOCKUTIL%','OR_SCHED%','OR_STFF_BLK%','OR_TEMPLATE%','OR_STAFF_BL%', 'OR_LOG_PNLCNT%','OR_CASE_APPTS%','OR_LOG_PANEL%')
		-- ---------------------------- 
		-- exclude test and work tables 
		-- ---------------------------- 
		AND TRIM(t0.tablename) NOT LIKE 'UPGR%'
		-- -----------------------------------------------------
		-- and where there is no reporting table for the view
		-- -----------------------------------------------------
		AND NOT EXISTS (
			SELECT	1
			FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES AS tr
			WHERE	tr.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB', 'MY_LEAD_REPORTDB')
			AND 	tr.tablename = t0.tablename)
group by 1,2,3,4,5,6,7;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-302, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  302;


-- --------------------------------------------------------- 
-- Check for tables that have more columns than associated 
-- USER view.  These may denote USER views that need 
-- recompile or redefinition  
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname, issue_comment)
SELECT 	303, 'Column in reporting base table but not in user level view.' AS msg,
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_USERDB',  c.tablename, c.columnname,
		case when mt.tablename is not null then 'Column Add per upgrade manifest' end
FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c
		LEFT OUTER JOIN (
			SELECT	mt1.tablename, mt1.columnname
			FROM	MY_USHAREDB.UPGRADE_MANIFEST_LOAD mt1
			WHERE 	mt1.chg_type in ('Col Add','Column Add')
			AND 	NOT EXISTS (
						SELECT 1 
						FROM MY_USHAREDB.UPGRADE_MANIFEST_LOAD mt2 
						WHERE mt2.chg_type ='Column Drop'
						AND	mt2.tablename = mt1.tablename
						AND mt2.columnname = mt1.columnname)
			GROUP BY 1,2) as mt
			ON c.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND c.tablename = mt.tablename
			AND c.columnname = mt.columnname
WHERE	c.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
		-- ------------------------------------------------------------
		-- only check for table ir the table is valid for the deployment
		-- ------------------------------------------------------------
		AND EXISTS (
			SELECT	1
			FROM	MY_USERDB.CLARITY_TBL tbl
			WHERE 	tbl.table_name = c.tablename
			AND 	tbl.cm_phy_owner_id = 'MY_OWNER_ID')
		-- ------------------------------------------------------------
		-- only check for table if the table is also in the user database
		-- ------------------------------------------------------------
		AND EXISTS (
			SELECT	1
			FROM	MY_USHAREDB.UPGRADE_DBC_TABLES t
			WHERE 	t.databasename = 'MY_USERDB'
			AND 	t.tablename = c.tablename)
		-- ------------------------------------- 
		-- check if this error should be skipped  
		-- ------------------------------------- 
		AND 303 NOT IN (0)
		-- ------------------------------------------------------------
		-- exclude tables that do not have views - views are union alls
		-- -------------------------------------------------------------
		AND	c.tablename NOT LIKE ALL (
			'OR_BLOCKUTIL%','OR_SCHED%','OR_STFF_BLK%','OR_TEMPLATE%','OR_STAFF_BL%','OR_LOG_PNLCNT%','OR_CASE_APPTS%','OR_LOG_PANEL%')
		-- ---------------------------- 
		-- exclude derived views 
		-- ---------------------------- 
		AND TRIM(c.tablename) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
		'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS','CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
		'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
		'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
		'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
		'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
		'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_REQUESTER_CREATION', 'V_ROI_STATUS_HISTORY','V_ZC_CANCEL_REASON'
		
		,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

)		
		AND NOT EXISTS (
			SELECT 	1 
			FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS AS c2 
			WHERE 	c2.databasename = 'MY_USERDB'
			AND 	c2.tablename = c.tablename 
			AND 	c2.columnname = c.columnname);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-303, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM 	MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  303;

-- ----------------------------------------------------------------------- 
-- Check for HCCLxx views that have columns not in Reporting tables 
-- Ok if derived column otherwise possible error 
-- ----------------------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT 	304, 'Column in user level view but not in reporting database table.' AS msg,
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_USERDB', c.tablename, c.columnname
FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS AS c
WHERE  	c.databasename = 'MY_USERDB'
		-- ------------------------------------------------------------
		-- only check for table if the table is valid for the deployment
		-- ------------------------------------------------------------
		AND EXISTS (
			SELECT	1
			FROM 	MY_USERDB.CLARITY_TBL tbl
			WHERE	tbl.table_name = c.tablename
			AND 	tbl.cm_phy_owner_id = 'MY_OWNER_ID')
		-- ------------------------------------------------------------
		-- only check for table if the table is also in the reporting database
		-- ------------------------------------------------------------
		AND EXISTS (
			SELECT	1
			FROM 	MY_USHAREDB.UPGRADE_DBC_TABLES t
			WHERE	t.databasename in ('MY_REPORT_DB','MY_MATVIEW_DB')
			AND		t.tablename = c.tablename)
		-- ------------------------------------- 
		-- check if this error should be skipped  
		-- ------------------------------------- 
		AND 304 NOT IN (0)
		-- ------------------------------------------------------------
		-- exclude tables that do not have views - views are union alls
		-- -------------------------------------------------------------
		AND	c.tablename NOT LIKE ALL (
			'OR_BLOCKUTIL%','OR_SCHED%','OR_STFF_BLK%','OR_TEMPLATE%','OR_STAFF_BL%','OR_LOG_PNLCNT%','OR_CASE_APPTS%','OR_LOG_PANEL%')		
		-- ---------------------------- 
		-- exclude derived views 
		-- ---------------------------- 
		AND TRIM(c.tablename) NOT IN ('ACCESS_LOG', 'ACCESS_WRKF', 'ACCESS_LOG_DTL', 'ACCESS_LOG_MTLDTL', 'ACCESS_WRKF_DTL',
			'ACCESS_WRKF_MTLDTL', 'CLARITY_TDL', 'CLARITY_TDL_SYNCH', 'CR_EPT_APPNTS','CR_REMAP_CIDS','CR_TAR_CHGROUT','CR_TAR_CHGSESHST',
			'CR_TAR_CHG_REW','CR_TAR_CHG_TRAN','CR_TAR_DIAGNOSIS','CR_TAR_PROCEDURE','IP_FLO_CNT_OFF_OLD','IP_MAR', 
			'IP_MAR_EDITED', 'IP_MAR_EDIT_ALT_ID', 'IP_MAR_FSD_ID',  'IP_MAR_FSD_ID_EDIT', 'IP_MAR_FSD_LINE',
			'IP_MAR_FSD_LN_EDIT', 'IP_MAR_OVR_ALT_ID','OR_BLOCKNAMES',	'OR_CASE_APPTS','OR_LOG_PANEL_TIMES',
			'OR_LOG_PNLCNT_CMTS', 'OR_LOG_PNLCNT_INFO', 'OR_SCHED', 'OR_STAFF_BLOCKS', 'OR_TEMPLATE',
			'PATIENT_TYPE_xID', 'V_CLM_RECON_SVC_STAT', 'V_ROI_STATUS_HISTORY','V_ROI_REQUESTER_CREATION', 'V_ZC_CANCEL_REASON'
			
			,'IP_MAR_FSD_ID'
,'IP_MAR'
,'IP_MAR_EDIT_ALT_ID'
,'IP_MAR_EDITED'
,'IP_MAR_FSD_ID_EDIT'
,'IP_MAR_FSD_LINE'
,'IP_MAR_FSD_LN_EDIT'
,'IP_MAR_OVR_ALT_ID'
,'ACCESS_LOG_DTL'
,'ACCESS_LOG_MTLDTL'
,'ACCESS_WRKF_DTL'
,'ACCESS_WRKF_MTLDTL'
,'ORDER_TRANSCRIPTN'
,'IP_NOTES_PROC'
,'IP_NOTES_DX2'
,'HNO_PROC_NOTE_ID'
,'IP_NOTES_DX1'
,'TRANS_IB_NOTES'
,'TRANS_AUTH_NOTES'
,'HNO_ENC_INFO'
,'TRANS_OT_INFO'
,'IP_NOTE'
,'ENC_NOTE_INFO'
,'IP_PEND_NOTE'
,'CLARITY_TDL'
,'AP_CLAIM_EOB_CODE'
,'REFERRAL_PX'
,'OR_TEMPLATE'
,'OR_LOG_PANEL_TIMES'
,'OR_CASE_APPTS'
,'MNEM_SETUP'
,'MNEM_RES_ITEM_REL'
,'ALT_DRUG_AGE'
,'ALT_DRUG_ALLERGY'
,'ALT_BPA_TRGR_ACT'
,'ALT_DRUG_DISEASE'
,'ALT_DRUG_DUPTHERPY'
,'ALT_DRUG_IV'
,'ALT_DRUG_LACTATION'
,'ALT_DRUG_PREGNANCY'
,'ALT_DRUG_TPN'
,'ALT_DRUG_DFALC'
,'ALT_DRUG_DIS_MED'
,'ALT_DRUG_DOSE'
,'ALT_DRUG_DUPTHYMED'
,'ALT_DRUG_AGE_MED'
,'ALT_DRUG_IVMED'
,'ALT_DRUG_LACTMED'
,'ALT_DRUG_PREGMED'
,'IMG_ORD_VIEW'

)		
AND NOT EXISTS (
		SELECT	1 
		FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS AS c2 
		WHERE 	c2.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
		AND 	c2.tablename = c.tablename 
		AND 	c2.columnname = c.columnname);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-304, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  304;

-- -------------------------------------------------------------------------------- 
-- check for view columns in USHARE but not in HCCLxx view (column dropped) 
-- -------------------------------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname)
SELECT	305, 'Column in Ushare view info but not in user view.' AS msg,
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_USERDB', v1.tablename, v1.columnname
FROM	MY_USHAREDB.upgrade_columns_views v1
		-- ---------------------------------------------- 
		-- only check if table exists in MY_USERDB database 
		-- ---------------------------------------------- 
		INNER JOIN MY_USHAREDB.UPGRADE_DBC_TABLES t1
		ON v1.tablename = UPPER(TRIM(t1.tablename))
		AND UPPER(TRIM(t1.databasename)) = 'MY_USERDB'
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
		AND 305 NOT IN (0)
		-- ------------------------------------- 
		-- check if column exists in user view
		-- ------------------------------------- 
		AND NOT EXISTS (
			SELECT	1
			FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c1
			WHERE 	c1.databasename = 'MY_USERDB'
			AND		c1.tablename = v1.tablename
			AND		c1.columnname = v1.columnname);

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-305, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no = 305;
		
-- ----------------------------------------------------------------------------------- 
--  check view columns in HCCLxx view but not in USHARE table (column adds) 
-- ----------------------------------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, columnname, issue_comment, testing_rqrd)
SELECT	306, 'Column in user level (MY_USERDB) view but not in Ushare list for view.' AS msg,
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_USERDB',  UPPER(TRIM(c.tablename)), UPPER(TRIM(c.columnname)),
		CASE 
			WHEN mt.chg_type = 'Column Add'  THEN 'Column Add per upgrade manifest'
			WHEN mt.chg_type = 'Column Drop' THEN 'Column Drop per upgrade manifest'
		END,
		mt.testing_rqrd
FROM 	MY_USHAREDB.UPGRADE_DBC_COLUMNS c
		-- -----------------------------------------------------------------------
		-- join to ensure we only compare tables in ushare list and in user db
		-- -----------------------------------------------------------------------
		INNER JOIN (
			SELECT tablename 
			FROM MY_USHAREDB.upgrade_columns_views 
			GROUP BY 1) AS v
		ON c.tablename = v.tablename 
		AND c.databasename = 'MY_USERDB'
		AND 306 NOT IN (0)
		AND NOT EXISTS (
			SELECT 	1
			FROM	MY_USHAREDB.upgrade_columns_views v1
			WHERE	v1.tablename = c.tablename
			AND 	v1.columnname = c.columnname)
		LEFT OUTER JOIN	MY_USHAREDB.UPGRADE_MANIFEST_LOAD mt
			ON c.databasename = 'MY_USERDB'
			AND c.tablename = mt.tablename
			AND c.columnname = mt.columnname
			AND (mt.testing_Rqrd IS NULL OR mt.testing_rqrd = 'Y')
			AND mt.chg_type in ('Column Drop','Column Add')
-- needed to remove duplicates
GROUP BY 1,2,3,4,5,6,7,8,9;

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-306, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  306;	

-- ********************************** Start of Work/Temp table check  ********************************************** 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, issue_comment)
SELECT 	
	901, 
	'Possible Work-Temp '||CASE WHEN DTL.tablekind = 'T' THEN 'Table'WHEN DTL.tablekind = 'V' THEN 'View'	END||
	' found in reporting database - CREATED BY : '||TRIM(DTL.creatorname)||'  - ON : '||(DTL.createtimestamp (FORMAT 'MM/DD/YYYY') (DATE)) AS err_msg,
	'MY_RUNNAME', 'MY_REPORT_DB', DTL.databasename, UPPER(TRIM(DTL.tablename))tablename,
	CASE WHEN DTL.tablename not like 'BF$%' THEN 'Table not found in Clarity Compass' END issue_comment
FROM	MY_USHAREDB.UPGRADE_DBC_TABLES DTL
WHERE	DTL.databasename IN ('MY_REPORT_DB','MY_MATVIEW_DB')
AND		DTL.tablename NOT IN ('TOKEN_X')
AND		DTL.tablekind IN ('V','T')
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
AND 	901 NOT IN (0)
		-- ------------------------------------------------
		-- Skip if the table exists in the clarity compass
		-- ------------------------------------------------
AND		NOT EXISTS (
		SELECT 	1
		FROM 	MY_USERDB.CLARITY_TBL tbl
		WHERE	tbl.table_name = DTL.tablename 
		AND 	tbl.cm_phy_owner_id = 'MY_OWNER_ID'
		AND 	tbl.table_name not like all ('BF%','UPD%','UTL%', '%/_DELETE','%/_UPDATE', '%/_DELETE_CT','%/_UPDATE_CT' ) ESCAPE '/');

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-901, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  901;

-- --------------------------------------------------------- 
-- report work tables in staging database 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, issue_comment)
SELECT 902, 
		'Possible Work-Temp '||CASE WHEN DTL.tablekind = 'T' THEN 'Table'WHEN DTL.tablekind = 'V' THEN 'View'	END||
		' found in staging database - Created by: '||TRIM(DTL.creatorname)||' - on: '||(DTL.createtimestamp (FORMAT 'MM/DD/YYYY') (DATE)) AS err_msg,
		'MY_RUNNAME', 'MY_REPORT_DB', DTL.databasename, UPPER(TRIM(DTL.tablename))tablename,
		CASE WHEN DTL.tablename not like 'BF$%' THEN 'Table not found in Clarity Compass' END issue_comment
FROM	MY_USHAREDB.UPGRADE_DBC_TABLES DTL
WHERE	DTL.databasename IN ('MY_STAGE_DB')
AND		DTL.tablename NOT IN ('TOKEN_X')
AND		TRIM(DTL.tablename) NOT LIKE ALL ('%ERROR1','%ERROR2')
AND		DTL.tablekind IN ('V','T')
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
AND 	902 NOT IN (0)
AND		NOT EXISTS (
		SELECT 	1
		FROM 	MY_USERDB.CLARITY_TBL tbl
		WHERE	tbl.table_name = DTL.tablename
		AND 	tbl.cm_phy_owner_id = 'MY_OWNER_ID'
		AND		tbl.table_name not like 'BF%');
		
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-902, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  902;

-- --------------------------------------------------------- 
-- report work tables in epic view database 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, issue_comment)
SELECT 	903, 'Possible Work-Temp '||CASE WHEN DTL.tablekind = 'T' THEN 'Table'WHEN DTL.tablekind = 'V' THEN 'View'	END||
		' found in middle layer (MY_EPICDB) database - CREATED BY : '||TRIM(DTL.creatorname)||'  - ON : '||(DTL.createtimestamp (FORMAT 'MM/DD/YYYY') (DATE)) AS err_msg,
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_EPICDB', UPPER(TRIM(DTL.tablename))tablename,
		CASE 
			WHEN tbl.table_name is null THEN 'Table not found in Clarity Compass' 
			WHEN tbl.cm_phy_owner_id = '9001' THEN 'Warning - Physical Owner is 9001.'
			WHEN tbl.cm_phy_owner_id = 'MY_OWNER_ID' and tbl.is_extracted_yn = 'N' THEN 'Table is not extracted per compass.'
		END issue_comment
FROM	MY_USHAREDB.UPGRADE_DBC_TABLES DTL
		LEFT OUTER JOIN MY_USERDB.CLARITY_TBL tbl
		ON DTL.tablename = tbl.table_name 
		AND tbl.cm_phy_owner_id in ('MY_OWNER_ID','9001')
		AND DTL.databasename ='MY_EPICDB'
		AND DTL.tablekind IN ('V','T')
		AND	DTL.tablename not like 'BF%'
WHERE	DTL.databasename ='EPIC_VIEWS'
AND		DTL.tablekind IN ('V','T')
		-- ------------------------------------- 
		-- check if this error should be skipped 
		-- ------------------------------------- 
AND 	903 NOT IN (0)
AND		(tbl.cm_phy_owner_id = '9001' 
		OR tbl.table_name is null
		OR (tbl.table_name is not null and tbl.is_extracted_yn = 'N'));
		
INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-903, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  903;

-- --------------------------------------------------------- 
-- report work tables in user view database 
-- --------------------------------------------------------- 
INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, runname, dbname, erroring_dbname, tablename, issue_comment)
SELECT 	904, 'Possible Work-Temp '||CASE WHEN DTL.tablekind = 'T' THEN 'Table'WHEN DTL.tablekind = 'V' THEN 'View'	END||
		' found in user layer (MY_USERDB) database - CREATED BY : '||TRIM(DTL.creatorname)||'  - ON : '||(DTL.createtimestamp (FORMAT 'MM/DD/YYYY') (DATE)) AS err_msg,
		'MY_RUNNAME', 'MY_REPORT_DB', 'MY_USERDB', UPPER(TRIM(DTL.tablename))tablename,
		CASE
			WHEN tbl.table_name is null and tbl9.table_name is not null Then 'Warning - Physical Owner is 9001.'
			WHEN tbl.is_extracted_yn = 'N' THEN 'Table is not extracted per compass.'
		END AS issue_comments
FROM	(
		SELECT	tablename, tablekind, creatorname, createtimestamp
		FROM	MY_USHAREDB.UPGRADE_DBC_TABLES
		WHERE	databasename ='MY_USERDB'
		AND		tablekind IN ('V','T')
				-- ------------------------------------- 
				-- check if this error should be skipped 
				-- ------------------------------------- 
		AND 	904 NOT IN (0)
		) as DTL
		LEFT OUTER JOIN MY_USERDB.CLARITY_TBL tbl
		ON 		DTL.tablename = tbl.table_name
		AND		tbl.cm_phy_owner_id = 'MY_OWNER_ID'
		LEFT OUTER JOIN MY_USERDB.CLARITY_TBL tbl9
		ON 		DTL.tablename = tbl9.table_name
		AND		tbl9.cm_phy_owner_id = '9001'		
WHERE	(tbl.table_name is null and tbl9.table_name is null)
OR	 	(tbl.table_name is null and tbl9.table_name is not null)
OR		tbl.is_extracted_yn = 'N';

INSERT INTO MY_USHAREDB.UPGRADE_ISSUES (err_no, err_msg, dbname, runname, create_dttm) 
	SELECT 	-904, 'End of Error Check : '||TRIM(COUNT(*))||' errors found.', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0)
	FROM MY_USHAREDB.UPGRADE_ISSUES 
	WHERE 	dbname IN ( 'MY_REPORT_DB','MY_MATVIEW_DB')
	AND 	err_no =  904;	

-- ********************************** End of Work/Temp table check  ********************************************** 

INSERT INTO MY_USHAREDB.upgrade_issues (err_no, err_msg, dbname, runname, create_dttm) VALUES
	(-1000,'End of Validation', 'MY_REPORT_DB', 'MY_RUNNAME',	CURRENT_TIMESTAMP(0));


-- ********************************** Combined Tables  ********************************************** 
INSERT INTO MY_USHAREDB.upgrade_issues_combined
SELECT * FROM MY_USHAREDB.upgrade_issues;

	
	
-- ********************************** Final Report  ********************************************** 

-- Export Summary
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_NUID/dbmig/outfiles/MY_RUNNAME_validation_report_summary.dat;

SELECT	
COALESCE(TRIM(runname),'')   || '|' ||  COALESCE( (create_dttm(FORMAT 'YYYY-MM-DD')(CHAR(10))),'')  ||  ' ' || 
COALESCE( (create_dttm(FORMAT 'HH:MI:SS')(CHAR(8))),'')  ||  '|' ||  
COALESCE(TRIM(err_no),'') || '|' || COALESCE(TRIM(err_msg),'') || '|' || COALESCE(TRIM(dbname),'') || '|' || COALESCE(TRIM(erroring_dbname),'') || '|' || 
COALESCE(TRIM(tablename),'') || '|' || COALESCE(TRIM(stg_db),'') || '|' || COALESCE(TRIM(stg_table),'') || '|' || COALESCE(TRIM(columnname),'') || '|' || 
COALESCE(TRIM(slno),'') || '|' || COALESCE(TRIM(is_preserved),'') || '|' || COALESCE(TRIM(on_demand),'') || '|' || COALESCE(TRIM(is_extracted),'') || '|' || 
COALESCE(TRIM(data_retained),'') || '|' || COALESCE(TRIM(is_deprecated),'') || '|' || COALESCE(TRIM(testing_rqrd),'') || '|' || COALESCE(TRIM(cm_phy_owner_id),'') || '|' || 
COALESCE(TRIM(ushare_or_cmps_or_S_def),'') || '|' || COALESCE(TRIM(hccl_or_T_def),'') || '|' || 
COALESCE(TRIM(mfst_def),'') || '|' || COALESCE(TRIM(ushare_or_cmps_or_S_cmprs),'') || '|' || 
COALESCE(TRIM(hccl_or_T_cmprs),'') || '|' || COALESCE(TRIM(issue_comment),'') || '|' ||
COALESCE(TRIM(tbl_ini),'') || '|' || COALESCE(TRIM(col_fmt_ini),'') || '|' || COALESCE(TRIM(col_fmt_item),'')

FROM	MY_USHAREDB.UPGRADE_ISSUES
WHERE err_no < 0;

-- ORDER BY 4, 8, 7;



-- Export Detail
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_NUID/dbmig/outfiles/MY_RUNNAME_validation_report_detail.dat;

SELECT 
	
		TRIM(TEMP.err_no) || '|' || 
		COALESCE(TRIM(TEMP.err_msg),'')|| '|' ||
		COALESCE(TRIM(TEMP.db_name),'')  || '|' ||
		COALESCE(TRIM(TEMP.erroring_dbname),'')  || '|' ||
		COALESCE(TRIM(TEMP.stg_db),'')  || '|' ||
		COALESCE(TRIM(TEMP.stg_table),'') || '|' ||
		COALESCE(TRIM(TEMP.tablename),'') || '|' ||
		COALESCE(TRIM(TEMP.columnname),'') || '|' ||
		COALESCE(TRIM(TEMP.is_deprecated),'') || '|' ||
		COALESCE(TRIM(TEMP.is_preserved),'') || '|' ||
		COALESCE(TRIM(TEMP.on_demand),'') || '|' ||
		COALESCE(TRIM(TEMP.is_extracted),'') || '|' ||
		COALESCE(TRIM(TEMP.data_retained),'') || '|' ||
		COALESCE(TRIM(TEMP.cm_phy_owner_id),'') || '|' ||
		COALESCE(TRIM(TEMP."Rpting Definition"),'') || '|' ||
		COALESCE(TRIM(TEMP."Ushare or Staging Definition"),'') || '|' ||
		COALESCE(TRIM(TEMP."Manifest Definition"),'')  || '|' ||
		COALESCE(TRIM(TEMP.slno),'') || '|' ||
		COALESCE(TRIM(TEMP.testing_rqrd),'')		
	
	FROM
	(
			SELECT 
				a.err_no, 
				CASE WHEN (b.db_cnt > 5 OR b.stg_db_cnt > 5 ) AND a.err_no between 900 AND 999
					THEN 'Possible Work-Temp Table found in staging database' 
					ELSE a.err_msg 
				END err_msg,
				CASE WHEN b.db_cnt >5 OR b.stg_db_cnt >5 OR b.err_db_cnt >5 THEN 'All Deployments' ELSE a.dbname END db_name,
				CASE WHEN b.db_cnt >5 OR b.stg_db_cnt >5 OR b.err_db_cnt >5 THEN 'All Deployments' ELSE a.erroring_dbname END erroring_dbname,
				CASE WHEN b.db_cnt >5 OR b.stg_db_cnt >5 OR b.err_db_cnt >5 THEN 'All Deployments' ELSE a.stg_db END stg_db,
				a.stg_table,
				a.tablename,
				a.columnname,
				a.is_deprecated,
				a.is_preserved,
				a.on_demand,
				a.is_extracted,
				a.data_retained,
				CASE WHEN b.db_cnt > 5 OR b.stg_db_cnt > 5 THEN NULL ELSE a.cm_phy_owner_id END cm_phy_owner_id,
				b.hccl_or_t_def AS "Rpting Definition",
				b.ushare_or_cmps_or_s_def AS "Ushare or Staging Definition",
				a.mfst_def AS "Manifest Definition"  ,
				b.slno,
				b.testing_rqrd		
		FROM	MY_USHAREDB.upgrade_issues a
				LEFT OUTER  JOIN (
					SELECT
						err_no, 
						err_msg, 
						COUNT(DISTINCT dbname) db_cnt,
						COUNT(DISTINCT erroring_dbname) err_db_cnt,
						COUNT(DISTINCT stg_db) stg_db_cnt,
						stg_table,
						tablename,
						columnname,
						hccl_or_t_def ,
						ushare_or_cmps_or_s_def,
						mfst_def,
						slno,
						testing_rqrd
					FROM MY_USHAREDB.upgrade_issues
					WHERE ERR_NO > 0
					GROUP BY 1,2,6,7,8,9,10,11,12,13
				) AS b
				ON  a.err_no = b.err_no
				AND a.err_msg = b.err_msg
				AND COALESCE(a.tablename,'') = COALESCE(b.tablename,'')
				AND COALESCE(a.stg_table,'') = COALESCE(b.stg_table,'')
				AND COALESCE(a.columnname,'') = COALESCE(b.columnname,'')
				AND COALESCE(a.hccl_or_t_def ,'') = COALESCE(b.hccl_or_t_def ,'')
				AND COALESCE(a.ushare_or_cmps_or_s_def,'') = COALESCE(b.ushare_or_cmps_or_s_def,'')
				AND COALESCE(a.mfst_def,'') = COALESCE(b.mfst_def,'')
				AND COALESCE(a.slno,'') = COALESCE(b.slno,'')
				AND COALESCE(a.testing_rqrd,'') = COALESCE(b.testing_rqrd,'')
		WHERE a.ERR_NO > 0 
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
	) TEMP;
		
-- ORDER BY 1,7,8,3,4,5,6;