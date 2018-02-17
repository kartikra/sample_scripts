-- -----------------------------------------------------------------------------------------
--  MY_RUNNAME  ---
--  MY_USHAREDB ---
--  MY_ENV      ---
--  MY_USERDB   ---
--  MY_EPICDB ---
--  MY_REPORT_DB ---
--  MY_MATVIEW_DB --
-- MY_KPBIVIEW_DB --
--  MY_STAGE_DB  ---
--  MY_OWNER_ID  ---
-- -----------------------------------------------------------------------------------------


DELETE FROM MY_USHAREDB.UPGRADE_ISSUES_REVIEW ALL;


CREATE VOLATILE TABLE VALIDATION_EPICVIEW AS
(
SELECT 
CAST('Converted to Epic View' AS VARCHAR(50)) AS EV_NOTES,TRIM(I.dbname) EV_DBNAME,TRIM(I.TableName) EV_TableName
FROM MY_USHAREDB.UPGRADE_ISSUES I
WHERE 
EXISTS
(
SELECT 1 FROM DBC.TablesV V WHERE V.TableKind='V' AND V.TableName=I.TableName
AND V.DatabaseName='MY_EPICDB'
)
AND NOT EXISTS
(
SELECT 1 FROM DBC.TablesV T WHERE T.TableKind='T' AND T.TableName=I.TableName
AND T.DatabaseName='MY_REPORT_DB'
)
GROUP BY 1,2,3
)WITH DATA
ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE VALIDATION_MATVIEW AS
(
SELECT 
CAST('Converted to Materilaized View' AS VARCHAR(50)) AS MV_NOTES,TRIM(A.dbname) MV_dbname,TRIM(A.TableName) MV_TableName
FROM DBC.TablesV B
JOIN MY_USHAREDB.UPGRADE_ISSUES A ON
(A.TableName=SUBSTR(TRIM(B.TableName),3) OR
A.TableName=SUBSTR(TRIM(B.TableName),1,LENGTH(TRIM(B.TableName))-1) OR
A.TableName=TRIM(B.TableName)
)
AND B.DatabaseName IN ('MY_MATVIEW_DB','MY_KPBIVIEW_DB')
GROUP BY 1,2,3
)WITH DATA
ON COMMIT PRESERVE ROWS;





-----------------------------------------------------------------------------------------------------------------------
-- STEP-1  Analysis of errors 54,55 and 58.   (ETL Compass - Teradata Differences at Table Level)
-----------------------------------------------------------------------------------------------------------------------

	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW

	-- Manual Review Needed if exisiting difference in Production is also being changed as part of manifest

		SELECT SRC.*,'Y','Difference found in Production but Changes are needed as per manifest.Review to see if new changes have been applied.'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (54,55,58)

				-- Is Exisiting Difference
			AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (54,55,58)
				AND C.Table_Name=SRC.TableName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Required as part of Manifest
				AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table') AND 
				M.TABLENAME=SRC.TableName 
			)

	UNION
			
	-- Review Not Needed if exisiting difference in Production is not being changed as part of manifest

		SELECT SRC.*,'N','Mismatch Exists in Production and is not being addressed by current manifest'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (54,55,58)

				-- Is Exisiting Difference
			AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (54,55,58)
				AND C.Table_Name=SRC.TableName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Required as part of Manifest
				AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table') AND 
				M.TABLENAME=SRC.TableName 
			)

	UNION
			
	-- Review Needed if change was not in Prod and Not part of manifest

		SELECT SRC.*,'Y','Changed Not needed as per manifest and was not an existing difference in Production'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (54,55,58)

				-- Is Not Exisiting Difference
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (54,55,58)
				AND C.Table_Name=SRC.TableName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Not Required as part of Manifest
				AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table') AND 
				M.TABLENAME=SRC.TableName 
			)
	
	UNION
	
	-- Review Needed if change is not in Prod but needed as part of manifest and is also not converted to a view

		SELECT SRC.*,'Y','Changed needed as per manifest and is Table is not converted to a View'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (54,55,58)

			-- Is Not Exisiting Difference
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (54,55,58)
				AND C.Table_Name=SRC.TableName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Required as part of Manifest
				AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table') AND 
				M.TABLENAME=SRC.TableName 
			)
	
			-- Not Converted to a View
				AND NOT EXISTS
			(
				SEL 1 FROM 	DBC.TablesV UV
				WHERE TRIM(UV.DatabaseName)='MY_USERDB' 
				AND UV.TableName=SRC.TableName 
			)
	
	UNION
	
	-- Review Not Needed since table is converted to a view

		SELECT SRC.*,'N','Changed needed as per manifest but Table is converted to a View'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (54,55,58)

			-- Is Not Exisiting Difference
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (54,55,58)
				AND C.Table_Name=SRC.TableName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Required as part of Manifest
				AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table') AND 
				M.TABLENAME=SRC.TableName 
			)
	
			--  Converted to a View
				AND EXISTS
			(
				SEL 1 FROM 	DBC.TablesV UV
				WHERE TRIM(UV.DatabaseName)='MY_USERDB' 
				AND UV.TableName=SRC.TableName 
			)
	
	;
	

-----------------------------------------------------------------------------------------------------------------------
-- STEP-2  Analysis of errors 52,53,56 and 57.   (ETL Compass - Teradata Differences at Column Level)
-----------------------------------------------------------------------------------------------------------------------

	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW

	-- Manual Review Needed if exisiting difference in Production is also being changed as part of manifest

		SELECT SRC.*,'Y','Difference found in Production but Changes are needed as per manifest.Review to see if new changes have been applied.'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (52,53,56,57)

				-- Is Exisiting Difference
			AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (52,53,56,57)
				AND C.Table_Name=SRC.TableName
				AND C.Column_Name=SRC.ColumnName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Required as part of Manifest
				AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column') AND 
				M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
			)

	UNION
			
	-- Review Not Needed if exisiting difference in Production is not being changed as part of manifest

		SELECT SRC.*,'N','Mismatch Exists in Production and is not being addressed by current manifest'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (52,53,56,57)

				-- Is Exisiting Difference
			AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (52,53,56,57)
				AND C.Table_Name=SRC.TableName
				AND C.Column_Name=SRC.ColumnName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Required as part of Manifest
				AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column') AND 
				M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
			)

	UNION
			
	-- Review Needed if change was not in Prod and Not part of manifest

		SELECT SRC.*,'Y','Changed Not needed as per manifest and was not an existing difference in Production'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (52,53,56,57)

				-- Is Not Exisiting Difference
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (52,53,56,57)
				AND C.Table_Name=SRC.TableName
				AND C.Column_Name=SRC.ColumnName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Not Required as part of Manifest
				AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column') AND 
				M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName 
			)
	
	UNION
	
	-- Review Needed if change is not in Prod but needed as part of manifest and is also not converted to a view

		SELECT SRC.*,'Y','Changed needed as per manifest and is Table is not converted to a View'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (52,53,56,57)

			-- Is Not Exisiting Difference
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (52,53,56,57)
				AND C.Table_Name=SRC.TableName
				AND C.Column_Name=SRC.ColumnName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Required as part of Manifest
				AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column') AND 
				M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName 
			)
	
			-- Not Converted to a View
				AND NOT EXISTS
			(
				SEL 1 FROM 	DBC.ColumnsV UV
				WHERE TRIM(UV.DatabaseName)='MY_USERDB' 
				AND UV.TableName=SRC.TableName AND UV.COLUMNNAME=SRC.ColumnName 
			)
	
	UNION
	
	-- Review Not Needed since table is converted to a view

		SELECT SRC.*,'N','Changed needed as per manifest but Table is converted to a View'

		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 

		WHERE TRIM(SRC.err_no) IN (52,53,56,57)

			-- Is Not Exisiting Difference
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (52,53,56,57)
				AND C.Table_Name=SRC.TableName
				AND C.Column_Name=SRC.ColumnName
				AND C.ERROR_NO=SRC.err_no
			)
			
			-- Change Required as part of Manifest
				AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column') AND 
				M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName 
			)
	
			--  Converted to a View
				AND EXISTS
			(
				SEL 1 FROM 	DBC.ColumnsV UV
				WHERE TRIM(UV.DatabaseName)='MY_USERDB' 
				AND UV.TableName=SRC.TableName AND UV.COLUMNNAME=SRC.ColumnName 
			)
	
	;


----------------------------------------------------------------------------------------------------------------------------------
-- STEP-3  Analysis of errors 77,78 and 75,76,79. (In Teradata but not in manifest.Analysis done at both Table and Column Level)
----------------------------------------------------------------------------------------------------------------------------------

	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW
	
	-- Review Needed if Change was made to database but was not part of manifest
	
		SELECT SRC.*,'Y','Change was made to database but was not part of manifest'
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (77,78)
			-- Change Not Added to Manifest
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table')
				AND M.TABLENAME=SRC.TableName
			)
			-- Table is part of console
	        AND  EXISTS  ( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
					)
		
		UNION
		
		SELECT SRC.*,'Y','Change was made to database but was not part of manifest and is also not added to ETL console'
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (77,78)
			-- Change Not Added to Manifest
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table')
				AND M.TABLENAME=SRC.TableName
			)
			-- Table is not part of console
	        AND  NOT EXISTS  ( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
					)
		
			
		UNION
		
		SELECT SRC.*,'N','Change was made to database but is also part of manifest'
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (77,78)
			-- Change Not Added to Manifest
			AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table')
				AND M.TABLENAME=SRC.TableName
			)
		
		UNION
		
		SELECT SRC.*,'Y','Change was made to database but was not part of manifest'
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (75,76,79)
			-- Change Not Added to Manifest
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
				AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
			)
			-- Column is part of console
	        AND  EXISTS  ( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					JOIN MY_REPORT_DB.CLARITY_COL CCOL ON CCOL.TABLE_ID=CTBL.TABLE_ID
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CCOL.COLUMN_NAME=SRC.ColumnName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
						AND CCOL.CM_PHY_OWNER_ID='MY_OWNER_ID'
					)
			
		UNION
		
		
		SELECT SRC.*,'Y','Change was made to database but was not part of manifest and is also not added to ETL Console'
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (75,76,79)
			-- Change Not Added to Manifest
			AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
				AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
			)
			-- Column is not part of console
	        AND  NOT EXISTS  ( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					JOIN MY_REPORT_DB.CLARITY_COL CCOL ON CCOL.TABLE_ID=CTBL.TABLE_ID
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CCOL.COLUMN_NAME=SRC.ColumnName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
						AND CCOL.CM_PHY_OWNER_ID='MY_OWNER_ID'
					)
			
			
		UNION
		
		
		SELECT SRC.*,'N','Change was made to database but is also part of manifest'
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (75,76,79)
			-- Change Not Added to Manifest
			AND EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
				AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
			)
	;	
			

----------------------------------------------------------------------------------------------------------------------------
-- STEP-4  Analysis of errors 80-88.  (In manifest but not in Teradata. Analysis done at both Table and Column Level)
----------------------------------------------------------------------------------------------------------------------------
	
	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW
	
	-- Review Needed if Change is part of manifest and testing_ind='Y'. Ignore errors where manifest datatype has typo errors like ; instead of ,
		SELECT SRC.*
		,CASE WHEN testing_rqrd='Y' AND OREPLACE(mfst_def,';',',') <> hccl_or_T_def 
			THEN 'Y' ELSE 'N' 
		END
		,CASE WHEN testing_rqrd='Y' AND OREPLACE(mfst_def,';',',') <> hccl_or_T_def
			THEN 'Change required as part of manifest but has not been made to the database'
			ELSE 'Change has been made to the database or Testing Ind is set to N'
		END
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) BETWEEN 80 AND 88
	; 
		

		
		
-------------------------------------------------------------------------------------------------------------------------------------
-- STEP-5  Analysis of errors 101-107.  (Staging - Reporting Mismatch in Teradata. Analysis done at both Table and Column Level)
-----------------------------------------------------------------------------------------------------------------------------------
	
	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW
			
	-- Review Needed if Change is part of manifest or table is part of console. Can be ignored if not either of these 2
	
		SELECT SRC.*,'Y','Change needed to Table as part of manifest or Table is part of console.Analyze if Staging/Reporting Table is set up correctly' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (101,102)
		-- Table is part of console or change needed as per manifest
	        AND  
			(
				EXISTS  
				( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				OR
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)
			
		UNION

		SELECT SRC.*,'N','Table is not part of console and is also not part of manifest. Error can be ignored.' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (101,102)
		-- Table is Not part of console and not in manifest
	        AND  
			(
				NOT EXISTS  
				( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				AND
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)		
		
		UNION
		
		SELECT SRC.*,'Y','Change needed to Table as part of manifest or Table is part of console.Analyze if Staging/Reporting Table is set up correctly' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) BETWEEN 103 AND 107
		-- Table is part of console or change needed as per manifest
	        AND  
			(
				EXISTS  
				( 	SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					JOIN MY_REPORT_DB.CLARITY_COL CCOL ON CCOL.TABLE_ID=CTBL.TABLE_ID
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CCOL.COLUMN_NAME=SRC.ColumnName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
						AND CCOL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				OR
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			)
			
		UNION

		SELECT SRC.*,'N','Table is not part of console and is also not part of manifest. Error can be ignored.' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) BETWEEN 103 AND 107
		-- Table is Not part of console and not in manifest
	        AND  
			(
				NOT EXISTS  
				( 	SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					JOIN MY_REPORT_DB.CLARITY_COL CCOL ON CCOL.TABLE_ID=CTBL.TABLE_ID
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CCOL.COLUMN_NAME=SRC.ColumnName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
						AND CCOL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				AND
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			)		
		
	; 
			
		
-------------------------------------------------------------------------------------------------------------------------------
-- STEP-6  Analysis of errors 201,301 and 301,303. Epic/User View - Reporting Mismatch in Teradata. 
--         (Table or Column is present in Reporting but is missing in the view. Analysis done at both Table and Column Level)
-------------------------------------------------------------------------------------------------------------------------------
	
	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW
			
	-- Review Needed if Change is part of manifest or table is part of console. Can be ignored if not either of these 2
	-- Additionally Ignore the View if its replicated or referred in a different epic or user view
		SELECT SRC.*,'Y','Action Item for DBA. Epic or User View Needs to be refreshed.' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC
		LEFT JOIN DBC.TablesV EVUV
				ON TRIM(SRC.err_no) IN (201,301)
				AND TRIM(EVUV.RequestText) LIKE '%' || SRC.TableName || '%'
				AND TRIM(EVUV.TableKind)='V'
				AND TRIM(EVUV.TableName) NOT LIKE '%UPGR%'
				AND TRIM(EVUV.DatabaseName) IN ('MY_EPICDB','MY_USERDB')
				
		WHERE TRIM(SRC.err_no) IN (201,301)
		-- Table is part of console or change needed as per manifest
	        AND  
			(
				EXISTS  
				( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				OR
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)
			AND EVUV.TableName IS NULL
			
			
		UNION

		
		SELECT SRC.*,'N','Backup or Replicated Table. Table is being referred in a different EPIC or USER View' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC
		
		JOIN DBC.TablesV EVUV
				ON TRIM(SRC.err_no) IN (201,301)
				AND TRIM(EVUV.RequestText) LIKE '%' || SRC.TableName || '%'
				AND TRIM(EVUV.TableKind)='V'
				AND TRIM(EVUV.TableName) NOT LIKE '%UPGR%'
				AND TRIM(EVUV.DatabaseName) IN ('MY_EPICDB','MY_USERDB')
				
		WHERE TRIM(SRC.err_no) IN (201,301)
		-- Table is part of console or change needed as per manifest
	        AND  
			(
				EXISTS  
				( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				OR
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)
			AND EXISTS
			(
				SELECT 1 FROM  DBC.TablesV EVUV
				WHERE TRIM(SRC.err_no) IN (201,301)
				AND TRIM(EVUV.RequestText) LIKE '%' || SRC.TableName || '%'
				AND TRIM(EVUV.TableKind)='V'
				AND TRIM(EVUV.TableName) NOT LIKE '%UPGR%'
				AND TRIM(EVUV.DatabaseName) IN ('MY_EPICDB','MY_USERDB')
			)
			
			
		UNION
		
		
		SELECT SRC.*,'N','Table is not part of console and is also not part of manifest. Error can be ignored.' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (201,301)
		-- Table is Not part of console and not in manifest
	        AND  
			(
				NOT EXISTS  
				( SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				AND
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)		
		
	;
	
	
	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW
	-- Analysis at column Level
	-- Review Needed if Change is part of manifest or column is part of console. Can be ignored if not either of these 2
	
		SELECT SRC.*,'Y','Action Item for DBA. Epic or User View Needs to be refreshed.'  
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (203,303)
		-- Table is part of console or change needed as per manifest
	        AND  
			(
				EXISTS  
				( 	SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					JOIN MY_REPORT_DB.CLARITY_COL CCOL ON CCOL.TABLE_ID=CTBL.TABLE_ID
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CCOL.COLUMN_NAME=SRC.ColumnName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
						AND CCOL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				OR
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			)
			
		UNION

		SELECT SRC.*,'N','Table is not part of console and is also not part of manifest. Error can be ignored.' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (203,303)
		-- Table is Not part of console and not in manifest
	        AND  
			(
				NOT EXISTS  
				( 	SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					JOIN MY_REPORT_DB.CLARITY_COL CCOL ON CCOL.TABLE_ID=CTBL.TABLE_ID
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CCOL.COLUMN_NAME=SRC.ColumnName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
						AND CCOL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				AND
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			)		
		
	; 		
		

--------------------------------------------------------------------------------------------------------------------------------------
-- STEP-7  Analysis of errors 202,302.  Reporting - Epic/User View Mismatch in Teradata
--         (Table is present in View but is missing in the Reporting Table. Analysis done at Table Level)
--------------------------------------------------------------------------------------------------------------------------------------
	
	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW
	-- Review Needed if Change is part of manifest and difference is not an existing difference in Production. 
	
		SELECT SRC.*,'Y','Action Item for DBA.This Change to Production is not needed as per manifest.Either Drop View or find out if Table is Missing.' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (202,302)
		-- Is not an Existing difference and change is not needed as per manifest
	        AND  
			(
				-- Is NOT Exisiting Difference
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (202,302)
					AND C.Table_Name=SRC.TableName
					AND C.ERROR_NO=SRC.err_no
				)
				AND
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)
				
		UNION
		
		SELECT SRC.*,'N','View Being Added or Table being converted to a view. Valid Change as per manifest' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (202,302)
		-- Is not an Existing difference but change is needed as per manifest
	        AND  
			(
				-- Is NOT Exisiting Difference
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (202,302)
					AND C.Table_Name=SRC.TableName
					AND C.ERROR_NO=SRC.err_no
				)
				AND
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)
			
		UNION
		
		SELECT SRC.*,'N','View Being Added or Table being converted to a view. Valid Change as per manifest' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (202,302)
		-- Existing difference but change needed as per manifest
	        AND  
			(
				-- Is Exisiting Difference
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (202,302)
					AND C.Table_Name=SRC.TableName
					AND C.ERROR_NO=SRC.err_no
				)
				AND
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)	
		
		UNION
		
		SELECT SRC.*,'N','Ignore Error as Difference exists in Production and Change not required as per manifest' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (202,302)
		-- Existing difference and change not needed as per manifest
	        AND  
			(
				-- Is Exisiting Difference
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (202,302)
					AND C.Table_Name=SRC.TableName
					AND C.ERROR_NO=SRC.err_no
				)
				AND
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Table Drop','View Added','View Change','Deprec Table')
					AND M.TABLENAME=SRC.TableName
				)
			)		
	;


--------------------------------------------------------------------------------------------------------------------------------------
-- STEP-8  Analysis of errors 204,304.  Reporting - Epic/User View Mismatch in Teradata
--         (Column is present in View but is missing in the Reporting Table. Analysis done at Column Level)
--------------------------------------------------------------------------------------------------------------------------------------
	
	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW
	-- Review Needed if Change is part of manifest and difference is not an existing difference in Production. 
	
		SELECT SRC.*,'Y','Action Item for DBA.This Change to Production is not needed as per manifest.Either Drop Column from View or find out if Column Needs to be added to reporting Table is Missing.' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (204,304)
		-- Is not an Existing difference and change is not needed as per manifest
	        AND  
			(
				-- Is NOT Exisiting Difference
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (204,304)
					AND C.Table_Name=SRC.TableName AND C.Column_Name=SRC.ColumnName
					AND C.ERROR_NO=SRC.err_no
				)
				AND
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			)
				
		UNION
		
		SELECT SRC.*,'N','Column Being Added or Modified in Table or View. Valid Change as per manifest' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (204,304)
		-- Is not an Existing difference but change is needed as per manifest
	        AND  
			(
				-- Is NOT Exisiting Difference
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (204,304)
					AND C.Table_Name=SRC.TableName AND C.Column_Name=SRC.ColumnName
					AND C.ERROR_NO=SRC.err_no
				)
				AND
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			)
			
		UNION
		
		SELECT SRC.*,'N','Column Being Added or Modified in Table or View. Valid Change as per manifest' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (204,304)
		-- Existing difference but change needed as per manifest
	        AND  
			(
				-- Is Exisiting Difference
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (204,304)
					AND C.Table_Name=SRC.TableName AND C.Column_Name=SRC.ColumnName
					AND C.ERROR_NO=SRC.err_no
				)
				AND
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			)	
		
		UNION
		
		SELECT SRC.*,'N','Ignore Error as Difference exists in Production and Change not required as per manifest' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (204,304)
		-- Existing difference and change not needed as per manifest
	        AND  
			(
				-- Is Exisiting Difference
				EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (204,304)
					AND C.Table_Name=SRC.TableName AND C.Column_Name=SRC.ColumnName
					AND C.ERROR_NO=SRC.err_no
				)
				AND
				NOT EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Column Add','Column Modify','Deprec Column','UnDeprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			)		
	;	
	
	
	
	
--------------------------------------------------------------------------------------------------------------------------------------
-- STEP-8  Analysis of errors 305,306.  User View Mismatch in Teradata between Source and Target(Analysis done at Column Level)
--------------------------------------------------------------------------------------------------------------------------------------	
	INSERT INTO MY_USHAREDB.UPGRADE_ISSUES_REVIEW
	-- Review Needed if Change is part of manifest and difference is not an existing difference in Production. 
	
		SELECT SRC.*,'Y','Action Item for DBA or RSC.Change not included in manifest. Add column to manifest or remove from view since column is present in target view but not in source view' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (306)
			-- change not needed as per manifest
	        AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Column Add','Column Modify','Deprec Column','UnDeprec Column')
				AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
			)
			-- Is Not Exisiting Difference
			AND NOT	EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
					WHERE C.ERROR_NO IN (304)
					AND C.Table_Name=SRC.TableName AND C.Column_Name=SRC.ColumnName
				)
			AND EXISTS  
				( 	SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
		UNION
		
		SELECT SRC.*,'N','Valid Change as per manifest or exisiting difference. Can be ignored' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (306) AND
		-- change needed as per manifest
	     (
			EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Table Add','Column Add','Column Modify','Deprec Column','UnDeprec Column')
				AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
			)
			OR
			EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_CURRENT_ISSUES C
				WHERE C.ERROR_NO IN (304)
				AND C.Table_Name=SRC.TableName AND C.Column_Name=SRC.ColumnName
			)
			OR NOT EXISTS  
				( 	SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
		 )
		
		
		UNION
	
		SELECT SRC.*,'Y','Action Item for DBA.Change not needed as per manifest. Column Not in Target but was present in source' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (305)
			-- change not needed as per manifest
	        AND NOT EXISTS
			(
				SEL 1 FROM 
				MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
				WHERE TRIM(M.CHG_TYPE) IN ('Deprec Column')
				AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
			)
			AND EXISTS  
				( 	SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
				
		UNION
	
		SELECT SRC.*,'N','Valid Change as per manifest. Ignore Error' 
		FROM MY_USHAREDB.UPGRADE_ISSUES SRC 
		WHERE TRIM(SRC.err_no) IN (305)
			-- change needed as per manifest
	        AND 
			(
			EXISTS
				(
					SEL 1 FROM 
					MY_USHAREDB.UPGRADE_MANIFEST_LOAD M
					WHERE TRIM(M.CHG_TYPE) IN ('Deprec Column')
					AND M.TABLENAME=SRC.TableName AND M.COLUMNNAME=SRC.ColumnName
				)
			OR
			NOT EXISTS  
				( 	SEL	1 
					FROM	MY_REPORT_DB.CLARITY_TBL CTBL
					WHERE	CTBL.TABLE_NAME=SRC.TableName 
						AND CTBL.CM_PHY_OWNER_ID='MY_OWNER_ID'
				)
			)
	;

	
SELECT err_no,err_msg,dbname,erroring_dbname,tablename,ushare_or_cmps_or_S_def,hccl_or_T_def,mfst_def,columnname,
CASE WHEN EV_NOTES IS NOT NULL THEN 'Y' ELSE 'N' END CONVERTED_TO_EPIC_VIEW,
CASE WHEN MV_NOTES IS NOT NULL THEN 'Y' ELSE 'N' END MATERIALIZED,
MANUAL_REVIEW_REQD,REVIEW_COMMENTS
FROM MY_USHAREDB.UPGRADE_ISSUES_REVIEW 
LEFT JOIN VALIDATION_EPICVIEW EV ON EV_dbname=dbname AND EV_tableName=tablename
LEFT JOIN VALIDATION_MATVIEW MV ON MV_dbname=dbname AND MV_tableName=tablename
ORDER BY MANUAL_REVIEW_REQD DESC, err_no,erroring_dbname,tablename,columnname;

