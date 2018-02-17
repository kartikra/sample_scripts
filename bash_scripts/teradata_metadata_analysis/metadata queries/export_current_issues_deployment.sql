 
-- Deployment : MY_OWNER_ID
-- Databases  : MY_REPORT_DB, MY_STAGE_DB
INSERT INTO MY_USHAREDB.UPGRADE_CURRENT_ISSUES
SELECT MY_OFFSET + 55 AS ERROR_NO,CAST('Table In Reporting but Not in Compass' AS VARCHAR(500)) COMMENTS, 
CAST(TRIM(A.TableName) AS VARCHAR(30)) TABLE_NAME , CAST(''  AS VARCHAR(30)) COLUMN_NAME
FROM DBC.TablesV A 
WHERE    TRIM(A.DatabaseName)='MY_REPORT_DB'
    AND NOT EXISTS  ( SEL    1 
                    FROM    MY_LEAD_REPORTDB.CLARITY_TBL B
                    WHERE    B.TABLE_NAME=A.TableName 
                        AND B.CM_PHY_OWNER_ID='MY_OWNER_ID'
                    )
;

INSERT INTO MY_USHAREDB.UPGRADE_CURRENT_ISSUES
SELECT MY_OFFSET + 53 AS ERROR_NO,CAST('Column In Reporting but Not in Compass' AS VARCHAR(500)) COMMENTS, 
CAST(TRIM(A.TableName) AS VARCHAR(30)) TABLE_NAME , CAST(TRIM(A.ColumnName)  AS VARCHAR(30)) COLUMN_NAME
FROM DBC.ColumnsV A 
--JOIN MY_LEAD_REPORTDB.CLARITY_TBL D ON  D.TABLE_NAME=A.TableName AND D.CM_PHY_OWNER_ID='MY_OWNER_ID'
WHERE    TRIM(A.DatabaseName)='MY_REPORT_DB'
    AND NOT EXISTS  ( SEL    1 
                    FROM    MY_LEAD_REPORTDB.CLARITY_COL B
                    JOIN MY_LEAD_REPORTDB.CLARITY_TBL C ON C.TABLE_ID=B.TABLE_ID
                    WHERE    C.TABLE_NAME=A.TableName 
                        AND B.COLUMN_NAME=A.ColumnName 
                        AND C.CM_PHY_OWNER_ID='MY_OWNER_ID'
                        AND B.CM_PHY_OWNER_ID='MY_OWNER_ID'
                    )
;

INSERT INTO MY_USHAREDB.UPGRADE_CURRENT_ISSUES
SELECT MY_OFFSET + 54 AS ERROR_NO,CAST('Table In Compass but Not in Reporting' AS VARCHAR(500)) COMMENTS, 
CAST(TRIM(A.TABLE_NAME) AS VARCHAR(30)) TABLE_NAME , CAST(''  AS VARCHAR(30)) COLUMN_NAME
FROM MY_LEAD_REPORTDB.CLARITY_TBL A
WHERE A.CM_PHY_OWNER_ID='MY_OWNER_ID'
AND     NOT EXISTS  ( SEL    1 
                    FROM    DBC.TablesV B
                    WHERE    B.DatabaseName IN ('MY_REPORT_DB','MY_LEAD_REPORTDB') 
                     AND B.TableName=A.TABLE_NAME 
                    )
;

INSERT INTO MY_USHAREDB.UPGRADE_CURRENT_ISSUES
SELECT MY_OFFSET + 52 AS ERROR_NO,CAST('Column In Compass but Not in Reporting' AS VARCHAR(500)) COMMENTS, 
CAST(TRIM(C.TABLE_NAME) AS VARCHAR(30)) TABLE_NAME , CAST(A.COLUMN_NAME  AS VARCHAR(30)) COLUMN_NAME
FROM MY_LEAD_REPORTDB.CLARITY_COL A
JOIN MY_LEAD_REPORTDB.CLARITY_TBL C ON C.TABLE_ID=A.TABLE_ID

WHERE A.CM_PHY_OWNER_ID='MY_OWNER_ID'
AND     NOT EXISTS  ( SEL    1 
                    FROM    DBC.ColumnsV B
                    WHERE    B.DatabaseName IN ('MY_REPORT_DB','MY_LEAD_REPORTDB') 
                     AND B.TableName=C.TABLE_NAME 
                     AND B.ColumnNAme=A.COLUMN_NAME 
                    )
;

-- 56, 57 Error Numbers
INSERT INTO MY_USHAREDB.UPGRADE_CURRENT_ISSUES
SELECT     MY_OFFSET + a.err_no, a.change_type AS error_msg, 
        a.tablename, a.columnname
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
        CASE WHEN "COMPASS_DTYPE" <> "HCCL_DTYPE"     THEN 'Database to Compass column datatype difference.'
            WHEN "COMPASS_SIZE" <> "HCCL_SIZE"         THEN 'Database to Compass column size difference.'
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
        DBC.ColumnsV c2
        -- ---------------------------------- 
        -- get clarity compass defintions 
        -- ---------------------------------- 
        INNER JOIN MY_LEAD_REPORTDB.CLARITY_TBL t1
            ON  TRIM(t1.table_name)  = TRIM(c2.tablename)
            AND t1.cm_phy_owner_id = 'MY_OWNER_ID'
            AND t1.is_extracted_yn = 'Y'
            AND t1.table_name NOT LIKE ALL ('BF%','V/_%') ESCAPE '/'
            AND c2.databasename IN ('MY_REPORT_DB','MY_LEAD_REPORTDB')
        INNER JOIN MY_LEAD_REPORTDB.CLARITY_COL c1 
            ON t1.table_id = c1.table_id
            AND TRIM(c2.columnname) = TRIM(c1.column_name)
            AND t1.cm_phy_owner_id  = c1.cm_phy_owner_id
            AND c1.is_extracted_yn = 'Y'
    WHERE     "CHANGE_TYPE" IS NOT NULL
    AND     "ERR_NO" NOT IN (0)
    ) AS a
    
;

INSERT INTO MY_USHAREDB.UPGRADE_CURRENT_ISSUES
SELECT  MY_OFFSET + 58 AS ERROR_NO,CAST('Table In Compass but Not in Staging' AS VARCHAR(500)) COMMENTS, 
CAST(TRIM(A.TABLE_NAME) AS VARCHAR(30)) TABLE_NAME , CAST(''  AS VARCHAR(30)) COLUMN_NAME
FROM MY_LEAD_REPORTDB.CLARITY_TBL A
WHERE A.CM_PHY_OWNER_ID='MY_OWNER_ID'
AND     NOT EXISTS  ( SEL    1 
                    FROM    DBC.TablesV B
                    WHERE   B.DatabaseName IN ('MY_STAGE_DB','MY_LEAD_STAGEDB')
                     AND B.TableName=A.TABLE_NAME 
                    )
;

COLLECT STATISTICS COLUMN(ERROR_NO,TABLE_NAME) ON MY_USHAREDB.UPGRADE_CURRENT_ISSUES;


