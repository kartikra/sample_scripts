SELECT
' Size taken in production by tables in ' || 
TRIM(TS.DatabaseName) ||  ' - ' AS SPACE_TAKEN_IN_PROD
,CAST(SUM(TS.CurrentPerm) / (1024*1024*1024) AS DECIMAL(21,3)) AS Current_Size_GB
FROM DBC.TableSize TS
JOIN
(
SELECT
TRIM(ObjectDatabaseName) ObjectDatabaseName,
TRIM(ObjectTableName) ObjectTableName
FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS
WHERE ObjectType='Tab' AND ObjectDatabaseName IS NOT NULL 
AND ObjectDatabaseName <> 'CLARITY_DBA_MAINT' 
AND ObjectTableName IS NOT NULL
GROUP BY 1,2
 ) LIST ON TRIM(TS.DatabaseName)=TRIM(LIST.ObjectDatabaseName)
 AND TRIM(TS.TableName)=TRIM(LIST.ObjectTableName)
 GROUP BY 1;

 
SELECT 
' Space Summary for ' || 
TRIM(DatabaseName) ||  ' - ' AS ARCHIVE_TEMP_SPACE_DETAIL
,CAST(SUM(CurrentPerm) / (1024*1024*1024) AS DECIMAL(21,3)) AS Current_Size_GB
,CAST(SUM(MaxPerm) / (1024*1024*1024) AS DECIMAL(21,3)) AS TotalSpace
,CAST((SUM(MaxPerm) -  SUM(CurrentPerm)) / (1024*1024*1024) AS DECIMAL(21,3))  AS AvailableSpace
FROM  DBC.diskspace 
WHERE DatabaseName LIKE ANY ( '%Archive%','MYCP%_UPG_AK_%_CHANGE','MYCP%_UPG_AK_%_CHANGE_%' ) 
GROUP BY 1
HAVING (AvailableSpace >= 300 OR TotalSpace >= 1000)
ORDER BY 4 DESC;
 
 
SELECT CAST ( 'Found Backup DB|' || TRIM(A.ObjectDatabaseName) AS VARCHAR(200)) AS OBJ_LIST
FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS A
WHERE A.ObjectType='DB' AND A.ObjectDatabaseName IS NOT NULL AND A.ObjectDatabaseName <> 'CLARITY_DBA_MAINT'
AND EXISTS
(
SELECT B.DatabaseName FROM DBC.TablesV B
WHERE B.TableKind='T' AND B.DatabaseName IS NOT NULL
      AND B.DatabaseName = A.ObjectDatabaseName
)
GROUP BY 1

 UNION ALL
 -- Logic Needs Change in case Table Name is different from EP/USER View Name
 -- Tables Present in Reporting but not in staging
 SELECT CAST ( 'Found Backup Table|' || TRIM(ObjectDatabaseName) || '|' || TRIM(ObjectTableName) AS VARCHAR(200)) AS OBJ_LIST
FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS
WHERE ObjectType='Tab' AND ObjectDatabaseName IS NOT NULL AND ObjectDatabaseName <> 'CLARITY_DBA_MAINT' AND ObjectTableName IS NOT NULL
 GROUP BY 1

UNION ALL
 
 -- NC Mat View Tables
 SELECT CAST ( 'Found Backup Table|' || TRIM(ObjectDatabaseName) || '|' || 
CASE WHEN SUBSTRING(trim(ObjectTableName) FROM LENGTH(trim(ObjectTableName)) FOR 1) = '1' 
			THEN SUBSTRING(trim(ObjectTableName) FROM 1 FOR LENGTH(trim(ObjectTableName)) - 1) || '2'
	 WHEN SUBSTRING(trim(ObjectTableName) FROM LENGTH(trim(ObjectTableName)) FOR 1) = '2' 
			THEN SUBSTRING(trim(ObjectTableName) FROM 1 FOR LENGTH(trim(ObjectTableName)) - 1) || '1'
END
 AS VARCHAR(200)) AS OBJ_LIST
FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS
WHERE ObjectType='Tab' AND ObjectDatabaseName IS NOT NULL AND ObjectDatabaseName = 'MYCPPNCKP_T' AND TRIM(ObjectTableName) LIKE  ANY ('%1','%2')
 GROUP BY 1

 UNION ALL
 
  -- SC Mat View Tables
 SELECT CAST ( 'Found Backup Table|' || TRIM(ObjectDatabaseName) || '|' || 
 CASE WHEN SUBSTRING(TRIM(ObjectTableName)  FROM 1 FOR 2) = '1_' 
		THEN '2_' || SUBSTRING(TRIM(ObjectTableName) FROM 3)
		ELSE '1_' || SUBSTRING(TRIM(ObjectTableName)  FROM 3) END 
 AS VARCHAR(200)) AS OBJ_LIST
FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS
WHERE ObjectType='Tab' AND ObjectDatabaseName IS NOT NULL AND ObjectDatabaseName = 'MYCPPSCKP_T' AND TRIM(ObjectTableName) LIKE  ANY ('1_%','2_%') 
 GROUP BY 1 
 
;
 
 
 SELECT CAST ( 'Found Backup View|' ||  TRIM(T1.ObjectDatabaseName) || '|' || TRIM(T1.View_Type) || '|' || 
TRIM(MAX(T2.OBJ_COUNT))  || '|' || TRIM(T1.ObjectTableName) AS VARCHAR(200) ) AS OBJ_LIST

	FROM
	
	(
		-- Simple Views
		SELECT 	B.ObjectType,
				CAST(B.SessionId AS INTEGER) SessionId, 
				CAST(B.RequestNum AS INTEGER) RequestNum, 
				CAST(B.ObjectDatabaseName AS VARCHAR(30)) ObjectDatabaseName,
				CAST(B.ObjectTableName AS VARCHAR(30)) ObjectTableName,
				'S' As View_Type
		FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS B
		WHERE B.ObjectType='Viw' 
		AND B.ObjectDatabaseName IS NOT NULL  
		AND B.ObjectTableName IS NOT NULL
		AND NOT EXISTS
		(
		SELECT A.ObjectTableName
		FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS A
		WHERE A.ObjectType='Viw' 
		AND A.ObjectDatabaseName IS NOT NULL AND A.ObjectDatabaseName <> 'CLARITY_DBA_MAINT'
		AND A.ObjectTableName IS NOT NULL
		AND A.SessionID=B.SessionId AND A.requestNum=B.RequestNum
		AND A.ObjectTableName <> B.ObjectTableName
		)

		UNION

		--  Views not part of original list(Additional Views that need backing up)
		SELECT B.ObjectType,B.SessionId, B.RequestNum, 
			B.ObjectDatabaseName,B.ObjectTableName,'A'
		FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS B
		WHERE B.ObjectType='Viw' 
		AND B.ObjectDatabaseName IS NOT NULL AND B.ObjectDatabaseName <> 'CLARITY_DBA_MAINT'
		AND B.ObjectTableName IS NOT NULL
		AND EXISTS
		(
		SELECT A.ObjectTableName
		FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS A
		WHERE A.ObjectType='Viw' 
		AND A.ObjectDatabaseName IS NOT NULL AND A.ObjectDatabaseName <> 'CLARITY_DBA_MAINT'
		AND A.ObjectTableName IS NOT NULL
		AND A.SessionID=B.SessionId AND A.requestNum=B.RequestNum
		AND A.ObjectTableName <> B.ObjectTableName
		)
		AND NOT EXISTS
		(
		SELECT C.BKUP_TABNAME FROM CLARITY_DBA_MAINT.UPG_BKUP_ANALYSIS C
		WHERE C.BKUP_TABNAME=B.ObjectTableName
		)

		UNION

		-- Complex Views
		SELECT B.ObjectType,B.SessionId, B.RequestNum, 
				 B.ObjectDatabaseName,B.ObjectTableName,'C'
		FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS B
		WHERE B.ObjectType='Viw' 
		AND B.ObjectDatabaseName IS NOT NULL AND B.ObjectDatabaseName <> 'CLARITY_DBA_MAINT'
		AND B.ObjectTableName IS NOT NULL
		AND EXISTS
		(
		SELECT A.ObjectTableName
		FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS A
		WHERE A.ObjectType='Viw' 
		AND A.ObjectDatabaseName IS NOT NULL AND A.ObjectDatabaseName <> 'CLARITY_DBA_MAINT'
		AND A.ObjectTableName IS NOT NULL
		AND A.SessionID=B.SessionId AND A.requestNum=B.RequestNum
		AND A.ObjectTableName <> B.ObjectTableName
		)
		AND EXISTS
		(
		SELECT C.BKUP_TABNAME FROM CLARITY_DBA_MAINT.UPG_BKUP_ANALYSIS C
		WHERE C.BKUP_TABNAME=B.ObjectTableName
		)

	) T1

		JOIN
	
	(
		SELECT 
		B.SessionId, 
		B.RequestNum,
		B.ObjectDatabaseName,
		COUNT(DISTINCT B.ObjectTableName) AS OBJ_COUNT
		FROM CLARITY_DBA_MAINT.QUERY_LOG_RESULTS B
		WHERE B.ObjectDatabaseName IS NOT NULL AND B.ObjectDatabaseName <> 'CLARITY_DBA_MAINT'
		AND B.ObjectTableName IS NOT NULL
		AND B.ObjectType='Viw' 
		GROUP BY 1,2,3
		
	) T2 ON T1.SessionId=T2.SessionId AND T1.RequestNum=T2.RequestNum
					AND T2.ObjectDatabaseName=T1.ObjectDatabaseName
				
GROUP BY T1.ObjectDatabaseName, T1.ObjectTableName, T1.View_Type
ORDER BY 1




