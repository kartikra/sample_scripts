SELECT CASE 
	WHEN TableKind='T' THEN 'SHOW TABLE  ' || DatabaseName || '."' || TableName || '";'
	WHEN TableKind='V' THEN 'SHOW VIEW  ' || DatabaseName || '."' || TableName  || '";'
	WHEN TableKind='P' THEN 'SHOW PROCEDURE  ' || DatabaseName || '."' || TableName  || '";'
    END AS SQL_QUERY
FROM DBC.TablesV WHERE DatabaseName LIKE '$$DBNAME' AND TableName IS NOT NULL AND TableKind IN ('T', 'V','P')
AND TRIM(tablename) NOT LIKE ALL ('3/_%','BF%','%_Error1','%_Error2') ESCAPE '/';
