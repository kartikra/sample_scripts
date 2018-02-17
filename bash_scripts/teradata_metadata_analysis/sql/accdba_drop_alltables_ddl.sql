SELECT CASE 
	WHEN TableKind='T' THEN 'DROP TABLE /*$$TICKET*/ ' || DatabaseName || '."' || TableName || '";'
	WHEN TableKind='V' THEN 'DROP VIEW /*$$TICKET*/ ' || DatabaseName || '."' || TableName  || '";'
	WHEN TableKind='P' THEN 'DROP PROCEDURE /*$$TICKET*/ ' || DatabaseName || '."' || TableName  || '";'
    END AS SQL_QUERY
FROM DBC.TablesV WHERE DatabaseName LIKE '$$DBNAME' AND TableName IS NOT NULL AND TableName NOT LIKE 'TokenX_%' AND TableKind IN ('T', 'V','P');
