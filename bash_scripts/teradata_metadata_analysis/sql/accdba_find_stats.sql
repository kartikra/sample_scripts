SELECT 'COLUMN(' || TRIM(ColumnName) || ')' AS QRY_RESULT
FROM
(
	SELECT DatabaseName,TableName, ColumnName
	from dbc.StatsV  
	WHERE DatabaseName='$$DBNAME' AND ColumnName IS NOT NULL AND TableName='$$TABLE'
	ORDER BY StatsId
) TEMP;