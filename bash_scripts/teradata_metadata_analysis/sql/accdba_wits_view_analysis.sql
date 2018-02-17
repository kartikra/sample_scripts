
CREATE VOLATILE TABLE FOUND_WITS_TABLE AS
(
SEL A.*
FROM MY_USHARE_DB.WITS_UPDATE_VIEW_ANALYSIS A
JOIN DBC.TablesV B ON A.WITS_DatabaseName=TRIM(B.DatabaseName) 
AND A.WITS_TableName=TRIM(B.TableName)
AND B.DatabaseName IN ('MY_REPORT_DB','MY_EPVIEW_DB')
)
WITH DATA 
ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE EXISTING_EP_VIEWS AS
(
SEL B.DatabaseName, A.WITS_TableName, 
CASE WHEN B.RequestText LIKE '%ROLE%' AND 
B.RequestText NOT LIKE '%SEL%*%' AND B.RequestText NOT LIKE '%SELECT%*%' 
THEN 'DR' ELSE 'NO DR' END AS VIEW_TYPE
FROM MY_USHARE_DB.WITS_UPDATE_VIEW_ANALYSIS A
JOIN DBC.TablesV B ON  A.WITS_TableName=TRIM(B.TableName)
		AND B.DatabaseName='MY_EPVIEW_DB' AND A.TypeofRefresh='TABLE'
JOIN FOUND_WITS_TABLE C ON A.WITS_TableName=C.WITS_TableName
		AND A.WITS_DatabaseName=C.WITS_DatabaseName
)
WITH DATA 
ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE EXISTING_EP_VIEWS_COLADD AS
(
SEL A.DatabaseName,  A.WITS_TableName, T1.ColumnName
FROM EXISTING_EP_VIEWS A
JOIN DBC.ColumnsV T1 ON T1.TableName=A.WITS_TableName
WHERE A.VIEW_TYPE='DR' AND T1.DatabaseName='MY_REPORT_DB' 
MINUS
SEL A.DatabaseName,  A.WITS_TableName, T2.ColumnName
FROM EXISTING_EP_VIEWS A
JOIN DBC.ColumnsV T2 ON T2.TableName=A.WITS_TableName
WHERE A.VIEW_TYPE='DR' AND T2.DatabaseName='MY_EPVIEW_DB' 
)
WITH DATA 
ON COMMIT PRESERVE ROWS;


CREATE VOLATILE TABLE EXISTING_USER_VIEWS AS
(
SEL B.DatabaseName, A.WITS_TableName, 
CASE WHEN B.RequestText LIKE '%ROLE%' AND 
B.RequestText NOT LIKE '%SEL%*%' AND B.RequestText NOT LIKE '%SELECT%*%' 
THEN 'DR' ELSE 'NO DR' END AS VIEW_TYPE
FROM MY_USHARE_DB.WITS_UPDATE_VIEW_ANALYSIS A
JOIN DBC.TablesV B ON  A.WITS_TableName=TRIM(B.TableName)
		    AND B.DatabaseName='MY_USERVIEW_DB'
JOIN FOUND_WITS_TABLE C ON A.WITS_TableName=C.WITS_TableName
		AND A.WITS_DatabaseName=C.WITS_DatabaseName
)
WITH DATA 
ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE EXISTING_USER_VIEWS_COLADD AS
(
SEL A.DatabaseName,  A.WITS_TableName, T1.ColumnName
FROM EXISTING_USER_VIEWS A
JOIN DBC.ColumnsV T1 ON T1.TableName=A.WITS_TableName
WHERE A.VIEW_TYPE='DR' AND T1.DatabaseName='MY_REPORT_DB' 
MINUS
SEL A.DatabaseName,  A.WITS_TableName, T2.ColumnName
FROM EXISTING_USER_VIEWS A
JOIN DBC.ColumnsV T2 ON T2.TableName=A.WITS_TableName
WHERE A.VIEW_TYPE='DR' AND T2.DatabaseName='MY_USERVIEW_DB' 
)
WITH DATA 
ON COMMIT PRESERVE ROWS;




-- New EP Views
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_USER/dbmig/outfiles/MY_EPVIEW_DB_new_EP_views.dat;

SEL 'MY_EPVIEW_DB|' || WITS_TableName
FROM FOUND_WITS_TABLE WHERE TypeOfRefresh='TABLE'
MINUS
SEL  'MY_EPVIEW_DB|' || WITS_TableName
FROM EXISTING_EP_VIEWS;




-- Existing EP Views with no Column Additions
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_USER/dbmig/outfiles/MY_EPVIEW_DB_refresh_EP_views.sql;

SEL 'SHOW VIEW ' || DatabaseName || '."' || WITS_TableName || '";' FROM EXISTING_EP_VIEWS
WHERE VIEW_TYPE='NO DR'

UNION

SEL 'SHOW VIEW ' || DatabaseName || '."' || WITS_TableName || '";' 
FROM EXISTING_EP_VIEWS A
WHERE VIEW_TYPE='DR' AND NOT EXISTS
(
  SEL B.*
  FROM EXISTING_EP_VIEWS_COLADD B
  WHERE A.WITS_TableName=B.WITS_TableName
);




-- Existing EP Views with Data Restriction and Column Additions
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_USER/dbmig/outfiles/MY_EPVIEW_DB_existing_custom_EP_views.dat;


SEL 'MY_EPVIEW_DB|' || A.WITS_TableName || '|' || B.ColumnName
FROM EXISTING_EP_VIEWS A
  JOIN EXISTING_EP_VIEWS_COLADD B ON A.WITS_TableName=B.WITS_TableName
 WHERE A.VIEW_TYPE='DR' AND EXISTS
(
  SEL B.*
  FROM EXISTING_EP_VIEWS_COLADD B
  WHERE A.WITS_TableName=B.WITS_TableName
)
ORDER BY 1;



-- New User Views
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_USER/dbmig/outfiles/MY_USERVIEW_DB_new_user_views.dat;

SEL 'MY_USERVIEW_DB|' || WITS_TableName
FROM FOUND_WITS_TABLE
MINUS
SEL 'MY_USERVIEW_DB|' || WITS_TableName
FROM EXISTING_USER_VIEWS;


--  User Views for Additional User View
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_USER/dbmig/outfiles/MY_OTHER_USERVIEW_DB_user_views.dat;

SEL 'MY_OTHER_USERVIEW_DB|' || WITS_TableName
FROM FOUND_WITS_TABLE

GROUP BY 1;



-- Existing User Views with no Column Addition
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_USER/dbmig/outfiles/MY_USERVIEW_DB_refresh_user_views.sql;

SEL 'SHOW VIEW ' || DatabaseName || '."' || WITS_TableName || '";' 
FROM EXISTING_USER_VIEWS
WHERE VIEW_TYPE='NO DR'

UNION

SEL 'SHOW VIEW ' || DatabaseName || '."' || WITS_TableName || '";' 
FROM EXISTING_USER_VIEWS A
WHERE VIEW_TYPE='DR' AND NOT EXISTS
(
  SEL B.*
  FROM EXISTING_USER_VIEWS_COLADD B
  WHERE A.WITS_TableName=B.WITS_TableName
);




-- Existing User Views with Data Restrictions and Column Addition
.EXPORT RESET; 
.EXPORT REPORT FILE = /users/MY_USER/dbmig/outfiles/MY_USERVIEW_DB_existing_custom_user_views.dat;

SEL 'MY_USERVIEW_DB|' || A.WITS_TableName || '|' || B.ColumnName
FROM EXISTING_USER_VIEWS A
  JOIN EXISTING_USER_VIEWS_COLADD B ON A.WITS_TableName=B.WITS_TableName
 WHERE A.VIEW_TYPE='DR' AND EXISTS
(
  SEL B.*
  FROM EXISTING_USER_VIEWS_COLADD B
  WHERE A.WITS_TableName=B.WITS_TableName
)
 ORDER BY 1;
 

