SELECT 
TRIM(A.WITS_DatabaseName) || '|'
TRIM(A.WITS_TableName) || '|'
TRIM(B.DatabaseName) || '|' ||
TRIM(B.TableName) AS QUERY_RESULT

FROM DBC.TablesV B
JOIN MYCPSC_USHARE.WITS_UPDATE_VIEW_ANALYSIS A ON
(A.WITS_TableName=SUBSTR(TRIM(B.TableName),3) OR
A.WITS_TableName=SUBSTR(TRIM(B.TableName),1,LENGTH(TRIM(B.TableName))-1) OR
A.WITS_TableName=TRIM(B.TableName)
)
AND A.TypeofRefresh='TABLE'
AND B.DatabaseName IN ('MYCPDSCKP8_T','KPBIDSC_T');


SELECT 
TRIM(A.WITS_DatabaseName) || '|' ||
TRIM(A.WITS_TableName) || '|' ||
TRIM(B.DatabaseName) || '|' ||
TRIM(B.TableName) AS QUERY_RESULT

FROM DBC.TablesV B
JOIN MYCPNC_USHARE.WITS_UPDATE_VIEW_ANALYSIS A ON
(A.WITS_TableName=SUBSTR(TRIM(B.TableName),3) OR
A.WITS_TableName=SUBSTR(TRIM(B.TableName),1,LENGTH(TRIM(B.TableName))-1) OR
A.WITS_TableName=TRIM(B.TableName)
)
AND A.TypeofRefresh='TABLE'
AND B.DatabaseName IN ('MYCPVNCKP_T','KPBIVNC_T')

GROUP BY 1;


SELECT 
TRIM(A.WITS_DatabaseName) || '|' ||
TRIM(A.WITS_TableName) || '|' ||
TRIM(B.DatabaseName) || '|' ||
TRIM(B.TableName) AS QUERY_RESULT

FROM DBC.TablesV B
JOIN MYCPNC_USHARE.WITS_UPDATE_VIEW_ANALYSIS A ON
(A.WITS_TableName=SUBSTR(TRIM(B.TableName),3) OR
A.WITS_TableName=SUBSTR(TRIM(B.TableName),1,LENGTH(TRIM(B.TableName))-1) OR
A.WITS_TableName=TRIM(B.TableName)
)
AND A.TypeofRefresh='TABLE'
AND B.DatabaseName IN ('MYCPVNC_TPF_T','MYCPVNCA_TPF_T','MYCPVNCB_TPF_T','MYCPVNCC_TPF_T','MYCPVNCD_TPF_T','MYCPVNCE_TPF_T','MYCPVNCF_TPF_T','MYCPVNCG_TPF_T',
'MYCPVNC_TPF_S','MYCPVNCA_TPF_S','MYCPVNCB_TPF_S','MYCPVNCC_TPF_S','MYCPVNCD_TPF_S','MYCPVNCE_TPF_S','MYCPVNCF_TPF_S','MYCPVNCG_TPF_S')

GROUP BY 1;


