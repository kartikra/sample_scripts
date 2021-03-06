DROP TABLE MY_USHAREDB.UPGRADE_TABLES;

CREATE SET TABLE MY_USHAREDB.UPGRADE_TABLES,
	NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT
 (	dbname				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	tablename			VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	create_date			DATE FORMAT 'YYYY/MM/DD',
 	cm_phy_owner_id		VARCHAR(25) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX (dbname, tablename);

GRANT select ON MY_USHAREDB.UPGRADE_TABLES to PUBLIC;


DROP TABLE MY_USHAREDB.UPGRADE_COLUMNS;

CREATE SET TABLE MY_USHAREDB.UPGRADE_COLUMNS,
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

GRANT select ON MY_USHAREDB.UPGRADE_COLUMNS to PUBLIC;



DROP TABLE MY_USHAREDB.UPGRADE_COLUMNS_VIEWS;

CREATE SET TABLE MY_USHAREDB.UPGRADE_COLUMNS_VIEWS ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT
     (tablename VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      columnname VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      create_date				DATE FORMAT 'YYYY/MM/DD')
PRIMARY INDEX ( tablename ,columnname );

GRANT select ON MY_USHAREDB.UPGRADE_COLUMNS_VIEWS to PUBLIC;





DROP TABLE MY_USHAREDB.UPGRADE_STG_TBLS;

CREATE SET TABLE MY_USHAREDB.UPGRADE_STG_TBLS,
	NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT
(	dbname				VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	tablename			VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	create_date				DATE FORMAT 'YYYY/MM/DD',
 	cm_phy_owner_id		VARCHAR(25)	CHARACTER SET LATIN NOT CASESPECIFIC
)  PRIMARY INDEX (dbname, tablename);

GRANT ALL ON MY_USHAREDB.UPGRADE_STG_TBLS to PUBLIC;




DROP TABLE MY_USHAREDB.UPGRADE_STG_COLUMNS;

CREATE SET TABLE MY_USHAREDB.UPGRADE_STG_COLUMNS,
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

GRANT select ON MY_USHAREDB.UPGRADE_STG_COLUMNS to PUBLIC;