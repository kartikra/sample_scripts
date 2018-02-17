UPDATE MY_USHAREDB.UPGRADE_TABLES SET dbname='MY_REPORT_DB' WHERE dbname='MY_SOURCE_REPORT_DB' ;
UPDATE MY_USHAREDB.UPGRADE_COLUMNS SET dbname='MY_REPORT_DB' WHERE dbname='MY_SOURCE_REPORT_DB' ;
UPDATE MY_USHAREDB.UPGRADE_STG_TBLS SET dbname='MY_STAGE_DB' WHERE dbname='MY_SOURCE_STAGE_DB';
UPDATE MY_USHAREDB.UPGRADE_STG_COLUMNS SET dbname='MY_STAGE_DB' WHERE dbname='MY_SOURCE_STAGE_DB';

DROP TABLE MY_USHAREDB.upgrade_issues_combined;

CREATE MULTISET TABLE MY_USHAREDB.upgrade_issues_combined ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      runname VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      create_dttm TIMESTAMP(0),
      err_no INTEGER,
      err_msg VARCHAR(200) CHARACTER SET LATIN NOT CASESPECIFIC,
      dbname VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      erroring_dbname VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      tablename VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      stg_db VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      stg_table VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      columnname VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      slno VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      is_preserved VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      on_demand VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      is_extracted VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      data_retained VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      is_deprecated VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      testing_rqrd VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      cm_phy_owner_id VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      ushare_or_cmps_or_S_def VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      hccl_or_T_def VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      mfst_def VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      ushare_or_cmps_or_S_cmprs VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      hccl_or_T_cmprs VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      issue_comment VARCHAR(5000) CHARACTER SET LATIN NOT CASESPECIFIC,
      tbl_ini VARCHAR(254) CHARACTER SET LATIN NOT CASESPECIFIC,
      col_fmt_ini VARCHAR(254) CHARACTER SET LATIN NOT CASESPECIFIC,
      col_fmt_item DECIMAL(12,2))
PRIMARY INDEX ( err_no ,dbname ,tablename ,columnname );

