-- ---------------------			
-- UPGRADE_DB_OWNER_LINK
-- ---------------------			

-- ---------------------------------------------------------------------------------------------
-- Update the insert statements if the WITS or PROD envs have changed, then run the create table
-- statement followed by ALL the insert statements (PROD and WITS)
-- ---------------------------------------------------------------------------------------------

DROP TABLE MY_USHAREDB.UPGRADE_DB_OWNER_LINK;
		
CREATE SET TABLE MY_USHAREDB.UPGRADE_DB_OWNER_LINK,
	NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT
 (	cm_phy_owner_id			VARCHAR(25) CHARACTER SET LATIN NOT CASESPECIFIC,
	rpt_db					VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	stg_db					VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	epic_db					VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
	env					VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX (cm_phy_owner_id,env);

GRANT select ON MY_USHAREDB.UPGRADE_DB_OWNER_LINK to PUBLIC;	


-- --------------------------
-- Insert Statements for PROD
-- --------------------------
			
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121312', 'HCCLPNC2A_T' ,'HCCLPNC2A_S','HCCLPNC2','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121320', 'HCCLPNC2B_T' ,'HCCLPNC2B_S','HCCLPNC2','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121318', 'HCCLPNC2C_T' ,'HCCLPNC2C_S','HCCLPNC2','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121314', 'HCCLPNC2D_T' ,'HCCLPNC2D_S','HCCLPNC2','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121316', 'HCCLPNC2E_T' ,'HCCLPNC2E_S','HCCLPNC2','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121322', 'HCCLPNC2F_T' ,'HCCLPNC2F_S','HCCLPNC2','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('1216', 'HCCLPNC2G_T' ,'HCCLPNC2G_S','HCCLPNC2','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120160', 'HCCLPNC2_T' ,'HCCLPNC2_S','HCCLPNC2','PROD');

INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121212', 'HCCLPSCA_T' ,'HCCLPSCA_S','HCCLPSC','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121214', 'HCCLPSCB_T' ,'HCCLPSCB_S','HCCLPSC','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121216', 'HCCLPSCC_T' ,'HCCLPSCC_S','HCCLPSC','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121218', 'HCCLPSCD_T' ,'HCCLPSCD_S','HCCLPSC','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121220', 'HCCLPSCE_T' ,'HCCLPSCE_S','HCCLPSC','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121222', 'HCCLPSCF_T' ,'HCCLPSCF_S','HCCLPSC','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('1217', 'HCCLPSCG_T' ,'HCCLPSCG_S','HCCLPSC','PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120150', 'HCCLPSC_T' ,'HCCLPSC_S','HCCLPSC','PROD');

INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120140', 'HCCLPCO2_T', 'HCCLPCO2_S', 'HCCLPCO2', 'PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120200', 'HCCLPGA2_T', 'HCCLPGA2_S', 'HCCLPGA2', 'PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120130', 'HCCLPHI_T', 'HCCLPHI_S', 'HCCLPHI', 'PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120170', 'HCCLPMA_T', 'HCCLPMA_S', 'HCCLPMA', 'PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120190', 'HCCLPNW2_T', 'HCCLPNW2_S', 'HCCLPNW2', 'PROD');
INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120180', 'HCCLPOH2_T', 'HCCLPOH2_S', 'HCCLPOH2', 'PROD');			


-- --------------------------
-- Insert Statements for WITS
-- --------------------------

--INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('121312', 'HCCLDNC9A_T' ,'HCCLDNC9A_S','HCCLDNC9A','WITS');
--INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120140', 'HCCLDCO9_T', 'HCCLDCO9_S', 'HCCLDCO9', 'WITS');
--INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120200', 'HCCLDGA9_T', 'HCCLDGA9_S', 'HCCLDGA9', 'WITS');
--INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120130', 'HCCLDHI9_T', 'HCCLDHI9_S', 'HCCLDHI9', 'WITS');
--INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120170', 'HCCLDMA9_T', 'HCCLDMA9_S', 'HCCLDMA9', 'WITS');
--INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120190', 'HCCLDNW9_T', 'HCCLDNW9_S', 'HCCLDNW9', 'WITS');
--INSERT INTO MY_USHAREDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, epic_db, env) VALUES ('120180', 'HCCLDOH9_T', 'HCCLDOH9_S', 'HCCLDOH9', 'WITS');

