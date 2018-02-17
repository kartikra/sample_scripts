#!/usr/bin/ksh

	# STEP-1 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi



	#runId=""
	#relName=""			#   Region + Cutover Date
	#ticketNo=""			#  Get From Remedy or Ask ETL team
    	#regionProfile=""		#  Look at Db Change list and detrrmine which profile to use
	#region=""			#  Region of the upgrade 
       

	runId="2"
	relName="nw0702"
	ticketNo="CRQ000000216032"
    regionProfile="TESTNW"
	region="NW"

		# List of Input Files
	dbChgList="nw0702_chglist_v2.txt"


	# STEP-2 Create Log File

	scriptName=`basename $0`
	dateforlog=`date +%Y%m%d%H%M%S`
	logName=$scriptName-${dateforlog}.log
	logFileName=$LOGDIR/$logName

	
	rm -f $OUTDIR/drive_migration.out
	touch $OUTDIR/drive_migration.out



	USR_PROF=$HOMEDIR/region/$regionProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $regionProfile.profile cannot be found, Exiting"
                exit 902
        fi




	# STEP-3 (Migration Tasks Start from Here)


	#-------------------------------------------    Create the Tables on Temp Database ----------------------------------------------
	#$SCRIPTDIR/epdba_perform_dbmaint.sh 1 HCCL"$region"_UPG_RK_% $TDPROD $ticketNo
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_prod_new_temptables_ddl.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_prod_existing_temptables_ddl.sql  $OUTDIR/drive_migration.out  | tee -a $logFileName
  
	
	#-----------------------------------   Move Data from Prod to Temp Database (Dry Run) ---------------------------------------------

	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_tabcol_add.sql  $OUTDIR/drive_migration.out | tee -a  $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_dtype_change_3.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_dtype_change_4.sql  $OUTDIR/drive_migration.out | tee -a  $logFileName

	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_stats_exist.sql  $OUTDIR/drive_migration.out  | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_stats_new.sql  $OUTDIR/drive_migration.out | tee -a $logFileName


	#-----------------------------------   Move Data from Prod to Temp Database (Migration) ---------------------------------------------

	# Delete data from the tables before running
	#$SCRIPTDIR/epdba_perform_dbmaint.sh 2 HCCL"$region"_UPG_AK_% $TDPROD $ticketNo

	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_tabcol_add.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_dtype_change_3.sql  $OUTDIR/drive_migration.out | tee -a  $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_dtype_change_4.sql  $OUTDIR/drive_migration.out | tee -a  $logFileName


	#Exclude New Tables
	#cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND TABLE ENTRY" | tr '[a-z]' '[A-Z]' | grep -v -i "UPG_AK_TAB_ADD" | cut -f5,6  -d'|' | sort | uniq  > $AUDITDIR/"$relName"_source_migration_tables.dat
	#$SCRIPTDIR/epdba_load_audit.sh "$relName"_source_migration_tables.dat $TDPROD $ticketNo $runId 104


	#cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND TABLE ENTRY" | tr '[a-z]' '[A-Z]' | grep -v -i "UPG_AK_Tab_Add" | cut -f4,6  -d'|' | sort | uniq  > $AUDITDIR/"$relName"_temp_migration_tables.dat
	#$SCRIPTDIR/epdba_load_audit.sh "$relName"_temp_migration_tables.dat $TDPROD $ticketNo $runId 204

	
	#-----------------------------------   Staging Table Creation ---------------------------------------------

	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_staging_tables_drop_current.sql  $OUTDIR/drive_migration.out | tee -a  $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql  $OUTDIR/drive_migration.out | tee -a  $logFileName
	

	#-----------------------------------   Move New Structure from Temp to Prod Database (Cutover) ---------------------------------------------

	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_rename.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_copy_from_wits.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userviews_refresh_with_dr.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_without_dr.sql  $OUTDIR/drive_migration.out | tee -a $logFileName

	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_customviews.sql $OUTDIR/drive_migration.out | tee -a $logFileName

	#cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND TABLE ENTRY" | tr '[a-z]' '[A-Z]' | grep -v -i "UPG_AK_Tab_Add"  | cut -f5,6  -d'|' | sort | uniq   > $AUDITDIR/"$relName"_final_migration_tables.dat
	#$SCRIPTDIR/epdba_load_audit.sh "$relName"_final_migration_tables.dat $TDPROD $ticketNo $runId 304


	#-----------------------------------------  Additional Steps for NCAL and SCAL (TPF)  ------------------------------------------#

	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_tpf_new_structure_ddl.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_tpf_migrate_data.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
    #$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_tpf_change_storeproc.sql $OUTDIR/drive_migration.out | tee -a $logFileName


	#-----------------  Reasonable Volume Check ---------------------------------------
	#cp $DIR/$reason $AUDITDIR

	$SCRIPTDIR/epdba_load_audit.sh "$relName"_reasonable_volume.dat $TDPROD $ticketNo $runId 105

	#$SCRIPTDIR/epdba_load_audit.sh "$relName"_reasonable_volume.dat $TDPROD $ticketNo $runId 205

