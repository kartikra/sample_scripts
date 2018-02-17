#!/usr/bin/ksh

	relName="nw0221"
	ticketNo="CRQ000000279721"
    regionProfile="DMDLNW2"
	dbChgList="nw0221_chglist_v1.txt"
	stagingList="nw0221_staging_list_v1.txt"
	backupList="nw0221_backup_list_v2.txt"
	
	
	prefix=`echo $relName | awk '{print substr($0,3,4)}'`
	
	if [ -z "$relName" ] || [ -z "$ticketNo" ] || [ -z "$regionProfile" ] || [ -z "$dbChgList" ] || [ -z "$backupList" ] 
	then
		echo "Not All Mandatory Parameters available.. Aborting Script"
		exit 901
	fi
	
	
# STEP-1 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi

		
# STEP-2 Run Region Profile File

	USR_PROF=$HOMEDIR/region/$regionProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $regionProfile.profile cannot be found, Exiting"
                exit 902
		else
			. $HOMEDIR/region/PROD_"$region".profile
			rt_cd=$?
			if [ $rt_cd -ne 0 ]
			then
                echo "Profile file PROD_"$region".profile cannot be found, Exiting"
                exit 902
			fi
        fi

		
		
	scriptName=`basename $0`
	rm -f $TEMPDIR/"$scriptName"_email_attach.dat
	touch $TEMPDIR/"$scriptName"_email_attach.dat
	rm -f $TEMPDIR/"$scriptName"_email_additional_info.dat
	touch $TEMPDIR/"$scriptName"_email_additional_info.dat
	
	if [ ! -f $DIR/$dbChgList ]
	then
		echo "DB Change List $DIR/$dbChgList Not Found.. Aborting Script"
		exit 903
	fi
	if [ ! -f $DIR/$backupList ]
	then
		echo "Backup List $DIR/$backupList Not Found.. Aborting Script"
		exit 904
	fi

# Define Functions
#--------------------------------------------------------------------------------------------------------------------------------------------------------

get_report () {
	sed -e 's/'MY_RUN_ID'/'$runId'/g' $SQLDIR/accdba_upgrade_validation_reports.sql  > $SQLDIR/archive/$relName/"$relName"_validation_reports.sql
	reportPos=`grep -i -w -n ""$1"" $SQLDIR/archive/$relName/"$relName"_validation_reports.sql | cut -f1 -d':'`
	grep -i -w -n "SELECT" $SQLDIR/archive/$relName/"$relName"_validation_reports.sql | cut -f1 -d':' | while read -r selectPos; do
		if [ $selectPos -gt $reportPos ]
		then
			break;
		else
			startPos=$selectPos
		fi
	done
	grep -i -n ";" $SQLDIR/archive/$relName/"$relName"_validation_reports.sql | cut -f1 -d':' | while read -r endPos; do
		if [ $endPos -gt $reportPos ]
		then
			break;
		fi
	done
	
	sed -n ''$startPos','$endPos' p' $SQLDIR/archive/$relName/"$relName"_validation_reports.sql > $TEMPDIR/"$relName"_"$1".sql
	$SCRIPTDIR/epdba_runSQLFile2.sh $TDPROD $TEMPDIR/"$relName"_"$1".sql $TEMPDIR/"$relName"_"$1".out | tee -a  $logFileName

	sed '1d' $TEMPDIR/"$relName"_"$1".out | sort -t',' $3 > $TEMPDIR/"$relName"_"$1".tmp
	mv $TEMPDIR/"$relName"_"$1".tmp $TEMPDIR/"$relName"_"$1".out
	
	if [ ! -z $2 ]
	then
		awk -v n=1 -v s="$2" 'NR == n {print s} {print }' $TEMPDIR/"$relName"_"$1".out > $OUTDIR/"$relName"_"$1".csv
	else
		mv $TEMPDIR/"$relName"_"$1".out  $OUTDIR/"$relName"_"$1".csv
	fi
	
	rm -f $TEMPDIR/"$relName"_"$1".sql
	rm -f $TEMPDIR/"$relName"_"$1".out

}



send_email () {

emailStatus=$1
stepDescription=$2

	ename=`echo $relName | tr '[a-z]' '[A-Z]'`
	emailSubjectLine="$ename  UPGRADE STATUS - Completed $stepDescription"
	rm -f $TEMPDIR/"$scriptName"_email_body.dat
	
	if [ -s $TEMPDIR/"$scriptName"_email_additional_info.dat ]
	then
		cat $TEMPDIR/"$scriptName"_email_additional_info.dat >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_body.dat
	fi
	
	if [ $emailStatus == "FAILURE" ]
	then
		echo "$stepDescription for $relName completed with errors." >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "Scripts that failed during execution are attached to this email. Analyze them before proceeding to next step."  >>  $TEMPDIR/"$scriptName"_email_body.dat
	fi
	if [ $emailStatus == "SUCCESS" ]
	then
		echo "$stepDescription for $relName completed without any errors. Please proceed to next step" >> $TEMPDIR/"$scriptName"_email_body.dat
	fi
	if [ $emailStatus == "WARNING" ]
	then
		echo "$stepDescription for $relName completed without any execution errors." >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "However 1 or more validation issues have been found. Look at log files and audit tables for details." >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "Please do not proceed to next step without resolving validation issues." >> $TEMPDIR/"$scriptName"_email_body.dat
	fi
	
	
	if [ $emailStatus != "FAILURE" ]
	then
		$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"
	else
		# FAILURE - Make sure only DBA group is notified
		$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"
	fi

}



create_upgr_view_report () 
{


	# Take Count of UPGR_VIEWS
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_original_view_count.dat $TDPROD $ticketNo $runId 102 >> $LOGDIR/archive/"$relName"_backup_migration_run.log
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_upgr_view_count.dat $TDPROD $ticketNo $runId 202 >> $LOGDIR/archive/"$relName"_backup_migration_run.log
	

		#------------------------------------------------------------------------------------------------------------------#
		
		get_report "UPGR_VIEW_COUNT_REPORT" "UPGR_VIEW_NAME,UPGR_DATABASE_NAME,UPGR_COUNT,ORIG_VIEW_NAME,ORIG_DATABASE_NAME,ORIG_COUNT,DIFFERENCE" "-k 2,2 -k 1,1"
		echo "$OUTDIR|"$relName"_"UPGR_VIEW_COUNT_REPORT".csv" >> $TEMPDIR/"$scriptName"_email_attach.dat
		
		rm -f $TEMPDIR/"$scriptName"_email_info.dat
		tail +2 $OUTDIR/"$relName"_"UPGR_VIEW_COUNT_REPORT".csv | while read -r line; do
			diff=`echo $line | cut -f7 -d','`
			if [ "$diff" != "0" ]
			then
				emailStatus="WARNING"
				echo "$line" >> $TEMPDIR/"$scriptName"_email_info.dat
			fi
		done
		
		
		echo "UPGR_VIEW_COUNT_REPORT is also attached to this email." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat

		if [ -s $TEMPDIR/"$scriptName"_email_info.dat ]
		then
			echo " Source and UPGR View Count does not match for some Views. Please review the list below - " >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			cat $TEMPDIR/"$scriptName"_email_info.dat >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		fi
		

}



run_backup_tables () {
					# Load Backup Tables
	rm -f $LOGDIR/archive/"$relName"_backup_migration_run.log
	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 6 $SQLDIR/archive/$relName/  "$relName"_backup_migrate_data_from_prod_tables.sql $OUTDIR/drive_backup.out $LOGDIR/archive/"$relName"_backup_migration_run.log	
	
					# Take Count of Backup Tables
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_source_table_count.dat $TDPROD $ticketNo $runId 101 >> $LOGDIR/archive/"$relName"_backup_migration_run.log
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_backup_table_count.dat $TDPROD $ticketNo $runId 201 >> $LOGDIR/archive/"$relName"_backup_migration_run.log

	rm -f $LOGDIR/archive/"$relName"_errors_backup_migration_run.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_backup_migration_run.log $LOGDIR/archive/"$relName"_errors_backup_migration_run.log

	#------------------------------------------------------------------------------------------------------------------#
	
	get_report "BACKUP_TABLE_COUNT_REPORT" "BACKUP_TABLE_NAME,BACKUP_DATABASE_NAME,BACKUP_COUNT,SOURCE_TABLE_NAME,SOURCE_DATABASE_NAME,SOURCE_COUNT,DIFFERENCE" "-k 2,2 -k 1,1"
	echo "$OUTDIR|"$relName"_"BACKUP_TABLE_COUNT_REPORT".csv" > $TEMPDIR/"$scriptName"_email_attach.dat
	
	rm -f $TEMPDIR/"$scriptName"_email_info.dat
	tail +2 $OUTDIR/"$relName"_"BACKUP_TABLE_COUNT_REPORT".csv | while read -r line; do
		diff=`echo $line | cut -f7 -d','`
		if [ "$diff" != "0" ]
		then
			emailStatus="WARNING"
			echo "$line" >> $TEMPDIR/"$scriptName"_email_info.dat
		fi
	done
	
	
	echo "Please find the BACKUP_TABLE_COUNT_REPORT attached to this email." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
	echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
	
	if [ -s $TEMPDIR/"$scriptName"_email_info.dat ]
	then
		echo " Source and Backup Table Count does not match for some tables. Please review the list below - " >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		cat $TEMPDIR/"$scriptName"_email_info.dat >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
	fi

	
		# Get upgr_ view report as well
		create_upgr_view_report
	
	
	rm -f $LOGDIR/archive/"$relName"_errors_backup_migration_run.log
	stepDescription="Loading Backup Tables before cutover"
	if [ -s $LOGDIR/archive/"$relName"_errors_backup_migration_run.log ]
	then
		# Errors Found in Execution
		emailStatus="FAILURE"
		echo "$LOGDIR/archive|"$relName"_errors_backup_migration_run.log|"$relName"_errors_backup_migration_run.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
	else
		if [ "$emailStatus" != "WARNING" ]
		then
			# No Errors Found
			emailStatus="SUCCESS"
		fi
	fi
	send_email $emailStatus "$stepDescription"
	
	#------------------------------------------------------------------------------------------------------------------#
	
	
}


run_backup_views () {		

		# Get The Start Time
	start_dt=`date +%Y-%m-%d`
	start_tm=`date +%H:%M:%S`

					# Create UPGR_Views
	logFileName="$LOGDIR/archive/"$relName"_backup_create_upgr_views.log"
	rm -f $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup__view_simple.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup__view_additional.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup__view_complex.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup__view_materialize.sql $OUTDIR/drive_migration.out | tee -a $logFileName

	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup_user_view_simple.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup_user_view_additional.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup_user_view_complex.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	
	
		# Get The End Time
	end_dt=`date +%Y-%m-%d`
	end_tm=`date +%H:%M:%S`
	
	
	echo "SELECT '$start_dt $start_tm' AS START_TIME " > $SQLDIR/"$relName"_validation_backup_views.sql
	
	echo " SELECT COUNT(*) AS UPGR_VIEWS_MODIFIED FROM DBC.TablesV WHERE TRIM(TableName) LIKE 'UPGR_%' AND TableKind='V' AND TRIM(DatabaseName) <> '' AND TRIM(DatabaseName) " >> $SQLDIR/"$relName"_validation_backup_views.sql
	echo " IN ( '$prodView','$prodUserView','$prodMatView','$prodKPBIView','$prodTpfView','$prodTpfUserView') " >> $SQLDIR/"$relName"_validation_backup_views.sql
	echo " AND LastAlterTimestamp BETWEEN CAST('$start_dt $start_tm' AS TIMESTAMP(0))  AND CAST('$end_dt $end_tm' AS TIMESTAMP(0)); " >> $SQLDIR/"$relName"_validation_backup_views.sql
	
	echo " SELECT COUNT(*) AS ORIGINAL_VIEWS_MODIFIED  FROM DBC.TablesV WHERE TRIM(TableName) NOT LIKE 'UPGR_%' AND TableKind='V' AND TRIM(DatabaseName) <> '' AND TRIM(DatabaseName) " >> $SQLDIR/"$relName"_validation_backup_views.sql
	echo " IN ( '$prodView','$prodUserView','$prodMatView','$prodKPBIView','$prodTpfView','$prodTpfUserView') " >> $SQLDIR/"$relName"_validation_backup_views.sql
	echo " AND LastAlterTimestamp BETWEEN CAST('$start_dt $start_tm' AS TIMESTAMP(0))  AND CAST('$end_dt $end_tm' AS TIMESTAMP(0)); " >> $SQLDIR/"$relName"_validation_backup_views.sql
	
	echo " SELECT TRIM(DatabaseName) || '.' || TRIM(TableName) AS LIST_ORIGINAL_VIEWS_MODIFIED FROM DBC.TablesV WHERE TRIM(TableName) NOT LIKE 'UPGR_%' AND TableKind='V' AND TRIM(DatabaseName) <> '' AND TRIM(DatabaseName) " >> $SQLDIR/"$relName"_validation_backup_views.sql
	echo " IN ( '$prodView','$prodUserView','$prodMatView','$prodKPBIView','$prodTpfView','$prodTpfUserView') " >> $SQLDIR/"$relName"_validation_backup_views.sql
	echo " AND LastAlterTimestamp BETWEEN CAST('$start_dt $start_tm' AS TIMESTAMP(0))  AND CAST('$end_dt $end_tm' AS TIMESTAMP(0)); " >> $SQLDIR/"$relName"_validation_backup_views.sql
		
	echo "SELECT '$end_dt $end_tm' AS END_TIME " >> $SQLDIR/"$relName"_validation_backup_views.sql

	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_validation_backup_views.sql $OUTDIR/"$relName"_validation_backup_views.out | tee -a $logFileName
	echo "$LOGDIR/archive|"$relName"_validation_backup_views.log|"$relName"_validation_backup_views.txt" > $TEMPDIR/"$scriptName"_email_attach.dat

	echo " UPGR_ View Validation Results Attached to this email. Please make sure no ORIGINAL_VIEW has been replaced" > $TEMPDIR/"$scriptName"_email_additional_info.dat

	
					# Get All Errors
	rm -f $LOGDIR/archive/"$relName"_errors_backup_create_upgr_views.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_backup_create_upgr_views.log $LOGDIR/archive/"$relName"_errors_backup_create_upgr_views.log

	
		stepDescription="Creation of UPGR_ Views"
		if [ -s $LOGDIR/archive/"$relName"_errors_backup_create_upgr_views.log ]
		then
			# Errors Found in Execution
			emailStatus="FAILURE"
			echo "$LOGDIR/archive|"$relName"_errors_backup_create_upgr_views.log|"$relName"_errors_backup_create_upgr_views.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		else
			if [ "$emailStatus" != "WARNING" ]
			then
				# No Errors Found
				emailStatus="SUCCESS"
			fi
		fi
		send_email $emailStatus "$stepDescription"
		
		#------------------------------------------------------------------------------------------------------------------#
}


reasonable_volume_check_before_etl ()
{

	logFileName="$LOGDIR/archive/"$relName"_reasonable_volume_pre_etl.log"
	rm -f $logFileName
	
	#-----------------  Reasonable Volume Check (Pre-ETL) ---------------------------------------
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_reasonable_volume.dat $TDPROD $ticketNo $runId 105 > $logFileName	

	get_report "REASONABLE_VOLUME_REPORT" "TABLE_NAME,DATABASE_NAME,PRE_ETL_COUNT,POST_ETL_COUNT,DIFFERENCE" "-k 1,1"
	echo "$OUTDIR|"$relName"_"REASONABLE_VOLUME_REPORT".csv" > $TEMPDIR/"$scriptName"_email_attach.dat
	
	echo "Please find the REASONABLE_VOLUME_REPORT with pre-etl run counts attached to this email." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
	echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
	
	
	rm -f $LOGDIR/archive/"$relName"_errors__reasonable_volume_pre_etl.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_reasonable_volume_pre_etl.log $LOGDIR/archive/"$relName"_errors_reasonable_volume_pre_etl.log


	stepDescription="Reasonable Volume Check Report before ETL Run"
	if [ -s $LOGDIR/archive/"$relName"_errors_reasonable_volume_pre_etl.log ]
	then
		# Errors Found in Execution
		emailStatus="FAILURE"
		echo "$LOGDIR/archive|"$relName"_errors_reasonable_volume_pre_etl.log|"$relName"_errors_reasonable_volume_pre_etl.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
	else
		if [ "$emailStatus" != "WARNING" ]
		then
			# No Errors Found
			emailStatus="SUCCESS"
		fi
	fi
	send_email $emailStatus "$stepDescription"
	
	
}
#--------------------------------------------------------------------------------------------------------------------------------------------------------
		

# STEP-2 Get RunId based on db change list 

	echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_FIND_RUN_ID ( '$dbChgList','$region','$regionProfile','$stagingList','$backupList','$ticketNo','$TDDEV','$TDPROD',RunId );" > $TEMPDIR/get_run_id.sql
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/get_run_id.sql $OUTDIR/get_run_id.out | tee -a  $logFileName
	outRun=`tail -1 $OUTDIR/get_run_id.out | sed -e 's/\ //g'`
	
	if [ ! -d $SQLDIR/archive/"$relName" ]
	then
		mkdir $SQLDIR/archive/"$relName"
	fi
		
	if [ ! -f $SQLDIR/archive/"$relName"/runid.profile ]
	then
		echo "runId=$outRun; export runId" > $SQLDIR/archive/"$relName"/runid.profile
	fi
		chmod 775 $SQLDIR/archive/"$relName"/runid.profile
		. $SQLDIR/archive/"$relName"/runid.profile
		
		if [ -z "$runId" ]
		then
			echo "Invalid Value for runId .. Aborting Script "
			exit 911
		fi
		
	
	
	if [ ! -f $SQLDIR/archive/$relName/backupdb_replace.list ]
	then
		echo "$prodStgDB||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
		echo "$prodReportDB||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
		echo "$prodMatReportDB||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
		echo "$prodKPBIReportDB||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
		
		if [ $region == "NC" ] || [ $region == "SC" ]
		then
			echo "$prodTpfStageDB||T" > $SQLDIR/archive/$relName/tpf_backupdb_replace.list
			echo "$prodTpfReportDB||T" >> $SQLDIR/archive/$relName/tpf_backupdb_replace.list
			echo "$prodTpfCalcReportDB1||T" >> $SQLDIR/archive/$relName/tpf_backupdb_replace.list
			echo "$prodTpfCalcReportDB2||T" >> $SQLDIR/archive/$relName/tpf_backupdb_replace.list
			echo "$prodTpfCalcReportDB3||T" >> $SQLDIR/archive/$relName/tpf_backupdb_replace.list
			echo "$prodTpfCalcReportDB4||T" >> $SQLDIR/archive/$relName/tpf_backupdb_replace.list
			echo "$prodTpfCalcReportDB5||T" >> $SQLDIR/archive/$relName/tpf_backupdb_replace.list
			echo "$prodTpfCalcReportDB6||T" >> $SQLDIR/archive/$relName/tpf_backupdb_replace.list

			echo "$prodCalcReportDB1||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
			echo "$prodCalcReportDB2||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
			echo "$prodCalcReportDB3||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
			echo "$prodCalcReportDB4||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
			echo "$prodCalcReportDB5||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
			echo "$prodCalcReportDB6||T" >> $SQLDIR/archive/$relName/backupdb_replace.list
		fi
	fi
	if [ ! -f $SQLDIR/archive/$relName/backup_ddl.list ]
	then
		echo "$prodStgDB|export_"$prodStgDB"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		echo "$prodReportDB|export_"$prodReportDB"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		echo "$prodView|export_"$prodView"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		echo "$prodUserView|export_"$prodUserView"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		echo "$prodMatReportDB|export_"$prodMatReportDB"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		echo "$prodMatView|export_"$prodMatView"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		echo "$prodKPBIReportDB|export_"$prodKPBIReportDB"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		echo "$prodKPBIView|export_"$prodKPBIView"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		
		if [ $region == "NC" ] || [ $region == "SC" ]
		then
			echo "$prodTpfReportDB|export_"$prodTpfReportDB"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfView|export_"$prodTpfView"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfUserView|export_"$prodTpfUserView"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			
			echo "$prodCalcReportDB1|export_"$prodCalcReportDB1"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodCalcReportDB2|export_"$prodCalcReportDB2"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodCalcReportDB3|export_"$prodCalcReportDB3"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodCalcReportDB4|export_"$prodCalcReportDB4"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodCalcReportDB5|export_"$prodCalcReportDB5"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodCalcReportDB6|export_"$prodCalcReportDB6"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodDeployStgDB1|export_"$prodDeployStgDB1"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodDeployStgDB2|export_"$prodDeployStgDB2"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodDeployStgDB3|export_"$prodDeployStgDB3"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodDeployStgDB4|export_"$prodDeployStgDB4"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodDeployStgDB5|export_"$prodDeployStgDB5"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodDeployStgDB6|export_"$prodDeployStgDB6"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			
			echo "$prodTpfCalcReportDB1|export_"$prodTpfCalcReportDB1"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfCalcReportDB2|export_"$prodTpfCalcReportDB2"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfCalcReportDB3|export_"$prodTpfCalcReportDB3"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfCalcReportDB4|export_"$prodTpfCalcReportDB4"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfCalcReportDB5|export_"$prodTpfCalcReportDB5"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfCalcReportDB6|export_"$prodTpfCalcReportDB6"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfDeployStgDB1|export_"$prodTpfDeployStgDB1"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfDeployStgDB2|export_"$prodTpfDeployStgDB2"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfDeployStgDB3|export_"$prodTpfDeployStgDB3"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfDeployStgDB4|export_"$prodTpfDeployStgDB4"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfDeployStgDB5|export_"$prodTpfDeployStgDB5"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
			echo "$prodTpfDeployStgDB6|export_"$prodTpfDeployStgDB6"_ddl.sql" >> $SQLDIR/archive/$relName/backup_ddl.list
		fi
	fi


# STEP-3 Control the Sequence of Execution of Backup Scripts

	
	if [ ! -f $HOMEDIR/flag/"$relName"_backup_analysis.flag ]
	then
		# 1. Perform Backup Analysis
		$SCRIPTDIR/epdba_upgrade_perform_backup_analysis.sh $relName $ticketNo $regionProfile $dbChgList $backupList > $LOGDIR/archive/"$relName"_backup_analysis.log	
		rt_cd=$?
		if [ $rt_cd -ne 0 ]
		then
			echo "epdba_upgrade_perform_backup_analysis.sh aborted abruptly"
			exit 900
		fi
		
		rm -f $LOGDIR/archive/"$relName"_errors_backup_analysis.log
		$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_backup_analysis.log $LOGDIR/archive/"$relName"_errors_backup_analysis.log
		touch $HOMEDIR/flag/"$relName"_backup_analysis.flag
		
		stepDescription="Initial Backup Analysis"
		if [ -s $LOGDIR/archive/"$relName"_errors_backup_analysis.log ]
		then
			# Errors Found in Execution
			emailStatus="FAILURE"
			echo "$LOGDIR/archive|"$relName"_errors_backup_analysis.log|"$relName"_errors_backup_analysis.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		else
			# No Errors Found
			emailStatus="SUCCESS"
		fi
		
		if [ -s $OUTDIR/"$relName"_backup_tables_notfound.dat ]
		then
			echo "Review the list of backup tables not found attached to this email." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "$OUTDIR|"$relName"_backup_tables_notfound.dat|"$relName"_backup_tables_notfound.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		fi
		if [ -s $OUTDIR/"$relName"_backup_analysis_collect_results.out ]
		then
			echo "Backup analysis report is attached to this email." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "$OUTDIR|"$relName"_backup_analysis_collect_results.out|"$relName"_backup_analysis_report.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		fi
		send_email $emailStatus "$stepDescription"
		
	else
		if [ ! -f $HOMEDIR/flag/"$relName"_backup_create_scripts.flag ]
		then
		
			# 2. Generate the Backup Scripts
			$SCRIPTDIR/epdba_upgrade_create_backup_scripts.sh $relName $ticketNo $regionProfile > $LOGDIR/archive/"$relName"_create_backup_scripts.log
			rt_cd=$?
			if [ $rt_cd -ne 0 ]
			then
				echo "epdba_upgrade_create_backup_scripts.sh aborted abruptly"
				exit 900
			fi
			
			rm -f $LOGDIR/archive/"$relName"_errors_create_backup_scripts.log
			$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_create_backup_scripts.log $LOGDIR/archive/"$relName"_errors_create_backup_scripts.log
			touch $HOMEDIR/flag/"$relName"_backup_create_scripts.flag
			
			stepDescription="Creation of Backup Scripts"
			if [ -s $LOGDIR/archive/"$relName"_errors_create_backup_scripts.log ]
			then
				# Errors Found in Execution
				emailStatus="FAILURE"
				echo "$LOGDIR/archive|"$relName"_errors_create_backup_scripts.log|"$relName"_errors_create_backup_scripts.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
			else
				if [ -s $SQLDIR/"$relName"_backup_script_creation_errors.sql ]
				then
					emailStatus="WARNING"
					echo "$SQLDIR|"$relName"_backup_script_creation_errors.sql|"$relName"_backup_suspect_scripts.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat

					echo " Original View may accidently get replaced. Look at suspect scripts attached." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
					echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
				else
					# No Errors Found
					emailStatus="SUCCESS"
				fi
			fi
			send_email $emailStatus "$stepDescription"
			
		else
			if [ ! -f $HOMEDIR/flag/"$relName"_backup_perform_inital_run.flag ]
			then
			
				# 3. Initial Creation of Backup
				rm -f $LOGDIR/archive/"$relName"_backup_create_tables.log
				rm -f $LOGDIR/archive/"$relName"_backup_enable_blocklevelcomp.log
				rm -f $LOGDIR/archive/"$relName"_errors_backup_create_tables.log
				rm -f $LOGDIR/archive/"$relName"_errors_backup_enable_blocklevelcomp.log
				
				$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 7 $SQLDIR/archive/$relName/  "$relName"_backup_create_tables.sql $OUTDIR/drive_backup.out $LOGDIR/archive/"$relName"_backup_create_tables.log	
				$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_backup_create_tables.log $LOGDIR/archive/"$relName"_errors_backup_create_tables.log

				#$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 15 $SQLDIR/archive/$relName/  "$relName"_backup_delete_data_from_backup_tables.sql $OUTDIR/drive_backup.out $LOGDIR/archive/"$relName"_backup_enable_blocklevelcomp.log	
				$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 15 $SQLDIR/archive/$relName/  "$relName"_backup_enable_blocklevel_compression.sql $OUTDIR/drive_backup.out $LOGDIR/archive/"$relName"_backup_enable_blocklevelcomp.log	
				$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_backup_enable_blocklevelcomp.log $LOGDIR/archive/"$relName"_errors_backup_enable_blocklevelcomp.log
				
				touch $HOMEDIR/flag/"$relName"_backup_perform_inital_run.flag
				
				stepDescription="Creation of Backup Tables and Enabling Block Level Compression"
				if [ -s $LOGDIR/archive/"$relName"_errors_backup_create_tables.log ] || [ -s $LOGDIR/archive/"$relName"_errors_backup_enable_blocklevelcomp.log ]
				then
					# Errors Found in Execution
					emailStatus="FAILURE"
					echo "$LOGDIR/archive|"$relName"_errors_backup_create_tables.log|"$relName"_errors_backup_create_tables.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
					echo "$LOGDIR/archive|"$relName"_errors_backup_enable_blocklevelcomp.log|"$relName"_errors_backup_enable_blocklevelcomp.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
				else
					# No Errors Found
					emailStatus="SUCCESS"
				fi
				send_email $emailStatus "$stepDescription"
				
			else
			
				# 4. Run the scripts for creating upgr_ view (view backups)
				if [ ! -f $HOMEDIR/flag/"$relName"_backup_create_upgr_views.flag ]
				then
					run_backup_views
					touch $HOMEDIR/flag/"$relName"_backup_create_upgr_views.flag	
				
				else
				
					# 5. Run the scripts for copying data from reporting to backup on the day of Upgrade
					if [ ! -f $HOMEDIR/flag/"$relName"_backup_perform_production_run.flag ]
					then				
						run_backup_tables
						touch $HOMEDIR/flag/"$relName"_backup_perform_production_run.flag	
						
					else
					
						# 6. Run the scripts for reasonable volume check
						if [ ! -f $HOMEDIR/flag/"$relName"_reasonable_volume_check_before_etl.flag ]
						then
							reasonable_volume_check_before_etl
							touch $HOMEDIR/flag/"$relName"_reasonable_volume_check_before_etl.flag 
						else
							reasonable_volume_check_before_etl
							echo "No Action required for $relName - All Backup Steps have been completed !!"
						fi
					fi
				fi
				
			fi
		fi
	fi
	
	
		# Check if all tables that are being changed are in backup list
	# echo "SELECT 'V5|DB List in Backup List Check|' || Count(*) FROM (  MINUS  ); " >> $SQLDIR/"$relName"_validation_summary_scripts.sql
		
