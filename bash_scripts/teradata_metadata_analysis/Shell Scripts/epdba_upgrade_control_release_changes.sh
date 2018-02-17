#!/usr/bin/ksh

	relName="co0310"
	ticketNo="CRQ000000292490"
	regionProfile="TESTCO5"
	dbChgList="co0310_chglist_v1.txt"
	stagingList="co0310_staging_list_v1.txt"
	backupList="co0310_backup_list_v1.txt"
	customViewPurpose=" 2014 CP8"
	customViewFile="CO_CUSTOM_VIEW.sql"
	
	cutoverFlag=""
	mockCutoverFlag=""
	

	prefix=`echo $relName | awk '{print substr($0,3,4)}'`
	
	if [ -z "$relName" ] || [ -z "$ticketNo" ] || [ -z "$regionProfile" ] || [ -z "$dbChgList" ] || [ -z "$stagingList" ] 
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
	rm -f $TEMPDIR/"$scriptName"_email_additional_info.dat
	
	if [ ! -f $DIR/$dbChgList ]
	then
		echo "DB Change List $DIR/$dbChgList Not Found.. Aborting Script"
		exit 903
	fi
	if [ ! -f $DIR/$stagingList ]
	then
		echo "Staging Table List $DIR/$stagingList Not Found.. Aborting Script"
		exit 905
	fi


# STEP-3 Get RunId based on db change list 

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

#-------------------------------------------------------------------------------------------------------------------------------------

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
		$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"  -e "cd_bio_etl"
	else
		# FAILURE - Make sure only DBA group is notified
		$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"
	fi

}
		
#-------------------------------------------------------------------------------------------------------------------------------------		
	
create_temp_tables() {	
	
	logFileName="$LOGDIR/archive/"$relName"_migration_create_temptables.log"
	rm -f $logFileName
	$SCRIPTDIR/epdba_perform_dbmaint.sh 1 HCCL"$region"_UPG_AK_% $TDPROD $ticketNo > $logFileName
	
	if [ -f "$SQLDIR/archive/$relName/"$relName"_tempstructure_for_existing_tables.sql" ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_tempstructure_for_existing_tables.sql  $OUTDIR/drive_migration.out | tee -a $logFileName &
		sleep 2
	fi
	if [ -f "$SQLDIR/archive/$relName/"$relName"_tempstructure_for_new_tables.sql" ]
	then 
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_tempstructure_for_new_tables.sql  $OUTDIR/drive_migration.out  | tee -a $logFileName &
		sleep 2
	fi
	if [ -f "$SQLDIR/archive/$relName/"$relName"_tempstructure_for_existing_tables_with_PI_change.sql" ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_tempstructure_for_existing_tables_with_PI_change.sql  $OUTDIR/drive_migration.out  | tee -a $logFileName &
		sleep 2
	fi

	
	# Wait for Temp Table Creation to complete
	sleep 5
	procCount=`ps -ef | grep -i "$relName"_tempstructure | grep -i $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep -i "$relName"_tempstructure | grep -i $USER | wc -l`
	done
	
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_stats_exist.sql  $OUTDIR/drive_migration.out  | tee -a $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_stats_new.sql  $OUTDIR/drive_migration.out | tee -a $logFileName
	
	
	
	#----------------------------------------------------------------------------------------------------------------------------------------------------
	# 2.1 Validate Temp Table Structures
	rm -f $OUTDIR/"$relName"_tempstructure_validation_results.out
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_validation_of_tempstructure.sql $OUTDIR/"$relName"_tempstructure_validation_results.out | tee -a  $logFileName
	rm -f $OUTDIR/"$relName"_tempstructure_validation_errors.out
	
	cat $OUTDIR/"$relName"_migration_analysis.out  | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6 -d'|' | sort | uniq | while read -r line2 ; 
	do
	
		type=`echo $line2 | cut -f1 -d'|'`
		srcDB=`echo $line2 | cut -f2 -d'|'`
		tempDB=`echo $line2 | cut -f3 -d'|'`
		prodDB=`echo $line2 | cut -f4 -d'|'`
		table=`echo $line2 | cut -f5 -d'|'`
	
	
		if [ "$type" != "1" ]
		then
		
			# Check if column Added in manifest for the table
			colAddInd=`cat $DIR/$dbChgList | grep -w -i "$table" | grep -w -i "Column Add" | wc -l`
			if [ $colAddInd -ne 0 ]
			then
				# For Column Add, Ignore COLUMN COUNT CHECK
				cat $OUTDIR/"$relName"_tempstructure_validation_results.out | grep -w -i "$table" | grep -w -v -i "COLUMN COUNT  BAD - CHECK" > $TEMPDIR/failure_columns.dat
			else
				cat $OUTDIR/"$relName"_tempstructure_validation_results.out | grep -w -i "$table" > $TEMPDIR/failure_columns.dat
			fi
			
			
			# Get Manifest Changes for the table
			cat $DIR/$dbChgList | grep -w -i "$table" | grep -w -v -i "TABLE DROP" | grep -w -v -i "RENAME TABLE" | grep -w -v -i "VIEW ONLY" | cut -f3 -d'|' > $TEMPDIR/manifest_changes.out

			rm -f  $TEMPDIR/table_validation_results.dat
			cat $TEMPDIR/failure_columns.dat | while read -r resultLine ;
			do
				columnName=`echo $resultLine | awk '{print substr($0,130)}'`
				
				# Check if the columns that don't match are part of manifest
				findInd=`cat $TEMPDIR/manifest_changes.out | grep -w -i "$columnName" | wc -l` 
				if [ $findInd -eq 0 ]
				then
					cat $resultLine >> $TEMPDIR/table_validation_results.dat
				fi				
				
			done
			
			cat $TEMPDIR/failure_columns.dat | grep -i "INDEX Count BAD - CHECK" | grep -v -i "UPG_AK_PI_Change" >> $TEMPDIR/table_validation_results.dat
			cat $TEMPDIR/failure_columns.dat | grep -i "INDEX PROPERTIES BAD - CHECK" | grep -v -i "UPG_AK_PI_Change" >> $TEMPDIR/table_validation_results.dat
			cat $TEMPDIR/failure_columns.dat | grep -v -i "COLUMN MISSING IN TGT" | grep -v -i "COLUMN PROPERTIES BAD - CHECK" | \
			grep -v -i "INDEX Count BAD - CHECK" | grep -v -i "INDEX PROPERTIES BAD - CHECK" >> $TEMPDIR/table_validation_results.dat

			if [ -s $TEMPDIR/table_validation_results.dat ]
			then
				cat $TEMPDIR/table_validation_results.dat >> $OUTDIR/"$relName"_tempstructure_validation_errors.out
			fi
			
		fi
	
	done

	
	cat $DIR/$dbChgList | cut -f1,2,3 -d'|' | while read -r line; do
	
		type=`echo $line | cut -f1 -d'|'`
		table=`echo $line | cut -f2 -d'|'`
		column=`echo $line | cut -f3 -d'|'`
		
		if [ ! -z "$column" ]
		then
			if [ "$type" == "Column Add" ]
			then
				findInd1=`cat $OUTDIR/"$relName"_tempstructure_validation_results.out  | grep -w -i $table | grep -w -i $column | grep -i "COLUMN MISSING IN TGT" | wc -l`
				if [ $findInd1 -eq 0 ]
				then
					echo "Table $table : Column Add $column part of manifest but missing in Temp Table" >> $OUTDIR/"$relName"_tempstructure_validation_errors.out
				fi
			fi
			
			if [ "$type" != "Column Add" ] && [ "$type" != "PI Change" ] && [ "$type" != "Table Add" ] && [ "$type" != "Rename Table" ] && [ "$type" != "Table Drop" ] && [ "$type" != "VIEW ONLY" ]
			then
				findInd2=`cat $OUTDIR/"$relName"_tempstructure_validation_results.out | grep -w -i $table | grep -w -i $column | grep -i "COLUMN PROPERTIES BAD - CHECK" | wc -l`
				if [ $findInd2 -eq 0 ]
				then
					echo "Table $table : Column Change $column part of manifest but missing in Temp Table" >> $OUTDIR/"$relName"_tempstructure_validation_errors.out
				fi
			fi
		fi
		
	done
	
	
	tempErrors=`cat $OUTDIR/"$relName"_tempstructure_validation_errors.out | wc -l`
	diffErrors=`expr 0 - $tempErrors`
	echo "INSERT INTO CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION " >> $SQLDIR/archive/"$relName"/"$relName"_validation_summary_scripts.sql
	echo "VALUES ($runId, '2.1', 'Table Structure Differences - Temp vs Prod Reporting',0,$tempErrors, $diffErrors);" >> $SQLDIR/archive/"$relName"/"$relName"_validation_summary_scripts.sql

	
	#----------------------------------------------------------------------------------------------------------------------------------------------------
	

	# Run Validation Queries
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql  $OUTDIR/"$relName"_validation_summary_scripts.out | tee -a $logFileName

	get_report "VALIDATION_REPORT" "TEST_CASE_NO,TEST_CASE_NM,EXPECTED_RESULT,ACTUAL_RESULT,DIFFERENCE" "-k 1,1"
	echo "$OUTDIR|"$relName"_"VALIDATION_REPORT".csv" >> $TEMPDIR/"$scriptName"_email_attach.dat	
	
	
	rm -f $LOGDIR/archive/"$relName"_errors_create_temptables.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_migration_create_temptables.log $LOGDIR/archive/"$relName"_errors_create_temptables.log
	
	stepDescription="Creation of Temporary Tables"
	if [ -s $LOGDIR/archive/"$relName"_errors_create_temptables.log ]
	then
		# Errors Found in Execution
		emailStatus="FAILURE"
		echo "$LOGDIR/archive|"$relName"_errors_create_temptables.log|"$relName"_errors_create_temptables.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
	else
		if [ "$emailStatus" != "WARNING" ]
		then
			# No Errors Found
			emailStatus="SUCCESS"
		fi
	fi
	
	send_email $emailStatus "$stepDescription"

}
#-------------------------------------------------------------------------------------------------------------------------------------

dry_run() {	
	
	logFileName="$LOGDIR/archive/"$relName"_migration_perform_dry_run.log"
	
	# Delete data from the tables before running
	$SCRIPTDIR/epdba_perform_dbmaint.sh 2 HCCL"$region"_UPG_AK_%_CHANGE $TDPROD $ticketNo > $logFileName

	# Run the conversion scripts
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_column_add_1.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_column_add_1.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"1" &
		sleep 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_column_add_2.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_column_add_2.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"2" &
		sleep 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_simple_dtype_change_3.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_simple_dtype_change_3.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"3" &
		sleep 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_simple_dtype_change_4.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_simple_dtype_change_4.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"4" &
		sleep 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_complex_dtype_change_5.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_complex_dtype_change_5.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"5" &
		sleep 2
	fi
	
	
	# Wait for Data Migration to complete
	sleep 5
	procCount=`ps -ef | grep -i ""$relName"_migrate" | grep sql | grep $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep -i ""$relName"_migrate" | grep sql | grep $USER | wc -l`
	done
	
	
	cat ""$logFileName"1" >> $logFileName
	cat ""$logFileName"2" >> $logFileName
	cat ""$logFileName"3" >> $logFileName
	cat ""$logFileName"4" >> $logFileName
	cat ""$logFileName"5" >> $logFileName
	
	rm -f ""$logFileName"1"
	rm -f ""$logFileName"2"
	rm -f ""$logFileName"3"
	rm -f ""$logFileName"4"
	rm -f ""$logFileName"5"
	
	
	# 3.1 Check for trailing spaces
	
	if [ -f "$SQLDIR/archive/$relName/"$relName"_validation_check_for_dataconversion.sql" ]
	then
	
		$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 10 $SQLDIR/archive/$relName/  "$relName"_validation_check_for_dataconversion.sql  $OUTDIR/data_validation.out "$logFileName"
		
		trailErrors1=`cat $OUTDIR/"$relName"_validation_check_for_dataconversion.out | grep -i '\ |' | wc -l`
		trailErrors2=`cat $OUTDIR/"$relName"_validation_check_for_dataconversion.out | grep -i '\.' | wc -l`

		trailErrors=`expr $trailErrors1 + $trailErrors2`
		diffErrors=`expr 0 - $trailErrors`

		echo "DELETE FROM CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION WHERE RUN_ID=$runId AND TEST_CASE_NO IN ('3.1');" > $SQLDIR/archive/$relName/"$relName"_validation_dryrun_scripts.sql
		echo "INSERT INTO CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION " >> $SQLDIR/archive/$relName/"$relName"_validation_dryrun_scripts.sql
		echo "VALUES ($runId, '3.1', 'Rows with Trailing Spaces or Decimal',0,$trailErrors, $diffErrors);" >> $SQLDIR/archive/$relName/"$relName"_validation_dryrun_scripts.sql
	fi
	
	
	
	# Run Validation Queries
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_validation_dryrun_scripts.sql  $OUTDIR/"$relName"_validation_dryrun_scripts.out | tee -a $logFileName

	get_report "VALIDATION_REPORT" "TEST_CASE_NO,TEST_CASE_NM,EXPECTED_RESULT,ACTUAL_RESULT,DIFFERENCE" "-k 1,1"
	echo "$OUTDIR|"$relName"_"VALIDATION_REPORT".csv" >> $TEMPDIR/"$scriptName"_email_attach.dat
	
	rm -f $LOGDIR/archive/"$relName"_errors_migration_perform_dry_run.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_migration_perform_dry_run.log $LOGDIR/archive/"$relName"_errors_migration_perform_dry_run.log
	
	stepDescription="Completion of Dry Run"
	if [ -s $LOGDIR/archive/"$relName"_errors_migration_perform_dry_run.log ]
	then
		# Errors Found in Execution
		emailStatus="FAILURE"
		echo "$LOGDIR/archive|"$relName"_errors_migration_perform_dry_run.log|"$relName"_errors_migration_perform_dry_run.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
	else
		if [ "$emailStatus" != "WARNING" ]
		then
			# No Errors Found
			emailStatus="SUCCESS"
		fi
	fi
	
	send_email $emailStatus "$stepDescription"
	
	
}
#------------------------------------------------------------------------------------------------------------------------------------- 

create_mock_cutover_scripts ()
{
	cp $1 $2
	
	perl -pi -e 's/''WITH\ DATA\ AND\ STATS''/''WITH\ NO\ DATA''/gi'  $2
	# perl -pi -e 's/'HCCLP'/''HCCL'$region'_UPG_DRYRUN_HCCLP''/gi'  $2

	perl -pi -e 's/'$prodStgDB'\./'$dryrunStgDB'\./gi'  $2
	perl -pi -e 's/'$prodReportDB'\./'$dryrunReportDB'\./gi'  $2
	perl -pi -e 's/'$prodMatReportDB'\./'$dryrunMatReportDB'\./gi'  $2
	perl -pi -e 's/'$prodKPBIReportDB'\./'$dryrunKPBIReportDB'\./gi'  $2

	
	# perl -pi -e 's/'$prodView'\./'$dryrunView'\./gi'  $2
	# perl -pi -e 's/'$prodMatView'\./'$dryrunMatView'\./gi'  $2
	# perl -pi -e 's/'$prodUserView'\./'$dryrunUserView'\./gi'  $2
	# perl -pi -e 's/'$prodKPBIView'\./'$dryrunKPBIView'\./gi'  $2

} 
 
mock_cutover_run ()
{

	if [ ! -d "$SQLDIR/archive/$relName/dryrun/" ]
	then
		mkdir $SQLDIR/archive/$relName/dryrun/
	fi

	
	. $HOMEDIR/region/DRYRUN_"$region".profile
		rt_cd=$?
		if [ $rt_cd -ne 0 ]
		then
			echo "Profile file DRYRUN_"$region".profile cannot be found, Exiting"
			exit 902
		fi
	
	
	# Create Mockup Cutover Scripts from Prod Cutover Scripts
	
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_1.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_1.sql
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_2.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_2.sql
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_3.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_3.sql
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_4.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_4.sql
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_5.sql

	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_staging_tables_drop_current.sql $SQLDIR/archive/$relName/dryrun/"$relName"_staging_tables_drop_current.sql
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql $SQLDIR/archive/$relName/dryrun/"$relName"_staging_tables_create.sql

	
	startTS=`date +%Y-%m-%d\ %H:%M:%S`
	stageCount=`head -1 $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql | cut -f2 -d':' | sed 's/\ //g'`
	cutoverCount=`head -2 $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql | tail -1 | cut -f2 -d':' | sed 's/\ //g'`
	
	echo "DELETE FROM CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION WHERE RUN_ID=$runId AND TEST_CASE_NO IN ('5.1','5.2');" > $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql

	echo "INSERT INTO CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION " >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo "SELECT $runId, '4.1', 'Staging Tables Created during dryrun',$stageCount,COUNT(*), (COUNT(*) - $stageCount) FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('$dryrunStagingDB'" >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo ",'$dryrunDeployStgDB1','$dryrunDeployStgDB2','$dryrunDeployStgDB3','$dryrunDeployStgDB4','$dryrunDeployStgDB5','$dryrunDeployStgDB6') "  >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo " AND LastAlterTimeStamp >= TIMESTAMP'$startTS';" >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	
	echo "INSERT INTO CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION " >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo "SELECT $runId, '4.2', 'Reporting Tables Created during dryrun',$cutoverCount,COUNT(*), (COUNT(*) - $cutoverCount) FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('$dryrunReportDB'" >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo ",'$dryrunCalcReportDB1','$dryrunCalcReportDB2','$dryrunCalcReportDB3','$dryrunCalcReportDB4','$dryrunCalcReportDB5','$dryrunCalcReportDB6') " >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo " AND TRIM(TableName) NOT LIKE 'U_'$prefix'%' AND LastAlterTimeStamp >= TIMESTAMP'$startTS';" >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql

	
	logFileName="$LOGDIR/archive/"$relName"_mock_cutover.log"
	rm -f $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_1.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"1" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_2.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"2" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_3.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"3" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_4.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"4" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_temp_to_prod_5.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"5" &
	sleep 2
		
	
	if [ -f $SQLDIR/archive/$relName/dryrun/"$relName"_staging_tables_drop_current.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_staging_tables_drop_current.sql  $OUTDIR/drive_staging_table.out | tee -a $logFileName 
	fi
	if [ -f $SQLDIR/archive/$relName/dryrun/"$relName"_staging_tables_create.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_staging_tables_create.sql  $OUTDIR/drive_staging_table.out | tee -a $logFileName
	fi
	
	
	# Wait for table cutover to complete
	procCount=`ps -ef | grep -i "$relName"_cutover_temp_to_prod | grep sql | grep $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep -i "$relName"_cutover_temp_to_prod | grep sql | grep $USER | wc -l`
	done
	
	
	cat ""$logFileName"1" >> $logFileName
	cat ""$logFileName"2" >> $logFileName
	cat ""$logFileName"3" >> $logFileName
	cat ""$logFileName"4" >> $logFileName
	cat ""$logFileName"5" >> $logFileName
	
	rm -f ""$logFileName"1"
	rm -f ""$logFileName"2"
	rm -f ""$logFileName"3"
	rm -f ""$logFileName"4"
	rm -f ""$logFileName"5"
	
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_exisiting_rename.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_exisiting_rename.sql
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_exisiting_alter.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_exisiting_alter.sql
	create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_create_final.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_create_final.sql
	
	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 10 $SQLDIR/archive/$relName/dryrun/ "$relName"_cutover_prod_exisiting_rename.sql  $OUTDIR/drive_cutover.out ""$logFileName"1"
	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 10 $SQLDIR/archive/$relName/dryrun/ "$relName"_cutover_prod_exisiting_alter.sql  $OUTDIR/drive_cutover.out ""$logFileName"2"
	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 10 $SQLDIR/archive/$relName/dryrun/ "$relName"_cutover_prod_create_final.sql  $OUTDIR/drive_cutover.out ""$logFileName"3"

	
	cat ""$logFileName"1" >> $logFileName
	cat ""$logFileName"2" >> $logFileName
	cat ""$logFileName"3" >> $logFileName

	rm -f ""$logFileName"1"
	rm -f ""$logFileName"2"
	rm -f ""$logFileName"3"
		
	# Wait for Additional Steps to complete
	procCount=`ps -ef | grep -i "$relName"_migrate_tpfmat_tables | grep $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep -i "$relName"_migrate_tpfmat_tables | grep $USER | wc -l`
	done
	
	
	
	# Refresh all the views in the background
	# $SCRIPTDIR/epdba_upgrade_refresh_views.sh $relName $ticketNo $regionProfile $dbChgList $stagingList M > $LOGDIR/archive/"$relName"_cutover_refresh_views.log &

	# Check Cutover Table Count (new + existing)
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql  $OUTDIR/"$relName"_validation_dryrun_cutover_tables.out | tee -a $logFileName
	
	
	rm -f $LOGDIR/archive/"$relName"_errors_mock_cutover.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_mock_cutover.log $LOGDIR/archive/"$relName"_errors_mock_cutover.log
	
	
		stepDescription="Mock Cutover Run"
		if [ -s $LOGDIR/archive/"$relName"_errors_mock_cutover.log ] 
		then
			# Errors Found in Execution
			emailStatus="FAILURE"
			echo "$LOGDIR/archive|"$relName"_errors_mock_cutover.log|"$relName"_errors_mock_cutover.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		else
			if [ "$emailStatus" != "WARNING" ]
			then
				# No Errors Found
				emailStatus="SUCCESS"
			fi
		fi
		send_email $emailStatus "$stepDescription"
		#-----------------------------------------------------------------------------------------------------------
	
}



#-------------------------------------------------------------------------------------------------------------------------------------
production_run() {

	#-----------------------------------   Staging Table Creation ---------------------------------------------------------------------------------
	stgLogFileName="$LOGDIR/archive/"$relName"_migrate_staging_tables.log"
	$SQLDIR/epdba_upgrade_migrate_staging_tables.sh "$relName" "$ticketNo" "$regionProfile" "$dbChgList" "$stagingList" > $stgLogFileName &
	sleep 5
	

	#-------------------------------   Copy Data to Temp Tables  ---------------------------------------------
	logFileName="$LOGDIR/archive/"$relName"_migration_complete_production_run.log"
	
	# Delete data from the tables before running
	$SCRIPTDIR/epdba_perform_dbmaint.sh 2 HCCL"$region"_UPG_AK_%_CHANGE $TDPROD $ticketNo > $logFileName

	# Run the conversion scripts
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_column_add_1.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_column_add_1.sql  $OUTDIR/drive_migration.out | tee -a ""$logFileName"1" &
		sleep 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_column_add_2.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_column_add_2.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"2" &
		sleep 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_simple_dtype_change_3.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_simple_dtype_change_3.sql  $OUTDIR/drive_migration.out | tee -a ""$logFileName"3" &
		sleep 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_simple_dtype_change_4.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_simple_dtype_change_4.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"4" &
		sleep 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_complex_dtype_change_5.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_complex_dtype_change_5.sql  $OUTDIR/drive_migration.out | tee -a  ""$logFileName"5" &
		sleep 2
	fi
	
	
	# Wait for Data Migration to complete
	sleep 5
	procCount=`ps -ef | grep -i "$relName"_migrate | grep sql | grep $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep -i "$relName"_migrate | grep sql | grep $USER | wc -l`
	done
	
	cat ""$logFileName"?"  >> $logFileName
	
	
	#-----------------------------------   Get the Counts  ---------------------------------------------
	# Prod Table (Source Count) - Exclude New Tables. Include DROP and RENAME Tables as well
	cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND TABLE ENTRY" | tr '[a-z]' '[A-Z]' | grep -v -i "UPG_AK_TAB_ADD" | cut -f5,6  -d'|' | sort | uniq  > $AUDITDIR/"$relName"_source_migration_tables.dat
	cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND DROP TABLE" | tr '[a-z]' '[A-Z]' | cut -f2,3  -d'|' | sort | uniq  >> $AUDITDIR/"$relName"_source_migration_tables.dat
	cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND RENAME TABLE" | tr '[a-z]' '[A-Z]' | cut -f2,3  -d'|' | sort | uniq  >> $AUDITDIR/"$relName"_source_migration_tables.dat
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_source_migration_tables.dat $TDPROD $ticketNo $runId 104

	# Temp Table (Target Count) - Exclude New Tables
	cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND TABLE ENTRY" | tr '[a-z]' '[A-Z]' | grep -v -i "UPG_AK_Tab_Add" | cut -f4,6  -d'|' | sort | uniq  > $AUDITDIR/"$relName"_temp_migration_tables.dat
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_temp_migration_tables.dat $TDPROD $ticketNo $runId 204

	
	rm -f $LOGDIR/archive/"$relName"_errors_migration_complete_production_run.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_migration_complete_production_run.log $LOGDIR/archive/"$relName"_errors_migration_complete_production_run.log
	
	
	
	# Wait for Staging Table Creation to complete
	sleep 5
	procCount=`ps -ef | grep -i "epdba_upgrade_migrate_staging_tables" | grep $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep -i "epdba_upgrade_migrate_staging_tables" | grep $USER | wc -l`
	done
	rm -f $LOGDIR/archive/"$relName"_errors_migrate_staging_tables.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_migrate_staging_tables.log $LOGDIR/archive/"$relName"_errors_migrate_staging_tables.log


	
	# Report Generation Process
		#-----------------------------------------------------------------------------------------------------------
		get_report "MIGRATION_REPORT" "AUDIT_TABLE_NAME,TEMP_COUNT,PROD_CCOUNT,DIFFERENCE,TEMP_DATABASE_NAME,PROD_DATABASE_NAME" "-k 5,5 -k 1,1"
		echo "$OUTDIR|"$relName"_"MIGRATION_REPORT".csv" >> $TEMPDIR/"$scriptName"_email_attach.dat
		
		rm -f $TEMPDIR/"$scriptName"_email_info.dat
		tail +2 $OUTDIR/"$relName"_"MIGRATION_REPORT".csv | while read -r line; do
			diff=`echo $line | cut -f4 -d','`
			if [ "$diff" != "0" ]
			then
				emailStatus="WARNING"
				echo "$line" >> $TEMPDIR/"$scriptName"_email_info.dat
			fi
		done
		
		get_report "VALIDATION_REPORT" "TEST_CASE_NO,TEST_CASE_NM,EXPECTED_RESULT,ACTUAL_RESULT,DIFFERENCE" "-k 1,1"
		echo "$OUTDIR|"$relName"_"VALIDATION_REPORT".csv" >> $TEMPDIR/"$scriptName"_email_attach.dat
				
		if [ -s $TEMPDIR/"$scriptName"_email_info.dat ]
		then
			echo " Production and TEMP Table Count does not match for some Tables. Please review the list below - " >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			cat $TEMPDIR/"$scriptName"_email_info.dat >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		fi
		echo "Please find the MIGRATION_REPORT attached to this email." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		
		stepDescription="Production Migration Run"
		if [ -s $LOGDIR/archive/"$relName"_errors_migration_complete_production_run.log ] || [ -s $LOGDIR/archive/"$relName"_errors_migrate_staging_tables.log ]
		then
			# Errors Found in Execution
			emailStatus="FAILURE"
			if [ -s $LOGDIR/archive/"$relName"_errors_migration_complete_production_run.log ]
			then
				echo "$LOGDIR/archive|"$relName"_errors_migration_complete_production_run.log|"$relName"_errors_migration_complete_production_run.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
			fi
			if [ -s $LOGDIR/archive/"$relName"_errors_migrate_staging_tables.log ]
			then
				echo "$LOGDIR/archive|"$relName"_errors_migrate_staging_tables.log|"$relName"_errors_migrate_staging_tables.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
			fi	
		else
			if [ "$emailStatus" != "WARNING" ]
			then
				# No Errors Found
				emailStatus="SUCCESS"
			fi
		fi
		send_email $emailStatus "$stepDescription"
		#-----------------------------------------------------------------------------------------------------------


	# Collect Stats after data migration to temp
	logFileName="$LOGDIR/archive/"$relName"_migration_stats_collection_run.log"
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_stats_exist.sql  $OUTDIR/drive_migration.out  >  $logFileName &

	
}		

production_run_copy_temp()
{
	logFileName="$LOGDIR/archive/"$relName"_migration_copy_temp_to_prod.log"
	rm -f $logFileName
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_1.sql  $OUTDIR/drive_migration.out | tee -a ""$logFileName"1" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_2.sql  $OUTDIR/drive_migration.out | tee -a ""$logFileName"2" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_3.sql  $OUTDIR/drive_migration.out | tee -a ""$logFileName"3" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_4.sql  $OUTDIR/drive_migration.out | tee -a ""$logFileName"4" &
	sleep 2
	

	# Wait for table copy from temp to prod to complete
	procCount=`ps -ef | grep "$relName"_cutover_temp_to_prod | grep sql | grep $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep "$relName"_cutover_temp_to_prod | grep sql | grep $USER | wc -l`
	done
	
	$SCRIPTDIR/epdba_split_file.sh "INSERT INTO" $SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql 8
	
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql"_1  $OUTDIR/drive_migration.out | tee -a ""$logFileName"5" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql"_2  $OUTDIR/drive_migration.out | tee -a ""$logFileName"6" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql"_3  $OUTDIR/drive_migration.out | tee -a ""$logFileName"7" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql"_4  $OUTDIR/drive_migration.out | tee -a ""$logFileName"8" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql"_5  $OUTDIR/drive_migration.out | tee -a ""$logFileName"9" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql"_6  $OUTDIR/drive_migration.out | tee -a ""$logFileName"10" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql"_7  $OUTDIR/drive_migration.out | tee -a ""$logFileName"11" &
	sleep 2
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_cutover_temp_to_prod_5.sql"_8  $OUTDIR/drive_migration.out | tee -a ""$logFileName"12" &
	sleep 2
	
	# Wait for table copy from temp to prod to complete
	procCount=`ps -ef | grep "$relName"_cutover_temp_to_prod | grep sql | grep $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep "$relName"_cutover_temp_to_prod | grep sql | grep $USER | wc -l`
	done
	
	cat ""$logFileName"1" >> $logFileName
	cat ""$logFileName"2" >> $logFileName
	cat ""$logFileName"3" >> $logFileName
	cat ""$logFileName"4" >> $logFileName
	cat ""$logFileName"5" >> $logFileName
	cat ""$logFileName"6" >> $logFileName
	cat ""$logFileName"7" >> $logFileName
	cat ""$logFileName"8" >> $logFileName
	cat ""$logFileName"9" >> $logFileName
	cat ""$logFileName"10" >> $logFileName
	cat ""$logFileName"11" >> $logFileName
	cat ""$logFileName"12" >> $logFileName

	rm -f $LOGDIR/archive/"$relName"_errors_migration_copy_temp_to_prod.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_migration_copy_temp_to_prod.log $LOGDIR/archive/"$relName"_errors_migration_copy_temp_to_prod.log
	
	
}


cutover_matviewdb_and_tpf_run()
{

	#-----------------------------------------  Additional Steps for MAT VIEW DB and TPF Tables  ------------------------------------------#
	tpfmatlogFileName="$LOGDIR/archive/"$relName"_production_tpfmat_cutover.log"
	rm -f $tpfmatlogFileName
	
	echo "#!/usr/bin/ksh" > $SQLDIR/archive/$relName/"$relName"_migrate_tpfmat_tables.sh
	
	
			# TPF Changes
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_tpf_new_structure_ddl.sql ]
	then
		echo "$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_tpf_new_structure_ddl.sql  $OUTDIR/drive_tpfmat.out" >> $SQLDIR/archive/$relName/"$relName"_migrate_tpfmat_tables.sh
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_tpf_migrate_data.sql ]
	then
		echo "$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_tpf_migrate_data.sql  $OUTDIR/drive_tpfmat.out" >> $SQLDIR/archive/$relName/"$relName"_migrate_tpfmat_tables.sh
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_tpf_storeproc_changes.sql ]
	then	
		echo "$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_tpf_storeproc_changes.sql $OUTDIR/drive_tpfmat.out" >> $SQLDIR/archive/$relName/"$relName"_migrate_tpfmat_tables.sh
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_tpf_view.sql ]
	then
		echo "$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_tpf_view.sql  $OUTDIR/drive_cutover.out" >> $SQLDIR/archive/$relName/"$relName"_cutover_refresh_views.sh
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_tpf_userview.sql ]
	then
		echo "$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_tpf_userview.sql  $OUTDIR/drive_cutover.out" >> $SQLDIR/archive/$relName/"$relName"_cutover_refresh_views.sh
	fi
	
	
	# MAT VIEW CHANGES
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_matviewdb_new_structure_ddl.sql ]
	then
		"echo $SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_matviewdb_new_structure_ddl.sql  $OUTDIR/drive_tpfmat.out" >> $SQLDIR/archive/$relName/"$relName"_migrate_tpfmat_tables.sh
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_materialized.sql ]
	then
		echo "$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_materialized.sql  $OUTDIR/drive_cutover.out" >> $SQLDIR/archive/$relName/"$relName"_cutover_refresh_views.sh
	fi
	
	
	if [ -f $SQLDIR/archive/$relName/"$relName"_migrate_tpfmat_tables.sh ]
	then
		chmod 775 $SQLDIR/archive/$relName/"$relName"_migrate_tpfmat_tables.sh
		$SQLDIR/archive/$relName/"$relName"_migrate_tpfmat_tables.sh > $tpfmatlogFileName &
	fi

}





cutover_run ()
{

	cutover_matviewdb_and_tpf_run
	
	
	startTS=`date +%Y-%m-%d\ %H:%M:%S`
	cutoverCount=`head -2 $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql | tail -1 | cut -f2 -d':' | sed 's/\ //g'`
	
	echo "DELETE FROM CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION WHERE RUN_ID=$runId AND TEST_CASE_NO IN ('6.1');" > $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql

	echo "INSERT INTO CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION " >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo "SELECT $runId, '6.1', 'Reporting Tables Created during dryrun',$cutoverCount,COUNT(*), (COUNT(*) - $cutoverCount) FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('$dryrunReportDB'" >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo ",'$dryrunCalcReportDB1','$dryrunCalcReportDB2','$dryrunCalcReportDB3','$dryrunCalcReportDB4','$dryrunCalcReportDB5','$dryrunCalcReportDB6') " >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql
	echo " AND TRIM(TableName) NOT LIKE 'U_'$prefix'%' AND LastAlterTimeStamp >= TIMESTAMP'$startTS';" >> $SQLDIR/archive/$relName/dryrun/"$relName"_validation_dryrun_cutover_tables.sql


	
	logFileName="$LOGDIR/archive/"$relName"_production_cutover.log"
	rm -f $logFileName

	
	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 15 $SQLDIR/archive/$relName/ "$relName"_cutover_prod_exisiting_rename.sql  $OUTDIR/drive_cutover.out ""$logFileName"1"
	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 15 $SQLDIR/archive/$relName/ "$relName"_cutover_prod_exisiting_alter.sql  $OUTDIR/drive_cutover.out ""$logFileName"2"
	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 15 $SQLDIR/archive/$relName/ "$relName"_cutover_prod_create_final.sql  $OUTDIR/drive_cutover.out ""$logFileName"3"

	
	cat ""$logFileName"1" >> $logFileName
	cat ""$logFileName"2" >> $logFileName
	cat ""$logFileName"3" >> $logFileName

	rm -f ""$logFileName"1"
	rm -f ""$logFileName"2"
	rm -f ""$logFileName"3"
	
		
	# Wait for Additional Steps to complete
	procCount=`ps -ef | grep -i "$relName"_migrate_tpfmat_tables | grep $USER | wc -l`
	while [ $procCount -gt 0 ]
	do
		sleep 30
		procCount=`ps -ef | grep -i "$relName"_migrate_tpfmat_tables | grep $USER | wc -l`
	done
	
	
	# Refresh all the views in the background
	$SCRIPTDIR/epdba_upgrade_refresh_views.sh $relName $ticketNo $regionProfile $dbChgList $stagingList  > $LOGDIR/archive/"$relName"_cutover_refresh_views.log &

	# Check Cutover Table Count (new + existing)
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_validation_cutover_tables.sql  $OUTDIR/"$relName"_validation_cutover_tables.out | tee -a $logFileName
	
	# Take Cutover Counts for Report
	cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND TABLE ENTRY" | tr '[a-z]' '[A-Z]' | grep -v -i "UPG_AK_Tab_Add"  | cut -f5,6  -d'|' | sort | uniq   > $AUDITDIR/"$relName"_final_migration_tables.dat
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_final_migration_tables.dat $TDPROD $ticketNo $runId 304
	
	rm -f $LOGDIR/archive/"$relName"_errors_production_cutover.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_production_cutover.log $LOGDIR/archive/"$relName"_errors_production_cutover.log
	
	if [ -f $LOGDIR/archive/"$relName"_production_tpfmat_cutover.log ]
	then
		$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_production_tpfmat_cutover.log $LOGDIR/archive/"$relName"_errors_production_tpfmat_cutover.log
	fi
	
	
		# Report Generation Process
		#-----------------------------------------------------------------------------------------------------------
		get_report "CUTOVER_REPORT" "AUDIT_TABLE_NAME,TEMP_COUNT,PRE_CTUOVER_CCOUNT,TEMP_PRE_DIFFERENCE,TEMP_DATABASE_NAME,PROD_DATABASE_NAME,POST_CTUOVER_CCOUNT,PRE_POST_DIFFERENCE" "-k 5,5 -k 1,1"
		
		echo "$OUTDIR|"$relName"_"CUTOVER_REPORT".csv" >> $TEMPDIR/"$scriptName"_email_attach.dat
		rm -f $TEMPDIR/"$scriptName"_email_info.dat
		tail +2 $OUTDIR/"$relName"_"CUTOVER_REPORT".csv | while read -r line; do
			diff1=`echo $line | cut -f4 -d','`
			diff2=`echo $line | cut -f8 -d','`
			if [ "$diff1" != "0" ] || [ "$diff2" != "0" ] 
			then
				emailStatus="WARNING"
				echo "$line" >> $TEMPDIR/"$scriptName"_email_info.dat
			fi
		done
		
		get_report "VALIDATION_REPORT" "TEST_CASE_NO,TEST_CASE_NM,EXPECTED_RESULT,ACTUAL_RESULT,DIFFERENCE" "-k 1,1"
		echo "$OUTDIR|"$relName"_"VALIDATION_REPORT".csv" >> $TEMPDIR/"$scriptName"_email_attach.dat
		
		if [ -s $TEMPDIR/"$scriptName"_email_info.dat ]
		then
			echo " Pre and Post Cutover Table Count does not match for some Tables. Please review the list below - " >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			cat $TEMPDIR/"$scriptName"_email_info.dat >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		fi
		echo "Please find the CUTOVER_REPORT attached to this email." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		
		stepDescription="Production Cutover Run"
		if [ -s $LOGDIR/archive/"$relName"_errors_production_cutover.log ] || [ -s $LOGDIR/archive/"$relName"_errors_production_tpfmat_cutover.log ]
		then
			# Errors Found in Execution
			emailStatus="FAILURE"
			if [ -s $LOGDIR/archive/"$relName"_errors_production_cutover.log ]
			then
				echo "$LOGDIR/archive|"$relName"_errors_production_cutover.log|"$relName"_errors_production_cutover.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
			fi
			if [ -s $LOGDIR/archive/"$relName"_errors_production_tpfmat_cutover.log ]
			then
				echo "$LOGDIR/archive|"$relName"_errors_production_tpfmat_cutover.log|"$relName"_errors_production_tpfmat_cutover.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
			fi
		else
			if [ "$emailStatus" != "WARNING" ]
			then
				# No Errors Found
				emailStatus="SUCCESS"
			fi
		fi
		send_email $emailStatus "$stepDescription"
		#-----------------------------------------------------------------------------------------------------------
	
	
}



reasonable_volume_post_etl ()
{

	logFileName="$LOGDIR/archive/"$relName"_reasonable_volume_post_etl.log"
	rm -f $logFileName
	
	#-----------------  Reasonable Volume Check (Post-ETL) ---------------------------------------
	$SCRIPTDIR/epdba_load_audit.sh "$relName"_reasonable_volume.dat $TDPROD $ticketNo $runId 205 > $logFileName	

	get_report "REASONABLE_VOLUME_REPORT" "TABLE_NAME,DATABASE_NAME,PRE_ETL_COUNT,POST_ETL_COUNT,DIFFERENCE" "-k 1,1"
	echo "$OUTDIR|"$relName"_"REASONABLE_VOLUME_REPORT".csv" >> $TEMPDIR/"$scriptName"_email_attach.dat
	
	rm -f $SQLDIR/archive/$relName/"$relName"_post_cutover_collect_stats_datachange_tables.sql
	rm -f $TEMPDIR/"$scriptName"_email_info.dat
	
	tail +2 $OUTDIR/"$relName"_"REASONABLE_VOLUME_REPORT".csv | while read -r line; do
	
		tableName=`echo $line | cut -f1 -d','`
		databaseName=`echo $line | cut -f2 -d','`
		preCount=`echo $line | cut -f3 -d','`
		diff1=`echo $line | cut -f5 -d','`

		if [ "$preCount" == "0" ] || [ -z "$preCount" ]
		then
			preCount="1"  # Avoid Division by 0
		fi
		
		percent=`expr $diff1 / $preCount`
		if [ $diff1 -lt 0 ] && [ "$databaseName" != "$prodStgDB" ]
		then
			emailStatus="WARNING"
			echo "$line" >> $TEMPDIR/"$scriptName"_email_info.dat
		fi
		
		if [ "$percent" -gt 0.1 ]
		then
			echo "COLLECT STATISTICS ON $prodReportDB.$tableName;" >> $SQLDIR/archive/$relName/"$relName"_post_cutover_collect_stats_datachange_tables.sql
		fi
		
	done
		
		
	if [ -s $TEMPDIR/"$scriptName"_email_info.dat ]
	then
		echo " Post ETL Count is less than Pre ETl Count for some Tables. Please review the list below - " >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		cat $TEMPDIR/"$scriptName"_email_info.dat >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
	fi
	echo "Please find the REASONABLE_VOLUME_REPORT with post-etl run counts attached to this email." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
	echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
	
		
	
	rm -f $LOGDIR/archive/"$relName"_errors_reasonable_volume_post_etl.log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_reasonable_volume_post_etl.log $LOGDIR/archive/"$relName"_errors_reasonable_volume_post_etl.log


	stepDescription="Reasonable Volume Check Report after ETL Run"
	if [ -s $LOGDIR/archive/"$relName"_errors_reasonable_volume_post_etl.log ]
	then
		# Errors Found in Execution
		emailStatus="FAILURE"
		echo "$LOGDIR/archive|"$relName"_errors_reasonable_volume_post_etl.log|"$relName"_errors_reasonable_volume_post_etl.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
	else
		if [ "$emailStatus" != "WARNING" ]
		then
			# No Errors Found
			emailStatus="SUCCESS"
		fi
	fi
	send_email $emailStatus "$stepDescription"
	
	
}



post_cutover_run ()
{

	rm -f $LOGDIR/archive/"$relName"_production_post_cutover.log

	#-----------------  Reasonable Volume Check (Post-ETL) ---------------------------------------
	# Generate Report
	reasonable_volume_post_etl
	
		
	#-------------------------  Collect Stats on New Tables ------------------------------------
	# if [ -f $SQLDIR/archive/$relName/"$relName"_post_cutover_collect_stats_new_tables.sql ]
	# then
		# $SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_post_cutover_collect_stats_new_tables.sql  $OUTDIR/post_cutover.out | tee -a $LOGDIR/archive/"$relName"_production_post_cutover.log
	# fi
	
	# Collect Stats on Any Table where change in volume is more than 10%
	if [ -f $SQLDIR/archive/$relName/"$relName"_post_cutover_collect_stats_datachange_tables.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD"  $SQLDIR/archive/$relName/"$relName"_post_cutover_collect_stats_datachange_tables.sql $OUTDIR/post_cutover.out | tee -a $LOGDIR/archive/"$relName"_production_post_cutover.log
	fi

	#-------------------------  Collect Stats on any other Table that has seen a change in data volume ------------------------------------
	if [ -f $SQLDIR/archive/$relName/"$relName"_post_cutover_collect_stats_other_tables.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD"  $SQLDIR/archive/$relName/"$relName"_post_cutover_collect_stats_other_tables.sql $OUTDIR/post_cutover.out | tee -a $LOGDIR/archive/"$relName"_production_post_cutover.log
	fi
	
}


#-------------------------------------------------------------------------------------------------------------------------------------


		
#-------------------------------------------------------------------------------------------------------------------------------------
# STEP-4 (MAIN SCRIPT STARTS Here). Controlling sequence of events for migration and cutover
#-------------------------------------------------------------------------------------------------------------------------------------

	

	if [ ! -f $HOMEDIR/flag/"$relName"_create_migration_and_cutover_scripts.flag ]
	then
		#---------------------------------------  1. Generate all SQL Scripts  ----------------------------------------------

		# Create the SQL Scripts using the dbchange list, staginglist and profile file
		$SCRIPTDIR/epdba_upgrade_create_migration_scripts.sh $relName $ticketNo $regionProfile $dbChgList $stagingList "$customViewPurpose"  > $LOGDIR/archive/"$relName"_create_migration_and_cutover_scripts.log
		rt_cd=$?
		if [ $rt_cd -ne 0 ]
		then
			echo "epdba_upgrade_create_migration_scripts.sh aborted abruptly"
			exit 900
		fi
		
		# Prepare Custom View File Provided by RSC
		if [ -f $SQLDIR/"$customViewFile"  ] && [ ! -z "$customViewFile" ]
		then
			$SCRIPTDIR/epdba_create_target_view.sh  $SQLDIR/$customViewFile $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_custom_views.sql $ticketNo >> $LOGDIR/archive/"$relName"_create_migration_and_cutover_scripts.log
			rt_cd=$?
			if [ $rt_cd -ne 0 ]
			then
				echo "epdba_create_target_view.sh aborted abruptly"
				exit 900
			fi
		fi
		
		if [ -s $SQLDIR/"$relName"_script_generated_exceptions.sql ]
		then
			echo "$SQLDIR|"$relName"_script_generated_exceptions.sql|"$relName"_script_generated_exceptions.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
			echo "Review script generated execptions encountered while generating scripts. Fix those scripts manually." >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		fi
		
		if [ -s $SQLDIR/"$relName"_cutover_prod_view_complexity_alert.sql ]
		then
			echo "$SQLDIR|"$relName"_cutover_prod_view_complexity_alert.sql|"$relName"__view_changes_for_performance_review.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
			echo "Some  views might need to be validated for potential performance issues. These are attached to the email" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
			echo "" >> $TEMPDIR/"$scriptName"_email_additional_info.dat
		fi
		
		
		rm -f $LOGDIR/archive/"$relName"_errors_create_migration_and_cutover_scripts.log
		$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_create_migration_and_cutover_scripts.log $LOGDIR/archive/"$relName"_errors_create_migration_and_cutover_scripts.log
		stepDescription="Creation of Migration and Cutover Scripts"
		if [ -s $LOGDIR/archive/"$relName"_errors_create_migration_and_cutover_scripts.log ]
		then
			# Errors Found in Execution
			emailStatus="FAILURE"
			echo "$LOGDIR/archive|"$relName"_errors_create_migration_and_cutover_scripts.log|"$relName"_errors_create_migration_and_cutover_scripts.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		else
			# No Errors Found
			emailStatus="SUCCESS"
		fi
		send_email $emailStatus "$stepDescription"
		touch $HOMEDIR/flag/"$relName"_create_migration_and_cutover_scripts.flag
		
	else
		if [ ! -f $HOMEDIR/flag/"$relName"_migration_create_temptables.flag ]
		then	
			#------------------------------------ 2. Create the Tables on Temp Database  ----------------------------------------------
			create_temp_tables
			touch $HOMEDIR/flag/"$relName"_migration_create_temptables.flag
		else
			if [ ! -f $HOMEDIR/flag/"$relName"_migration_perform_dry_run.flag ]
			then
				#-------------------------------- 3. Move Data from Prod to Temp Database (Dry Run) ---------------------------------------------
				dry_run
				touch $HOMEDIR/flag/"$relName"_migration_perform_dry_run.flag 
			else
				if [ ! -f $HOMEDIR/flag/"$relName"_migration_perform_mock_cutover_run.flag ]
				then
				#-------------------------------- 4. Move New Structure from Temp to DRYRUN Database (Mock Cutover) ---------------------------------------------
					if [ "$mockCutoverFlag" == "Y" ]
					then
						mock_cutover_run
					fi
					touch $HOMEDIR/flag/"$relName"_migration_perform_mock_cutover_run.flag 
				else
					if [ ! -f $HOMEDIR/flag/"$relName"_migration_complete_production_run.flag ]
					then
						#------------------------ 5. Move Data from Prod to Temp Database (Prod Migration) ---------------------------------------------
						production_run
						touch $HOMEDIR/flag/"$relName"_migration_complete_production_run.flag
					else
						#------------------------ 6. Move New Structure from Temp to Prod Database (Pre-Cutover) ---------------------------------------------
						if [ ! -f $HOMEDIR/flag/"$relName"_production_run_copy_temptable.flag ]
						then
							production_run_copy_temp
							touch $HOMEDIR/flag/"$relName"_production_run_copy_temptable.flag
						else
							if [ ! -f $HOMEDIR/flag/"$relName"_production_cutover.flag ]
							then
								#---------------- 7. Move New Structure from Temp to Prod Database (Cutover) ---------------------------------------------
								if [ "$cutoverFlag" == "Y" ]
								then
									cutover_run
									touch $HOMEDIR/flag/"$relName"_production_cutover.flag
								else
									echo "Cutover Flag Not Set. Aborting Step" 
								fi
							else
								if [ ! -f $HOMEDIR/flag/"$relName"_production_postcutover.flag ]
								then
									#------------ 8. Tasks to perform after 1st ETL Run  ---------------------------------------------
									post_cutover_run
									touch $HOMEDIR/flag/"$relName"_production_postcutover.flag
								else
									echo "No Action required for $relName - All Migration and Cutover Steps have been completed !!"
								fi
							fi
						fi
					fi
				fi
			fi
		fi
	fi
	
	
	chmod 777 $HOMEDIR/flag/"$relName"_*.flag
	
	
#-------------------------------------------------------------------------------------------------------------------------------------

	


