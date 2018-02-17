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


	# relName="sc0730"
	# ticketNo="CRQ000000218034"
    # regionProfile="RESC"
	# dbChgList="sc0730_chglist_v2.txt"
	# backupList="sc0730_backup_list_v2.txt"
	
	relName=$1
	ticketNo=$2
	regionProfile=$3
	dbChgList=$4
	backupList=$5

	. $SQLDIR/archive/"$relName"/runid.profile
		
	prefix=`echo $relName | awk '{print substr($0,3,4)}'`
	
	
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
	
	
# STEP-2 Create Log File

	scriptName=`basename $0`
	dateforlog=`date +%Y%m%d%H%M%S`
	logName=$scriptName-${dateforlog}.log
	logFileName=$LOGDIR/$logName


	echo "---------------------------------------------------------------" >> $logFileName
	echo "--------------- Analyzing List of Backup Tables -------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
	
	
# STEP-3 load tables from flat file to temporary database

	if [ -s "$DIR/$dbChgList" ]
	then
		# Load DB Change List in metadata tables
		$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o CLARITY_DBA_MAINT.UPG_CHG_LIST  -d $DIR/$dbChgList -l $logFileName 
	else
		echo "File - $DIR/$dbChgList is empty or not Found. Invalid change list"
		exit 920
	fi
	
	if [ -s "$DIR/$backupList" ]
	then
		# Load DB Backup List in metadata tables
		$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o CLARITY_DBA_MAINT.UPG_BKUP_ANALYSIS  -d $DIR/$backupList -l $logFileName 
	else
		echo "File - $DIR/$backupList is empty or not Found. Invalid backup list"
		exit 925
	fi

	
# STEP-4 Analyze Backup Tables

	rm -f $SQLDIR/"$relName"_perform_querylog_analysis_on_"$prodUserView".sql
	rm -f $AUDITDIR/"$relName"_reasonable_volume.dat
	rm -f $OUTDIR/"$relName"_perform_querylog_analysis_on_rest.dat
	rm -f $OUTDIR/"$relName"_backup_tables_notfound.dat

	sed -e 's/'MY_NUID'/'$USER'/g' -e 's/'MY_REL_NAME'/'$relName'/g' -e 's/'MY_RUN_ID'/'$runId'/g'   \
	-e 's/'MY_TPF__VIEW'/'$prodTpfView'/g' -e 's/'MY_TPF_USER_VIEW'/'$prodTpfUserView'/g' \
	-e 's/'MY_USER_VIEW'/'$prodUserView'/g'  -e 's/'MY__VIEW1'/'$prodMatView'/g' -e 's/'MY__VIEW2'/'$prodKPBIView'/g' \
	-e 's/'MY__VIEW'/'$prodView'/g'  -e 's/'MY_MATVIEW_DB1'/'$prodMatReportDB'/g' -e 's/'MY_MATVIEW_DB2'/'$prodKPBIReportDB'/g' \
	-e 's/'MY_REPORT_DB'/'$prodReportDB'/g' -e 's/'MY_STAGE_DB'/'$prodStgDB'/g' $SQLDIR/accdba_backup_analysis.sql \
	> $TEMPDIR/"$relName"_backup_analysis.sql

	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_backup_analysis.sql $OUTDIR/"$relName"_backup_analysis.out | tee -a  $teelogFileName


	
# STEP-5 Run the Sample Query on each table and check results from DBQL Log Tables


	# Delete first 2 lines
	sed '1,2d'  $SQLDIR/"$relName"_perform_querylog_analysis_on_"$prodUserView".sql > $TEMPDIR/"$relName"_perform_querylog_analysis_on_"$prodUserView".sql
	mv $TEMPDIR/"$relName"_perform_querylog_analysis_on_"$prodUserView".sql $SQLDIR/"$relName"_perform_querylog_analysis_on_"$prodUserView".sql 
	
	# Get The Start Time
	start_dt=`date +%Y-%m-%d`
	start_tm=`date +%H:%M:%S`
	
	# Run the SQL File containing sample queries
	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 20 $SQLDIR "$relName"_perform_querylog_analysis_on_"$prodUserView".sql $OUTDIR/"$relName"_perform_querylog_analysis_on_"$prodUserView".out $logFileName 	
	
	# Get The End Time
	end_dt=`date +%Y-%m-%d`
	end_tm=`date +%H:%M:%S`
	
	# Wait for 10 minutes for the log to be captured
	sleep 600


	
# STEP-6 Load the results from the log table
	
	sed -e 's/'MY_NUID'/'$USER'/g' -e 's/'MY_START_DT'/'$start_dt'/g' -e 's/'MY_END_DT'/'$end_dt'/g' \
	-e 's/'MY_START_TM'/'$start_tm'/g' -e 's/'MY_END_TM'/'$end_tm'/g' $SQLDIR/accdba_get_query_log.sql > $TEMPDIR/"$relName"_get_query_log.sql
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_get_query_log.sql $OUTDIR/"$relName"_get_query_log.out | tee -a  $logFileName


# STEP-7  Collect Results from DBQ Log Table and use this as input for backup creation scripts

	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/accdba_backup_analysis_collect_results.sql $OUTDIR/"$relName"_backup_analysis_collect_results.out | tee -a  $logFileName

	
# STEP-8 Cleanup all the output files

	sed '1,2d'  $AUDITDIR/"$relName"_reasonable_volume.dat > $TEMPDIR/"$relName"_reasonable_volume.dat 
	mv $TEMPDIR/"$relName"_reasonable_volume.dat $AUDITDIR/"$relName"_reasonable_volume.dat
	
	sed '1,2d' $OUTDIR/"$relName"_perform_querylog_analysis_on_rest.dat > $TEMPDIR/"$relName"_perform_querylog_analysis_on_rest.dat
	mv $TEMPDIR/"$relName"_perform_querylog_analysis_on_rest.dat $SQLDIR/"$relName"_perform_querylog_analysis_on_rest.dat
	
	
	echo "---------------------------------------------------------------" >> $logFileName
	echo "--------------- Completed Analysis of Backup Tables -------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
	