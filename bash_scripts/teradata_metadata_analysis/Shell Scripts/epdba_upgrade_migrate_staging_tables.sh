#!/usr/bin/ksh

	relName=$1
	ticketNo=$2
	regionProfile=$3
	dbChgList=$4
	stagingList=$5
	
	
	
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

	. $SQLDIR/archive/"$relName"/runid.profile
	
	
	#-----------------------------------   Staging Table Creation ---------------------------------------------
	startTS=`date +%Y-%m-%d\ %H:%M:%S`
	stageCount=	`head -1 $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql | cut -f2 -d':' | sed 's/\ //g'`
	
	echo "DELETE FROM CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION WHERE RUN_ID=$runId AND TEST_CASE_NO IN ('4.1');" > $SQLDIR/"$relName"_validation_staging_tables.sql
	echo "INSERT INTO CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION " >> $SQLDIR/archive/$relName/"$relName"_validation_staging_tables.sql
	echo "SELECT $runId, '5.1', 'Staging Tables Created',$stageCount,COUNT(*), (COUNT(*) - $stageCount) FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('$prodStgDB'  " >> $SQLDIR/archive/$relName/"$relName"_validation_staging_tables.sql
	echo ",'$prodDeployStgDB1','$prodDeployStgDB2','$prodDeployStgDB3','$prodDeployStgDB4','$prodDeployStgDB5','$prodDeployStgDB6') AND LastAlterTimeStamp >= TIMESTAMP'$startTS';" >> $SQLDIR/archive/$relName/"$relName"_validation_staging_tables.sql

	if [ "$region" == "SC" ]
	then
		echo ",'$prodTpfStageDB','HCCLPSC%_S','HCCLPSC%_TPF_S'" >> $SQLDIR/archive/$relName/"$relName"_validation_staging_tables.sql
	fi
	if [ "$region" == "NC" ]
	then
		echo ",'$prodTpfStageDB','HCCLPNC%_S','HCCLPNC%_TPF_S'" >> $SQLDIR/archive/$relName/"$relName"_validation_staging_tables.sql
	fi
	echo ");" >> $SQLDIR/archive/$relName/"$relName"_validation_staging_tables.sql
	

	logFileName="$LOGDIR/archive/"$relName"_migrate_staging_tables.log"

	# Drop Current Structures
	if [ -f $SQLDIR/archive/$relName/"$relName"_staging_tables_drop_current.sql ]
	then	
		$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 9 $SQLDIR/archive/$relName/ "$relName"_staging_tables_drop_current.sql $OUTDIR/drive_staging_table.out $logFileName	
	fi
	
	
	# Create Staging Tables
	if [ -f $SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql ]
	then
	
		$SCRIPTDIR/epdba_split_file.sh "CREATE SET TABLE" $SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql 8

		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql"_1  $OUTDIR/drive_staging_table.out | tee -a $logFileName"1" &
		sleep 2
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql"_2  $OUTDIR/drive_staging_table.out | tee -a $logFileName"2" &
		sleep 2
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql"_3  $OUTDIR/drive_staging_table.out | tee -a $logFileName"3" &
		sleep 2
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql"_4  $OUTDIR/drive_staging_table.out | tee -a $logFileName"4" &
		sleep 2
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql"_5  $OUTDIR/drive_staging_table.out | tee -a $logFileName"5" &
		sleep 2
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql"_6  $OUTDIR/drive_staging_table.out | tee -a $logFileName"6" &
		sleep 2
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql"_7  $OUTDIR/drive_staging_table.out | tee -a $logFileName"7" &
		sleep 2
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$SQLDIR/archive/$relName/"$relName"_staging_tables_create.sql"_8  $OUTDIR/drive_staging_table.out | tee -a $logFileName"8" &
		sleep 2
	fi
	
	# Create TPF Staging Tables
	if [ -f $SQLDIR/archive/$relName/"$relName"_staging_tables_tpf_create.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_staging_tables_tpf_create.sql  $OUTDIR/drive_staging_table.out | tee -a $logFileName"9" &
		sleep 2
	fi
	
	
	# Wait Till all Staging Table Creation Scripts are complete
	row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i "$relName"_staging_tables" | wc -l`
	while [ $row_cnt -gt 0 ]
	do
		sleep 10
		row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i "$relName"_staging_tables" | wc -l`
	done
	
	cat ""$logFileName"?" >> $logFileName
	rm -f ""$logFileName"?" 
	
	
	# Drop Any Secondary Index
	if [ -f $SQLDIR/archive/$relName/"$relName"_staging_tables_check_secondary_index.sql ]
	then
		$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_staging_tables_check_secondary_index.sql  $OUTDIR/"$relName"_staging_tables_drop_secondary_index.out
		sed '1d' $OUTDIR/"$relName"_staging_tables_drop_secondary_index.out > $SQLDIR/archive/"$relName"_staging_tables_drop_secondary_index.sql 
		if [ -f $SQLDIR/archive/$relName/"$relName"_staging_tables_drop_secondary_index.sql ] 
		then 
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_staging_tables_drop_secondary_index.sql  $OUTDIR/"$relName"_staging_tables_drop_secondary_index.out | tee -a $logFileName
		fi 
	fi
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_validation_staging_tables.sql  $OUTDIR/"$relName"_validation_staging_tables.out | tee -a $logFileName
	
	
	
	