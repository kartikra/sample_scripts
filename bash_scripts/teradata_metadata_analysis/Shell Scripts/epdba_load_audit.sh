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



# STEP-2 Create Log File

	scriptName=`basename $0`
	dateforlog=`date +%Y%m%d%H%M%S`
	logName=$scriptName-${dateforlog}.log
	logFileName=$LOGDIR/$logName

	echo "---------------------------------------------------------------" >> $logFileName
	echo "---------- Preparing Audit Load Job for $inputFile ------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

	
	inputFile=$1
	TDRegion=$2
	ticketNo=$3
	runId=$4
	auditId=$5



# STEP-3 Start Audit Process

	rm -f $SQLDIR/audit_"$inputFile"
	rm -f $OUTDIR/audit_"$inputFile" 
	rm -f $TEMPDIR/audit_cleanup.sql
	rm -f $TEMPDIR/audit_cleanup.out
	
	echo "DELETE FROM /*$ticketNo*/ CLARITY_DBA_MAINT.CLARITY_UPG_AUDIT WHERE RUN_ID=$runId AND AUDIT_ID=$auditId;" > $TEMPDIR/audit_cleanup.sql
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDRegion" $TEMPDIR/audit_cleanup.sql $TEMPDIR/audit_cleanup.out | tee -a $logFileName 


	cat $AUDITDIR/$inputFile | while read -r line ; do
	
		db=`echo $line | cut -f1 -d'|'`
		table=`echo $line | cut -f2 -d'|'`

		if [ ! -z "$db" ] && [ ! -z "$table" ]
		then
			echo "INSERT INTO /*$ticketNo*/ CLARITY_DBA_MAINT.CLARITY_UPG_AUDIT SELECT $runId, $auditId, CAST('$db' AS VARCHAR(50)), CAST('$table' AS VARCHAR(50)), CAST(COUNT(\\*) AS DECIMAL(18,2)) FROM \"$db\".\"$table\"; "  >> $SQLDIR/audit_"$inputFile"
		fi
		
	done


	$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDRegion 20 $SQLDIR audit_"$inputFile" $OUTDIR/audit_"$inputFile" $logFileName 	



	echo "---------------------------------------------------------------" >> $logFileName
	echo "------------------ Completed Audit Load Job -------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
