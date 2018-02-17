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

		
backup_ddl () {		
		
	TDENV=$1
	backupDB=$2
	outBackupDDL=$3
	logFileName=$4
	parallelCount=$5
	
	echo "--------------------------------------------------------------------------------------------------" >> $logFileName
	echo "------------------ Starting DDL backup for $2 in $1 ------------------------" >> $logFileName
	echo "---------------------------------------------------------------------------------------------------" >> $logFileName



	# STEP-4 : Remove Temporary and Output files

		# Remove Temp Files
		rm -f $TEMPDIR/temp_"$outBackupDDL"
		rm -f $TEMPDIR/"$backupDB"_alltables_ddl.sql
		rm -f $TEMPDIR/"$backupDB"_alltables_ddl.out
		# Remove Output File
		rm -f $DDLBACKUPDIR/$outBackupDDL


	# STEP-5 : Create all Show Table and Show View Scripts

		cat $SQLDIR/accdba_export_alltables_ddl.sql | sed -e 's/$$DBNAME/'$backupDB'/g' > $TEMPDIR/"$backupDB"_alltables_ddl.sql
		
		$SCRIPTDIR/epdba_runSQLFile.sh $TDENV $TEMPDIR/"$backupDB"_alltables_ddl.sql $TEMPDIR/"$backupDB"_alltables_ddl.out | tee -a $logFileName

	
		
	# STEP-6 : Export to final output file

		sed '1,2d'  $TEMPDIR/"$backupDB"_alltables_ddl.out > $TEMPDIR/temp_"$outBackupDDL"
		cd $SCRIPTDIR
		
		if [ -z "$parallelCount" ]
		then
			$SCRIPTDIR/epdba_runSQLFile2.sh $TDENV $TEMPDIR/temp_"$outBackupDDL" $DDLBACKUPDIR/$outBackupDDL | tee -a $logFileName
		else
			$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDENV $parallelCount $TEMPDIR temp_"$outBackupDDL" $DDLBACKUPDIR/$outBackupDDL $logFileName
		fi
		
	

	# STEP-7 Cleanup the DDL Output File
		#epdba_cleanup_sql_scripts.sh $DDLBACKUPDIR $outBackupDDL N


	# tar cvf - $DDLBACKUPDIR/$outBackupDDL | gzip > $DDLBACKUPDIR/$outBackupDDL.tar.gz 
	# tar -xvfz  $DDLBACKUPDIR/$outBackupDDL.tar.gz

		
	echo "--------------------------------------------------------------------------------------------------" >> $logFileName
	echo "------------------ DDL Backup Completed for $2 in $1 ------------------------" >> $logFileName
	echo "--------------------------------------------------------------------------------------------------" >> $logFileName

		
}
		


	# STEP-2  Read the input variables


	
 	while getopts h:d:o:p:l: par
        do      case "$par" in
                h)      TDENV="$OPTARG";;
                d)      backupDB="$OPTARG";;
                o)      outBackupDDL="$OPTARG";;
				p)      parallelCount="$OPTARG";;
                l)      in_listFile="$OPTARG";;

                [?])    echo "Correct Usage -->  ksh epdba_runFastExport.sh -f <fastExportScriptName> -h <td hostname> -d <output data file> -i <dbname.table> -u <utiltiy_db> -l <logfilename>"
                        exit 998;;
                esac
        done

		
	if [ -z "$TDENV" ]
	then
		echo " Teradata Region Missing !! "
		exit 905
	fi
	

	# STEP-3 : Create the logfile

	scriptName=`basename $0`
	dateforlog=`date +%Y%m%d%H%M%S`
	logName=$scriptName-${dateforlog}.log
	logFileName=$LOGDIR/$logName

	if [ -f "$in_listFile" ]
	then
			counter="0"
			echo "Table/View defintions from the following databases have been exported -  " > $TEMPDIR/"$scriptName"_email_body.dat

			cat $in_listFile | while  read -r line ; do
			
				backupDB=`echo $line | cut -f1 -d'|'`
				outBackupDDL=`echo $line | cut -f2 -d'|'`
				
				if [ -z "$outBackupDDL" ]
				then
					outBackupDDL="export_"$backupDB"_ddl.sql"
				fi
				
				if [ ! -f $DDLBACKUPDIR/$outBackupDDL ]
				then
					backup_ddl $TDENV $backupDB $outBackupDDL $logFileName $parallelCount
					counter=`expr "$counter" + 1`
					echo "$backupDB" >> $TEMPDIR/"$scriptName"_email_body.dat
				fi
				
			done
			
			# Send Notification if any database defintion is being exported
			if [ $counter -gt 0 ]
			then
				emailSubjectLine="Database List was $in_listFile. Export Directory was $DDLBACKUPDIR"
				$SCRIPTDIR/epdba_send_mail.sh -s "DDL EXPORT COMPLETED" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat"  -t "cd_bio_dba"
			fi
	else
			if [ ! -f "$outBackupDDL" ]
			then
				backup_ddl $TDENV $backupDB $outBackupDDL $logFileName $parallelCount
			fi
	fi

	
	