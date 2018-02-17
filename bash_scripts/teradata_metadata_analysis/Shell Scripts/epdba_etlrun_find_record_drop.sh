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
	errorlogName="$scriptName"_errors-${dateforlog}.log
	
	inputList="ga_record_drop.dat"
    regionProfile="PROD_GA"
	sampleRows="50"
	ticketNo="CRQ000000249612"	
	
	
# STEP-3 Run Region Profile File
	USR_PROF=$HOMEDIR/region/$regionProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $regionProfile.profile cannot be found, Exiting"
                exit 902
        fi	
		
		if [ ! -z $TDPROD ]
		then
			TDHOST=$TDPROD
		else
			TDHOST=$TDDEV
		fi
		
	
	
	# rm -f $TEMPDIR/find_missing_records.sql
	# rm -f $TEMPDIR/find_missing_records.out
	# cat $SRCDIR/$inputList | while read -r scriptLine ;
	# do
		# prodDbName=`echo $scriptLine  | cut -f1 -d'|'  | sed -e 's/\ //g'`
		# prodTabName=`echo $scriptLine  | cut -f2 -d'|'  | sed -e 's/\ //g'`
		# bkupDbName=`echo $scriptLine  | cut -f3 -d'|'  | sed -e 's/\ //g'`
		# bkupTabName=`echo $scriptLine  | cut -f4 -d'|'  | sed -e 's/\ //g'`

		# echo "CALL CLARITY_DBA_MAINT.CLARITY_GET_RECORD_DROP($sampleRows,'$prodDbName','$prodTabName','$bkupDbName','$bkupTabName',line1);" >> $TEMPDIR/find_missing_records.sql		
		
	# done
						
	# $SCRIPTDIR/epdba_runSQLFile2.sh "$TDHOST" $TEMPDIR/find_missing_records.sql $TEMPDIR/find_missing_records.out  | tee -a  $logFileName
	
	
	
	rm -f $TEMPDIR/find_missing_records_final.sql
	cat $TEMPDIR/find_missing_records.out | grep -v "GetMissingRecordSQL" | while read -r line;
	do
		fileName=`echo $line | cut -f1 -d '|' | cut -f2 -d'.'`
		dbName=`echo $line | cut -f1 -d '|' | cut -f1 -d'.'`
		echo ".EXPORT RESET;" >> $TEMPDIR/find_missing_records_final.sql
		echo ".EXPORT REPORT FILE = $SRCDIR/$regionProfile/"$dbName"-"$fileName".txt;" >> $TEMPDIR/find_missing_records_final.sql
		sqlQuery=`cat $TEMPDIR/find_missing_records.out | grep -v "GetMissingRecordSQL" | cut -f2 -d '|' | grep -i -w $fileName`
		echo "$sqlQuery" >> $TEMPDIR/find_missing_records_final.sql
	done
	

		
	if [ -d "$SRCDIR/$regionProfile" ]
	then
		rm -f $SRCDIR/$regionProfile/*.*
	else
		mkdir "$SRCDIR/$regionProfile" 
	fi
	
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDHOST" $TEMPDIR/find_missing_records_final.sql $TEMPDIR/find_missing_records_final.out  | tee -a  $logFileName
	
	
	