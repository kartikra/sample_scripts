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
	
	
run_multiple_sqlFile()
{
	TDHOST=$1
	sqlFileName=$2
	keyword=$3
	outFileName=$4
	logFileName=$5
	fileCount=$6
	
	
	echo "$1"
	echo "$2"
	echo "$3"
	echo "$4"
	echo "$5"
	echo "$6"


	$SCRIPTDIR/epdba_split_file.sh "$keyword" $sqlFileName $fileCount

	i="1"
	while [ $i -le $fileCount ]
	do
		$SCRIPTDIR/epdba_runSQLFile.sh $TDHOST "$sqlFileName"_"$i"  "$outFileName"_"$i" > "$logFileName"_"$i".log &
		sleep 2
		i=`expr $i + 1`
	done
	
	
	row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i $sqlFileName | wc -l`
	while [ $row_cnt -gt 1 ]
	do
		sleep 10
		row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i $sqlFileName | wc -l`
	done
	
	
	i="1"
	while [ $i -le $fileCount ]
	do
		if [ -f "$logFileName"_"$i" ]
		then
			cat "$logFileName"_"$i".log >> $logFileName
			rm -f "$logFileName"_"$i".log
		fi
		
		if [ -f "$outFileName"_"$i" ]
		then
			cat "$outFileName"_"$i" >> $outFileName
			rm -f "$outFileName"_"$i"
		fi
		
		if [ -f "$sqlFileName"_"$i" ]
		then
			rm -f "$sqlFileName"_"$i"
		fi
		
		i=`expr $i + 1`
	done
	
	
}

	#$SCRIPTDIR/epdba_runMultipleSQLFile.sh "tdp5.kp.org" 15 $SQLDIR ma0124_revert_structures.sql $OUTDIR/revert_changes.out $logFileName	
	run_multiple_sqlFile "tdd3.kp.org" "$DDLBACKUPDIR/VALID_NC_create_CLMSNC2.sql" "REPLACE VIEW"  "$OUTDIR/revert_changes.out" $logFileName 10
	
