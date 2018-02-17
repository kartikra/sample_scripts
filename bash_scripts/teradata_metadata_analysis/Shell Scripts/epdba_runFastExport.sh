#!/usr/bin/ksh


# USAGE-1 : ksh epdba_runFastExport.sh -h tdd1.didi.com -i HCCLOH_USHARE.UPGRADE_TABLES -d /users/q932624/dbmig/scripts/out1.dat -l <log1.log> -u <utiltiy_db>
# USAGE-2 : ksh epdba_runFastExport.sh -h tdd1.didi.com -f export_final_HCCLOH_USHARE.UPGRADE_TABLES.fexp -l <log1.log> 


# STEP-1 Read Input Parameters 


 	while getopts h:d:i:u:f:l: par
        do      case "$par" in
                h)      TDHOST="$OPTARG";;
                d)      outFile="$OPTARG";;
                i)      inputDBTable="$OPTARG";;
				u)      utility_db="$OPTARG";;
                f)      in_scriptFileName="$OPTARG";;
                l)      in_logFileName="$OPTARG";;

                [?])    echo "Correct Usage -->  ksh epdba_runFastExport.sh -f <fastExportScriptName> -h <td hostname> -d <output data file> -i <dbname.table> -u <utiltiy_db> -l <logfilename>"
                        exit 998;;
                esac
        done




# STEP-2 Run the user profile file and set user credentials

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi


	USR_PROF=$HOME/user.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi


	case "$TDHOST" in
   		"tdp1.didi.com") REPO=$RECP1
   		;;
   		"tdp2.didi.com") REPO=$RECP2
   		;;
   		"tdp3.didi.com") REPO=$RECP3
   		;;
		"tdp5.didi.com") REPO=$RECP5
   		;;
   		"tdd1.didi.com") REPO=$RECD1
   		;;
   		"tdd3.didi.com") REPO=$RECD3
   		;;
		"tdd4.didi.com") REPO=$RECD4
   		;;
	esac


# STEP-3 Create Log File if not passed as parameter

	scriptName=`basename $0`
	dateforlog=`date +%Y%m%d%H%M%S`
	logName=$scriptName-${dateforlog}.log
	if [ -z "$4" ]
	then
		logFileName=$LOGDIR/$logName
	else
		logFileName=$in_logFileName
	fi




if [ -z "$in_scriptFileName" ]
then

	echo "--------------------------------------------------------------------------------------------------" >> $logFileName
	echo "---------------------- Preparing FastExport for $inputDBTable  ---------------------" >> $logFileName
	echo "--------------------------------------------------------------------------------------------------" >> $logFileName



# STEP-4 and STEP-5 required only if fexp script has to be generated from scratch. Proceed to Step-6 if fload script exists

# STEP-4 Get all the columns in the inout table


	in_database=`echo $inputDBTable | cut -f1 -d'.'`
	in_table=`echo $inputDBTable | cut -f2 -d'.'`



	if [ -z "$utility_db" ]
	then
		utility_db=$in_database
	fi


	rm -f $TEMPDIR/"fexp"_$inputDBTable.dat

	echo "SELECT TRIM(columnName) || '|' || TRIM(columnFormat) || '|' ||  TRIM(SUM(columnLength) OVER (PARTITION BY tableName)) " > $TEMPDIR/"fexp_temp1"_$inputDBTable.sql
        echo " FROM DBC.ColumnsV WHERE tableName='$in_table' AND databasename='$in_database' " >> $TEMPDIR/"fexp_temp1"_$inputDBTable.sql
        echo " ORDER BY ColumnId " >> $TEMPDIR/"fexp_temp1"_$inputDBTable.sql


	$SCRIPTDIR/epdba_runSQLFile.sh "$TDHOST" $TEMPDIR/"fexp_temp1"_$inputDBTable.sql  $TEMPDIR/"fexp"_$inputDBTable.dat | tee -a  $logFileName

	sed '1,2d'  $TEMPDIR/"fexp"_$inputDBTable.dat > $TEMPDIR/"fexp_final"_$inputDBTable.dat


	tableNameLen=`echo $in_table | wc -c`
	if [ $tableNameLen -gt 25 ]
	then
		t_uTable=`echo $in_table | awk '{print substr($0,1,length-5)}'`
	else
		t_uTable=`echo $in_table`
	fi

# STEP-5 Create the fexp script

	touch $HOME/"export"_$inputDBTable.fexp
	chmod 700 $HOME/"export"_$inputDBTable.fexp

	echo ".LOGON $TDHOST/$USER,$REPO;" 			> $HOME/"export"_$inputDBTable.fexp
	echo ".LOGTABLE $utility_db."LOG"_$t_uTable;" 	>> $HOME/"export"_$inputDBTable.fexp
	echo ".BEGIN EXPORT" 					>> $HOME/"export"_$inputDBTable.fexp
	echo " SESSIONS 20;" 					>> $HOME/"export"_$inputDBTable.fexp
	echo ".EXPORT OUTFILE $outFile.tmp" 			>> $HOME/"export"_$inputDBTable.fexp
	echo "MODE RECORD FORMAT TEXT;" 			>> $HOME/"export"_$inputDBTable.fexp
	echo "SELECT CAST (" 					>> $HOME/"export"_$inputDBTable.fexp
	echo "(" 						>> $HOME/"export"_$inputDBTable.fexp
	

	cat $TEMPDIR/"fexp_final"_$inputDBTable.dat | while read -r line ; do

		t_column=`echo $line | cut -f1 -d'|'`
		t_format=`echo $line | cut -f2 -d'|'`
		t_length=`echo $line | cut -f3 -d'|'`
		
		dateTimeCounter=`echo "$t_format" | grep "YYYY-MM-DD" | wc -l`

		if [ "$dateTimeCounter" -eq 0  ]
		then
			echo "COALESCE(TRIM($t_column),'') "  >> $HOME/"export"_$inputDBTable.fexp
		else
			echo "COALESCE( ($t_column(FORMAT '$t_format')(CHAR(20))),'') " >> $HOME/"export"_$inputDBTable.fexp
		fi

		echo " || '|' || " 			>> $HOME/"export"_$inputDBTable.fexp
	done

	sed '$d' $HOME/"export"_$inputDBTable.fexp > $HOME/"export_final"_$inputDBTable.fexp

	rm -f $HOME/"export"_$inputDBTable.fexp

	echo ")"  							>> $HOME/"export_final"_$inputDBTable.fexp

	t_length=`echo $t_length | sed -e 's/,//g'`
	t_length=`expr $t_length + 120`

	echo " AS CHAR($t_length)) QUERY_RESULT FROM $inputDBTable; " 	>> $HOME/"export_final"_$inputDBTable.fexp
	#echo " WHERE QUERY_RESULT IS NOT NULL;" 			>> $HOME/"export_final"_$inputDBTable.fexp
	echo ".END EXPORT;" 						>> $HOME/"export_final"_$inputDBTable.fexp

	chmod 700 $HOME/"export_final"_$inputDBTable.fexp

	scriptFileName=$HOME/"export_final"_$inputDBTable.fexp

	echo "Fast Export Script for $inputDBTable has been Generated !! " >> $logFileName


else
	scriptFileName=$FEXPDIR/"$in_scriptFileName"

fi


# STEP-6  Run the fast export script


	fexp < $scriptFileName | tee -a  $logFileName

	echo " Creating Final file by removing spaces ... " >> $logFileName

	# Remove Trailing Spaces
	sed 's/[ \t]*$//' $outFile.tmp > $outFile

	echo "Final Output File Generated !! " >> $logFileName



# STEP-7 Cleanup unwanted data file and remove the password from script before sending it to FEXPDIR

	rm -f $outFile.tmp


	lno="1"
	
	sed ''$lno's/$/\
&\.LOGON '$TDHOST'\/'$USER'\,\;/' $scriptFileName | sed ''$lno'd' > "$scriptFileName"-tmp


	rm -f $scriptFileName

	if [ -z "$in_scriptFileName" ]
	then
		mv "$scriptFileName"-tmp $FEXPDIR/"export_final"_$inputDBTable.fexp
		chmod 775 $FEXPDIR/"export_final"_$inputDBTable.fexp

	else
		mv $scriptFileName-tmp $scriptFileName
		chmod 775 $scriptFileName
	fi



	echo "--------------------------------------------------------------------------------------------------" >> $logFileName
	echo "-------------------- FastExport Completed for $inputDBTable  ---------------------" >> $logFileName
	echo "--------------------------------------------------------------------------------------------------" >> $logFileName
