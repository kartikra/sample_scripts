#!/usr/bin/ksh

# USAGE-1 : ksh epdba_runFastLoad.sh -h tdp2.didi.com -d /users/q932624/dbmig/scripts/out1.dat -o HCCLOH_USHARE.UPGRADE_TABLES  -l <logfilename> -u <utiltiy_db>
# USAGE-2 : ksh epdba_runFastLoad.sh -h tdp2.didi.com -f import_HCCLOH_USHARE.UPGRADE_TABLES.fload -l <logfilename>


# STEP-1 Read Input Parameters


 	while getopts h:d:o:f:u:l: par
        do      case "$par" in
                h)      TDHOST="$OPTARG";;
                d)      inFile="$OPTARG";;
                o)      outputDBTable="$OPTARG";;
                f)      in_scriptFileName="$OPTARG";;
                u)      utiltiy_db="$OPTARG";;
                l)      in_logFileName="$OPTARG";;

                [?])    echo "Correct Usage -->  ksh epdba_runFastLoad.sh -h <td hostname> -f <fastloadScriptName> -u <utiltiy_db> -d <input data file> -o <dbname.table>  -l <logfilename>"
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

	
# STEP-4 DROP and RECREATE the TABLE

	echo "SHOW TABLE $outputDBTable;" > $TEMPDIR/cleanup_script.sql
	echo "DROP TABLE $outputDBTable;" >> $TEMPDIR/cleanup_script.sql	
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDHOST" $TEMPDIR/cleanup_script.sql  $TEMPDIR/cleanup_script.dat | tee -a  $logFileName
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDHOST" $TEMPDIR/cleanup_script.dat  $TEMPDIR/cleanup_script.out | tee -a  $logFileName
	
	rm -f $TEMPDIR/cleanup_script.sql
	rm -f $TEMPDIR/cleanup_script.dat
	rm -f $TEMPDIR/cleanup_script.out
	

# STEP-5 and STEP-6 required only if fload script has to be generated from scratch. Proceed to Step-6 if fload script exists

if [ -z "$in_scriptFileName" ]
then

	echo "--------------------------------------------------------------------------------------------------" >> $logFileName
	echo "---------------------- Preparing FastLoad for $outputDBTable  ---------------------" >> $logFileName
	echo "--------------------------------------------------------------------------------------------------" >> $logFileName



# STEP-5 Get all the columns in the inout table


	in_database=`echo $outputDBTable | cut -f1 -d'.'`
	in_table=`echo $outputDBTable | cut -f2 -d'.'`

	if [ -z "$utility_db" ]
	then
		utility_db=$in_database
	fi


	rm -f $TEMPDIR/"fload"_$outputDBTable.dat
	rm -f $TEMPDIR/"import1"_$outputDBTable.fload
	rm -f $TEMPDIR/"import2"_$outputDBTable.fload
	rm -f $TEMPDIR/"import3"_$outputDBTable.fload


		echo "SELECT TRIM(columnName) || '|' || TRIM(columnFormat) || '|' || TRIM(columnLength) " > $TEMPDIR/"fload_temp1"_$outputDBTable.sql
        echo " FROM DBC.ColumnsV WHERE tableName='$in_table' AND databasename='$in_database' " >> $TEMPDIR/"fload_temp1"_$outputDBTable.sql
        echo " ORDER BY ColumnId " >> $TEMPDIR/"fload_temp1"_$outputDBTable.sql


	$SCRIPTDIR/epdba_runSQLFile.sh "$TDHOST" $TEMPDIR/"fload_temp1"_$outputDBTable.sql  $TEMPDIR/"fload"_$outputDBTable.dat | tee -a  $logFileName

	sed '1,2d'  $TEMPDIR/"fload"_$outputDBTable.dat > $TEMPDIR/"fload_final"_$outputDBTable.dat


# STEP-6 Create the fastload script


	scriptFileName=$HOME/"import"_$outputDBTable.fload
	touch $scriptFileName
	chmod 700 $scriptFileName
	
	errLimitNo="0"
	
	tableNameLen=`echo $in_table | wc -c`
	if [ $tableNameLen -gt 23 ]
	then
		t_errTable=`echo $in_table | awk '{print substr($0,1,length-7)}'`
	else
		t_errTable=`echo $in_table`
	fi
	
	
	echo ".LOGON $TDHOST/$USER,$REPO;" 		> $scriptFileName
	echo "" 								>> $scriptFileName
	echo "DELETE FROM $outputDBTable ALL;" 					>> $scriptFileName
	echo "DROP TABLE $utility_db."$t_errTable"_err1;" 			>> $scriptFileName
	echo "DROP TABLE $utility_db."$t_errTable"_err2;" 			>> $scriptFileName

	echo "" 								>> $scriptFileName
	echo "BEGIN LOADING" 							>> $scriptFileName
	echo "" 								>> $scriptFileName
	echo "$outputDBTable " 							>> $scriptFileName
	echo "ERRORFILES $utility_db."$t_errTable"_err1, $utility_db."$t_errTable"_err2;" 	>> $scriptFileName
	echo "" 								>> $scriptFileName
	echo ".RECORD 1;"		 				>> $scriptFileName
	# echo "errlimit $errLimitNo;" >> $scriptFileName
	echo "SET RECORD VARTEXT \"|\";"					>> $scriptFileName
	echo "" 								>> $scriptFileName
	echo "DEFINE" 								>> $scriptFileName
	echo "" 								>> $scriptFileName


	cat $TEMPDIR/"fload_final"_$outputDBTable.dat | while read -r line ; do

		t_column=`echo $line | cut -f1 -d'|'`
		t_format=`echo $line | cut -f2 -d'|'`
		t_length=`echo $line | cut -f3 -d'|' | sed 's/\,//g'`


		fieldNameLen=`echo $t_column | wc -c`
		if [ $fieldNameLen -gt 28 ]
		then
			t_column1=`echo $t_column | awk '{print substr($0,1,length-2)}'`
		else
			t_column1=`echo $t_column`
		fi

		varcharCounter=`echo "$t_format" | grep "X" | wc -l`
		if [ "$varcharCounter" -eq 0  ] 
		then
			t_length="30"

		fi

		echo "f_"$t_column1" (VARCHAR($t_length))" 	>> $TEMPDIR/"import1"_$outputDBTable.fload
		echo "," 					>> $TEMPDIR/"import1"_$outputDBTable.fload


		echo "$t_column" 	>> $TEMPDIR/"import2"_$outputDBTable.fload
		echo "," 		>> $TEMPDIR/"import2"_$outputDBTable.fload


		dateCounter=`echo "$t_format" | grep "YY/MM" | wc -l`
		if [ "$dateCounter" -eq 1  ] 
		then
			echo ":f_"$t_column1" (DATE, FORMAT 'YYYY/MM/DD')" 	>> $TEMPDIR/"import3"_$outputDBTable.fload
		else
			echo ":f_"$t_column1"" 				>> $TEMPDIR/"import3"_$outputDBTable.fload
		fi
		echo "," 		>> $TEMPDIR/"import3"_$outputDBTable.fload


	done


	sed '$d' $TEMPDIR/"import1"_$outputDBTable.fload	>> $scriptFileName
	echo "" 						>> $scriptFileName
	echo "FILE=$inFile;" 					>> $scriptFileName
	echo "" 						>> $scriptFileName

	echo "INSERT INTO $outputDBTable"			>> $scriptFileName
	echo "("						>> $scriptFileName
	sed '$d' $TEMPDIR/"import2"_$outputDBTable.fload	>> $scriptFileName
	echo ")" 						>> $scriptFileName
	echo "VALUES" 						>> $scriptFileName
	echo "("						>> $scriptFileName
	sed '$d' $TEMPDIR/"import3"_$outputDBTable.fload	>> $scriptFileName
	echo ");" 						>> $scriptFileName
	echo ""							>> $scriptFileName
	echo "END LOADING;"					>> $scriptFileName
	echo ""							>> $scriptFileName
	echo ".LOGOFF;"						>> $scriptFileName


	echo "Fast Load Script for $outputDBTable  has been Generated !! " >> $logFileName

else
	scriptFileName=$FLOADDIR/"$in_scriptFileName"
fi




# STEP-7 Run the fastload

	fastload < $scriptFileName | tee -a  $logFileName
	echo "Table Has Been Loaded !! " >> $logFileName



# STEP-8 Remove the password from the first line and export the script to FLOAD Directory


	lno="1"
	
	sed ''$lno's/$/\
&\.LOGON '$TDHOST'\/'$USER'\,;/' $scriptFileName | sed ''$lno'd' > "$scriptFileName"-tmp


	rm -f $scriptFileName

	if [ -z "$in_scriptFileName" ]
	then
		mv "$scriptFileName"-tmp $FLOADDIR/"import"_$outputDBTable.fload
		chmod 775 $FLOADDIR/"import"_$outputDBTable.fload

	else
		mv $scriptFileName-tmp $scriptFileName
		chmod 775 $scriptFileName
	fi


	echo "--------------------------------------------------------------------------------------------------" >> $logFileName
	echo "-------------------- FastLoad Completed for $outputDBTable  ---------------------" >> $logFileName
	echo "--------------------------------------------------------------------------------------------------" >> $logFileName
